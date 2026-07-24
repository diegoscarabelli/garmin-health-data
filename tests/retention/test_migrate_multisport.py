"""
Tests for :func:`garmin_health_data.retention.operations.migrate_multisport`.

Covers the #72 activity-table migration: adding ``parent_activity_id`` and swapping the
table-level ``UNIQUE (user_id, start_ts)`` constraint for a partial unique index, while
preserving data and child foreign-key integrity.
"""

import sqlite3

import pytest

from garmin_health_data.retention.operations import migrate_multisport

# Minimal pre-#72 activity schema: table-level UNIQUE, no parent_activity_id. Only the
# NOT-NULL-without-default columns need to be present; the new table's other columns
# (booleans with DEFAULT 0, parent_activity_id) are filled by their defaults on copy.
_OLD_SCHEMA = """
CREATE TABLE user (user_id BIGINT PRIMARY KEY);
CREATE TABLE activity (
    activity_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    activity_type_id INTEGER NOT NULL,
    activity_type_key TEXT NOT NULL,
    event_type_id INTEGER NOT NULL,
    event_type_key TEXT NOT NULL,
    start_ts DATETIME NOT NULL,
    end_ts DATETIME NOT NULL,
    timezone_offset_hours FLOAT NOT NULL,
    create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user (user_id),
    UNIQUE (user_id, start_ts)
);
CREATE INDEX activity_user_id_start_ts_idx ON activity (user_id, start_ts DESC);
CREATE TABLE running_agg_metrics (
    activity_id BIGINT PRIMARY KEY,
    avg_power FLOAT,
    FOREIGN KEY (activity_id) REFERENCES activity (activity_id) ON DELETE CASCADE
);
"""


def _insert_activity(conn, activity_id, start_ts):
    conn.execute(
        "INSERT INTO activity (activity_id, user_id, activity_type_id, "
        "activity_type_key, event_type_id, event_type_key, start_ts, end_ts, "
        "timezone_offset_hours) VALUES (?, 999, 1, 'running', 9, 'other', ?, ?, -7.0)",
        (activity_id, start_ts, start_ts),
    )


@pytest.fixture
def old_db(tmp_path):
    """
    Create a pre-#72 database with two activities and one child agg row.
    """
    db_path = str(tmp_path / "garmin.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_SCHEMA)
    conn.execute("INSERT INTO user (user_id) VALUES (999)")
    _insert_activity(conn, 111, "2026-05-01 10:00:00")
    _insert_activity(conn, 222, "2026-05-02 10:00:00")
    conn.execute(
        "INSERT INTO running_agg_metrics (activity_id, avg_power) VALUES (111, 250.0)"
    )
    conn.commit()
    conn.close()
    return db_path


def test_migrate_adds_column_and_swaps_constraint(old_db):
    """
    Migration adds parent_activity_id, drops the table UNIQUE, adds the partial index.
    """
    result = migrate_multisport(old_db, backup=False)
    assert result["migrated"] is True

    conn = sqlite3.connect(old_db)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(activity)")]
    assert "parent_activity_id" in cols

    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity'"
    ).fetchone()[0]
    assert "UNIQUE (user_id, start_ts)" not in table_sql

    uniq = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name='activity_user_id_start_ts_unique_idx'"
    ).fetchone()
    assert uniq is not None and "parent_activity_id IS NULL" in uniq[0]


def test_migrate_preserves_data_and_child_fk(old_db):
    """
    All activity rows and child agg rows survive; FK integrity holds.
    """
    migrate_multisport(old_db, backup=False)
    conn = sqlite3.connect(old_db)
    conn.execute("PRAGMA foreign_keys = ON")
    assert conn.execute("SELECT count(*) FROM activity").fetchone()[0] == 2
    assert conn.execute(
        "SELECT avg_power FROM running_agg_metrics WHERE activity_id=111"
    ).fetchone() == (250.0,)
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_migrate_constraint_behavior(old_db):
    """
    Standalone duplicate is rejected; a child leg may share a start instant.
    """
    migrate_multisport(old_db, backup=False)
    conn = sqlite3.connect(old_db)
    # Standalone duplicate of (999, 2026-05-01 10:00:00) is still rejected.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO activity (activity_id, user_id, activity_type_id, "
            "activity_type_key, event_type_id, event_type_key, start_ts, end_ts, "
            "timezone_offset_hours) VALUES (333, 999, 1, 'x', 9, 'other', "
            "'2026-05-01 10:00:00', '2026-05-01 10:00:00', -7.0)"
        )
    # A child leg (parent_activity_id set) at the same instant is allowed.
    conn.execute(
        "INSERT INTO activity (activity_id, user_id, activity_type_id, "
        "activity_type_key, event_type_id, event_type_key, start_ts, end_ts, "
        "timezone_offset_hours, parent_activity_id) VALUES (444, 999, 1, 'running', "
        "9, 'other', '2026-05-01 10:00:00', '2026-05-01 10:00:00', -7.0, 222)"
    )
    conn.commit()
    assert conn.execute("SELECT count(*) FROM activity").fetchone()[0] == 3


def test_migrate_is_idempotent(old_db):
    """
    A second run is a no-op on the already-migrated database.
    """
    migrate_multisport(old_db, backup=False)
    result = migrate_multisport(old_db, backup=False)
    assert result["migrated"] is False
    assert "already migrated" in result["reason"]

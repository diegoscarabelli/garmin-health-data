"""
Tests for :func:`garmin_health_data.retention.operations.migrate_cascade`.

These tests exercise the cascade-retrofit migration end-to-end against real on-disk
SQLite databases. The conftest fixtures are intentionally not used here: ``db_engine``
builds a fresh, cascade-correct schema via SQLAlchemy, and several scenarios need an
"old-style" schema (action-less FKs) to validate the migration mechanism.
"""

import re
import sqlite3
from pathlib import Path
from typing import List

import pytest

from garmin_health_data.db import create_tables
from garmin_health_data.retention.operations import (
    _MIGRATION_TARGETS,
    migrate_cascade,
)


# Target tables by name, for quick lookups in assertions.
ALL_TARGETS = [t for t, _parent in _MIGRATION_TARGETS]


def _has_cascade(db_path: str, table: str) -> bool:
    """
    Return True when the live ``CREATE TABLE`` for ``table`` includes cascade.

    :param db_path: Path to the SQLite database file.
    :param table: Table name to inspect.
    :return: Whether ``ON DELETE CASCADE`` appears anywhere in the CREATE statement
        stored by ``sqlite_master``.
    """
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
    finally:
        conn.close()
    if row is None or row[0] is None:
        return False
    return bool(re.search(r"ON\s+DELETE\s+CASCADE", row[0], flags=re.IGNORECASE))


def _make_old_style_db(db_path: str, tables: List[str]) -> None:
    """
    Build a realistic pre-cascade database for the listed child tables.

    The fixture starts from the current packaged schema (via
    :func:`create_tables`) so every parent table, every index, and every
    sibling child table is exactly as a real user's database would be at
    upgrade time. We then surgically strip ``ON DELETE CASCADE`` off the
    requested child tables, simulating a database that predates the
    cascade introduction. Stripping uses the standard SQLite recreate
    dance (rename, CREATE without cascade, INSERT SELECT, DROP) so the
    resulting ``sqlite_master.sql`` for each rewritten table looks exactly
    like an action-less FK declared by hand.

    Sourcing parents from production DDL (instead of the prior approach
    of hand-rolling stripped column lists) keeps the fixture honest as
    the schema evolves: a future migration that introduces a new index
    referencing a real column won't surprise these tests with "no such
    column" errors that are an artifact of the fixture, not the code.

    :param db_path: Path where the new database should be created.
    :param tables: Child tables that should end up without cascade. Must
        be a subset of :data:`ALL_TARGETS`.
    """
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    try:
        # Disable enforcement around the rename/insert/drop dance so the
        # transient renamed table does not trigger constraint checks.
        conn.execute("PRAGMA foreign_keys = OFF")
        cur = conn.cursor()
        for table in tables:
            row = cur.execute(
                "SELECT sql FROM sqlite_master " "WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if row is None or row[0] is None:
                raise KeyError(f"Table {table!r} not present in fresh schema.")
            sql_with_cascade = row[0]
            sql_without_cascade = re.sub(
                r"\s+ON\s+DELETE\s+CASCADE",
                "",
                sql_with_cascade,
                flags=re.IGNORECASE,
            )
            if sql_without_cascade == sql_with_cascade:
                raise AssertionError(
                    f"Table {table!r} did not have ON DELETE CASCADE in "
                    "the freshly created schema; the fixture cannot "
                    "strip what is not there."
                )
            old_name = f"{table}__strip"
            cur.execute(f"ALTER TABLE {table} RENAME TO {old_name}")
            cur.execute(sql_without_cascade)
            cur.execute(f"INSERT INTO {table} SELECT * FROM {old_name}")
            cur.execute(f"DROP TABLE {old_name}")
        conn.commit()
    finally:
        conn.close()


def test_fresh_db_already_cascade_correct_is_noop(tmp_path: Path) -> None:
    """
    A freshly initialized DB skips every target since they all already cascade.
    """
    db_path = str(tmp_path / "fresh.db")
    create_tables(db_path)

    summary = migrate_cascade(db_path)

    assert summary["migrated"] == []
    assert sorted(summary["skipped"]) == sorted(ALL_TARGETS)
    assert summary["dry_run"] is False
    assert summary["backup_path"] is None  # No-op skips backup creation.


def test_old_style_db_gets_migrated(tmp_path: Path) -> None:
    """
    An action-less child FK is rebuilt with cascade and existing rows survive.
    """
    db_path = str(tmp_path / "old.db")
    _make_old_style_db(db_path, ["swimming_agg_metrics", "sleep_level"])

    # Seed parent + child rows so we can verify both row preservation and
    # post-migration cascade behavior.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("INSERT INTO user (user_id) VALUES (1)")
        conn.execute(
            "INSERT INTO activity (activity_id, user_id, activity_type_id, "
            "activity_type_key, event_type_id, event_type_key, start_ts, "
            "end_ts, timezone_offset_hours) "
            "VALUES (100, 1, 1, 'running', 1, 'other', "
            "'2026-01-01 12:00:00', '2026-01-01 12:30:00', 0.0)"
        )
        conn.execute(
            "INSERT INTO swimming_agg_metrics (activity_id, pool_length) "
            "VALUES (100, 25.0)"
        )
        conn.execute(
            "INSERT INTO sleep (sleep_id, user_id, start_ts, end_ts, "
            "timezone_offset_hours) VALUES "
            "(200, 1, '2026-01-01 22:00:00', '2026-01-01 23:00:00', 0.0)"
        )
        conn.execute(
            "INSERT INTO sleep_level "
            "(sleep_id, start_ts, end_ts, stage, stage_label) "
            "VALUES (200, '2026-01-01 22:00:00', '2026-01-01 22:30:00', "
            "0, 'DEEP')"
        )
        conn.commit()
    finally:
        conn.close()

    # Pre-condition: neither table has cascade.
    assert not _has_cascade(db_path, "swimming_agg_metrics")
    assert not _has_cascade(db_path, "sleep_level")

    summary = migrate_cascade(db_path, backup=False)

    assert "swimming_agg_metrics" in summary["migrated"]
    assert "sleep_level" in summary["migrated"]
    # Targets that don't exist in this old DB show up as skipped.
    for table in ALL_TARGETS:
        if table not in ("swimming_agg_metrics", "sleep_level"):
            assert table in summary["skipped"]

    # Post-condition: rows are preserved and cascade is now declared.
    assert _has_cascade(db_path, "swimming_agg_metrics")
    assert _has_cascade(db_path, "sleep_level")

    conn = sqlite3.connect(db_path)
    try:
        swim_count = conn.execute(
            "SELECT COUNT(*) FROM swimming_agg_metrics"
        ).fetchone()[0]
        level_count = conn.execute("SELECT COUNT(*) FROM sleep_level").fetchone()[0]
        assert swim_count == 1
        assert level_count == 1

        # Cascade now fires when we delete the parent.
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("DELETE FROM activity WHERE activity_id = 100")
        conn.execute("DELETE FROM sleep WHERE sleep_id = 200")
        conn.commit()
        swim_after = conn.execute(
            "SELECT COUNT(*) FROM swimming_agg_metrics"
        ).fetchone()[0]
        level_after = conn.execute("SELECT COUNT(*) FROM sleep_level").fetchone()[0]
        assert swim_after == 0, "swimming_agg_metrics did not cascade."
        assert level_after == 0, "sleep_level did not cascade."
    finally:
        conn.close()


def test_idempotent_second_run_is_noop(tmp_path: Path) -> None:
    """
    Running migrate_cascade twice on the same old-style DB is safe.
    """
    db_path = str(tmp_path / "old.db")
    _make_old_style_db(db_path, ["swimming_agg_metrics", "sleep_level"])

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("INSERT INTO user (user_id) VALUES (1)")
        conn.execute(
            "INSERT INTO activity (activity_id, user_id, activity_type_id, "
            "activity_type_key, event_type_id, event_type_key, start_ts, "
            "end_ts, timezone_offset_hours) "
            "VALUES (100, 1, 1, 'running', 1, 'other', "
            "'2026-01-01 12:00:00', '2026-01-01 12:30:00', 0.0)"
        )
        conn.execute(
            "INSERT INTO swimming_agg_metrics (activity_id, pool_length) "
            "VALUES (100, 25.0)"
        )
        conn.commit()
    finally:
        conn.close()

    first = migrate_cascade(db_path, backup=False)
    assert "swimming_agg_metrics" in first["migrated"]

    second = migrate_cascade(db_path, backup=False)
    assert second["migrated"] == []
    # Every existing target should now be in skipped.
    assert "swimming_agg_metrics" in second["skipped"]
    assert "sleep_level" in second["skipped"]

    # Data survived both runs.
    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM swimming_agg_metrics").fetchone()[0]
        assert count == 1
    finally:
        conn.close()


def test_orphan_rows_reject_migration(tmp_path: Path) -> None:
    """
    Pre-flight foreign_key_check rejects a DB with existing orphans.
    """
    db_path = str(tmp_path / "orphan.db")
    _make_old_style_db(db_path, ["sleep_level"])

    # Inject an orphan: child row whose parent does not exist. This is
    # only possible because foreign_keys is off (the SQLite default).
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO sleep_level "
            "(sleep_id, start_ts, end_ts, stage, stage_label) "
            "VALUES (999, '2026-01-01 22:00:00', '2026-01-01 22:30:00', "
            "0, 'DEEP')"
        )
        conn.commit()
    finally:
        conn.close()

    # Snapshot the live CREATE TABLE so we can prove the DB is unchanged.
    conn = sqlite3.connect(db_path)
    try:
        before = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' " "AND name='sleep_level'"
        ).fetchone()[0]
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="foreign_key_check"):
        migrate_cascade(db_path, backup=False)

    # Schema must be untouched after the failed pre-flight.
    conn = sqlite3.connect(db_path)
    try:
        after = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' " "AND name='sleep_level'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert before == after


def test_backup_created_when_requested(tmp_path: Path) -> None:
    """
    With ``backup=True``, a ``.bak.<timestamp>`` file lands next to the DB.
    """
    db_path = str(tmp_path / "old.db")
    _make_old_style_db(db_path, ["swimming_agg_metrics"])

    summary = migrate_cascade(db_path, backup=True)

    assert summary["backup_path"] is not None
    backup_file = Path(summary["backup_path"])
    assert backup_file.exists()
    assert backup_file.name.startswith("old.db.bak.")


def test_backup_skipped_when_disabled(tmp_path: Path) -> None:
    """
    With ``backup=False``, no backup file is created.
    """
    db_path = str(tmp_path / "old.db")
    _make_old_style_db(db_path, ["swimming_agg_metrics"])

    summary = migrate_cascade(db_path, backup=False)

    assert summary["backup_path"] is None
    siblings = list(Path(db_path).parent.glob("old.db.bak.*"))
    assert siblings == []


def test_dry_run_does_not_modify_database(tmp_path: Path) -> None:
    """
    ``dry_run=True`` reports the plan but leaves the DB untouched.
    """
    db_path = str(tmp_path / "old.db")
    _make_old_style_db(db_path, ["swimming_agg_metrics", "sleep_level"])

    # Snapshot CREATE statements before the dry run.
    conn = sqlite3.connect(db_path)
    try:
        before = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        conn.close()

    summary = migrate_cascade(db_path, dry_run=True, backup=True)

    assert summary["dry_run"] is True
    assert "swimming_agg_metrics" in summary["migrated"]
    assert "sleep_level" in summary["migrated"]
    # Backup path is None for dry runs even when ``backup=True`` so the
    # operator can re-run without accumulating spurious copies.
    assert summary["backup_path"] is None

    conn = sqlite3.connect(db_path)
    try:
        after = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        conn.close()
    assert before == after, "Dry run modified the schema."

    # And the migrated tables still lack cascade.
    assert not _has_cascade(db_path, "swimming_agg_metrics")
    assert not _has_cascade(db_path, "sleep_level")


def test_missing_database_raises(tmp_path: Path) -> None:
    """
    A non-existent ``db_path`` raises ``FileNotFoundError`` up front.
    """
    db_path = str(tmp_path / "does_not_exist.db")
    with pytest.raises(FileNotFoundError):
        migrate_cascade(db_path)


def test_cross_version_skip_creates_all_new_tables(tmp_path: Path) -> None:
    """
    Regression: a user upgrading from a pre-2.8 release straight to 2.9 must end up with
    BOTH the 2.8-new ``body_composition`` table and the 2.9-new
    ``activity_ts_metric_downsampled`` table after running ``migrate-cascade``.

    A previous narrowing of ``_ensure_schema_current`` only created the
    2.9-new table, leaving the cross-version skipper missing
    ``body_composition`` until they next ran ``garmin extract``.
    """
    db_path = str(tmp_path / "old.db")
    # Build a fresh schema, then drop the two tables that "didn't exist
    # yet" in the version we're simulating an upgrade from. The point of
    # the test is to prove migrate-cascade backfills BOTH on its own.
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DROP TABLE body_composition")
        conn.execute("DROP TABLE activity_ts_metric_downsampled")
        conn.commit()
    finally:
        conn.close()

    pre = (
        sqlite3.connect(db_path)
        .execute("SELECT name FROM sqlite_master WHERE type='table'")
        .fetchall()
    )
    assert ("body_composition",) not in pre
    assert ("activity_ts_metric_downsampled",) not in pre

    migrate_cascade(db_path, backup=False)

    post = (
        sqlite3.connect(db_path)
        .execute("SELECT name FROM sqlite_master WHERE type='table'")
        .fetchall()
    )
    assert ("body_composition",) in post, (
        "body_composition (added in 2.8) was not created by migrate-cascade; "
        "pre-2.8 -> 2.9 upgraders would be missing the table."
    )
    assert ("activity_ts_metric_downsampled",) in post

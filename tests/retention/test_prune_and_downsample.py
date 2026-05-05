"""
Tests for prune_ts_metrics and downsample_activities.

Together these cover the SQL behavior the CLI commands rely on. The CLI tests in the
`tests/test_cli_*.py` modules assert the command-surface (flags, prompts, output
formatting); these tests assert the underlying mutation semantics.
"""

from datetime import date, datetime, timezone
from typing import List, Tuple

import pytest
from sqlalchemy import insert
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    Activity,
    ActivityTsMetric,
    ActivityTsMetricDownsampled,
    User,
)
from garmin_health_data.retention.operations import (
    downsample_activities,
    prune_ts_metrics,
)


def _seed_user(session: Session, user_id: int = 42) -> int:
    """
    Insert a User row so activity FKs are satisfied.

    :param session: Active SQLAlchemy session.
    :param user_id: User primary key to seed.
    :return: The seeded user_id.
    """
    session.add(User(user_id=user_id))
    session.commit()
    return user_id


def _seed_activity(
    session: Session,
    activity_id: int,
    user_id: int,
    start: datetime,
    duration_minutes: int = 10,
) -> Activity:
    """
    Insert a minimal Activity row valid against the schema's NOT NULL columns.

    :param session: Active SQLAlchemy session.
    :param activity_id: Activity primary key.
    :param user_id: Owning user_id.
    :param start: Activity start timestamp (UTC).
    :param duration_minutes: Activity duration used to derive ``end_ts``.
    :return: The inserted Activity instance.
    """
    end = datetime.fromtimestamp(
        start.timestamp() + duration_minutes * 60, tz=timezone.utc
    )
    activity = Activity(
        activity_id=activity_id,
        user_id=user_id,
        activity_type_id=1,
        activity_type_key="running",
        event_type_id=1,
        event_type_key="other",
        start_ts=start,
        end_ts=end,
        timezone_offset_hours=0.0,
    )
    session.add(activity)
    session.commit()
    return activity


def _seed_ts_metrics(
    session: Session,
    activity_id: int,
    rows: List[Tuple[datetime, str, float, str]],
) -> None:
    """
    Insert a list of activity_ts_metric rows via Core insert.

    Uses :func:`sqlalchemy.insert` rather than ``session.add_all`` because the ORM's
    batched RETURNING flow tries to sentinel-match returned rows back to the original
    instances, which fails on the composite PK that includes a timezone-aware DATETIME
    column once SQLite drops the timezone on round-trip.

    :param session: Active SQLAlchemy session.
    :param activity_id: Parent activity id.
    :param rows: Iterable of ``(timestamp, name, value, units)`` tuples.
    """
    if not rows:
        return
    session.execute(
        insert(ActivityTsMetric),
        [
            {
                "activity_id": activity_id,
                "timestamp": ts,
                "name": name,
                "value": value,
                "units": units,
            }
            for ts, name, value, units in rows
        ],
    )
    session.commit()


# ---------------------------------------------------------------------------
# prune_ts_metrics
# ---------------------------------------------------------------------------


def test_prune_empty_range_is_noop(temp_db_path, db_engine, db_session):
    """
    With no activities in range, prune reports zero counts and changes nothing.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    _seed_ts_metrics(db_session, 1, [(start, "heart_rate", 140.0, "bpm")])

    result = prune_ts_metrics(temp_db_path, end=date(2026, 1, 1))

    assert result == {"activity_count": 0, "rows_affected": 0, "dry_run": False}
    # Rows untouched.
    assert db_session.query(ActivityTsMetric).count() == 1


def test_prune_deletes_rows_in_range(temp_db_path, db_engine, db_session):
    """
    Activities whose start_ts falls in [start, end) have their ts-metric rows deleted;
    the activity row itself is preserved.
    """
    user_id = _seed_user(db_session)
    in_range_start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    out_of_range_start = datetime(2026, 2, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, in_range_start)
    _seed_activity(db_session, 2, user_id, out_of_range_start)
    _seed_ts_metrics(
        db_session,
        1,
        [
            (in_range_start, "heart_rate", 140.0, "bpm"),
            (in_range_start, "power", 220.0, "watts"),
        ],
    )
    _seed_ts_metrics(db_session, 2, [(out_of_range_start, "heart_rate", 145.0, "bpm")])

    result = prune_ts_metrics(
        temp_db_path,
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
    )

    assert result["activity_count"] == 1
    assert result["rows_affected"] == 2
    assert result["dry_run"] is False
    # Activity 1's ts rows gone, activity 2's preserved, both activities still exist.
    assert db_session.query(ActivityTsMetric).filter_by(activity_id=1).count() == 0
    assert db_session.query(ActivityTsMetric).filter_by(activity_id=2).count() == 1
    assert db_session.query(Activity).count() == 2


def test_prune_dry_run_counts_without_writing(temp_db_path, db_engine, db_session):
    """
    Dry-run returns the matching row count and does not delete.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    _seed_ts_metrics(
        db_session,
        1,
        [(start, "heart_rate", 140.0, "bpm"), (start, "power", 220.0, "watts")],
    )

    result = prune_ts_metrics(
        temp_db_path,
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        dry_run=True,
    )

    assert result == {"activity_count": 1, "rows_affected": 2, "dry_run": True}
    assert db_session.query(ActivityTsMetric).count() == 2


def test_prune_user_scoping(temp_db_path, db_engine, db_session):
    """
    With user_ids provided, only matching users' activities are pruned.
    """
    _seed_user(db_session, user_id=10)
    _seed_user(db_session, user_id=20)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, 10, start)
    _seed_activity(db_session, 2, 20, start)
    _seed_ts_metrics(db_session, 1, [(start, "heart_rate", 140.0, "bpm")])
    _seed_ts_metrics(db_session, 2, [(start, "heart_rate", 150.0, "bpm")])

    result = prune_ts_metrics(
        temp_db_path,
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        user_ids=[10],
    )

    assert result["activity_count"] == 1
    assert result["rows_affected"] == 1
    assert db_session.query(ActivityTsMetric).filter_by(activity_id=1).count() == 0
    assert db_session.query(ActivityTsMetric).filter_by(activity_id=2).count() == 1


def test_prune_same_day_includes_that_day(temp_db_path, db_engine, db_session):
    """
    The start == end special case must include that single day, matching extract.
    """
    user_id = _seed_user(db_session)
    same_day = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, same_day)
    _seed_ts_metrics(db_session, 1, [(same_day, "heart_rate", 140.0, "bpm")])

    result = prune_ts_metrics(
        temp_db_path,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    assert result["activity_count"] == 1
    assert result["rows_affected"] == 1


def test_prune_missing_db_raises():
    """
    A non-existent database path raises FileNotFoundError before doing any work.
    """
    with pytest.raises(FileNotFoundError):
        prune_ts_metrics("/nonexistent/path/garmin.db", end=date(2026, 1, 1))


# ---------------------------------------------------------------------------
# downsample_activities
# ---------------------------------------------------------------------------


def test_downsample_aggregate_strategy(temp_db_path, db_engine, db_session):
    """
    AGGREGATE buckets carry avg, min, max, and sample_count; min/max bracket avg.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    # Three rows in the first 60s bucket: heart_rate values 140, 150, 160.
    rows = [
        (
            datetime(2026, 1, 5, 12, 0, 5, tzinfo=timezone.utc),
            "heart_rate",
            140.0,
            "bpm",
        ),
        (
            datetime(2026, 1, 5, 12, 0, 25, tzinfo=timezone.utc),
            "heart_rate",
            150.0,
            "bpm",
        ),
        (
            datetime(2026, 1, 5, 12, 0, 55, tzinfo=timezone.utc),
            "heart_rate",
            160.0,
            "bpm",
        ),
    ]
    _seed_ts_metrics(db_session, 1, rows)

    result = downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    assert result["activity_count"] == 1
    assert result["rows_inserted"] == 1
    assert result["rows_deleted"] == 0
    assert result["dry_run"] is False
    bucket = db_session.query(ActivityTsMetricDownsampled).one()
    assert bucket.value == pytest.approx(150.0)
    assert bucket.min_value == 140.0
    assert bucket.max_value == 160.0
    assert bucket.sample_count == 3
    assert bucket.bucket_seconds == 60
    assert bucket.units == "bpm"


def test_downsample_last_strategy(temp_db_path, db_engine, db_session):
    """
    LAST strategy keeps the most recent value in each bucket and leaves min/max NULL.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    rows = [
        (datetime(2026, 1, 5, 12, 0, 5, tzinfo=timezone.utc), "distance", 10.0, "m"),
        (datetime(2026, 1, 5, 12, 0, 25, tzinfo=timezone.utc), "distance", 25.0, "m"),
        (datetime(2026, 1, 5, 12, 0, 55, tzinfo=timezone.utc), "distance", 55.0, "m"),
    ]
    _seed_ts_metrics(db_session, 1, rows)

    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    bucket = db_session.query(ActivityTsMetricDownsampled).one()
    assert bucket.name == "distance"
    assert bucket.value == 55.0  # Last-in-bucket.
    assert bucket.min_value is None
    assert bucket.max_value is None
    assert bucket.sample_count == 3


def test_downsample_skip_strategy(temp_db_path, db_engine, db_session):
    """
    SKIP metrics (position_lat, position_long) produce no buckets.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    _seed_ts_metrics(
        db_session,
        1,
        [
            (start, "position_lat", 12345.0, "semicircles"),
            (start, "position_long", 67890.0, "semicircles"),
            (start, "heart_rate", 140.0, "bpm"),
        ],
    )

    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    names = [row.name for row in db_session.query(ActivityTsMetricDownsampled).all()]
    assert "position_lat" not in names
    assert "position_long" not in names
    assert "heart_rate" in names


def test_downsample_bucket_alignment_is_activity_relative(
    temp_db_path, db_engine, db_session
):
    """
    Bucket boundaries are anchored to ``activity.start_ts``, not the epoch.
    """
    user_id = _seed_user(db_session)
    # Activity starts at an awkward sub-minute offset.
    start = datetime(2026, 1, 5, 12, 0, 37, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start, duration_minutes=5)
    rows = [
        (
            datetime(2026, 1, 5, 12, 0, 37, tzinfo=timezone.utc),
            "heart_rate",
            100.0,
            "bpm",
        ),
        (
            datetime(2026, 1, 5, 12, 1, 36, tzinfo=timezone.utc),
            "heart_rate",
            110.0,
            "bpm",
        ),
        (
            datetime(2026, 1, 5, 12, 1, 37, tzinfo=timezone.utc),
            "heart_rate",
            120.0,
            "bpm",
        ),
        (
            datetime(2026, 1, 5, 12, 2, 36, tzinfo=timezone.utc),
            "heart_rate",
            130.0,
            "bpm",
        ),
    ]
    _seed_ts_metrics(db_session, 1, rows)

    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    buckets = (
        db_session.query(ActivityTsMetricDownsampled)
        .order_by(ActivityTsMetricDownsampled.bucket_ts)
        .all()
    )
    # Three bucket boundaries: 12:00:37, 12:01:37, 12:02:37 (activity-relative).
    bucket_secs = [b.bucket_ts.second for b in buckets]
    assert all(
        s == 37 for s in bucket_secs
    ), f"Buckets are not activity-relative: seconds {bucket_secs} should all be 37."


def test_downsample_replace_per_activity_with_different_grain(
    temp_db_path, db_engine, db_session
):
    """
    Re-running downsample for the same activity with a different grain wipes the
    activity's prior buckets and inserts the new grain's buckets.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start, duration_minutes=10)
    # 10 evenly spaced rows over 10 minutes.
    rows = [
        (
            datetime(2026, 1, 5, 12, i, 0, tzinfo=timezone.utc),
            "heart_rate",
            140.0 + i,
            "bpm",
        )
        for i in range(10)
    ]
    _seed_ts_metrics(db_session, 1, rows)

    # First pass: 60s grain → 10 buckets.
    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )
    grains_after_first = {
        b.bucket_seconds for b in db_session.query(ActivityTsMetricDownsampled).all()
    }
    assert grains_after_first == {60}
    count_after_first = db_session.query(ActivityTsMetricDownsampled).count()
    assert count_after_first == 10

    # Second pass: 300s grain → 2 buckets, prior 60s buckets must be gone.
    downsample_activities(
        temp_db_path,
        time_grain_seconds=300,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )
    rows_after_second = db_session.query(ActivityTsMetricDownsampled).all()
    grains = {b.bucket_seconds for b in rows_after_second}
    assert grains == {
        300
    }, f"60s buckets survived; expected only 300s grain, got {grains}."
    assert len(rows_after_second) == 2


def test_downsample_preserves_buckets_for_pruned_activities(
    temp_db_path, db_engine, db_session
):
    """
    An activity whose source rows have been pruned must be excluded from the replace
    set; its existing downsampled rows must survive a re-run.
    """
    user_id = _seed_user(db_session)
    start_a = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    start_b = datetime(2026, 1, 6, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start_a)
    _seed_activity(db_session, 2, user_id, start_b)
    _seed_ts_metrics(db_session, 1, [(start_a, "heart_rate", 100.0, "bpm")])
    _seed_ts_metrics(db_session, 2, [(start_b, "heart_rate", 200.0, "bpm")])

    # First pass: downsample both activities at 60s.
    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 7),
    )
    assert db_session.query(ActivityTsMetricDownsampled).count() == 2

    # Simulate a prune: drop activity 1's source rows.
    prune_ts_metrics(temp_db_path, start=date(2026, 1, 5), end=date(2026, 1, 6))
    db_session.expire_all()
    assert db_session.query(ActivityTsMetric).filter_by(activity_id=1).count() == 0

    # Re-run downsample over the same range: activity 1 has no source so must
    # be excluded entirely; activity 2 (in the second day) is re-downsampled.
    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 7),
    )
    db_session.expire_all()
    by_activity = {
        b.activity_id: b for b in db_session.query(ActivityTsMetricDownsampled).all()
    }
    assert 1 in by_activity, "Activity 1's downsampled row was wiped despite no source."
    assert by_activity[1].value == 100.0
    assert by_activity[2].value == 200.0


def test_downsample_user_scoping(temp_db_path, db_engine, db_session):
    """
    user_ids filter limits the replace set to matching users.
    """
    _seed_user(db_session, user_id=10)
    _seed_user(db_session, user_id=20)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, 10, start)
    _seed_activity(db_session, 2, 20, start)
    _seed_ts_metrics(db_session, 1, [(start, "heart_rate", 100.0, "bpm")])
    _seed_ts_metrics(db_session, 2, [(start, "heart_rate", 200.0, "bpm")])

    downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
        user_ids=[10],
    )

    by_activity = {
        b.activity_id: b for b in db_session.query(ActivityTsMetricDownsampled).all()
    }
    assert set(by_activity) == {1}


def test_downsample_dry_run(temp_db_path, db_engine, db_session):
    """
    Dry run reports the metric strategy table and counts without writing.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start)
    _seed_ts_metrics(
        db_session,
        1,
        [
            (start, "heart_rate", 100.0, "bpm"),
            (start, "distance", 5.0, "m"),
            (start, "position_lat", 12345.0, "semicircles"),
        ],
    )

    result = downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
        dry_run=True,
    )

    assert result["dry_run"] is True
    assert result["activity_count"] == 1
    assert result["rows_inserted"] == 0
    assert result["rows_deleted"] == 0
    names = [name for name, _ in result["metric_strategies"]]
    assert names == sorted(names) == ["distance", "heart_rate", "position_lat"]
    assert db_session.query(ActivityTsMetricDownsampled).count() == 0


def test_downsample_rejects_invalid_grain(temp_db_path, db_engine, db_session):
    """
    A non-positive time_grain_seconds must raise immediately.
    """
    with pytest.raises(ValueError):
        downsample_activities(temp_db_path, time_grain_seconds=0, end=date(2026, 1, 5))


def test_downsample_missing_db_raises():
    """
    A non-existent database path raises FileNotFoundError.
    """
    with pytest.raises(FileNotFoundError):
        downsample_activities(
            "/nonexistent/path/garmin.db",
            time_grain_seconds=60,
            end=date(2026, 1, 1),
        )


def test_downsample_rows_inserted_count_matches_table_for_last_strategy(
    temp_db_path, db_engine, db_session
):
    """
    Regression: ``cur.rowcount`` is unreliable for ``WITH ...

    INSERT INTO ...`` in Python's sqlite3 binding (the LAST-strategy query uses a CTE).

    The reported ``rows_inserted`` must match the actual destination-table count even
    when both AGGREGATE (no CTE) and LAST (with CTE) run together.
    """
    user_id = _seed_user(db_session)
    start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
    _seed_activity(db_session, 1, user_id, start, duration_minutes=30)
    rows = []
    for sec in range(0, 1800, 30):
        rows.append(
            (
                datetime.fromtimestamp(start.timestamp() + sec, tz=timezone.utc),
                "heart_rate",
                140.0 + sec / 100,
                "bpm",
            )
        )
        rows.append(
            (
                datetime.fromtimestamp(start.timestamp() + sec, tz=timezone.utc),
                "distance",
                float(sec),
                "m",
            )
        )
    _seed_ts_metrics(db_session, 1, rows)

    result = downsample_activities(
        temp_db_path,
        time_grain_seconds=60,
        start=date(2026, 1, 5),
        end=date(2026, 1, 5),
    )

    actual = db_session.query(ActivityTsMetricDownsampled).count()
    assert result["rows_inserted"] == actual, (
        f"rows_inserted={result['rows_inserted']} disagrees with table "
        f"count={actual}; CTE INSERT rowcount is being miscounted."
    )


def test_downsample_creates_new_table_on_pre_2_8_db(tmp_path):
    """
    Regression: a pre-2.8 database has no ``activity_ts_metric_downsampled`` table.

    Calling :func:`downsample_activities` must materialize the table on the fly via
    ``CREATE TABLE IF NOT EXISTS`` so users who run ``garmin migrate-cascade`` then
    ``garmin downsample`` directly (without an intervening ``garmin extract``) do not
    hit "no such table".
    """
    import sqlite3

    db_path = str(tmp_path / "garmin.db")
    # Construct a minimal pre-2.8 DB by hand: just the three tables the
    # downsample SQL touches, no downsampled table.
    conn = sqlite3.connect(db_path)
    conn.executescript(
        "CREATE TABLE user (user_id BIGINT PRIMARY KEY); "
        "CREATE TABLE activity ("
        "activity_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
        "activity_type_id INTEGER NOT NULL, activity_type_key TEXT NOT NULL, "
        "event_type_id INTEGER NOT NULL, event_type_key TEXT NOT NULL, "
        "start_ts DATETIME NOT NULL, end_ts DATETIME NOT NULL, "
        "timezone_offset_hours FLOAT NOT NULL); "
        "CREATE TABLE activity_ts_metric ("
        "activity_id BIGINT NOT NULL, timestamp DATETIME NOT NULL, "
        "name TEXT NOT NULL, value FLOAT, units TEXT, "
        "PRIMARY KEY (activity_id, timestamp, name));"
    )
    conn.commit()
    conn.close()

    pre_tables = (
        sqlite3.connect(db_path)
        .execute("SELECT name FROM sqlite_master WHERE type='table'")
        .fetchall()
    )
    assert ("activity_ts_metric_downsampled",) not in pre_tables

    result = downsample_activities(
        db_path,
        time_grain_seconds=60,
        end=date(2026, 1, 1),
    )

    # Empty result is fine; the assertion is that the call did not raise
    # OperationalError("no such table") and the table now exists.
    assert result["activity_count"] == 0
    post_tables = (
        sqlite3.connect(db_path)
        .execute("SELECT name FROM sqlite_master WHERE type='table'")
        .fetchall()
    )
    assert ("activity_ts_metric_downsampled",) in post_tables

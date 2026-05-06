"""
Integration tests verifying ON DELETE CASCADE fires for activity-child and sleep-child
foreign keys.

The pragma listener registered by :func:`garmin_health_data.db.get_engine` enables
SQLite's foreign-key enforcement, which is required for the cascade clauses in
`tables.ddl` to actually take effect at delete time.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    Activity,
    ActivityLapMetric,
    ActivityPath,
    ActivitySplitMetric,
    ActivityTsMetric,
    ActivityTsMetricDownsampled,
    BreathingDisruption,
    CyclingAggMetrics,
    HRV,
    RunningAggMetrics,
    Sleep,
    SleepLevel,
    SleepMovement,
    SleepRestlessMoment,
    SpO2,
    StrengthExercise,
    StrengthSet,
    SupplementalActivityMetric,
    SwimmingAggMetrics,
    User,
)


@pytest.fixture
def seeded_user(db_session: Session) -> int:
    """
    Insert a minimal user row so child FKs to user(user_id) are satisfied.

    :param db_session: Database session fixture.
    :return: The seeded user_id.
    """
    user = User(user_id=42)
    db_session.add(user)
    db_session.commit()
    return 42


def _make_activity(activity_id: int, user_id: int) -> Activity:
    """
    Build a minimal Activity row valid against the schema's NOT NULL columns.

    :param activity_id: Activity primary key.
    :param user_id: Owning user_id.
    :return: Activity instance ready for insert.
    """
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return Activity(
        activity_id=activity_id,
        user_id=user_id,
        activity_type_id=1,
        activity_type_key="running",
        event_type_id=1,
        event_type_key="other",
        start_ts=now,
        end_ts=now,
        timezone_offset_hours=0.0,
    )


def _make_sleep(sleep_id: int, user_id: int) -> Sleep:
    """
    Build a minimal Sleep row valid against the schema's NOT NULL columns.

    :param sleep_id: Sleep session primary key.
    :param user_id: Owning user_id.
    :return: Sleep instance ready for insert.
    """
    now = datetime(2026, 1, 1, 22, 0, 0, tzinfo=timezone.utc)
    return Sleep(
        sleep_id=sleep_id,
        user_id=user_id,
        start_ts=now,
        end_ts=now,
        timezone_offset_hours=0.0,
    )


def test_delete_activity_cascades_to_all_children(
    db_engine, db_session: Session, seeded_user: int
):
    """
    Deleting an activity removes rows in every activity-child table.

    This exercises all 10 child FKs declared with `ON DELETE CASCADE`, including the new
    `activity_ts_metric_downsampled` table.
    """
    activity = _make_activity(activity_id=1001, user_id=seeded_user)
    db_session.add(activity)
    db_session.flush()

    now = datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
    db_session.add_all(
        [
            SwimmingAggMetrics(activity_id=1001),
            CyclingAggMetrics(activity_id=1001),
            RunningAggMetrics(activity_id=1001),
            SupplementalActivityMetric(activity_id=1001, metric="custom", value=1.0),
            ActivityTsMetric(
                activity_id=1001, timestamp=now, name="heart_rate", value=140.0
            ),
            ActivitySplitMetric(
                activity_id=1001,
                split_idx=1,
                name="total_distance",
                value=100.0,
            ),
            ActivityLapMetric(
                activity_id=1001, lap_idx=1, name="distance", value=100.0
            ),
            ActivityPath(activity_id=1001, path_json=[], point_count=0),
            StrengthExercise(
                activity_id=1001,
                exercise_category="BENCH_PRESS",
                exercise_name="BARBELL_BENCH_PRESS",
            ),
            StrengthSet(activity_id=1001, set_idx=1, set_type="ACTIVE"),
            ActivityTsMetricDownsampled(
                activity_id=1001,
                bucket_ts=now,
                name="heart_rate",
                bucket_seconds=60,
                value=140.0,
                sample_count=1,
            ),
        ]
    )
    db_session.commit()

    # Sanity-check children exist.
    for model in (
        SwimmingAggMetrics,
        CyclingAggMetrics,
        RunningAggMetrics,
        SupplementalActivityMetric,
        ActivityTsMetric,
        ActivitySplitMetric,
        ActivityLapMetric,
        ActivityPath,
        StrengthExercise,
        StrengthSet,
        ActivityTsMetricDownsampled,
    ):
        count = db_session.query(model).filter_by(activity_id=1001).count()
        assert count == 1, f"{model.__name__} child was not inserted."

    # Cascade delete via the parent.
    db_session.delete(activity)
    db_session.commit()

    for model in (
        SwimmingAggMetrics,
        CyclingAggMetrics,
        RunningAggMetrics,
        SupplementalActivityMetric,
        ActivityTsMetric,
        ActivitySplitMetric,
        ActivityLapMetric,
        ActivityPath,
        StrengthExercise,
        StrengthSet,
        ActivityTsMetricDownsampled,
    ):
        count = db_session.query(model).filter_by(activity_id=1001).count()
        assert (
            count == 0
        ), f"{model.__name__} child survived parent delete; cascade did not fire."


def test_delete_sleep_cascades_to_all_children(
    db_engine, db_session: Session, seeded_user: int
):
    """
    Deleting a sleep session removes rows in every sleep-child table.

    Exercises all 6 child FKs declared with `ON DELETE CASCADE`.
    """
    sleep = _make_sleep(sleep_id=2001, user_id=seeded_user)
    db_session.add(sleep)
    db_session.flush()

    now = datetime(2026, 1, 1, 22, 30, 0, tzinfo=timezone.utc)
    db_session.add_all(
        [
            SleepLevel(
                sleep_id=2001,
                start_ts=now,
                end_ts=now,
                stage=0,
                stage_label="DEEP",
            ),
            SleepMovement(sleep_id=2001, timestamp=now, activity_level=0.5),
            SleepRestlessMoment(sleep_id=2001, timestamp=now, value=1),
            SpO2(sleep_id=2001, timestamp=now, value=95),
            HRV(sleep_id=2001, timestamp=now, value=42.0),
            BreathingDisruption(sleep_id=2001, timestamp=now, value=0),
        ]
    )
    db_session.commit()

    for model in (
        SleepLevel,
        SleepMovement,
        SleepRestlessMoment,
        SpO2,
        HRV,
        BreathingDisruption,
    ):
        count = db_session.query(model).filter_by(sleep_id=2001).count()
        assert count == 1, f"{model.__name__} child was not inserted."

    db_session.delete(sleep)
    db_session.commit()

    for model in (
        SleepLevel,
        SleepMovement,
        SleepRestlessMoment,
        SpO2,
        HRV,
        BreathingDisruption,
    ):
        count = db_session.query(model).filter_by(sleep_id=2001).count()
        assert (
            count == 0
        ), f"{model.__name__} child survived parent delete; cascade did not fire."


def test_delete_user_does_not_cascade(db_engine, db_session: Session, seeded_user: int):
    """
    User-scoped FKs (activity.user_id, sleep.user_id, biometric series) intentionally
    omit cascade.

    Deleting a user should NOT silently wipe years of activity/sleep history; that
    operation must remain explicit.
    """
    activity = _make_activity(activity_id=3001, user_id=seeded_user)
    db_session.add(activity)
    db_session.commit()

    user = db_session.get(User, seeded_user)
    db_session.delete(user)

    # FK constraint must reject the delete (or the activity must survive in the
    # absence of cascade; either way, we should NOT silently lose the activity).
    with pytest.raises(Exception):
        db_session.commit()
    db_session.rollback()

    activity_after = db_session.get(Activity, 3001)
    assert (
        activity_after is not None
    ), "Activity was wiped by user delete; user FK must not cascade."


def test_pragma_foreign_keys_on_session_connection(db_engine):
    """
    Confirm the pragma is enabled on the same connection the test session uses.
    """
    with db_engine.connect() as conn:
        value = conn.execute(text("PRAGMA foreign_keys")).scalar()
        assert value == 1

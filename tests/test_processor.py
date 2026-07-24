"""
Tests for GarminProcessor.

Covers FIT file delete+insert reprocessing, activity/sleep upsert column exclusion, and
strength training data processing.
"""

import copy
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import fitdecode
import pytest
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    HRV,
    Activity,
    ActivityLapMetric,
    ActivityPath,
    ActivitySplitMetric,
    ActivityTsMetric,
    BodyComposition,
    BreathingDisruption,
    MenstrualCycleDay,
    MenstrualCycleSummary,
    MenstrualCycleTag,
    Sleep,
    SleepLevel,
    SleepMovement,
    SleepRestlessMoment,
    SpO2,
    StrengthExercise,
    StrengthSet,
    User,
)
from garmin_health_data.constants import SEMICIRCLES_TO_DEGREES
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet, upsert_model_instances


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def processor():
    """
    Create a GarminProcessor instance for testing.

    The internal MagicMock session is pre-configured so
    ``session.execute().scalar_one_or_none()`` returns ``None`` by default, matching the
    ``mock_session`` fixture. See that fixture's docstring for rationale.

    :return: GarminProcessor instance.
    """
    file_set = FileSet(file_paths=[], files={})
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    proc = GarminProcessor(file_set, session)
    proc.user_id = 123456789
    return proc


@pytest.fixture
def mock_session():
    """
    Create a mock database session.

    The default ``session.execute().scalar_one_or_none()`` returns ``None`` so the
    activity processor's duplicate-detection check (which queries for an existing
    ``(user_id, start_ts)`` row with a different ``activity_id``) treats the activity as
    new in mock-based tests. Tests that want to exercise the duplicate path should
    override this on their session instance.

    :return: Mock session instance.
    """
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    return session


# --- FIT helpers ------------------------------------------------------------


def _make_field(name: str, value, units: str = None) -> MagicMock:
    """
    Create a mock FIT field.
    """
    field = MagicMock()
    field.name = name
    field.value = value
    field.units = units
    return field


def _make_frame(name: str, fields: list) -> MagicMock:
    """
    Create a mock FIT data frame.
    """
    frame = MagicMock()
    frame.frame_type = fitdecode.FIT_FRAME_DATA
    frame.name = name
    frame.fields = fields
    return frame


def _mock_fit_reader(frames: list) -> MagicMock:
    """
    Create a mock fitdecode.FitReader context manager that iterates the given frames.
    """
    reader = MagicMock()
    reader.__enter__ = MagicMock(return_value=reader)
    reader.__exit__ = MagicMock(return_value=False)
    reader.__iter__ = MagicMock(return_value=iter(frames))
    return reader


def _seed_activity(
    session: Session,
    activity_id: int = 12345,
    ts_data_available: bool = False,
) -> Activity:
    """
    Insert a user and activity record for FIT file tests.
    """
    upsert_model_instances(
        session=session,
        model_instances=[User(user_id=1, full_name="Test User")],
        conflict_columns=["user_id"],
        on_conflict_update=True,
    )
    activity = Activity(
        activity_id=activity_id,
        user_id=1,
        activity_name="Morning Run",
        activity_type_id=1,
        activity_type_key="running",
        event_type_id=1,
        event_type_key="uncategorized",
        start_ts=datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
        end_ts=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        timezone_offset_hours=0.0,
        has_polyline=False,
        has_images=False,
        has_video=False,
        has_heat_map=False,
        parent=False,
        purposeful=True,
        favorite=False,
        pr=False,
        auto_calc_calories=True,
        manual_activity=False,
    )
    upsert_model_instances(
        session=session,
        model_instances=[activity],
        conflict_columns=["activity_id"],
        on_conflict_update=True,
    )
    session.commit()

    # Set ts_data_available after upsert (bypasses column exclusion).
    persisted = (
        session.execute(select(Activity).where(Activity.activity_id == activity_id))
        .scalars()
        .first()
    )
    persisted.ts_data_available = ts_data_available
    session.commit()

    return persisted


FIT_FILENAME = "1_ACTIVITY_12345_2024-01-01T08:00:00Z.fit"


# --- FIT file processing tests ---------------------------------------------


class TestProcessFitFile:
    """
    Tests for _process_fit_file delete+insert and ts_data_available logic.
    """

    def _make_processor(self) -> GarminProcessor:
        """
        Create a GarminProcessor with a dummy file set.
        """
        file_set = FileSet(file_paths=[], files={})
        # session arg is unused (each method receives its own session).
        return GarminProcessor(file_set=file_set, session=MagicMock())

    def test_process_fit_file_success(self, db_session: Session):
        """
        First-time processing inserts metrics and sets ts_data_available.
        """
        activity = _seed_activity(db_session)
        assert activity.ts_data_available is False

        ts = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        record_frame = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("heart_rate", 150, "bpm"),
                _make_field("cadence", 90, "rpm"),
            ],
        )
        lap_frame = _make_frame(
            "lap",
            [
                _make_field("total_elapsed_time", 300.0, "s"),
                _make_field("avg_heart_rate", 155.0, "bpm"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader(
                [record_frame, lap_frame]
            )
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 2
        )
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityLapMetric)) == 2
        )

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        assert refreshed.ts_data_available is True

    def test_process_fit_file_reprocessing(self, db_session: Session):
        """
        Re-running deletes old rows and inserts fresh data.
        """
        activity = _seed_activity(db_session, ts_data_available=True)

        # Simulate pre-existing metrics from a previous run.
        old_ts = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        # Use core insert to bypass RETURNING sentinel mismatch
        # with DateTime(timezone=True) composite PKs on SQLite.
        db_session.execute(
            insert(ActivityTsMetric),
            [
                {
                    "activity_id": 12345,
                    "timestamp": old_ts,
                    "name": "old_metric",
                    "value": 1.0,
                    "units": None,
                },
            ],
        )
        db_session.execute(
            insert(ActivityLapMetric),
            [
                {
                    "activity_id": 12345,
                    "lap_idx": 1,
                    "name": "old_lap",
                    "value": 100.0,
                    "units": None,
                },
            ],
        )
        db_session.execute(
            insert(ActivitySplitMetric),
            [
                {
                    "activity_id": 12345,
                    "split_idx": 1,
                    "name": "old_split",
                    "value": 200.0,
                    "units": None,
                },
            ],
        )
        db_session.commit()
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 1
        )
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityLapMetric)) == 1
        )
        assert (
            db_session.scalar(select(func.count()).select_from(ActivitySplitMetric))
            == 1
        )

        # New FIT data with different metrics.
        new_ts = datetime(2024, 1, 1, 8, 0, 5, tzinfo=timezone.utc)
        record_frame = _make_frame(
            "record",
            [
                _make_field("timestamp", new_ts),
                _make_field("heart_rate", 160, "bpm"),
            ],
        )
        lap_frame = _make_frame(
            "lap",
            [_make_field("total_elapsed_time", 600.0, "s")],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader(
                [record_frame, lap_frame]
            )
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        # Old rows deleted, new rows inserted.
        ts_rows = db_session.execute(select(ActivityTsMetric)).scalars().all()
        assert len(ts_rows) == 1
        assert ts_rows[0].name == "heart_rate"
        assert ts_rows[0].value == 160.0

        lap_rows = db_session.execute(select(ActivityLapMetric)).scalars().all()
        assert len(lap_rows) == 1
        assert lap_rows[0].name == "total_elapsed_time"

        # Old splits deleted (no new splits in this FIT data).
        assert (
            db_session.scalar(select(func.count()).select_from(ActivitySplitMetric))
            == 0
        )

    def test_process_fit_file_laps_only(self, db_session: Session):
        """
        Activity with only laps (no record frames) still processes correctly.
        """
        activity = _seed_activity(db_session)

        lap_frame = _make_frame(
            "lap",
            [
                _make_field("total_elapsed_time", 300.0, "s"),
                _make_field("avg_heart_rate", 145.0, "bpm"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader([lap_frame])
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 0
        )
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityLapMetric)) == 2
        )

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        # No record frames means ts_data_available stays False.
        assert refreshed.ts_data_available is False

    def test_process_fit_file_activity_not_found(self, db_session: Session):
        """
        Raises ValueError when activity_id not in database.
        """
        # Seed user only, no activity.
        upsert_model_instances(
            session=db_session,
            model_instances=[User(user_id=1, full_name="Test User")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        db_session.commit()

        processor = self._make_processor()
        with pytest.raises(ValueError, match="Activity 12345 not found"):
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

    def test_process_fit_file_invalid_filename(self, db_session: Session):
        """
        Raises ValueError for non-matching filename pattern.
        """
        processor = self._make_processor()
        with pytest.raises(ValueError, match="Cannot extract activity_id"):
            processor._process_fit_file(Path("bad_name.fit"), db_session)

    def test_process_fit_file_creates_activity_path(self, db_session: Session):
        """
        Record frames with GPS coordinates produce an ActivityPath row with semicircles
        converted to decimal degrees and points sorted by timestamp.
        """
        _seed_activity(db_session)
        # Semicircle values chosen for exact float conversions:
        # 2**29 * (180 / 2**31) = 45.0; -(2**28) * (180 / 2**31) = -22.5
        # 2**28 * (180 / 2**31) = 22.5; -(2**27) * (180 / 2**31) = -11.25
        # 2**27 * (180 / 2**31) = 11.25; -(2**26) * (180 / 2**31) = -5.625
        ts1 = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 8, 0, 2, tzinfo=timezone.utc)
        ts3 = datetime(2024, 1, 1, 8, 0, 3, tzinfo=timezone.utc)

        # Insert out of order to verify timestamp sorting.
        frame_b = _make_frame(
            "record",
            [
                _make_field("timestamp", ts2),
                _make_field("position_lat", 2**28, "semicircles"),
                _make_field("position_long", -(2**27), "semicircles"),
            ],
        )
        frame_a = _make_frame(
            "record",
            [
                _make_field("timestamp", ts1),
                _make_field("position_lat", 2**29, "semicircles"),
                _make_field("position_long", -(2**28), "semicircles"),
            ],
        )
        frame_c = _make_frame(
            "record",
            [
                _make_field("timestamp", ts3),
                _make_field("position_lat", 2**27, "semicircles"),
                _make_field("position_long", -(2**26), "semicircles"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader(
                [frame_b, frame_a, frame_c]
            )
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        paths = db_session.execute(select(ActivityPath)).scalars().all()
        assert len(paths) == 1
        path = paths[0]
        assert path.activity_id == 12345
        assert path.point_count == 3
        # SQLAlchemy JSON auto-deserializes to a Python list on read.
        assert isinstance(path.path_json, list)
        # Sorted ascending by timestamp: ts1, ts2, ts3.
        assert path.path_json[0][0] == pytest.approx(-22.5)
        assert path.path_json[0][1] == pytest.approx(45.0)
        assert path.path_json[1][0] == pytest.approx(-11.25)
        assert path.path_json[1][1] == pytest.approx(22.5)
        assert path.path_json[2][0] == pytest.approx(-5.625)
        assert path.path_json[2][1] == pytest.approx(11.25)

    def test_process_fit_file_no_gps_skips_activity_path(self, db_session: Session):
        """
        Records without position_lat/position_long produce zero ActivityPath rows.
        """
        _seed_activity(db_session)

        ts = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        record_frame = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("heart_rate", 150, "bpm"),
                _make_field("cadence", 90, "rpm"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader([record_frame])
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        # Non-GPS ts metrics still inserted.
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 2
        )
        # No activity_path row.
        assert db_session.scalar(select(func.count()).select_from(ActivityPath)) == 0

    def test_process_fit_file_partial_gps_filtered(self, db_session: Session):
        """
        Frames with only position_lat (no position_long) are excluded; only frames with
        both coordinates produce path points.
        """
        _seed_activity(db_session)

        ts1 = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 8, 0, 2, tzinfo=timezone.utc)

        # Frame 1: only lat, no lon -> dropped.
        frame_partial = _make_frame(
            "record",
            [
                _make_field("timestamp", ts1),
                _make_field("position_lat", 2**29, "semicircles"),
            ],
        )
        # Frame 2: both lat and lon -> kept.
        frame_complete = _make_frame(
            "record",
            [
                _make_field("timestamp", ts2),
                _make_field("position_lat", 2**28, "semicircles"),
                _make_field("position_long", -(2**27), "semicircles"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader(
                [frame_partial, frame_complete]
            )
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        paths = db_session.execute(select(ActivityPath)).scalars().all()
        assert len(paths) == 1
        path = paths[0]
        assert path.point_count == 1
        assert path.path_json[0][0] == pytest.approx(-11.25)
        assert path.path_json[0][1] == pytest.approx(22.5)

    def test_process_fit_file_reprocessing_updates_path(self, db_session: Session):
        """
        Re-running replaces the existing ActivityPath row.

        A subsequent run without GPS data deletes the row entirely.
        """
        _seed_activity(db_session)

        ts1 = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 8, 0, 2, tzinfo=timezone.utc)
        ts3 = datetime(2024, 1, 1, 8, 0, 3, tzinfo=timezone.utc)

        # First run: 2 points.
        run1_frames = [
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts1),
                    _make_field("position_lat", 2**29, "semicircles"),
                    _make_field("position_long", -(2**28), "semicircles"),
                ],
            ),
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts2),
                    _make_field("position_lat", 2**28, "semicircles"),
                    _make_field("position_long", -(2**27), "semicircles"),
                ],
            ),
        ]

        def run_with_frames(frames: list) -> None:
            processor = self._make_processor()
            with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
                mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
                mock_fitdecode.FitReader.return_value = _mock_fit_reader(frames)
                processor._process_fit_file(Path(FIT_FILENAME), db_session)
            db_session.commit()

        run_with_frames(run1_frames)

        paths = db_session.execute(select(ActivityPath)).scalars().all()
        assert len(paths) == 1
        assert paths[0].point_count == 2

        # Second run: 3 different points (delete-before-insert).
        run2_frames = [
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts1),
                    _make_field("position_lat", 2**27, "semicircles"),
                    _make_field("position_long", -(2**26), "semicircles"),
                ],
            ),
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts2),
                    _make_field("position_lat", 2**26, "semicircles"),
                    _make_field("position_long", -(2**25), "semicircles"),
                ],
            ),
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts3),
                    _make_field("position_lat", 2**25, "semicircles"),
                    _make_field("position_long", -(2**24), "semicircles"),
                ],
            ),
        ]
        run_with_frames(run2_frames)

        paths = db_session.execute(select(ActivityPath)).scalars().all()
        assert len(paths) == 1
        assert paths[0].point_count == 3

        # Third run: no GPS -> existing row deleted, no new row.
        run3_frames = [
            _make_frame(
                "record",
                [
                    _make_field("timestamp", ts1),
                    _make_field("heart_rate", 160, "bpm"),
                ],
            ),
        ]
        run_with_frames(run3_frames)

        assert db_session.scalar(select(func.count()).select_from(ActivityPath)) == 0


# --- Activity base upsert tests --------------------------------------------


def _minimal_activity_json(activity_id: int, start_time_iso: str) -> dict:
    """
    Build the minimum activity JSON shape ``_process_activity_base`` needs to reach its
    duplicate-detection check.

    Only fields that ``pop()`` runs before the check are populated; intentionally omits
    sport-specific aggregates and supplemental fields because the duplicate path returns
    ``None`` before they would be read.

    :param activity_id: The Garmin activity ID.
    :param start_time_iso: ISO 8601 start timestamp (``"YYYY-MM-DDTHH:MM:SS"``, no
        offset suffix; used for both ``startTimeGMT`` and ``startTimeLocal`` so
        timezone_offset_hours computes to 0). End time is computed as ``start_time_iso +
        1 hour``.
    :return: Activity dict ready for ``_process_activity_base``.
    """
    end_iso = (datetime.fromisoformat(start_time_iso) + timedelta(hours=1)).isoformat()
    return {
        "activityId": activity_id,
        "activityType": {"typeId": 1, "typeKey": "running"},
        "eventType": {"typeId": 1, "typeKey": "uncategorized"},
        "startTimeGMT": start_time_iso,
        "startTimeLocal": start_time_iso,
        "endTimeGMT": end_iso,
    }


class TestActivityBaseUpsert:
    """
    Tests for column exclusion during activity upserts.
    """

    def test_upsert_preserves_ts_data_available(self, db_session: Session):
        """
        Activity upsert does not overwrite ts_data_available flag.
        """
        activity = _seed_activity(db_session, ts_data_available=True)
        assert activity.ts_data_available is True

        # Simulate a second activity list upsert with explicit update_columns
        # that excludes ts_data_available (matching _process_activity_base logic).
        update_columns = [
            col.name
            for col in Activity.__table__.columns
            if col.name not in ["activity_id", "ts_data_available", "create_ts"]
        ]
        updated_activity = Activity(
            activity_id=12345,
            user_id=1,
            activity_name="Renamed Run",
            activity_type_id=1,
            activity_type_key="running",
            event_type_id=1,
            event_type_key="uncategorized",
            start_ts=datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
            end_ts=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            timezone_offset_hours=0.0,
            has_polyline=False,
            has_images=False,
            has_video=False,
            has_heat_map=False,
            parent=False,
            purposeful=True,
            favorite=False,
            pr=False,
            auto_calc_calories=True,
            manual_activity=False,
        )
        upsert_model_instances(
            session=db_session,
            model_instances=[updated_activity],
            conflict_columns=["activity_id"],
            update_columns=update_columns,
            on_conflict_update=True,
        )
        db_session.commit()

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        assert refreshed.activity_name == "Renamed Run"
        # ts_data_available preserved despite upsert.
        assert refreshed.ts_data_available is True

    def test_upsert_preserves_create_ts(self, db_session: Session):
        """
        Activity upsert does not overwrite create_ts audit column.
        """
        _seed_activity(db_session)

        original = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        original_create_ts = original.create_ts

        update_columns = [
            col.name
            for col in Activity.__table__.columns
            if col.name not in ["activity_id", "ts_data_available", "create_ts"]
        ]
        updated_activity = Activity(
            activity_id=12345,
            user_id=1,
            activity_name="Updated Name",
            activity_type_id=1,
            activity_type_key="running",
            event_type_id=1,
            event_type_key="uncategorized",
            start_ts=datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
            end_ts=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            timezone_offset_hours=0.0,
            has_polyline=False,
            has_images=False,
            has_video=False,
            has_heat_map=False,
            parent=False,
            purposeful=True,
            favorite=False,
            pr=False,
            auto_calc_calories=True,
            manual_activity=False,
        )
        upsert_model_instances(
            session=db_session,
            model_instances=[updated_activity],
            conflict_columns=["activity_id"],
            update_columns=update_columns,
            on_conflict_update=True,
        )
        db_session.commit()

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        assert refreshed.activity_name == "Updated Name"
        assert refreshed.create_ts == original_create_ts

    def test_update_columns_excludes_correct_fields(self):
        """
        Verify the column exclusion list matches the processor logic.
        """
        update_columns = [
            col.name
            for col in Activity.__table__.columns
            if col.name not in ["activity_id", "ts_data_available", "create_ts"]
        ]
        assert "activity_id" not in update_columns
        assert "ts_data_available" not in update_columns
        assert "create_ts" not in update_columns
        assert "activity_name" in update_columns
        assert "update_ts" in update_columns


class TestActivityBaseDuplicateDedup:
    """
    Tests for the (user_id, start_ts) duplicate-detection guard in
    ``_process_activity_base`` (issue #66).

    Garmin Connect accepts multiple activities with identical start times (manual
    entries, two devices recording the same session, etc.) and returns each as a
    distinct ``activityId``. The activity table's ``UNIQUE (user_id, start_ts)``
    constraint correctly rejects the second insert — but a raw ``IntegrityError`` from
    the upsert would quarantine the whole ``(user, day)`` FileSet, losing sleep / HR /
    stress / etc. for that day along with the duplicate. The processor detects the
    conflict before the upsert, skips the duplicate with a warning, and keeps processing
    the rest of the day's data.
    """

    def test_duplicate_start_ts_with_different_activity_id_is_skipped(
        self, db_session: Session
    ):
        """
        A second activity with the same ``(user_id, start_ts)`` as a previously
        persisted activity (but a different ``activity_id``) must be dropped
        with a warning: ``_process_activity_base`` returns ``None``, no new row
        lands in the activity table, and the original row stays untouched.
        """
        _seed_activity(db_session, activity_id=12345)
        # Activity 12345 is now in the DB with
        # start_ts=2024-01-01T08:00:00Z, user_id=1, name="Morning Run".

        processor = GarminProcessor(FileSet(file_paths=[], files={}), db_session)
        processor.user_id = 1

        dup_payload = _minimal_activity_json(
            activity_id=99999,  # Distinct from the seeded 12345.
            start_time_iso="2024-01-01T08:00:00",  # Identical start_ts.
        )

        result = processor._process_activity_base(dup_payload, db_session)
        db_session.commit()

        # Skip signaled to caller.
        assert result is None
        # Only the original activity exists; the duplicate was not inserted.
        rows = (
            db_session.execute(select(Activity).order_by(Activity.activity_id))
            .scalars()
            .all()
        )
        assert [r.activity_id for r in rows] == [12345]
        # Original record untouched.
        assert rows[0].activity_name == "Morning Run"

    def test_fit_file_for_deduped_activity_is_skipped(
        self, db_session: Session, tmp_path
    ):
        """
        After ``_process_activity_base`` skips a duplicate, the per-activity file
        processors must also skip files for that ``activity_id`` instead of FK-failing
        on the missing parent row.

        Covers the FIT path: builds a
        correctly-named filename, registers the activity_id as skipped, and
        asserts ``_process_fit_file`` returns early without reading the file
        or hitting the DB (the file contents don't even need to be valid FIT
        because the skip check fires first).
        """
        processor = GarminProcessor(FileSet(file_paths=[], files={}), db_session)
        processor.user_id = 1
        processor._skipped_activity_ids.add(99999)

        # Filename pattern: <user_id>_ACTIVITY_<activity_id>_<timestamp>.fit
        fake_fit = tmp_path / "1_ACTIVITY_99999_2024-01-01T08-00-00Z.fit"
        fake_fit.write_bytes(b"not a real fit file")

        # No exception, no rows written, no Activity query executed.
        processor._process_fit_file(fake_fit, db_session)
        rows = db_session.execute(select(ActivityTsMetric)).scalars().all()
        assert rows == []

    def test_tcx_file_for_deduped_activity_is_skipped(
        self, db_session: Session, tmp_path
    ):
        """
        Same as the FIT case, for the TCX path.

        ``_process_tcx_file`` must skip files whose activity_id was deduped earlier in
        the same FileSet.
        """
        processor = GarminProcessor(FileSet(file_paths=[], files={}), db_session)
        processor.user_id = 1
        processor._skipped_activity_ids.add(99999)

        fake_tcx = tmp_path / "1_ACTIVITY_99999_2024-01-01T08-00-00Z.tcx"
        fake_tcx.write_bytes(b"<not><real></tcx>")

        processor._process_tcx_file(fake_tcx, db_session)
        rows = db_session.execute(select(ActivityTsMetric)).scalars().all()
        assert rows == []

    def test_exercise_sets_for_deduped_activity_is_skipped(
        self, db_session: Session, tmp_path
    ):
        """
        Same as the FIT/TCX cases, for the EXERCISE_SETS path.

        ``_process_exercise_sets`` reads activity_id from the JSON body (not the
        filename) but the skip-tracking logic is identical.
        """
        processor = GarminProcessor(FileSet(file_paths=[], files={}), db_session)
        processor.user_id = 1
        processor._skipped_activity_ids.add(99999)

        fake_es = tmp_path / "1_EXERCISE_SETS_99999_2024-01-01T08-00-00Z.json"
        fake_es.write_text(
            json.dumps(
                {
                    "activityId": 99999,
                    "exerciseSets": [{"messageIndex": 0, "setType": "ACTIVE"}],
                }
            )
        )

        processor._process_exercise_sets(fake_es, db_session)
        rows = db_session.execute(select(StrengthSet)).scalars().all()
        assert rows == []

    def test_reextract_same_activity_id_does_not_fire_dedup_query(
        self, db_session: Session
    ):
        """
        Idempotency guard: re-processing the same ``activity_id`` with the same
        ``start_ts`` (e.g. a user re-running ``garmin extract``) must NOT trip the
        duplicate path. Exercised at the dedup-query level: the existence query excludes
        rows with the same ``activity_id`` from the conflict set so the normal UPSERT-
        by-PK path runs unchanged.

        Tested directly via the same query the processor uses, rather than invoking the
        full ``_process_activity_base`` upsert path (which is already covered by
        ``TestActivityBaseUpsert``'s ``_seed_activity`` round-trip).
        """
        _seed_activity(db_session, activity_id=12345)

        same_id_query_result = db_session.execute(
            select(Activity.activity_id).where(
                Activity.user_id == 1,
                Activity.start_ts == datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
                Activity.activity_id != 12345,  # The exclusion under test.
            )
        ).scalar_one_or_none()

        # No "other" row exists; same activity_id is correctly excluded so the
        # dedup check would let the upsert proceed unchanged.
        assert same_id_query_result is None


# --- Sleep upsert tests ----------------------------------------------------


# --- Sleep orchestrator integration tests ----------------------------------


class TestProcessSleepOrchestrator:
    """
    Integration tests for the SLEEP processing pipeline against a real SQLite database.

    Regression coverage for #52: ``upsert_model_instances`` returns the input
    model_instances list rather than the rows persisted by SQLite, so an auto-increment
    primary key like ``sleep_id`` is never populated on the returned instance.
    ``_process_sleep_base`` therefore returned ``None`` and the orchestrator silently
    skipped every per-night detail extractor. These tests exercise the full end-to-end
    path that was previously untested.
    """

    @staticmethod
    def _seed_user(session: Session, user_id: int = 1) -> None:
        """
        Insert a User row required by the foreign key on sleep.user_id.

        :param session: SQLAlchemy Session bound to a real engine.
        :param user_id: User identifier to seed.
        """
        upsert_model_instances(
            session=session,
            model_instances=[User(user_id=user_id, full_name="Test User")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        session.commit()

    @staticmethod
    def _make_processor(user_id: int = 1) -> GarminProcessor:
        """
        Build a GarminProcessor pinned to the given user_id.

        :param user_id: User identifier the processor should attribute records to.
        :return: GarminProcessor instance with a dummy file set.
        """
        proc = GarminProcessor(
            file_set=FileSet(file_paths=[], files={}), session=MagicMock()
        )
        proc.user_id = user_id
        return proc

    @staticmethod
    def _minimal_sleep_payload() -> dict:
        """
        Build a SLEEP JSON containing one entry in each per-night detail array.

        :return: SLEEP-shaped dict with dailySleepDTO plus all six detail arrays.
        """
        return {
            "dailySleepDTO": {
                # 2025-01-02T00:00:00Z, 08:00:00Z, local == GMT for UTC offset 0.
                "sleepStartTimestampGMT": 1735776000000,
                "sleepEndTimestampGMT": 1735804800000,
                "sleepStartTimestampLocal": 1735776000000,
            },
            "sleepLevels": [
                {
                    "startGMT": "2025-01-02T00:00:00.0",
                    "endGMT": "2025-01-02T01:00:00.0",
                    "activityLevel": 1,
                },
            ],
            "sleepMovement": [
                {"startGMT": "2025-01-02T00:30:00.0", "activityLevel": 0.5},
            ],
            "sleepRestlessMoments": [
                {"startGMT": 1735777800000, "value": 3},
            ],
            "wellnessEpochSPO2DataDTOList": [
                {"epochTimestamp": "2025-01-02T01:00:00.0", "spo2Reading": 96},
            ],
            "hrvData": [
                {"startGMT": 1735779600000, "value": 50},
            ],
            "breathingDisruptionData": [
                {"startGMT": 1735781400000, "value": 1},
            ],
        }

    def test_process_sleep_base_returns_real_pk(self, db_session: Session):
        """
        ``_process_sleep_base`` must return the auto-generated sleep_id from the
        database after the upsert, not ``None`` (#52).

        :param db_session: Real SQLAlchemy Session against a temp SQLite DB.
        """
        self._seed_user(db_session)
        processor = self._make_processor()

        sleep_id = processor._process_sleep_base(
            self._minimal_sleep_payload(), db_session
        )
        db_session.commit()

        assert sleep_id is not None
        assert isinstance(sleep_id, int)
        row = db_session.execute(
            select(Sleep).where(Sleep.sleep_id == sleep_id)
        ).scalar_one()
        assert row.user_id == 1
        # SQLite strips tzinfo on read-back even with DateTime(timezone=True).
        assert row.start_ts.replace(tzinfo=timezone.utc) == datetime(
            2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc
        )

    def test_process_sleep_populates_detail_tables(
        self, db_session: Session, tmp_path: Path
    ):
        """
        ``_process_sleep`` must populate every per-night detail table when the SLEEP
        JSON contains the corresponding arrays (#52).

        :param db_session: Real SQLAlchemy Session against a temp SQLite DB.
        :param tmp_path: Pytest temp directory for the SLEEP JSON file.
        """
        self._seed_user(db_session)
        processor = self._make_processor()

        sleep_file = tmp_path / "1_SLEEP_2025-01-02.json"
        sleep_file.write_text(json.dumps(self._minimal_sleep_payload()))

        processor._process_sleep(sleep_file, db_session)
        db_session.commit()

        for model in (
            SleepLevel,
            SleepMovement,
            SleepRestlessMoment,
            SpO2,
            HRV,
            BreathingDisruption,
        ):
            count = db_session.scalar(select(func.count()).select_from(model))
            assert count == 1, (
                f"Expected 1 row in {model.__tablename__}, got {count}. "
                "Detail extractor was silently skipped."
            )

    def test_process_sleep_base_idempotent_returns_same_pk(self, db_session: Session):
        """
        Reprocessing the same SLEEP payload must return the existing sleep_id, so the
        second-pass detail extractors target the same parent row (idempotency for re-
        extraction of historical SLEEP JSONs from storage/, mentioned in the bug
        report's reprocessing notes).

        :param db_session: Real SQLAlchemy Session against a temp SQLite DB.
        """
        self._seed_user(db_session)
        processor = self._make_processor()

        first_id = processor._process_sleep_base(
            self._minimal_sleep_payload(), db_session
        )
        db_session.commit()
        second_id = processor._process_sleep_base(
            self._minimal_sleep_payload(), db_session
        )
        db_session.commit()

        assert first_id is not None
        assert first_id == second_id
        sleep_count = db_session.scalar(select(func.count()).select_from(Sleep))
        assert sleep_count == 1


# --- Sleep level tests ------------------------------------------------------


class TestProcessSleepLevel:
    """
    Tests for _process_sleep_level method.
    """

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_process_sleep_level(self, mock_upsert, processor, mock_session):
        """
        Test _process_sleep_level method.

        Verifies that sleepLevels intervals are converted to SleepLevel ORM instances
        with the correct UTC timestamps and stage labels, that the upsert is called with
        insert-or-ignore semantics on (sleep_id, start_ts), and that intervals with
        unknown stage codes are skipped.

        :param mock_upsert: Mock upsert function.
        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange.
        data = {
            "sleepLevels": [
                {
                    "startGMT": "2022-01-01T00:00:00.0",
                    "endGMT": "2022-01-01T01:00:00.0",
                    "activityLevel": 1,  # LIGHT.
                },
                {
                    "startGMT": "2022-01-01T01:00:00.0",
                    "endGMT": "2022-01-01T01:30:00.0",
                    "activityLevel": 0,  # DEEP.
                },
                {
                    "startGMT": "2022-01-01T01:30:00.0",
                    "endGMT": "2022-01-01T02:00:00.0",
                    "activityLevel": 2,  # REM.
                },
                {
                    "startGMT": "2022-01-01T02:00:00.0",
                    "endGMT": "2022-01-01T02:15:00.0",
                    "activityLevel": 3,  # AWAKE.
                },
                {
                    # Unknown code: should be skipped without raising.
                    "startGMT": "2022-01-01T02:15:00.0",
                    "endGMT": "2022-01-01T02:30:00.0",
                    "activityLevel": 99,
                },
            ]
        }

        # Act.
        processor._process_sleep_level(data, 123456, mock_session)

        # Assert: upsert called once with insert-or-ignore semantics.
        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["session"] == mock_session
        assert kwargs["conflict_columns"] == ["sleep_id", "start_ts"]
        assert kwargs["on_conflict_update"] is False

        # Four valid intervals (the unknown code was dropped).
        records = kwargs["model_instances"]
        assert len(records) == 4
        assert all(isinstance(rec, SleepLevel) for rec in records)

        # Verify field mapping for the first record (LIGHT).
        first = records[0]
        assert first.sleep_id == 123456
        assert first.start_ts == datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert first.end_ts == datetime(2022, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert first.stage == 1
        assert first.stage_label == "LIGHT"

        # Verify all stage labels in order.
        assert [rec.stage_label for rec in records] == [
            "LIGHT",
            "DEEP",
            "REM",
            "AWAKE",
        ]

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_process_sleep_level_empty(self, mock_upsert, processor, mock_session):
        """
        Test _process_sleep_level with no sleepLevels in payload.

        Should return early without calling upsert.

        :param mock_upsert: Mock upsert function.
        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange: payload with no sleepLevels key.
        data = {}
        # Act.
        processor._process_sleep_level(data, 123456, mock_session)

        # Assert.
        mock_upsert.assert_not_called()

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_process_sleep_level_real_garmin_format(
        self, mock_upsert, processor, mock_session
    ):
        """
        Regression test for Python 3.10 ``datetime.fromisoformat`` compatibility.

        Garmin Connect returns timestamps with a single-digit fractional second (e.g.
        ``"2026-04-06T05:47:59.0"``) which Python 3.10 cannot parse natively. This
        exercises the production format end-to-end via the
        :meth:`GarminProcessor._parse_garmin_gmt` helper to ensure the path stays green
        on every supported Python.

        :param mock_upsert: Mock upsert function.
        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange: real Garmin sleepLevels format with .0 fractional second.
        data = {
            "sleepLevels": [
                {
                    "startGMT": "2026-04-06T05:47:59.0",
                    "endGMT": "2026-04-06T05:48:59.0",
                    "activityLevel": 1.0,
                },
            ]
        }

        # Act.
        processor._process_sleep_level(data, 999, mock_session)

        # Assert: parsed correctly and tagged as UTC.
        records = mock_upsert.call_args.kwargs["model_instances"]
        assert len(records) == 1
        assert records[0].start_ts == datetime(
            2026, 4, 6, 5, 47, 59, tzinfo=timezone.utc
        )
        assert records[0].end_ts == datetime(2026, 4, 6, 5, 48, 59, tzinfo=timezone.utc)

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_process_sleep_level_all_invalid(
        self, mock_upsert, processor, mock_session
    ):
        """
        Test _process_sleep_level when every interval has an unknown stage code.

        Should log and return without calling upsert (no spurious empty insert).

        :param mock_upsert: Mock upsert function.
        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange: payload with only unknown stage codes.
        data = {
            "sleepLevels": [
                {
                    "startGMT": "2022-01-01T00:00:00.0",
                    "endGMT": "2022-01-01T01:00:00.0",
                    "activityLevel": 99,
                },
                {
                    "startGMT": "2022-01-01T01:00:00.0",
                    "endGMT": "2022-01-01T02:00:00.0",
                    "activityLevel": 100,
                },
            ]
        }

        # Act.
        processor._process_sleep_level(data, 123456, mock_session)

        # Assert.
        mock_upsert.assert_not_called()


class TestParseGarminIso:
    """
    Tests for ``GarminProcessor._parse_garmin_iso`` and ``_parse_garmin_gmt``.

    These helpers exist because Python 3.10's strict ``datetime.fromisoformat`` rejects
    Garmin's single-digit fractional second format. The class is the central regression
    test for that compatibility shim.
    """

    @pytest.mark.parametrize(
        "ts_str, expected",
        [
            # Garmin's real-world format: single-digit fractional second.
            ("2026-04-06T05:47:59.0", datetime(2026, 4, 6, 5, 47, 59)),
            # No fractional component at all.
            ("2026-04-06T05:47:59", datetime(2026, 4, 6, 5, 47, 59)),
            # Six-digit fractional (already isoformat-canonical).
            ("2026-04-06T05:47:59.123456", datetime(2026, 4, 6, 5, 47, 59, 123456)),
            # Three-digit fractional (millisecond precision).
            ("2026-04-06T05:47:59.500", datetime(2026, 4, 6, 5, 47, 59, 500000)),
            # Trailing Z suffix gets stripped.
            ("2026-04-06T05:47:59.0Z", datetime(2026, 4, 6, 5, 47, 59)),
            # Z with no fractional.
            ("2026-04-06T05:47:59Z", datetime(2026, 4, 6, 5, 47, 59)),
            # Explicit +00:00 offset behaves like Z (same wall clock).
            ("2026-04-06T05:47:59.0+00:00", datetime(2026, 4, 6, 5, 47, 59)),
            # Non-zero offset gets converted to UTC before tzinfo is dropped.
            ("2026-04-06T05:47:59.0+05:30", datetime(2026, 4, 6, 0, 17, 59)),
            # Negative offset converts the other way.
            ("2026-04-06T05:47:59-08:00", datetime(2026, 4, 6, 13, 47, 59)),
        ],
    )
    def test_parse_garmin_iso(self, ts_str, expected):
        """
        Parse a variety of Garmin ISO timestamp shapes into naive datetimes.

        :param ts_str: Input timestamp string.
        :param expected: Expected naive datetime.
        """
        result = GarminProcessor._parse_garmin_iso(ts_str)
        assert result == expected
        assert result.tzinfo is None

    def test_parse_garmin_gmt_tags_utc(self):
        """
        ``_parse_garmin_gmt`` should return the same wall clock as ``_parse_garmin_iso``
        but tagged with UTC timezone info.
        """
        result = GarminProcessor._parse_garmin_gmt("2026-04-06T05:47:59.0")
        assert result == datetime(2026, 4, 6, 5, 47, 59, tzinfo=timezone.utc)
        assert result.tzinfo == timezone.utc


# --- Strength training tests ------------------------------------------------


class TestProcessStrengthMetrics:
    """
    Tests for _process_strength_metrics method.
    """

    def test_field_mapping_and_pop_behavior(self, processor, mock_session) -> None:
        """
        Test field mapping and pop behavior.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange.
        activity_data = {
            "summarizedExerciseSets": [
                {
                    "category": "BENCH_PRESS",
                    "subCategory": "BARBELL_BENCH_PRESS",
                    "sets": 3,
                    "reps": 30,
                    "volume": 13500000.0,
                    "duration": 180000.0,
                    "maxWeight": 50000.0,
                },
                {
                    "category": "CURL",
                    "subCategory": "DUMBBELL_CURL",
                    "sets": 3,
                    "reps": 24,
                    "volume": 4320000.0,
                    "duration": 120000.0,
                    "maxWeight": 20000.0,
                },
            ],
            "totalSets": 6,
            "activeSets": 6,
            "totalReps": 54,
            "otherField": 42,
        }
        activity_id = 22320029355

        # Act.
        processor._process_strength_metrics(activity_data, activity_id, mock_session)

        # Assert - fields were popped.
        assert "summarizedExerciseSets" not in activity_data
        assert "totalSets" not in activity_data
        assert "activeSets" not in activity_data
        assert "totalReps" not in activity_data
        assert "otherField" in activity_data

        # Verify delete targets StrengthExercise for the correct activity_id.
        expected_delete = delete(StrengthExercise).where(
            StrengthExercise.activity_id == activity_id
        )
        delete_calls = [
            call
            for call in mock_session.execute.call_args_list
            if hasattr(call.args[0], "is_delete") and call.args[0].is_delete
        ]
        assert len(delete_calls) == 1
        stmt = delete_calls[0].args[0]
        assert stmt.table.name == StrengthExercise.__tablename__
        assert stmt.whereclause.compare(expected_delete.whereclause)

        # Assert - records were added.
        mock_session.add_all.assert_called_once()
        records = mock_session.add_all.call_args[0][0]
        assert len(records) == 2
        assert all(isinstance(r, StrengthExercise) for r in records)

        # Verify field mapping.
        bench = records[0]
        assert bench.activity_id == activity_id
        assert bench.exercise_category == "BENCH_PRESS"
        assert bench.exercise_name == "BARBELL_BENCH_PRESS"
        assert bench.sets == 3
        assert bench.reps == 30
        assert bench.volume == 13500000.0
        assert bench.duration_ms == 180000.0
        assert bench.max_weight == 50000.0

    def test_skips_missing_pk_fields(self, processor, mock_session) -> None:
        """
        Test that exercises missing category or name are skipped.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange.
        activity_data = {
            "summarizedExerciseSets": [
                {
                    "category": "BENCH_PRESS",
                    "subCategory": "BARBELL_BENCH_PRESS",
                    "sets": 3,
                    "reps": 30,
                },
                {
                    "category": None,
                    "subCategory": "UNKNOWN_EXERCISE",
                    "sets": 1,
                    "reps": 5,
                },
                {
                    "category": "CURL",
                    "sets": 2,
                    "reps": 10,
                },
            ],
            "totalSets": 6,
            "activeSets": 6,
            "totalReps": 45,
        }

        # Act.
        processor._process_strength_metrics(activity_data, 12345, mock_session)

        # Assert - only 1 valid record.
        mock_session.add_all.assert_called_once()
        records = mock_session.add_all.call_args[0][0]
        assert len(records) == 1
        assert records[0].exercise_category == "BENCH_PRESS"

    def test_empty_sets(self, processor, mock_session) -> None:
        """
        Test with no summarized exercise sets.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange.
        activity_data = {
            "totalSets": 0,
            "activeSets": 0,
            "totalReps": 0,
        }
        # Act.
        processor._process_strength_metrics(activity_data, 12345, mock_session)

        # Assert - scalars were still popped.
        assert "totalSets" not in activity_data
        assert "activeSets" not in activity_data
        assert "totalReps" not in activity_data

        # Verify delete targets StrengthExercise for the correct activity_id.
        expected_delete = delete(StrengthExercise).where(
            StrengthExercise.activity_id == 12345
        )
        delete_calls = [
            call
            for call in mock_session.execute.call_args_list
            if hasattr(call.args[0], "is_delete") and call.args[0].is_delete
        ]
        assert len(delete_calls) == 1
        stmt = delete_calls[0].args[0]
        assert stmt.table.name == StrengthExercise.__tablename__
        assert stmt.whereclause.compare(expected_delete.whereclause)

        # Assert - no insert since sets are empty.
        mock_session.add_all.assert_not_called()


class TestProcessExerciseSets:
    """
    Tests for _process_exercise_sets method.
    """

    def test_field_mapping_and_ml_selection(
        self, processor, mock_session, tmp_path
    ) -> None:
        """
        Test field mapping and ML exercise selection.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        :param tmp_path: Temporary directory fixture.
        """
        # Arrange.
        data = {
            "activityId": 22320029355,
            "exerciseSets": [
                {
                    "messageIndex": 0,
                    "setType": "ACTIVE",
                    "startTime": "2025-03-27T10:00:00",
                    "duration": 45.0,
                    "wktStepIndex": None,
                    "repetitionCount": 10,
                    "weight": 452000.0,
                    "exercises": [
                        {
                            "category": "BENCH_PRESS",
                            "name": ("BARBELL_BENCH_PRESS"),
                            "probability": 0.98,
                        },
                        {
                            "category": "SHOULDER_PRESS",
                            "name": "OVERHEAD_PRESS",
                            "probability": 0.02,
                        },
                    ],
                },
                {
                    "messageIndex": 1,
                    "setType": "REST",
                    "startTime": ("2025-03-27T10:00:45"),
                    "duration": 90.0,
                    "wktStepIndex": None,
                    "repetitionCount": None,
                    "weight": None,
                    "exercises": [],
                },
                {
                    "messageIndex": 2,
                    "setType": "ACTIVE",
                    "startTime": ("2025-03-27T10:02:15"),
                    "duration": 50.0,
                    "wktStepIndex": None,
                    "repetitionCount": 8,
                    "weight": 452000.0,
                    "exercises": [
                        {
                            "category": "BENCH_PRESS",
                            "name": ("BARBELL_BENCH_PRESS"),
                            "probability": 0.95,
                        },
                    ],
                },
            ],
        }

        file_name = "123_EXERCISE_SETS_22320029355_2025-03-27.json"
        file_path = tmp_path / file_name
        with open(file_path, "w") as f:
            json.dump(data, f)

        # Act.
        processor._process_exercise_sets(file_path, mock_session)

        # Verify delete targets StrengthSet for the correct activity_id.
        expected_delete = delete(StrengthSet).where(
            StrengthSet.activity_id == 22320029355
        )
        delete_calls = [
            call
            for call in mock_session.execute.call_args_list
            if hasattr(call.args[0], "is_delete") and call.args[0].is_delete
        ]
        assert len(delete_calls) == 1
        stmt = delete_calls[0].args[0]
        assert stmt.table.name == StrengthSet.__tablename__
        assert stmt.whereclause.compare(expected_delete.whereclause)

        # Assert - records were added.
        mock_session.add_all.assert_called_once()
        records = mock_session.add_all.call_args[0][0]
        assert len(records) == 3
        assert all(isinstance(r, StrengthSet) for r in records)

        # Verify ACTIVE set field mapping.
        active_set = records[0]
        assert active_set.activity_id == 22320029355
        assert active_set.set_idx == 0
        assert active_set.set_type == "ACTIVE"
        assert active_set.start_time == datetime(
            2025, 3, 27, 10, 0, 0, tzinfo=timezone.utc
        )
        assert active_set.duration == 45.0
        assert active_set.wkt_step_index is None
        assert active_set.repetition_count == 10
        assert active_set.weight == 452000.0
        assert active_set.exercise_category == "BENCH_PRESS"
        assert active_set.exercise_name == "BARBELL_BENCH_PRESS"
        assert active_set.exercise_probability == 0.98

        # Verify REST set (no exercises).
        rest_set = records[1]
        assert rest_set.set_type == "REST"
        assert rest_set.exercise_category is None
        assert rest_set.exercise_name is None
        assert rest_set.exercise_probability is None

    def test_skips_null_message_index(self, processor, mock_session, tmp_path) -> None:
        """
        Test that sets with null messageIndex are skipped.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        :param tmp_path: Temporary directory fixture.
        """
        # Arrange.
        data = {
            "activityId": 22320029355,
            "exerciseSets": [
                {
                    "messageIndex": 0,
                    "setType": "ACTIVE",
                    "duration": 45.0,
                    "repetitionCount": 10,
                    "weight": 452000.0,
                    "exercises": [],
                },
                {
                    "messageIndex": None,
                    "setType": "ACTIVE",
                    "duration": 30.0,
                    "repetitionCount": 5,
                    "weight": 200000.0,
                    "exercises": [],
                },
            ],
        }

        file_name = "123_EXERCISE_SETS_22320029355_2025-03-27.json"
        file_path = tmp_path / file_name
        with open(file_path, "w") as f:
            json.dump(data, f)

        # Act.
        processor._process_exercise_sets(file_path, mock_session)

        # Assert - only 1 record (null messageIndex
        # skipped).
        mock_session.add_all.assert_called_once()
        records = mock_session.add_all.call_args[0][0]
        assert len(records) == 1
        assert records[0].set_idx == 0

    def test_empty_exercise_sets(self, processor, mock_session, tmp_path) -> None:
        """
        Test with empty exercise sets.

        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        :param tmp_path: Temporary directory fixture.
        """
        # Arrange.
        data = {
            "activityId": 12345,
            "exerciseSets": None,
        }
        file_name = "123_EXERCISE_SETS_12345_2025-03-27.json"
        file_path = tmp_path / file_name
        with open(file_path, "w") as f:
            json.dump(data, f)

        # Act.
        processor._process_exercise_sets(file_path, mock_session)

        # Verify delete targets StrengthSet for the correct activity_id.
        expected_delete = delete(StrengthSet).where(StrengthSet.activity_id == 12345)
        delete_calls = [
            call
            for call in mock_session.execute.call_args_list
            if hasattr(call.args[0], "is_delete") and call.args[0].is_delete
        ]
        assert len(delete_calls) == 1
        stmt = delete_calls[0].args[0]
        assert stmt.table.name == StrengthSet.__tablename__
        assert stmt.whereclause.compare(expected_delete.whereclause)
        mock_session.add_all.assert_not_called()


class TestStrengthRouting:
    """
    Tests for strength training activity routing.
    """

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_strength_training_routes_to_processor(
        self, mock_upsert, processor, mock_session
    ) -> None:
        """
        Test that strength_training activities route correctly.

        :param mock_upsert: Mock upsert function.
        :param processor: GarminProcessor fixture.
        :param mock_session: Mock session fixture.
        """
        # Arrange.
        activity_data = {
            "activityId": 22320029355,
            "activityType": {
                "typeId": 71,
                "typeKey": "strength_training",
            },
            "eventType": {
                "typeId": 1,
                "typeKey": "training",
            },
            "startTimeGMT": "2025-03-27T10:00:00.000",
            "startTimeLocal": "2025-03-27T11:00:00.000",
            "endTimeGMT": "2025-03-27T11:00:00.000",
            "duration": 3600.0,
            "parent": False,
            "purposeful": True,
            "favorite": False,
            "pr": False,
            "hasPolyline": False,
            "hasImages": False,
            "hasVideo": False,
            "hasHeatMap": False,
            "manualActivity": False,
            "autoCalcCalories": True,
            "summarizedExerciseSets": [
                {
                    "category": "SQUAT",
                    "subCategory": "BARBELL_SQUAT",
                    "sets": 3,
                    "reps": 15,
                    "volume": 9000000.0,
                    "duration": 90000.0,
                    "maxWeight": 60000.0,
                },
            ],
            "totalSets": 3,
            "activeSets": 3,
            "totalReps": 15,
        }

        # Mock upsert to return a persisted activity.
        mock_activity = MagicMock()
        mock_activity.activity_id = 22320029355
        mock_upsert.return_value = [mock_activity]

        # Act.
        with patch.object(processor, "_process_strength_metrics") as mock_strength:
            processor._process_single_activity(
                copy.deepcopy(activity_data),
                mock_session,
            )

        # Assert.
        mock_strength.assert_called_once()


# --------------------------------------------------------------------------------------
# Sub-second timestamp precision and dedup tests for _process_fit_file
# --------------------------------------------------------------------------------------


class TestProcessFitSubSecond:
    """
    Cover the FIT record-frame timestamp precision and duplicate-coalescing fixes.
    """

    def _make_processor(self) -> GarminProcessor:
        """
        Build a minimal processor instance bound to FIT_FILENAME.
        """
        file_set = MagicMock(spec=FileSet)
        return GarminProcessor(file_set=file_set, session=MagicMock())

    def test_fractional_timestamp_preserves_subsecond_precision(
        self, db_session: Session
    ):
        """
        Two record frames with the same `timestamp` but distinct `fractional_timestamp`
        values produce two distinct rows with sub-second precision (no UNIQUE constraint
        collision).
        """
        _seed_activity(db_session)

        ts = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        frame_a = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("fractional_timestamp", 0.0, "s"),
                _make_field("heart_rate", 150, "bpm"),
            ],
        )
        frame_b = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("fractional_timestamp", 0.5, "s"),
                _make_field("heart_rate", 152, "bpm"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader([frame_a, frame_b])
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        rows = (
            db_session.execute(
                select(ActivityTsMetric).where(ActivityTsMetric.name == "heart_rate")
            )
            .scalars()
            .all()
        )
        assert len(rows) == 2
        timestamps = sorted(r.timestamp for r in rows)
        # 500 ms apart, both stored with microsecond precision.
        assert (timestamps[1] - timestamps[0]).total_seconds() == pytest.approx(0.5)

    def test_duplicate_records_coalesced_by_timestamp_and_name(
        self, db_session: Session
    ):
        """
        Two record frames at the same effective timestamp (no fractional_timestamp
        present) collapse into a single row whose value is the last-seen one.

        Prevents UNIQUE constraint failure (issue #36).
        """
        _seed_activity(db_session)

        ts = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        frame_a = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("heart_rate", 150, "bpm"),
            ],
        )
        frame_b = _make_frame(
            "record",
            [
                _make_field("timestamp", ts),
                _make_field("heart_rate", 152, "bpm"),
            ],
        )

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = _mock_fit_reader([frame_a, frame_b])
            processor._process_fit_file(Path(FIT_FILENAME), db_session)

        db_session.commit()

        rows = (
            db_session.execute(
                select(ActivityTsMetric).where(ActivityTsMetric.name == "heart_rate")
            )
            .scalars()
            .all()
        )
        # Coalesced to one row; last value wins.
        assert len(rows) == 1
        assert rows[0].value == 152.0


# --- Body composition tests -------------------------------------------------


class TestProcessBodyComposition:
    """
    Tests for _process_body_composition method.
    """

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_inserts_records_with_field_mapping(
        self, mock_upsert, processor, mock_session, tmp_path
    ):
        """
        Verify each ``dateWeightList`` entry is mapped to a BodyComposition record with
        insert-or-ignore semantics on (user_id, timestamp).
        """
        # Arrange: one fully-populated INDEX_SCALE entry plus one partial MANUAL
        # entry to confirm null fields propagate through .get().
        # Timestamps: 1714564800000 ms = 2024-05-01 12:00 UTC,
        #             1714651200000 ms = 2024-05-02 12:00 UTC.
        data = {
            "startDate": "2024-05-01",
            "endDate": "2024-05-01",
            "dateWeightList": [
                {
                    "samplePk": 1714564800000,
                    "date": 1714564800000,
                    "calendarDate": "2024-05-01",
                    "timestampGMT": 1714564800000,
                    "weight": 75300.0,
                    "bmi": 24.5,
                    "bodyFat": 22.5,
                    "bodyWater": 55.2,
                    "boneMass": 3500.0,
                    "muscleMass": 60000.0,
                    "physiqueRating": 5,
                    "visceralFat": 8,
                    "metabolicAge": 30,
                    "sourceType": "INDEX_SCALE",
                },
                {
                    "timestampGMT": 1714651200000,
                    "weight": 75100.0,
                    "sourceType": "MANUAL",
                },
            ],
        }
        file_path = tmp_path / "123_BODY_COMPOSITION_2025-05-01T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        # Act.
        processor._process_body_composition(file_path, mock_session)

        # Assert: upsert called once with insert-or-ignore semantics.
        mock_upsert.assert_called_once()
        kwargs = mock_upsert.call_args.kwargs
        assert kwargs["session"] == mock_session
        assert kwargs["conflict_columns"] == ["user_id", "timestamp"]
        assert kwargs["on_conflict_update"] is False

        records = kwargs["model_instances"]
        assert len(records) == 2
        assert all(isinstance(r, BodyComposition) for r in records)

        first = records[0]
        assert first.user_id == 123456789
        assert first.timestamp == datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert first.weight == 75300.0
        assert first.bmi == 24.5
        assert first.body_fat == 22.5
        assert first.body_water == 55.2
        assert first.bone_mass == 3500.0
        assert first.muscle_mass == 60000.0
        assert first.physique_rating == 5
        assert first.visceral_fat == 8
        assert first.metabolic_age == 30
        assert first.source_type == "INDEX_SCALE"
        assert first.sample_pk == 1714564800000

        second = records[1]
        assert second.weight == 75100.0
        assert second.source_type == "MANUAL"
        assert second.bmi is None
        assert second.body_fat is None
        assert second.sample_pk is None

    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_empty_date_weight_list_is_noop(
        self, mock_upsert, processor, mock_session, tmp_path
    ):
        """
        Days with no weigh-in return an empty ``dateWeightList`` and must not call
        upsert.
        """
        data = {"dateWeightList": [], "totalAverage": {}}
        file_path = tmp_path / "123_BODY_COMPOSITION_2025-05-01T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        processor._process_body_composition(file_path, mock_session)

        mock_upsert.assert_not_called()

    @patch("garmin_health_data.processor.click.secho")
    @patch("garmin_health_data.processor.upsert_model_instances")
    def test_falls_back_to_date_when_timestamp_gmt_missing(
        self,
        mock_upsert,
        mock_secho,
        processor,
        mock_session,
        tmp_path,
    ):
        """
        Some payloads omit ``timestampGMT``; ``date`` is the fallback.

        Entries with neither are skipped with a yellow warning.
        """
        data = {
            "dateWeightList": [
                {"date": 1714564800000, "weight": 75000.0},
                {"weight": 76000.0},  # No timestamp at all -- skipped.
            ],
        }
        file_path = tmp_path / "123_BODY_COMPOSITION_2025-05-01T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        processor._process_body_composition(file_path, mock_session)

        mock_upsert.assert_called_once()
        records = mock_upsert.call_args.kwargs["model_instances"]
        assert len(records) == 1
        assert records[0].weight == 75000.0
        assert records[0].timestamp == datetime(
            2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc
        )

        # Verify a yellow warning surfaced for the entry with no timestamp.
        warnings = [
            call for call in mock_secho.call_args_list if "no timestamp" in call.args[0]
        ]
        assert len(warnings) == 1
        assert warnings[0].kwargs.get("fg") == "yellow"

    def test_persists_to_real_database(self, db_session: Session, tmp_path):
        """
        End-to-end: write a real JSON file, run the processor against an in-memory
        SQLite DB, and verify the rows landed with correct field mapping.
        """
        # Arrange: seed user (FK target).
        upsert_model_instances(
            session=db_session,
            model_instances=[User(user_id=999, full_name="Test")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        db_session.commit()

        data = {
            "dateWeightList": [
                {
                    "timestampGMT": 1714564800000,
                    "weight": 75300.0,
                    "bmi": 24.5,
                    "bodyFat": 22.5,
                    "boneMass": 3500.0,
                    "muscleMass": 60000.0,
                    "sourceType": "INDEX_SCALE",
                }
            ],
        }
        file_path = tmp_path / "999_BODY_COMPOSITION_2025-05-01T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        proc = GarminProcessor(FileSet(file_paths=[], files={}), MagicMock())
        proc.user_id = 999

        # Act.
        proc._process_body_composition(file_path, db_session)
        db_session.commit()

        # Assert.
        rows = db_session.execute(select(BodyComposition)).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.user_id == 999
        # SQLite drops tzinfo on DateTime(timezone=True) round-trip; compare naive.
        assert row.timestamp.replace(tzinfo=timezone.utc) == datetime(
            2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc
        )
        assert row.weight == 75300.0
        assert row.body_fat == 22.5
        assert row.muscle_mass == 60000.0
        assert row.source_type == "INDEX_SCALE"

    def test_reprocess_is_idempotent(self, db_session: Session, tmp_path):
        """
        Re-running the processor on the same payload must not duplicate or modify
        rows -- ON CONFLICT DO NOTHING semantics.
        """
        upsert_model_instances(
            session=db_session,
            model_instances=[User(user_id=999, full_name="Test")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        db_session.commit()

        data = {
            "dateWeightList": [
                {"timestampGMT": 1714564800000, "weight": 75300.0},
            ],
        }
        file_path = tmp_path / "999_BODY_COMPOSITION_2025-05-01T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        proc = GarminProcessor(FileSet(file_paths=[], files={}), MagicMock())
        proc.user_id = 999

        proc._process_body_composition(file_path, db_session)
        db_session.commit()
        proc._process_body_composition(file_path, db_session)
        db_session.commit()

        rows = db_session.execute(select(BodyComposition)).scalars().all()
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# Menstrual cycle tests ----------------------------------------------------
# ---------------------------------------------------------------------------


def _menstrual_day_payload(
    symptoms=None,
    moods=None,
    discharge=None,
    phase=1,
    flow="HEAVY",
    notes="test",
    ovulation_day=True,
    report_ts="2026-05-25T18:34:49.9",
    calendar_date="2026-05-25",
    cycle_start_date="2026-05-23",
):
    """
    Build a representative MENSTRUAL_CYCLE_DAY JSON payload (probe-shaped).
    """
    return {
        "daySummary": {
            "startDate": cycle_start_date,
            "dayInCycle": 3,
            "periodLength": 3,
            "currentPhase": phase,
            "lengthOfCurrentPhase": 3,
            "daysUntilNextPhase": 1,
            "predictedCycleLength": 28,
            "cycleType": "REGULAR",
            "predictedCycle": False,
        },
        "dayLog": {
            "userProfilePk": 999,
            "calendarDate": calendar_date,
            "symptoms": symptoms if symptoms is not None else ["BACKACHE", "CRAMPS"],
            "moods": moods if moods is not None else ["HAPPY"],
            "discharge": discharge if discharge is not None else ["CREAMY"],
            "flow": flow,
            "sexDrive": "AVERAGE",
            "sexualActivity": "PROTECTED",
            "notes": notes,
            "reportTimestamp": report_ts,
            "hasBabyMovement": False,
            "ovulationDay": ovulation_day,
        },
    }


def _seed_user_999(db_session: Session) -> None:
    """
    Seed user_id=999 so menstrual_cycle_day FK targets exist.
    """
    upsert_model_instances(
        session=db_session,
        model_instances=[User(user_id=999, full_name="Test")],
        conflict_columns=["user_id"],
        on_conflict_update=True,
    )
    db_session.commit()


def _menstrual_processor(user_id=999) -> GarminProcessor:
    """
    Build a GarminProcessor wired with the test user_id.
    """
    proc = GarminProcessor(FileSet(file_paths=[], files={}), MagicMock())
    proc.user_id = user_id
    return proc


class TestProcessMenstrualCycleDay:
    """
    Tests for _process_menstrual_cycle_day covering scalar upsert and the critical
    delete-then-reinsert semantics for tag-shaped fields (symptoms, moods, discharge),
    which let user-removed tags propagate on reprocess.
    """

    def test_insert_day_and_tags(self, db_session: Session, tmp_path):
        """
        First-time insert populates the day row and all three tag kinds.
        """
        _seed_user_999(db_session)

        data = _menstrual_day_payload()
        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        day = db_session.execute(select(MenstrualCycleDay)).scalars().one()
        assert day.user_id == 999
        assert str(day.date) == "2026-05-25"
        assert day.current_phase == "MENSTRUAL"
        assert day.day_in_cycle == 3
        assert day.predicted_cycle is False
        assert day.flow == "HEAVY"
        assert day.notes == "test"
        assert day.ovulation_day is True

        tags = db_session.execute(select(MenstrualCycleTag)).scalars().all()
        # 2 symptoms + 1 mood + 1 discharge = 4.
        assert len(tags) == 4
        by_kind = {(t.kind, t.name) for t in tags}
        assert ("SYMPTOM", "BACKACHE") in by_kind
        assert ("SYMPTOM", "CRAMPS") in by_kind
        assert ("MOOD", "HAPPY") in by_kind
        assert ("DISCHARGE", "CREAMY") in by_kind

    def test_reprocess_updates_scalars(self, db_session: Session, tmp_path):
        """
        UPSERT semantics: reprocessing the same day with edited scalars overwrites the
        existing row in place rather than appending a new one.
        """
        _seed_user_999(db_session)

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(_menstrual_day_payload(flow="LIGHT", notes="initial"), f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        # User edits the day on Garmin Connect; rewrite the file with new values.
        with open(file_path, "w") as f:
            json.dump(_menstrual_day_payload(flow="HEAVY", notes="edited"), f)

        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        rows = db_session.execute(select(MenstrualCycleDay)).scalars().all()
        assert len(rows) == 1
        assert rows[0].flow == "HEAVY"
        assert rows[0].notes == "edited"

    def test_tag_removal_propagates(self, db_session: Session, tmp_path):
        """
        Critical scenario: a user removes a previously-logged symptom on Garmin Connect.

        The next extract must reflect the removal, not leave an orphan row. Validates
        the delete-then-reinsert pattern.
        """
        _seed_user_999(db_session)

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        # Start with three symptoms.
        with open(file_path, "w") as f:
            json.dump(
                _menstrual_day_payload(
                    symptoms=["BACKACHE", "CRAMPS", "HEADACHE"],
                    moods=["HAPPY"],
                    discharge=["CREAMY"],
                ),
                f,
            )

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        # User removes two symptoms and the mood; only BACKACHE remains.
        with open(file_path, "w") as f:
            json.dump(
                _menstrual_day_payload(
                    symptoms=["BACKACHE"], moods=[], discharge=["CREAMY"]
                ),
                f,
            )
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        tags = db_session.execute(select(MenstrualCycleTag)).scalars().all()
        by_kind = {(t.kind, t.name) for t in tags}
        assert by_kind == {("SYMPTOM", "BACKACHE"), ("DISCHARGE", "CREAMY")}

    def test_phase_label_mapping(self, db_session: Session, tmp_path):
        """
        Integer currentPhase translates to the denormalized text label.
        """
        _seed_user_999(db_session)

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(_menstrual_day_payload(phase=3), f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        day = db_session.execute(select(MenstrualCycleDay)).scalars().one()
        assert day.current_phase == "OVULATORY"

    def test_unknown_phase_label_stored_with_fallback(
        self, db_session: Session, tmp_path
    ):
        """
        Defensive: unknown phase integers are stored as 'UNKNOWN_<n>' rather than
        crashing the processor.
        """
        _seed_user_999(db_session)

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(_menstrual_day_payload(phase=99), f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        day = db_session.execute(select(MenstrualCycleDay)).scalars().one()
        assert day.current_phase == "UNKNOWN_99"

    def test_deleting_day_cascades_to_tags(self, db_session: Session, tmp_path):
        """
        FK with ON DELETE CASCADE means removing the parent day row removes its tag
        children automatically.
        """
        _seed_user_999(db_session)

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(_menstrual_day_payload(), f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        assert (
            db_session.execute(select(func.count(MenstrualCycleTag.name))).scalar() > 0
        )

        day = db_session.execute(select(MenstrualCycleDay)).scalars().one()
        db_session.delete(day)
        db_session.commit()

        assert (
            db_session.execute(select(func.count(MenstrualCycleTag.name))).scalar() == 0
        )

    def test_non_string_tag_entries_skipped(self, db_session: Session, tmp_path):
        """
        Defensive: if Garmin ever enriches the tag list shape (e.g. to dicts with a
        severity field), the processor must skip non-string entries with a warning
        rather than stringifying them into the ``name`` column.

        Valid string entries in the same list still land.
        """
        _seed_user_999(db_session)

        payload = _menstrual_day_payload(
            symptoms=["BACKACHE", {"name": "CRAMPS", "severity": "MILD"}, None],
            moods=["HAPPY"],
            discharge=[],
        )
        file_path = tmp_path / "999_MENSTRUAL_CYCLE_DAY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(payload, f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_day(file_path, db_session)
        db_session.commit()

        tags = db_session.execute(select(MenstrualCycleTag)).scalars().all()
        by_kind = {(t.kind, t.name) for t in tags}
        # Dict and None are skipped; BACKACHE and HAPPY survive.
        assert by_kind == {("SYMPTOM", "BACKACHE"), ("MOOD", "HAPPY")}


class TestProcessMenstrualCycleSummary:
    """
    Tests for _process_menstrual_cycle_summary covering the wipe-and-replace policy for
    predicted cycles alongside upsert-by-PK for observed (real) cycles.
    """

    def test_insert_observed_and_predicted(self, db_session: Session, tmp_path):
        """
        First-time insert populates both observed and predicted summary rows.
        """
        _seed_user_999(db_session)

        data = {
            "cycleSummaries": [
                {"startDate": "2026-05-23", "periodLength": 3, "predictedCycle": False},
                {"startDate": "2026-06-20", "periodLength": 5, "predictedCycle": True},
                {"startDate": "2026-07-18", "periodLength": 5, "predictedCycle": True},
            ],
        }
        file_path = tmp_path / "999_MENSTRUAL_CYCLE_SUMMARY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(data, f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_summary(file_path, db_session)
        db_session.commit()

        rows = (
            db_session.execute(
                select(MenstrualCycleSummary).order_by(MenstrualCycleSummary.start_date)
            )
            .scalars()
            .all()
        )
        assert [str(r.start_date) for r in rows] == [
            "2026-05-23",
            "2026-06-20",
            "2026-07-18",
        ]
        assert [r.predicted_cycle for r in rows] == [False, True, True]

    def test_predictions_replaced_observed_preserved(
        self, db_session: Session, tmp_path
    ):
        """
        Critical scenario: two extract runs produce different predicted start dates.

        The second run must wipe stale predictions but preserve the observed (real)
        cycle row.
        """
        _seed_user_999(db_session)

        # First run: 1 observed + 2 predicted.
        first = {
            "cycleSummaries": [
                {"startDate": "2026-05-23", "periodLength": 3, "predictedCycle": False},
                {"startDate": "2026-06-20", "periodLength": 5, "predictedCycle": True},
                {"startDate": "2026-07-18", "periodLength": 5, "predictedCycle": True},
            ],
        }
        file_path = tmp_path / "999_MENSTRUAL_CYCLE_SUMMARY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump(first, f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_summary(file_path, db_session)
        db_session.commit()

        # Second run: observed cycle unchanged; Garmin recomputed projections,
        # so the predicted start dates have shifted.
        second = {
            "cycleSummaries": [
                {"startDate": "2026-05-23", "periodLength": 3, "predictedCycle": False},
                {"startDate": "2026-06-22", "periodLength": 5, "predictedCycle": True},
                {"startDate": "2026-07-20", "periodLength": 5, "predictedCycle": True},
            ],
        }
        with open(file_path, "w") as f:
            json.dump(second, f)

        proc._process_menstrual_cycle_summary(file_path, db_session)
        db_session.commit()

        rows = (
            db_session.execute(
                select(MenstrualCycleSummary).order_by(MenstrualCycleSummary.start_date)
            )
            .scalars()
            .all()
        )
        # Stale 2026-06-20 / 2026-07-18 predictions are gone; observed and new
        # predictions remain.
        assert [str(r.start_date) for r in rows] == [
            "2026-05-23",
            "2026-06-22",
            "2026-07-20",
        ]
        # Observed cycle still flagged predicted=False.
        observed = next(r for r in rows if str(r.start_date) == "2026-05-23")
        assert observed.predicted_cycle is False

    def test_empty_cycle_summaries_wipes_predictions_only(
        self, db_session: Session, tmp_path
    ):
        """
        Defensive: an empty calendar response still wipes stale predicted rows so the
        table doesn't keep showing projections after the user clears them.

        Observed cycles are unaffected (only predicted_cycle=True is in scope).
        """
        _seed_user_999(db_session)

        # Pre-seed: 1 observed + 1 predicted.
        db_session.add_all(
            [
                MenstrualCycleSummary(
                    user_id=999,
                    start_date=date(2026, 5, 23),
                    period_length=3,
                    predicted_cycle=False,
                ),
                MenstrualCycleSummary(
                    user_id=999,
                    start_date=date(2026, 6, 20),
                    period_length=5,
                    predicted_cycle=True,
                ),
            ]
        )
        db_session.commit()

        file_path = tmp_path / "999_MENSTRUAL_CYCLE_SUMMARY_2026-05-25T12-00-00Z.json"
        with open(file_path, "w") as f:
            json.dump({"cycleSummaries": []}, f)

        proc = _menstrual_processor()
        proc._process_menstrual_cycle_summary(file_path, db_session)
        db_session.commit()

        rows = db_session.execute(select(MenstrualCycleSummary)).scalars().all()
        assert len(rows) == 1
        assert rows[0].predicted_cycle is False
        assert str(rows[0].start_date) == "2026-05-23"


# ---------------------------------------------------------------------------
# TCX helpers
# ---------------------------------------------------------------------------

TCX_FILENAME = "1_ACTIVITY_12345_2024-01-01T08-00-00Z.tcx"

# Two trackpoints: first has GPS + all scalar fields + Garmin extensions,
# second has GPS + a subset of fields.  One lap summarises both.
_MINIMAL_TCX = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
  xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
  xmlns:ax="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
  <Activities>
    <Activity Sport="Running">
      <Lap StartTime="2024-01-01T08:00:00Z">
        <TotalTimeSeconds>300.0</TotalTimeSeconds>
        <DistanceMeters>1000.0</DistanceMeters>
        <MaximumSpeed>3.5</MaximumSpeed>
        <Calories>50</Calories>
        <AverageHeartRateBpm><Value>150</Value></AverageHeartRateBpm>
        <MaximumHeartRateBpm><Value>165</Value></MaximumHeartRateBpm>
        <Cadence>85</Cadence>
        <Track>
          <Trackpoint>
            <Time>2024-01-01T08:00:01Z</Time>
            <Position>
              <LatitudeDegrees>47.6062</LatitudeDegrees>
              <LongitudeDegrees>-122.3321</LongitudeDegrees>
            </Position>
            <AltitudeMeters>56.0</AltitudeMeters>
            <DistanceMeters>10.0</DistanceMeters>
            <HeartRateBpm><Value>145</Value></HeartRateBpm>
            <Cadence>80</Cadence>
            <Extensions>
              <ax:TPX>
                <ax:Speed>2.8</ax:Speed>
                <ax:RunCadence>160</ax:RunCadence>
              </ax:TPX>
            </Extensions>
          </Trackpoint>
          <Trackpoint>
            <Time>2024-01-01T08:00:02Z</Time>
            <Position>
              <LatitudeDegrees>47.6065</LatitudeDegrees>
              <LongitudeDegrees>-122.3318</LongitudeDegrees>
            </Position>
            <AltitudeMeters>57.5</AltitudeMeters>
            <DistanceMeters>13.0</DistanceMeters>
            <HeartRateBpm><Value>148</Value></HeartRateBpm>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>
"""

_NO_GPS_TCX = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
  xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Running">
      <Lap StartTime="2024-01-01T08:00:00Z">
        <TotalTimeSeconds>300.0</TotalTimeSeconds>
        <Track>
          <Trackpoint>
            <Time>2024-01-01T08:00:01Z</Time>
            <HeartRateBpm><Value>145</Value></HeartRateBpm>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>
"""

_EMPTY_LAPS_TCX = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
  xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Running">
    </Activity>
  </Activities>
</TrainingCenterDatabase>
"""

# Two trackpoints with single-digit fractional seconds. Python 3.10's strict
# datetime.fromisoformat would reject these; _parse_garmin_gmt normalizes them.
_FRACTIONAL_SECONDS_TCX = """\
<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
  xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities>
    <Activity Sport="Running">
      <Lap StartTime="2024-01-01T08:00:00Z">
        <Track>
          <Trackpoint>
            <Time>2024-01-01T08:00:01.5Z</Time>
            <HeartRateBpm><Value>145</Value></HeartRateBpm>
          </Trackpoint>
          <Trackpoint>
            <Time>2024-01-01T08:00:02.25Z</Time>
            <HeartRateBpm><Value>148</Value></HeartRateBpm>
          </Trackpoint>
        </Track>
      </Lap>
    </Activity>
  </Activities>
</TrainingCenterDatabase>
"""


def _write_tcx(tmp_path: Path, content: str, filename: str = TCX_FILENAME) -> Path:
    """
    Write TCX content to a temp file and return the path.
    """
    p = tmp_path / filename
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# TestProcessTcxFile
# ---------------------------------------------------------------------------


class TestProcessTcxFile:
    """
    Tests for _process_tcx_file.
    """

    def _make_processor(self) -> GarminProcessor:
        return GarminProcessor(FileSet(file_paths=[], files={}), MagicMock())

    def test_success_inserts_ts_lap_and_path(self, db_session: Session, tmp_path: Path):
        """
        Full TCX parse: correct ts_metric, lap_metric, and activity_path row counts.
        """
        _seed_activity(db_session)
        path = _write_tcx(tmp_path, _MINIMAL_TCX)

        self._make_processor()._process_tcx_file(path, db_session)
        db_session.commit()

        # tp1: position_lat, position_long, altitude, distance, cadence,
        #      heart_rate, speed, run_cadence  = 8
        # tp2: position_lat, position_long, altitude, distance, heart_rate = 5
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 13
        )
        # 7 lap summary fields
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityLapMetric)) == 7
        )
        # 2 GPS trackpoints
        path_row = db_session.execute(select(ActivityPath)).scalars().first()
        assert path_row is not None
        assert path_row.point_count == 2

    def test_ts_data_available_set_true(self, db_session: Session, tmp_path: Path):
        """
        ts_data_available is True after a TCX with trackpoints is processed.
        """
        activity = _seed_activity(db_session)
        assert activity.ts_data_available is False

        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        assert refreshed.ts_data_available is True

    def test_no_trackpoints_ts_data_available_false(
        self, db_session: Session, tmp_path: Path
    ):
        """
        ts_data_available stays False when the TCX has no trackpoints.
        """
        _seed_activity(db_session)

        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _EMPTY_LAPS_TCX), db_session
        )
        db_session.commit()

        refreshed = (
            db_session.execute(select(Activity).where(Activity.activity_id == 12345))
            .scalars()
            .first()
        )
        assert refreshed.ts_data_available is False

    def test_ts_metric_values(self, db_session: Session, tmp_path: Path):
        """
        Spot-check specific metric values from both trackpoints.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        ts1 = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 8, 0, 2, tzinfo=timezone.utc)

        def get(ts, name):
            return db_session.scalar(
                select(ActivityTsMetric.value).where(
                    ActivityTsMetric.activity_id == 12345,
                    ActivityTsMetric.timestamp == ts,
                    ActivityTsMetric.name == name,
                )
            )

        assert get(ts1, "heart_rate") == pytest.approx(145.0)
        assert get(ts1, "altitude") == pytest.approx(56.0)
        assert get(ts1, "distance") == pytest.approx(10.0)
        assert get(ts1, "cadence") == pytest.approx(80.0)
        # position_lat/long are stored as semicircles in activity_ts_metric to
        # match FIT's contract; activity_path keeps decimal degrees.
        assert get(ts1, "position_lat") == pytest.approx(
            47.6062 / SEMICIRCLES_TO_DEGREES
        )
        assert get(ts1, "position_long") == pytest.approx(
            -122.3321 / SEMICIRCLES_TO_DEGREES
        )
        assert get(ts2, "heart_rate") == pytest.approx(148.0)
        assert get(ts2, "altitude") == pytest.approx(57.5)

    def test_garmin_extension_fields(self, db_session: Session, tmp_path: Path):
        """
        Ax:TPX Speed and RunCadence land in activity_ts_metric.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        ts1 = datetime(2024, 1, 1, 8, 0, 1, tzinfo=timezone.utc)

        speed = db_session.scalar(
            select(ActivityTsMetric.value).where(
                ActivityTsMetric.activity_id == 12345,
                ActivityTsMetric.timestamp == ts1,
                ActivityTsMetric.name == "speed",
            )
        )
        run_cadence = db_session.scalar(
            select(ActivityTsMetric.value).where(
                ActivityTsMetric.activity_id == 12345,
                ActivityTsMetric.timestamp == ts1,
                ActivityTsMetric.name == "run_cadence",
            )
        )
        assert speed == pytest.approx(2.8)
        assert run_cadence == pytest.approx(160.0)

    def test_lap_metric_values(self, db_session: Session, tmp_path: Path):
        """
        Lap summary fields map to the correct metric names and values.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        def get_lap(name):
            return db_session.scalar(
                select(ActivityLapMetric.value).where(
                    ActivityLapMetric.activity_id == 12345,
                    ActivityLapMetric.lap_idx == 1,
                    ActivityLapMetric.name == name,
                )
            )

        assert get_lap("total_elapsed_time") == pytest.approx(300.0)
        assert get_lap("total_distance") == pytest.approx(1000.0)
        assert get_lap("max_speed") == pytest.approx(3.5)
        assert get_lap("total_calories") == pytest.approx(50.0)
        assert get_lap("avg_cadence") == pytest.approx(85.0)
        assert get_lap("avg_heart_rate") == pytest.approx(150.0)
        assert get_lap("max_heart_rate") == pytest.approx(165.0)

    def test_gps_path_coords_and_order(self, db_session: Session, tmp_path: Path):
        """
        ActivityPath stores [lon, lat] pairs sorted ascending by timestamp.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        path_row = db_session.execute(select(ActivityPath)).scalars().first()
        assert path_row.point_count == 2
        coords = path_row.path_json
        # First point: tp1 [lon, lat]
        assert coords[0][0] == pytest.approx(-122.3321)
        assert coords[0][1] == pytest.approx(47.6062)
        # Second point: tp2 [lon, lat]
        assert coords[1][0] == pytest.approx(-122.3318)
        assert coords[1][1] == pytest.approx(47.6065)

    def test_no_gps_skips_activity_path(self, db_session: Session, tmp_path: Path):
        """
        Indoor activity with no <Position> elements produces no ActivityPath row.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _NO_GPS_TCX), db_session
        )
        db_session.commit()

        assert db_session.scalar(select(func.count()).select_from(ActivityPath)) == 0
        # Heart rate still lands in ts_metric.
        assert (
            db_session.scalar(select(func.count()).select_from(ActivityTsMetric)) == 1
        )

    def test_reprocessing_deletes_and_reinserts(
        self, db_session: Session, tmp_path: Path
    ):
        """
        Re-running _process_tcx_file replaces old rows rather than appending.
        """
        _seed_activity(db_session)
        path = _write_tcx(tmp_path, _MINIMAL_TCX)
        proc = self._make_processor()

        proc._process_tcx_file(path, db_session)
        db_session.commit()
        first_count = db_session.scalar(
            select(func.count()).select_from(ActivityTsMetric)
        )

        proc._process_tcx_file(path, db_session)
        db_session.commit()
        second_count = db_session.scalar(
            select(func.count()).select_from(ActivityTsMetric)
        )

        assert second_count == first_count

    def test_no_split_metrics_inserted(self, db_session: Session, tmp_path: Path):
        """
        TCX has no concept of splits; activity_split_metric stays empty.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _MINIMAL_TCX), db_session
        )
        db_session.commit()

        assert (
            db_session.scalar(select(func.count()).select_from(ActivitySplitMetric))
            == 0
        )

    def test_activity_not_found_raises(self, db_session: Session, tmp_path: Path):
        """
        Raises ValueError when the parent activity record is missing.
        """
        upsert_model_instances(
            session=db_session,
            model_instances=[User(user_id=1, full_name="Test User")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        db_session.commit()

        with pytest.raises(ValueError, match="Activity 12345 not found"):
            self._make_processor()._process_tcx_file(
                _write_tcx(tmp_path, _MINIMAL_TCX), db_session
            )

    def test_invalid_filename_raises(self, db_session: Session, tmp_path: Path):
        """
        Raises ValueError for a filename that does not match the expected pattern.
        """
        bad_path = _write_tcx(tmp_path, _MINIMAL_TCX, filename="bad_name.tcx")
        with pytest.raises(ValueError, match="Cannot extract activity_id"):
            self._make_processor()._process_tcx_file(bad_path, db_session)

    def test_malformed_xml_raises_value_error(
        self, db_session: Session, tmp_path: Path
    ):
        """
        Raises ValueError (not a raw XML ParseError) when the TCX file is corrupt.
        """
        _seed_activity(db_session)
        bad = _write_tcx(tmp_path, "<TrainingCenterDatabase><not closed")
        with pytest.raises(ValueError, match="Malformed TCX file"):
            self._make_processor()._process_tcx_file(bad, db_session)

    def test_fractional_second_timestamps_parse_on_py310(
        self, db_session: Session, tmp_path: Path
    ):
        """
        Trackpoints with single-/two-digit fractional seconds are parsed correctly.

        Python 3.10's ``datetime.fromisoformat`` rejects formats other than 0/3/6
        fractional digits, so a raw parse would silently drop these trackpoints.
        ``_parse_garmin_gmt`` normalizes them, so the heart_rate metrics land.
        """
        _seed_activity(db_session)
        self._make_processor()._process_tcx_file(
            _write_tcx(tmp_path, _FRACTIONAL_SECONDS_TCX), db_session
        )
        db_session.commit()

        ts1 = datetime(2024, 1, 1, 8, 0, 1, 500000, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 1, 8, 0, 2, 250000, tzinfo=timezone.utc)
        assert db_session.scalar(
            select(ActivityTsMetric.value).where(
                ActivityTsMetric.activity_id == 12345,
                ActivityTsMetric.timestamp == ts1,
                ActivityTsMetric.name == "heart_rate",
            )
        ) == pytest.approx(145.0)
        assert db_session.scalar(
            select(ActivityTsMetric.value).where(
                ActivityTsMetric.activity_id == 12345,
                ActivityTsMetric.timestamp == ts2,
                ActivityTsMetric.name == "heart_rate",
            )
        ) == pytest.approx(148.0)


# ---------------------------------------------------------------------------
# TestProcessActivityFileDispatch
# ---------------------------------------------------------------------------


class TestProcessActivityFileDispatch:
    """
    Tests for _process_activity_file routing.
    """

    def _make_processor(self) -> GarminProcessor:
        return GarminProcessor(FileSet(file_paths=[], files={}), MagicMock())

    def test_dispatches_fit_to_fit_processor(self, tmp_path: Path):
        """
        .fit files are forwarded to _process_fit_file.
        """
        proc = self._make_processor()
        fit_path = tmp_path / "1_ACTIVITY_12345_2024-01-01T08-00-00Z.fit"
        fit_path.touch()
        session = MagicMock()

        with patch.object(proc, "_process_fit_file") as mock_fit:
            proc._process_activity_file(fit_path, session)

        mock_fit.assert_called_once_with(fit_path, session)

    def test_dispatches_tcx_to_tcx_processor(self, tmp_path: Path):
        """
        .tcx files are forwarded to _process_tcx_file.
        """
        proc = self._make_processor()
        tcx_path = tmp_path / TCX_FILENAME
        tcx_path.touch()
        session = MagicMock()

        with patch.object(proc, "_process_tcx_file") as mock_tcx:
            proc._process_activity_file(tcx_path, session)

        mock_tcx.assert_called_once_with(tcx_path, session)

    def test_unknown_extension_logs_warning_and_does_not_raise(self, tmp_path: Path):
        """
        Unsupported extensions log a warning and return without raising.
        """
        proc = self._make_processor()
        gpx_path = tmp_path / "1_ACTIVITY_12345_2024-01-01T08-00-00Z.gpx"
        gpx_path.touch()
        session = MagicMock()

        with patch("garmin_health_data.processor.click") as mock_click:
            proc._process_activity_file(gpx_path, session)
            mock_click.secho.assert_called_once()
            assert ".gpx" in mock_click.secho.call_args[0][0]


# ---------------------------------------------------------------------------
# TestGarminFileTypesActivityPattern
# ---------------------------------------------------------------------------


class TestGarminFileTypesActivityPattern:
    """
    Tests for the GARMIN_FILE_TYPES pattern and _partition_processable_and_backup.
    """

    def test_tcx_activity_file_is_processable(self, tmp_path: Path):
        """
        TCX activity filenames pass through _partition_processable_and_backup.
        """
        from garmin_health_data.cli import _partition_processable_and_backup

        tcx = tmp_path / "5351450_ACTIVITY_629147134_2014-03-23T12-00-00Z.tcx"
        tcx.touch()
        processable, backup_only = _partition_processable_and_backup([tcx])
        assert tcx in processable
        assert tcx not in backup_only

    def test_fit_activity_file_still_processable(self, tmp_path: Path):
        """
        FIT activity filenames are not regressed by the pattern change.
        """
        from garmin_health_data.cli import _partition_processable_and_backup

        fit = tmp_path / "5351450_ACTIVITY_629147134_2014-03-23T12-00-00Z.fit"
        fit.touch()
        processable, backup_only = _partition_processable_and_backup([fit])
        assert fit in processable
        assert fit not in backup_only

    def test_gpx_activity_file_is_backup_only(self, tmp_path: Path):
        """
        GPX files (no processor yet) remain in backup-only.
        """
        from garmin_health_data.cli import _partition_processable_and_backup

        gpx = tmp_path / "5351450_ACTIVITY_629147134_2014-03-23T12-00-00Z.gpx"
        gpx.touch()
        processable, backup_only = _partition_processable_and_backup([gpx])
        assert gpx in backup_only
        assert gpx not in processable


def test_process_running_tolerance_upserts_rows(processor, tmp_path):
    """
    _process_running_tolerance builds a RunningTolerance record per daily row and
    upserts them keyed by (user_id, date) with on_conflict_update=True.
    """
    f = tmp_path / "123456789_RUNNING_TOLERANCE_2026-03-15T12-00-00Z.json"
    f.write_text(
        json.dumps(
            [
                {
                    "calendarDate": "2026-03-15",
                    "totalImpactLoad": 53800,
                    "totalDistance": 49615.0,
                    "tolerance": 60914,
                    "startOfWeek": "2026-03-11",
                    "endOfWeek": "2026-03-15",
                    "weekIndex": 1889,
                }
            ]
        )
    )
    session = MagicMock()
    with patch("garmin_health_data.processor.upsert_model_instances") as mock_upsert:
        processor._process_running_tolerance(f, session)

    records = mock_upsert.call_args.kwargs["model_instances"]
    assert len(records) == 1
    r = records[0]
    assert r.user_id == 123456789
    assert r.date == date(2026, 3, 15)
    assert r.total_impact_load == 53800
    assert r.total_distance == 49615.0
    assert r.tolerance == 60914
    assert r.start_of_week == date(2026, 3, 11)
    assert r.end_of_week == date(2026, 3, 15)
    assert r.week_index == 1889
    assert mock_upsert.call_args.kwargs["conflict_columns"] == ["user_id", "date"]
    assert mock_upsert.call_args.kwargs["on_conflict_update"] is True


def test_process_running_tolerance_skips_rows_without_date(processor, tmp_path):
    """
    Rows without a calendarDate are skipped; a file with only such rows upserts nothing.
    """
    f = tmp_path / "123456789_RUNNING_TOLERANCE_2026-03-15T12-00-00Z.json"
    f.write_text(json.dumps([{"totalImpactLoad": 1}, "junk"]))
    session = MagicMock()
    with patch("garmin_health_data.processor.upsert_model_instances") as mock_upsert:
        processor._process_running_tolerance(f, session)

    mock_upsert.assert_not_called()


def _multisport_child(type_key, type_id, summary):
    """
    Build a minimal multi-sport leg detail dict for tests.
    """
    return {
        "activityId": 999,
        "activityName": f"{type_key} leg",
        "activityTypeDTO": {"typeId": type_id, "typeKey": type_key},
        "eventTypeDTO": {"typeId": 9, "typeKey": "uncategorized"},
        "summaryDTO": {
            "startTimeGMT": "2026-05-09T16:41:14.0",
            "startTimeLocal": "2026-05-09T09:41:14.0",
            "duration": 2593.0,
            "distance": 9396.0,
            "averageHR": 160.0,
            **summary,
        },
    }


def test_process_multisport_child_running_builds_row_and_agg(processor):
    """
    A running leg yields a linked activity row plus running_agg_metrics from summary.
    """
    from garmin_health_data.models import Activity, RunningAggMetrics

    child = _multisport_child(
        "running",
        1,
        {
            "averageRunCadence": 169.1,
            "verticalOscillation": 9.19,
            "groundContactTime": 231.0,
            "averagePower": 355.0,
            "normalizedPower": 360.0,
        },
    )
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        ok = processor._process_multisport_child(child, 22824120751, MagicMock())

    assert ok is True
    inserted = [call.kwargs["model_instances"][0] for call in up.call_args_list]
    act = next(r for r in inserted if isinstance(r, Activity))
    agg = next(r for r in inserted if isinstance(r, RunningAggMetrics))
    assert act.activity_id == 999
    assert act.parent_activity_id == 22824120751
    assert act.activity_type_key == "running"
    assert act.start_ts == datetime(2026, 5, 9, 16, 41, 14)
    assert agg.avg_running_cadence == 169.1
    assert agg.avg_ground_contact_time == 231.0
    assert agg.avg_power == 355.0
    assert agg.normalized_power == 360.0


def test_process_multisport_child_openwater_swim_leaves_pool_fields_null(processor):
    """
    An open-water swim leg maps SWOLF/strokes; pool-length fields stay None.
    """
    from garmin_health_data.models import SwimmingAggMetrics

    child = _multisport_child(
        "open_water_swimming",
        26,
        {
            "averageSWOLF": 45.0,
            "averageSwimCadence": 32.0,
            "averageStrokeDistance": 1.6,
            "totalNumberOfStrokes": 919.0,
        },
    )
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        processor._process_multisport_child(child, 111, MagicMock())

    agg = next(
        r
        for call in up.call_args_list
        for r in call.kwargs["model_instances"]
        if isinstance(r, SwimmingAggMetrics)
    )
    assert agg.avg_swolf == 45.0
    assert agg.strokes == 919.0
    assert agg.avg_swim_cadence == 32.0
    assert agg.pool_length is None  # Open water: no pool lengths.
    assert agg.active_lengths is None
    assert agg.avg_strokes is None


def test_process_multisport_child_transition_writes_no_agg(processor):
    """
    A transition leg gets an activity row but no sport-specific aggregate table.
    """
    from garmin_health_data.models import (
        Activity,
        RunningAggMetrics,
        CyclingAggMetrics,
        SwimmingAggMetrics,
    )

    child = _multisport_child("transition_v2", 1000, {"averageHR": 140.0})
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        processor._process_multisport_child(child, 111, MagicMock())

    inserted = [r for call in up.call_args_list for r in call.kwargs["model_instances"]]
    assert any(isinstance(r, Activity) for r in inserted)
    assert not any(
        isinstance(r, (RunningAggMetrics, CyclingAggMetrics, SwimmingAggMetrics))
        for r in inserted
    )


def test_process_multisport_child_skips_when_missing_required_fields(processor):
    """
    A leg missing its sport type / start is skipped without writing anything.
    """
    child = {
        "activityId": 5,
        "activityTypeDTO": {},
        "eventTypeDTO": {},
        "summaryDTO": {},
    }
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        ok = processor._process_multisport_child(child, 111, MagicMock())
    assert ok is False
    up.assert_not_called()


def test_process_running_tolerance_skips_unparseable_date(processor, tmp_path):
    """
    A row with an unparseable calendarDate is skipped, not fatal to the FileSet.
    """
    f = tmp_path / "123456789_RUNNING_TOLERANCE_2026-03-15T12-00-00Z.json"
    f.write_text(
        json.dumps(
            [
                {"calendarDate": "not-a-date", "tolerance": 1},
                {"calendarDate": "2026-03-15", "tolerance": 60914},
            ]
        )
    )
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        processor._process_running_tolerance(f, MagicMock())
    records = up.call_args.kwargs["model_instances"]
    assert len(records) == 1  # bad-date row skipped, good row kept.
    assert records[0].date == date(2026, 3, 15)


def test_process_multisport_children_skips_non_dict_payload(processor, tmp_path):
    """
    A corrupted (non-dict) MULTISPORT_CHILDREN file is skipped without raising.
    """
    f = tmp_path / "123456789_MULTISPORT_CHILDREN_751_2026-05-09T12-00-00Z.json"
    f.write_text(json.dumps(["not", "a", "dict"]))
    with patch("garmin_health_data.processor.upsert_model_instances") as up:
        processor._process_multisport_children(f, MagicMock())
    up.assert_not_called()

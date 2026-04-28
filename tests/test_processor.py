"""
Tests for GarminProcessor.

Covers FIT file delete+insert reprocessing, activity/sleep upsert column exclusion, and
strength training data processing.
"""

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import fitdecode
import pytest
from sqlalchemy import delete, func, insert, select
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    Activity,
    ActivityLapMetric,
    ActivityPath,
    ActivitySplitMetric,
    ActivityTsMetric,
    SleepLevel,
    StrengthExercise,
    StrengthSet,
    User,
)
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet, upsert_model_instances


# --- Fixtures ---------------------------------------------------------------


@pytest.fixture
def processor():
    """
    Create a GarminProcessor instance for testing.

    :return: GarminProcessor instance.
    """

    file_set = FileSet(file_paths=[], files={})
    session = MagicMock()
    proc = GarminProcessor(file_set, session)
    proc.user_id = 123456789
    return proc


@pytest.fixture
def mock_session():
    """
    Create a mock database session.

    :return: Mock session instance.
    """

    return MagicMock()


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


# --- Sleep upsert tests ----------------------------------------------------


class TestSleepUpsert:
    """
    Tests for column exclusion during sleep upserts.
    """

    def test_sleep_update_columns_excludes_create_ts(self):
        """
        Verify sleep upsert excludes create_ts from update columns.
        """

        from garmin_health_data.models import Sleep

        update_columns = [
            col.name
            for col in Sleep.__table__.columns
            if col.name not in ["user_id", "start_ts", "sleep_id", "create_ts"]
        ]
        assert "sleep_id" not in update_columns
        assert "create_ts" not in update_columns
        assert "user_id" not in update_columns
        assert "start_ts" not in update_columns
        assert "end_ts" in update_columns
        assert "update_ts" in update_columns


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

        Garmin Connect returns timestamps with a single-digit fractional second
        (e.g. ``"2026-04-06T05:47:59.0"``) which Python 3.10 cannot parse natively.
        This exercises the production format end-to-end via the
        :meth:`GarminProcessor._parse_garmin_gmt` helper to ensure the path stays
        green on every supported Python.

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

    These helpers exist because Python 3.10's strict ``datetime.fromisoformat``
    rejects Garmin's single-digit fractional second format. The class is the
    central regression test for that compatibility shim.
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

        file_name = "123_EXERCISE_SETS_22320029355" "_2025-03-27.json"
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

        file_name = "123_EXERCISE_SETS_22320029355" "_2025-03-27.json"
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

    @patch("garmin_health_data.processor" ".upsert_model_instances")
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

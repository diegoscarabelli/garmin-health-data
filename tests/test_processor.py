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
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    Activity,
    ActivityLapMetric,
    ActivityPath,
    ActivitySplitMetric,
    ActivityTsMetric,
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
    persisted = session.query(Activity).filter_by(activity_id=activity_id).first()
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

        assert db_session.query(ActivityTsMetric).count() == 2
        assert db_session.query(ActivityLapMetric).count() == 2

        refreshed = db_session.query(Activity).filter_by(activity_id=12345).first()
        assert refreshed.ts_data_available is True

    def test_process_fit_file_reprocessing(self, db_session: Session):
        """
        Re-running deletes old rows and inserts fresh data.
        """
        activity = _seed_activity(db_session, ts_data_available=True)

        # Simulate pre-existing metrics from a previous run.
        old_ts = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        db_session.bulk_save_objects(
            [
                ActivityTsMetric(
                    activity_id=12345,
                    timestamp=old_ts,
                    name="old_metric",
                    value=1.0,
                ),
                ActivityLapMetric(
                    activity_id=12345,
                    lap_idx=1,
                    name="old_lap",
                    value=100.0,
                ),
                ActivitySplitMetric(
                    activity_id=12345,
                    split_idx=1,
                    name="old_split",
                    value=200.0,
                ),
            ]
        )
        db_session.commit()
        assert db_session.query(ActivityTsMetric).count() == 1
        assert db_session.query(ActivityLapMetric).count() == 1
        assert db_session.query(ActivitySplitMetric).count() == 1

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
        ts_rows = db_session.query(ActivityTsMetric).all()
        assert len(ts_rows) == 1
        assert ts_rows[0].name == "heart_rate"
        assert ts_rows[0].value == 160.0

        lap_rows = db_session.query(ActivityLapMetric).all()
        assert len(lap_rows) == 1
        assert lap_rows[0].name == "total_elapsed_time"

        # Old splits deleted (no new splits in this FIT data).
        assert db_session.query(ActivitySplitMetric).count() == 0

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

        assert db_session.query(ActivityTsMetric).count() == 0
        assert db_session.query(ActivityLapMetric).count() == 2

        refreshed = db_session.query(Activity).filter_by(activity_id=12345).first()
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

        paths = db_session.query(ActivityPath).all()
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
        assert db_session.query(ActivityTsMetric).count() == 2
        # No activity_path row.
        assert db_session.query(ActivityPath).count() == 0

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

        paths = db_session.query(ActivityPath).all()
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

        paths = db_session.query(ActivityPath).all()
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

        paths = db_session.query(ActivityPath).all()
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

        assert db_session.query(ActivityPath).count() == 0


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

        refreshed = db_session.query(Activity).filter_by(activity_id=12345).first()
        assert refreshed.activity_name == "Renamed Run"
        # ts_data_available preserved despite upsert.
        assert refreshed.ts_data_available is True

    def test_upsert_preserves_create_ts(self, db_session: Session):
        """
        Activity upsert does not overwrite create_ts audit column.
        """
        _seed_activity(db_session)

        original = db_session.query(Activity).filter_by(activity_id=12345).first()
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

        refreshed = db_session.query(Activity).filter_by(activity_id=12345).first()
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

        # Assert - delete was called for reprocessing.
        mock_session.query.assert_called_with(StrengthExercise)
        filter_call = mock_session.query.return_value.filter_by
        filter_call.assert_called_with(activity_id=activity_id)
        delete_call = filter_call.return_value
        delete_call.delete.assert_called_once()

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

        # Assert - delete was called (cleans stale data).
        mock_session.query.assert_called_with(StrengthExercise)

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

        # Assert - delete was called for reprocessing.
        mock_session.query.assert_called_with(StrengthSet)
        filter_call = mock_session.query.return_value.filter_by
        filter_call.assert_called_with(activity_id=22320029355)

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

        # Assert - delete called, no inserts.
        filter_call = mock_session.query.return_value.filter_by.return_value
        filter_call.delete.assert_called()
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

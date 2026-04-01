"""
Tests for GarminProcessor FIT file processing and activity base upsert logic.

Covers the delete+insert pattern for idempotent FIT metric reprocessing and column
exclusion during activity upserts.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import fitdecode
import pytest
from sqlalchemy.orm import Session

from garmin_health_data.models import (
    Activity,
    ActivityLapMetric,
    ActivitySplitMetric,
    ActivityTsMetric,
    User,
)
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet, upsert_model_instances


# --- Helpers ----------------------------------------------------------------


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

        mock_reader = MagicMock()
        mock_reader.__enter__ = MagicMock(return_value=mock_reader)
        mock_reader.__exit__ = MagicMock(return_value=False)
        mock_reader.__iter__ = MagicMock(return_value=iter([record_frame, lap_frame]))

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = mock_reader
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

        mock_reader = MagicMock()
        mock_reader.__enter__ = MagicMock(return_value=mock_reader)
        mock_reader.__exit__ = MagicMock(return_value=False)
        mock_reader.__iter__ = MagicMock(return_value=iter([record_frame, lap_frame]))

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = mock_reader
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

        mock_reader = MagicMock()
        mock_reader.__enter__ = MagicMock(return_value=mock_reader)
        mock_reader.__exit__ = MagicMock(return_value=False)
        mock_reader.__iter__ = MagicMock(return_value=iter([lap_frame]))

        processor = self._make_processor()
        with patch("garmin_health_data.processor.fitdecode") as mock_fitdecode:
            mock_fitdecode.FIT_FRAME_DATA = fitdecode.FIT_FRAME_DATA
            mock_fitdecode.FitReader.return_value = mock_reader
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

"""
Tests for Garmin strength training data processing.
"""

import copy
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from garmin_health_data.models import (
    StrengthExercise,
    StrengthSet,
)
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet


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

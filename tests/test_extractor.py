"""
Tests for Garmin exercise sets extraction.
"""

import io
import json
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from garmin_health_data.extractor import GarminExtractor, extract


class TestExtractExerciseSets:
    """
    Tests for exercise sets extraction methods.
    """

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """
        Create temporary directory for testing.

        :param tmp_path: Pytest tmp_path fixture.
        :return: Temporary directory path.
        """

        return tmp_path

    @pytest.fixture
    def extractor(self, temp_dir: Path) -> GarminExtractor:
        """
        Create GarminExtractor instance for testing.

        :param temp_dir: Temporary directory fixture.
        :return: GarminExtractor instance.
        """

        return GarminExtractor(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 3),
            ingest_dir=temp_dir,
        )

    @pytest.fixture
    def mock_garmin_client(self) -> MagicMock:
        """
        Create mock Garmin client for testing.

        :return: Mock Garmin client instance.
        """

        mock_client = MagicMock()
        mock_client.full_name = "Test User"
        return mock_client

    def test_extract_exercise_sets_success(
        self, extractor, mock_garmin_client, temp_dir
    ) -> None:
        """
        Test successful exercise sets extraction.

        :param extractor: GarminExtractor fixture.
        :param mock_garmin_client: Mock Garmin client fixture.
        :param temp_dir: Temporary directory fixture.
        """

        # Arrange.
        extractor.garmin_client = mock_garmin_client
        extractor.user_id = "123456789"

        exercise_data = {
            "activityId": 22320029355,
            "exerciseSets": [
                {
                    "messageIndex": 0,
                    "setType": "ACTIVE",
                    "duration": 45.0,
                    "repetitionCount": 10,
                    "weight": 452000.0,
                    "exercises": [
                        {
                            "category": "BENCH_PRESS",
                            "name": "BARBELL_BENCH_PRESS",
                            "probability": 0.98,
                        }
                    ],
                }
            ],
        }
        mock_garmin_client.get_activity_exercise_sets.return_value = exercise_data

        # Act.
        result = extractor._extract_exercise_sets(22320029355, "2025-01-01T12-00-00Z")

        # Assert.
        assert result is not None
        assert result.exists()
        assert "EXERCISE_SETS" in result.name
        assert result.name.endswith(".json")

        # Verify file contents.
        with open(result, "r") as f:
            saved_data = json.load(f)
        assert saved_data["activityId"] == 22320029355
        assert len(saved_data["exerciseSets"]) == 1

    def test_extract_exercise_sets_no_data(self, extractor, mock_garmin_client) -> None:
        """
        Test exercise sets extraction with no data returned.

        :param extractor: GarminExtractor fixture.
        :param mock_garmin_client: Mock Garmin client fixture.
        """

        # Arrange.
        extractor.garmin_client = mock_garmin_client
        extractor.user_id = "123456789"
        mock_garmin_client.get_activity_exercise_sets.return_value = {
            "activityId": 12345,
            "exerciseSets": None,
        }

        # Act.
        result = extractor._extract_exercise_sets(12345, "2025-01-01T12-00-00Z")

        # Assert.
        assert result is None

    def test_extract_exercise_sets_api_error(
        self, extractor, mock_garmin_client
    ) -> None:
        """
        Test exercise sets extraction with API error.

        :param extractor: GarminExtractor fixture.
        :param mock_garmin_client: Mock Garmin client fixture.
        """

        # Arrange.
        extractor.garmin_client = mock_garmin_client
        extractor.user_id = "123456789"
        mock_garmin_client.get_activity_exercise_sets.side_effect = Exception(
            "API error"
        )

        # Act.
        result = extractor._extract_exercise_sets(12345, "2025-01-01T12-00-00Z")

        # Assert.
        assert result is None

    @patch("garmin_health_data.extractor.time.sleep")
    def test_fit_extraction_triggers_exercise_sets(
        self,
        mock_sleep,
        extractor,
        mock_garmin_client,
        temp_dir,
    ) -> None:
        """
        Test that extract_fit_activities fetches exercise sets for strength training
        activities.

        :param mock_sleep: Mock sleep function.
        :param extractor: GarminExtractor fixture.
        :param mock_garmin_client: Mock Garmin client fixture.
        :param temp_dir: Temporary directory fixture.
        """

        # Arrange.
        extractor.garmin_client = mock_garmin_client
        extractor.user_id = "123456789"

        activities = [
            {
                "activityId": "22320029355",
                "startTimeLocal": "2025-01-01T10:00:00.000",
                "activityType": {
                    "typeId": 71,
                    "typeKey": "strength_training",
                },
            },
        ]
        mock_garmin_client.get_activities_by_date.return_value = activities

        # Create mock ZIP file.
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            zip_file.writestr("activity.fit", b"FIT_DATA")
        zip_buffer.seek(0)
        mock_garmin_client.download_activity.return_value = zip_buffer.getvalue()

        # Mock exercise sets API.
        mock_garmin_client.get_activity_exercise_sets.return_value = {
            "activityId": 22320029355,
            "exerciseSets": [
                {"messageIndex": 0, "setType": "ACTIVE"},
            ],
        }

        # Act.
        result = extractor.extract_fit_activities()

        # Assert - both FIT and exercise sets files saved.
        assert len(result) == 2
        mock_garmin_client.get_activity_exercise_sets.assert_called_once_with(
            "22320029355"
        )

        fit_files = list(temp_dir.glob("*.fit"))
        json_files = list(temp_dir.glob("*.json"))
        assert len(fit_files) == 1
        assert len(json_files) == 1
        assert "EXERCISE_SETS" in json_files[0].name

    @patch("garmin_health_data.extractor.time.sleep")
    def test_fit_extraction_skips_exercise_sets_for_running(
        self,
        mock_sleep,
        extractor,
        mock_garmin_client,
        temp_dir,
    ) -> None:
        """
        Test that extract_fit_activities does not fetch exercise sets for non-strength
        activities.

        :param mock_sleep: Mock sleep function.
        :param extractor: GarminExtractor fixture.
        :param mock_garmin_client: Mock Garmin client fixture.
        :param temp_dir: Temporary directory fixture.
        """

        # Arrange.
        extractor.garmin_client = mock_garmin_client
        extractor.user_id = "123456789"

        activities = [
            {
                "activityId": "99999",
                "startTimeLocal": "2025-01-01T10:00:00.000",
                "activityType": {
                    "typeId": 1,
                    "typeKey": "running",
                },
            },
        ]
        mock_garmin_client.get_activities_by_date.return_value = activities

        # Create mock ZIP file.
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            zip_file.writestr("activity.fit", b"FIT_DATA")
        zip_buffer.seek(0)
        mock_garmin_client.download_activity.return_value = zip_buffer.getvalue()

        # Act.
        result = extractor.extract_fit_activities()

        # Assert - only FIT file, no exercise sets fetch.
        assert len(result) == 1
        mock_garmin_client.get_activity_exercise_sets.assert_not_called()


class TestExtractFunctionExerciseSets:
    """
    Tests for extract() function with EXERCISE_SETS data type.
    """

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_exercise_sets_triggers_fit_extraction(
        self, mock_extractor_class, mock_discover
    ) -> None:
        """
        Test that data_types=["EXERCISE_SETS"] triggers extract_fit_activities.

        :param mock_extractor_class: Mock GarminExtractor class.
        :param mock_discover: Mock discover_accounts function.
        """

        # Arrange.
        mock_discover.return_value = [
            ("123456789", Path("/fake/token/dir")),
        ]

        mock_extractor = MagicMock()
        mock_extractor.extract_fit_activities.return_value = [
            Path("activity.fit"),
            Path("exercise_sets.json"),
        ]
        mock_extractor.extract_garmin_data.return_value = []
        mock_extractor_class.return_value = mock_extractor

        # Act.
        extract(
            Path("/tmp/test"),
            "2025-01-01",
            "2025-01-03",
            data_types=["EXERCISE_SETS"],
        )

        # Assert - extract_fit_activities should be called.
        mock_extractor.extract_fit_activities.assert_called_once()
        mock_extractor.extract_garmin_data.assert_called_once()

        # Verify authenticate is called with the discovered token dir.
        mock_extractor.authenticate.assert_called_once_with(
            token_store_dir=str(Path("/fake/token/dir"))
        )


class TestExtractMultiAccount:
    """
    Tests for multi-account extraction in extract() function.
    """

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_multi_account_success(self, mock_extractor_class, mock_discover):
        """
        Two accounts are extracted sequentially, each with own token dir.
        """
        mock_discover.return_value = [
            ("11111111", Path("/tokens/11111111")),
            ("22222222", Path("/tokens/22222222")),
        ]

        mock_extractor = MagicMock()
        mock_extractor.extract_garmin_data.return_value = [Path("file1.json")]
        mock_extractor.extract_fit_activities.return_value = [Path("fit1.fit")]
        mock_extractor_class.return_value = mock_extractor

        result = extract(Path("/tmp/test"), "2025-01-01", "2025-01-03")

        assert mock_extractor_class.call_count == 2
        assert mock_extractor.authenticate.call_count == 2

        # Verify each account gets its own token_store_dir.
        calls = mock_extractor.authenticate.call_args_list
        assert calls[0].kwargs["token_store_dir"] == str(Path("/tokens/11111111"))
        assert calls[1].kwargs["token_store_dir"] == str(Path("/tokens/22222222"))

        assert result["garmin_files"] == 2
        assert result["activity_files"] == 2

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_account_filter(self, mock_extractor_class, mock_discover):
        """
        Only matching accounts are extracted when filter is provided.
        """
        mock_discover.return_value = [
            ("11111111", Path("/tokens/11111111")),
            ("22222222", Path("/tokens/22222222")),
        ]

        mock_extractor = MagicMock()
        mock_extractor.extract_garmin_data.return_value = [Path("file1.json")]
        mock_extractor.extract_fit_activities.return_value = []
        mock_extractor_class.return_value = mock_extractor

        result = extract(
            Path("/tmp/test"),
            "2025-01-01",
            "2025-01-03",
            accounts=["11111111"],
        )

        # Only one extractor created for matching account.
        assert mock_extractor_class.call_count == 1
        assert result["garmin_files"] == 1

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_account_filter_no_match(self, mock_extractor_class, mock_discover):
        """
        Returns zero counts when account filter matches no discovered accounts.
        """
        mock_discover.return_value = [
            ("11111111", Path("/tokens/11111111")),
        ]

        result = extract(
            Path("/tmp/test"),
            "2025-01-01",
            "2025-01-03",
            accounts=["99999999"],
        )

        mock_extractor_class.assert_not_called()
        assert result == {"garmin_files": 0, "activity_files": 0}

    def test_accounts_string_raises(self):
        """
        Raises ValueError when accounts is a bare string instead of a list.
        """
        with pytest.raises(ValueError, match="must be a list or tuple"):
            extract(
                Path("/tmp/test"),
                "2025-01-01",
                "2025-01-03",
                accounts="12345678",
            )

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_error_isolation(self, mock_extractor_class, mock_discover):
        """
        One failing account does not block others.
        """
        mock_discover.return_value = [
            ("11111111", Path("/tokens/11111111")),
            ("22222222", Path("/tokens/22222222")),
        ]

        # First extractor fails on authenticate, second succeeds.
        failing_extractor = MagicMock()
        failing_extractor.authenticate.side_effect = RuntimeError("Auth failed")

        succeeding_extractor = MagicMock()
        succeeding_extractor.extract_garmin_data.return_value = [Path("file1.json")]
        succeeding_extractor.extract_fit_activities.return_value = []

        mock_extractor_class.side_effect = [failing_extractor, succeeding_extractor]

        result = extract(Path("/tmp/test"), "2025-01-01", "2025-01-03")

        # Second account still processed.
        assert result["garmin_files"] == 1
        succeeding_extractor.extract_garmin_data.assert_called_once()

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_all_accounts_fail(self, mock_extractor_class, mock_discover):
        """
        Returns zero counts when all accounts fail.
        """
        mock_discover.return_value = [
            ("11111111", Path("/tokens/11111111")),
        ]

        mock_extractor = MagicMock()
        mock_extractor.authenticate.side_effect = RuntimeError("Auth failed")
        mock_extractor_class.return_value = mock_extractor

        result = extract(Path("/tmp/test"), "2025-01-01", "2025-01-03")

        assert result == {"garmin_files": 0, "activity_files": 0}

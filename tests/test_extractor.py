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

from garmin_health_data.extractor import (
    GarminExtractor,
    _detect_format_from_magic,
    extract,
)

# Minimal valid FIT file header (14 bytes).
# Bytes 8–11 are the ANT+ FIT protocol magic: b'.FIT'.
_FIT_MAGIC = b"\x0e\x10\x00\x00\x00\x00\x00\x00.FIT\x00\x00"

_TCX_CONTENT = b'<?xml version="1.0" encoding="UTF-8"?><TrainingCenterDatabase/>'
_GPX_CONTENT = b'<?xml version="1.0"?><gpx version="1.1"/>'
_KML_CONTENT = b'<?xml version="1.0"?><kml/>'


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
            zip_file.writestr("activity.fit", _FIT_MAGIC)
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
            zip_file.writestr("activity.fit", _FIT_MAGIC)
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
        assert result["garmin_files"] == 0
        assert result["activity_files"] == 0
        assert result["failures"] == []
        assert result["failed_accounts"] == []

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

        assert result["garmin_files"] == 0
        assert result["activity_files"] == 0
        assert result["failures"] == []
        # Both accounts failed; both should be in failed_accounts.
        assert len(result["failed_accounts"]) > 0

    @patch("garmin_health_data.auth.discover_accounts")
    @patch("garmin_health_data.extractor.GarminExtractor")
    def test_partial_failures_preserved_when_account_crashes(
        self, mock_extractor_class, mock_discover
    ):
        """
        Per-date / per-data-type / per-activity failures recorded BEFORE an account-
        level crash must still appear in the merged ``failures`` list.

        Without the ``finally:`` merge, those granular failures would be dropped when
        the account also raises a fatal exception, so the end-of-run summary would only
        show "account failed" without the per-day detail captured before the crash.
        """
        mock_discover.return_value = [("11111111", Path("/tokens/11111111"))]

        from garmin_health_data.extractor import ExtractionFailure

        mock_extractor = MagicMock()
        mock_extractor.failures = [
            ExtractionFailure(
                data_type="SLEEP",
                date="2025-01-01",
                activity_id="",
                error="GarminConnectionError: 503",
            ),
            ExtractionFailure(
                data_type="SLEEP",
                date="2025-01-02",
                activity_id="",
                error="GarminConnectionError: 503",
            ),
        ]
        # extract_garmin_data succeeds (returns nothing) but
        # extract_fit_activities raises after partial failures were recorded.
        mock_extractor.extract_garmin_data.return_value = []
        mock_extractor.extract_fit_activities.side_effect = RuntimeError(
            "FIT download crashed mid-account"
        )
        mock_extractor_class.return_value = mock_extractor

        result = extract(Path("/tmp/test"), "2025-01-01", "2025-01-03")

        # Account-level crash recorded.
        assert "11111111" in result["failed_accounts"]
        # Pre-crash granular failures preserved (the bug fix).
        assert len(result["failures"]) == 2
        assert {f.date for f in result["failures"]} == {"2025-01-01", "2025-01-02"}


def _make_zip(inner_filename: str, content: bytes) -> bytes:
    """
    Build an in-memory ZIP containing one file.

    :param inner_filename: Name to give the file inside the ZIP.
    :param content: Raw bytes for the inner file.
    :return: ZIP archive bytes.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(inner_filename, content)
    return buf.getvalue()


class TestDetectFormatFromMagic:
    """
    Unit tests for ``_detect_format_from_magic``.
    """

    def test_fit_bytes_detected(self) -> None:
        """
        FIT magic at offset 8–11 returns 'fit'.

        :return: None.
        """
        assert _detect_format_from_magic(_FIT_MAGIC) == "fit"

    def test_tcx_detected(self) -> None:
        """
        XML containing TrainingCenterDatabase root element returns 'tcx'.

        :return: None.
        """
        assert _detect_format_from_magic(_TCX_CONTENT) == "tcx"

    def test_gpx_detected(self) -> None:
        """
        XML containing gpx root element returns 'gpx'.

        :return: None.
        """
        assert _detect_format_from_magic(_GPX_CONTENT) == "gpx"

    def test_kml_detected(self) -> None:
        """
        XML containing kml root element returns 'kml'.

        :return: None.
        """
        assert _detect_format_from_magic(_KML_CONTENT) == "kml"

    def test_unknown_returns_none(self) -> None:
        """
        Arbitrary bytes with no recognisable signature return None.

        :return: None.
        """
        assert _detect_format_from_magic(b"SOME_RANDOM_BYTES") is None

    def test_empty_bytes_returns_none(self) -> None:
        """
        Empty byte string returns None (no magic to inspect).

        :return: None.
        """
        assert _detect_format_from_magic(b"") is None

    def test_fit_magic_at_wrong_offset_not_detected(self) -> None:
        """
        b'.FIT' at offset 0 (not 8) must not be mistaken for a FIT file.

        :return: None.
        """
        content = b".FIT" + b"\x00" * 20
        assert _detect_format_from_magic(content) is None

    def test_content_shorter_than_12_bytes_not_fit(self) -> None:
        """
        Content shorter than 12 bytes cannot satisfy the FIT offset check.

        :return: None.
        """
        short = b"\x00" * 8 + b".FI"  # 11 bytes — one short
        assert _detect_format_from_magic(short) is None


class TestExtractActivityContent:
    """
    Unit tests for ``GarminExtractor._extract_activity_content``.
    """

    @pytest.fixture()
    def extractor(self, tmp_path: Path) -> GarminExtractor:
        """
        Return a GarminExtractor instance pointed at a temp directory.

        :param tmp_path: Pytest tmp_path fixture.
        :return: GarminExtractor instance.
        """
        return GarminExtractor(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            ingest_dir=tmp_path,
        )

    def test_fit_zip_returns_fit_extension(self, extractor: GarminExtractor) -> None:
        """
        ZIP containing a FIT file returns ('fit', content).

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        raw = _make_zip("12345_ACTIVITY.fit", _FIT_MAGIC)
        result = extractor._extract_activity_content(12345, raw)
        assert result is not None
        ext, content = result
        assert ext == "fit"
        assert content == _FIT_MAGIC

    def test_tcx_zip_returns_tcx_extension(self, extractor: GarminExtractor) -> None:
        """
        ZIP containing a TCX file returns ('tcx', content).

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        raw = _make_zip("12345.tcx", _TCX_CONTENT)
        result = extractor._extract_activity_content(12345, raw)
        assert result is not None
        ext, content = result
        assert ext == "tcx"
        assert content == _TCX_CONTENT

    def test_gpx_zip_returns_gpx_extension(self, extractor: GarminExtractor) -> None:
        """
        ZIP containing a GPX file returns ('gpx', content).

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        raw = _make_zip("12345.gpx", _GPX_CONTENT)
        result = extractor._extract_activity_content(12345, raw)
        assert result is not None
        ext, content = result
        assert ext == "gpx"

    def test_empty_zip_returns_none(self, extractor: GarminExtractor) -> None:
        """
        Empty ZIP archive returns None (activity is skipped).

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w"):
            pass
        result = extractor._extract_activity_content(12345, buf.getvalue())
        assert result is None

    def test_non_zip_raw_fit_bytes(self, extractor: GarminExtractor) -> None:
        """
        Non-ZIP bytes that are a valid FIT file are returned as 'fit'.

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        result = extractor._extract_activity_content(12345, _FIT_MAGIC)
        assert result is not None
        ext, content = result
        assert ext == "fit"
        assert content == _FIT_MAGIC

    def test_unknown_magic_falls_back_to_inner_filename_extension(
        self, extractor: GarminExtractor
    ) -> None:
        """
        When magic bytes are inconclusive, the inner filename extension is used.

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        unknown_content = b"UNKNOWN_FORMAT_BYTES"
        raw = _make_zip("activity.tcx", unknown_content)
        result = extractor._extract_activity_content(12345, raw)
        assert result is not None
        ext, content = result
        assert ext == "tcx"
        assert content == unknown_content

    def test_unknown_magic_and_unknown_extension_returns_bin(
        self, extractor: GarminExtractor
    ) -> None:
        """
        Completely unrecognised format is saved as '.bin'.

        :param extractor: GarminExtractor fixture.
        :return: None.
        """
        unknown_content = b"MYSTERY_BYTES"
        raw = _make_zip("activity.xyz", unknown_content)
        result = extractor._extract_activity_content(12345, raw)
        assert result is not None
        ext, _ = result
        assert ext == "bin"


@patch("garmin_health_data.extractor.time.sleep")
class TestExtractFitActivitiesFormat:
    """
    Integration tests verifying that extract_fit_activities saves files with the correct
    extension based on detected content format.
    """

    @pytest.fixture()
    def extractor(self, tmp_path: Path) -> GarminExtractor:
        """
        Return a GarminExtractor pointed at a temp directory.

        :param tmp_path: Pytest tmp_path fixture.
        :return: GarminExtractor instance.
        """
        inst = GarminExtractor(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 1),
            ingest_dir=tmp_path,
        )
        inst.user_id = "123456789"
        return inst

    def _activities(self, activity_id: str = "99999") -> list:
        """
        Return a minimal activity list fixture.

        :param activity_id: Activity ID string.
        :return: List with one activity dict.
        """
        return [
            {
                "activityId": activity_id,
                "startTimeLocal": "2025-01-01T10:00:00.000",
                "activityType": {"typeId": 1, "typeKey": "running"},
            }
        ]

    def test_fit_content_saved_as_fit(
        self, _mock_sleep, extractor: GarminExtractor, tmp_path: Path
    ) -> None:
        """
        FIT content inside ZIP is saved with a .fit extension.

        :param _mock_sleep: Patched sleep.
        :param extractor: GarminExtractor fixture.
        :param tmp_path: Pytest tmp_path fixture.
        :return: None.
        """
        mock_client = MagicMock()
        mock_client.get_activities_by_date.return_value = self._activities()
        mock_client.download_activity.return_value = _make_zip(
            "99999_ACTIVITY.fit", _FIT_MAGIC
        )
        extractor.garmin_client = mock_client

        paths = extractor.extract_fit_activities()

        assert len(paths) == 1
        assert paths[0].suffix == ".fit"
        assert len(list(tmp_path.glob("*.fit"))) == 1

    def test_tcx_content_saved_as_tcx(
        self, _mock_sleep, extractor: GarminExtractor, tmp_path: Path
    ) -> None:
        """
        TCX content inside ZIP is saved with a .tcx extension, not .fit.

        :param _mock_sleep: Patched sleep.
        :param extractor: GarminExtractor fixture.
        :param tmp_path: Pytest tmp_path fixture.
        :return: None.
        """
        mock_client = MagicMock()
        mock_client.get_activities_by_date.return_value = self._activities()
        mock_client.download_activity.return_value = _make_zip(
            "99999.tcx", _TCX_CONTENT
        )
        extractor.garmin_client = mock_client

        paths = extractor.extract_fit_activities()

        assert len(paths) == 1
        assert paths[0].suffix == ".tcx"
        assert len(list(tmp_path.glob("*.fit"))) == 0
        assert len(list(tmp_path.glob("*.tcx"))) == 1


# --------------------------------------------------------------------------------------
# Failure isolation tests
# --------------------------------------------------------------------------------------


def test_extract_day_by_day_isolates_per_date_failures(tmp_path):
    """
    A transient API failure on one date does not abort extraction of subsequent dates.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.constants import GARMIN_DATA_REGISTRY
    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        ingest_dir=tmp_path,
        data_types=("SLEEP",),
    )
    extractor.user_id = "test-user"

    sleep_type = GARMIN_DATA_REGISTRY.get_by_name("SLEEP")
    mock_api = MagicMock(
        side_effect=[
            {"value": "ok-day-1"},
            RuntimeError("transient API hiccup"),
            {"value": "ok-day-3"},
        ]
    )
    extractor.garmin_client = MagicMock()
    setattr(extractor.garmin_client, sleep_type.api_method, mock_api)

    saved = extractor._extract_day_by_day(
        sleep_type, date(2025, 1, 1), date(2025, 1, 3)
    )

    assert len(saved) == 2  # two successes, one failure skipped
    assert mock_api.call_count == 3
    assert any(
        "2025-01-02" in f.error or f.date == "2025-01-02" for f in extractor.failures
    )


def test_extract_garmin_data_isolates_per_data_type_failures(tmp_path):
    """
    A failure inside _extract_data_by_type for one type does not abort the others.
    """
    from datetime import date
    from unittest.mock import MagicMock, patch

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("SLEEP", "HEART_RATE"),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    fake_path = tmp_path / "fake.json"
    fake_path.write_text("{}")

    def fake_extract(data_type, *_):
        if data_type.name == "SLEEP":
            raise RuntimeError("SLEEP endpoint went away")
        return [fake_path]

    with patch.object(extractor, "_extract_data_by_type", side_effect=fake_extract):
        saved = extractor.extract_garmin_data()

    assert len(saved) == 1  # HEART_RATE succeeded
    assert any(f.data_type == "SLEEP" for f in extractor.failures)


def test_extract_fit_activities_handles_list_call_failure(tmp_path):
    """
    If activity-list API call fails, returns empty and records ACTIVITIES_LIST failure.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.side_effect = RuntimeError(
        "list endpoint 500"
    )

    result = extractor.extract_fit_activities()

    assert result == []
    assert any(f.data_type == "ACTIVITIES_LIST" for f in extractor.failures)
    extractor.garmin_client.download_activity.assert_not_called()


def test_extract_fit_activities_isolates_per_activity_failures(tmp_path):
    """
    A non-connection exception during one download does not abort the loop.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.return_value = [
        {
            "activityId": 1,
            "startTimeLocal": "2025-01-01 10:00:00",
            "activityType": {"typeKey": "running"},
        },
        {
            "activityId": 2,
            "startTimeLocal": "2025-01-01 12:00:00",
            "activityType": {"typeKey": "running"},
        },
    ]
    # First raises non-connection; second returns content that fails magic
    # detection -> _extract_activity_content returns None -> continues.
    extractor.garmin_client.download_activity.side_effect = [
        ValueError("boom"),
        b"not-a-fit-or-anything",
    ]

    extractor.extract_fit_activities()

    # Both attempts processed; first recorded as failure.
    assert any(
        f.activity_id == "1" and f.data_type == "ACTIVITY" for f in extractor.failures
    )
    # Loop did not abort: second download_activity call was attempted.
    assert extractor.garmin_client.download_activity.call_count == 2


def test_extract_fit_activities_reads_activities_list_from_disk(tmp_path):
    """
    When an ACTIVITIES_LIST JSON file exists in ingest_dir, the API is NOT called.
    """
    import json
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    list_file = tmp_path / ("test-user_ACTIVITIES_LIST_2025-01-01T12-00-00+00-00.json")
    list_file.write_text(json.dumps([]))

    extractor.extract_fit_activities()

    extractor.garmin_client.get_activities_by_date.assert_not_called()


def test_extract_fit_activities_falls_back_to_api_when_file_missing(tmp_path):
    """
    When no ACTIVITIES_LIST file is in ingest_dir, the API call is used.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.return_value = []

    extractor.extract_fit_activities()

    extractor.garmin_client.get_activities_by_date.assert_called_once()


def test_extract_returns_summary_with_failures(tmp_path):
    """
    Module-level extract() returns failures and failed_accounts in the result dict.
    """
    from unittest.mock import MagicMock, patch

    from garmin_health_data.extractor import (
        ExtractionFailure,
        extract,
    )

    fake_extractor = MagicMock()
    fake_extractor.extract_garmin_data.return_value = []
    fake_extractor.extract_fit_activities.return_value = []
    fake_extractor.failures = [
        ExtractionFailure(
            data_type="SLEEP",
            date="2025-01-02",
            activity_id="",
            error="RuntimeError: hiccup",
        )
    ]

    with (
        patch(
            "garmin_health_data.auth.discover_accounts",
            return_value=[("user-1", tmp_path)],
        ),
        patch(
            "garmin_health_data.extractor.GarminExtractor",
            return_value=fake_extractor,
        ),
    ):
        result = extract(
            ingest_dir=tmp_path,
            data_interval_start="2025-01-01",
            data_interval_end="2025-01-03",
        )

    assert "failures" in result
    assert "failed_accounts" in result
    assert len(result["failures"]) == 1
    assert result["failures"][0].data_type == "SLEEP"
    assert result["failed_accounts"] == []


# --------------------------------------------------------------------------------------
# Retry-with-backoff tests
# --------------------------------------------------------------------------------------


def test_with_retries_succeeds_after_transient_failures(monkeypatch):
    """
    The helper retries on transient errors and returns the eventual success.
    """
    from unittest.mock import MagicMock

    from garmin_health_data import extractor
    from garmin_health_data.garmin_client.exceptions import GarminConnectionError

    # Skip the actual sleep so the test runs instantly.
    monkeypatch.setattr(extractor.time, "sleep", lambda _: None)
    fn = MagicMock(
        side_effect=[
            GarminConnectionError("DNS hiccup 1"),
            GarminConnectionError("DNS hiccup 2"),
            {"ok": True},
        ]
    )

    result = extractor._with_retries(fn, "arg1", kwarg="value")

    assert result == {"ok": True}
    assert fn.call_count == 3
    fn.assert_called_with("arg1", kwarg="value")


def test_with_retries_exhausts_and_reraises(monkeypatch):
    """
    After exhausting all retries the last transient exception is re-raised.
    """
    from unittest.mock import MagicMock

    import pytest

    from garmin_health_data import extractor
    from garmin_health_data.garmin_client.exceptions import GarminConnectionError

    monkeypatch.setattr(extractor.time, "sleep", lambda _: None)

    fn = MagicMock(side_effect=GarminConnectionError("persistent failure"))

    with pytest.raises(GarminConnectionError, match="persistent failure"):
        extractor._with_retries(fn)

    # 1 initial + 3 retries = 4 total attempts.
    assert fn.call_count == 4


def test_with_retries_does_not_retry_non_transient(monkeypatch):
    """
    Non-transient exceptions (e.g. ValueError) propagate immediately.
    """
    from unittest.mock import MagicMock

    import pytest

    from garmin_health_data import extractor

    monkeypatch.setattr(extractor.time, "sleep", lambda _: None)

    fn = MagicMock(side_effect=ValueError("bad input"))

    with pytest.raises(ValueError, match="bad input"):
        extractor._with_retries(fn)

    # Only one attempt - no retries for non-transient exceptions.
    assert fn.call_count == 1


def test_extract_day_by_day_uses_retries_for_per_day_calls(tmp_path, monkeypatch):
    """
    A transient failure on one day is silently retried and absorbed.
    """
    from datetime import date
    from unittest.mock import MagicMock
    from garmin_health_data import extractor
    from garmin_health_data.constants import GARMIN_DATA_REGISTRY
    from garmin_health_data.extractor import GarminExtractor
    from garmin_health_data.garmin_client.exceptions import GarminConnectionError

    monkeypatch.setattr(extractor.time, "sleep", lambda _: None)

    instance = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("SLEEP",),
    )
    instance.user_id = "test-user"

    sleep_type = GARMIN_DATA_REGISTRY.get_by_name("SLEEP")
    mock_api = MagicMock(
        side_effect=[
            GarminConnectionError("transient"),
            GarminConnectionError("still flaky"),
            {"value": "ok"},
        ]
    )
    instance.garmin_client = MagicMock()
    setattr(instance.garmin_client, sleep_type.api_method, mock_api)

    saved = instance._extract_day_by_day(sleep_type, date(2025, 1, 1), date(2025, 1, 1))

    assert len(saved) == 1
    assert mock_api.call_count == 3  # two retries then success
    assert instance.failures == []  # no recorded failure since retry won


def test_load_activities_list_merges_multiple_files_dedupes_by_id(tmp_path):
    """
    Multi-day extracts produce one ACTIVITIES_LIST_<date>.json per day.

    The helper must merge all of them and dedupe by activityId so activities from
    earlier days are not silently dropped (round-4 review fix).
    """
    from datetime import date as date_cls
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date_cls(2025, 1, 1),
        end_date=date_cls(2025, 1, 3),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    # Three separate per-day list files. Activity 100 appears in two of them
    # (e.g. an activity that spans midnight) — should be deduped to one.
    (tmp_path / "test-user_ACTIVITIES_LIST_2025-01-01T12-00-00Z.json").write_text(
        json.dumps([{"activityId": 100, "name": "day1"}])
    )
    (tmp_path / "test-user_ACTIVITIES_LIST_2025-01-02T12-00-00Z.json").write_text(
        json.dumps(
            [
                {"activityId": 100, "name": "day1-dup"},
                {"activityId": 200, "name": "day2"},
            ]
        )
    )
    (tmp_path / "test-user_ACTIVITIES_LIST_2025-01-03T12-00-00Z.json").write_text(
        json.dumps([{"activityId": 300, "name": "day3"}])
    )

    result = extractor._load_activities_list_from_disk()

    # Did NOT call API.
    extractor.garmin_client.get_activities_by_date.assert_not_called()
    # All three distinct activities present, dedupe happened.
    ids = sorted(a["activityId"] for a in result)
    assert ids == [100, 200, 300]


def test_load_activities_list_falls_back_on_corrupt_file(tmp_path):
    """
    A single corrupt file (JSON parse error) returns None so the caller hits the API
    rather than partial-loading from the remaining files.

    Conservative by design — we'd rather refetch than silently miss activities.
    """
    from datetime import date as date_cls
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date_cls(2025, 1, 1),
        end_date=date_cls(2025, 1, 2),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    (tmp_path / "test-user_ACTIVITIES_LIST_2025-01-01T12-00-00Z.json").write_text(
        "{not valid json"
    )
    (tmp_path / "test-user_ACTIVITIES_LIST_2025-01-02T12-00-00Z.json").write_text(
        json.dumps([{"activityId": 200}])
    )

    assert extractor._load_activities_list_from_disk() is None


def test_no_date_api_calls_use_retries(tmp_path, monkeypatch):
    """
    NO_DATE data types (USER_PROFILE, PERSONAL_RECORDS, RACE_PREDICTIONS) must go
    through _with_retries like DAILY/RANGE types do, otherwise a single transient
    network hiccup would fail them on the first attempt.
    """
    from datetime import date as date_cls
    from unittest.mock import MagicMock
    from garmin_health_data import extractor
    from garmin_health_data.constants import GARMIN_DATA_REGISTRY
    from garmin_health_data.extractor import GarminExtractor
    from garmin_health_data.garmin_client.exceptions import GarminConnectionError

    # Skip the actual sleep so the test runs instantly.
    monkeypatch.setattr(extractor.time, "sleep", lambda _: None)

    instance = GarminExtractor(
        start_date=date_cls(2025, 1, 1),
        end_date=date_cls(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("USER_PROFILE",),
    )
    instance.user_id = "test-user"

    user_profile_type = GARMIN_DATA_REGISTRY.get_by_name("USER_PROFILE")
    # NO_DATE call: first two attempts raise transient, third succeeds.
    mock_api = MagicMock(
        side_effect=[
            GarminConnectionError("DNS hiccup"),
            GarminConnectionError("still flaky"),
            {"id": 12345, "displayName": "test"},
        ]
    )
    instance.garmin_client = MagicMock()
    instance.garmin_client.full_name = "Test User"
    setattr(instance.garmin_client, user_profile_type.api_method, mock_api)

    result = instance._extract_data_by_type(
        user_profile_type, date_cls(2025, 1, 1), date_cls(2025, 1, 1)
    )

    # Two retries then success: 3 total attempts, no failure recorded.
    assert mock_api.call_count == 3
    assert len(result) == 1  # one saved file
    assert instance.failures == []

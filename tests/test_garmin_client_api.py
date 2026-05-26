"""
Tests for thin API wrappers in :mod:`garmin_health_data.garmin_client.api`.

Covers normalization of endpoint-specific empty-response shapes that the extractor's
generic ``if data:`` truthiness check would otherwise misclassify as non-empty.
"""

from unittest.mock import MagicMock

from garmin_health_data.garmin_client import api


class TestGetBodyComposition:
    """
    Tests for :func:`api.get_body_composition` empty-response normalization.

    The Garmin weight endpoint returns a populated wrapper dict on no-data days
    (``{startDate, endDate, dateWeightList: [], totalAverage: {...nulls...}}``). Without
    normalization the extractor would write one useless JSON file per day for users
    without scale data, since a non-empty dict is truthy. ``get_body_composition`` must
    collapse that shape to ``None`` so the extractor short-circuits.
    """

    def test_returns_payload_when_weighins_present(self) -> None:
        """
        A populated ``dateWeightList`` must be returned verbatim so the extractor saves
        the file and the processor can map fields downstream.
        """
        payload = {
            "startDate": "2026-04-15",
            "endDate": "2026-04-15",
            "dateWeightList": [
                {
                    "timestampGMT": 1713182400000,
                    "weight": 75300.0,
                    "bmi": 24.5,
                    "sourceType": "INDEX_SCALE",
                }
            ],
            "totalAverage": {"weight": 75300.0},
        }
        client = MagicMock()
        client._connectapi.return_value = payload

        result = api.get_body_composition(client, "2026-04-15")

        assert result is payload
        client._connectapi.assert_called_once_with(
            api.WEIGHT_DATERANGE_URL,
            params={"startDate": "2026-04-15", "endDate": "2026-04-15"},
        )

    def test_returns_none_when_date_weight_list_empty(self) -> None:
        """
        On no-data days the API returns the wrapper dict with an empty
        ``dateWeightList``.

        ``get_body_composition`` must collapse that to ``None`` so the extractor's
        truthiness check skips the file write.
        """
        client = MagicMock()
        client._connectapi.return_value = {
            "startDate": "2026-04-15",
            "endDate": "2026-04-15",
            "dateWeightList": [],
            "totalAverage": {"weight": None, "bmi": None},
        }

        result = api.get_body_composition(client, "2026-04-15")

        assert result is None

    def test_returns_none_when_date_weight_list_missing(self) -> None:
        """
        Defensive: if the endpoint ever omits ``dateWeightList`` entirely, treat that as
        no data rather than letting a wrapper-only dict through.
        """
        client = MagicMock()
        client._connectapi.return_value = {
            "startDate": "2026-04-15",
            "endDate": "2026-04-15",
        }

        result = api.get_body_composition(client, "2026-04-15")

        assert result is None

    def test_returns_none_when_response_is_none(self) -> None:
        """
        If ``_connectapi`` returns ``None`` (e.g. transient empty body), pass that
        through as ``None`` rather than raising.
        """
        client = MagicMock()
        client._connectapi.return_value = None

        result = api.get_body_composition(client, "2026-04-15")

        assert result is None

    def test_default_enddate_matches_startdate(self) -> None:
        """
        When ``enddate`` is omitted, ``startdate`` is used for both bounds (single-day
        query), matching the day-by-day extraction loop in
        :meth:`GarminExtractor._extract_day_by_day`.
        """
        client = MagicMock()
        client._connectapi.return_value = {"dateWeightList": []}

        api.get_body_composition(client, "2026-04-15")

        client._connectapi.assert_called_once_with(
            api.WEIGHT_DATERANGE_URL,
            params={"startDate": "2026-04-15", "endDate": "2026-04-15"},
        )


class TestGetMenstrualDataForDate:
    """
    Tests for :func:`api.get_menstrual_data_for_date` empty-response normalization.

    The Garmin dayview endpoint returns a bare ``{}`` for unlogged days, which the
    extractor's generic truthiness check would otherwise misclassify as non-empty.
    ``get_menstrual_data_for_date`` must collapse that shape to ``None``.
    """

    def test_returns_payload_when_logged(self) -> None:
        """
        A populated dayview response with both ``daySummary`` and ``dayLog`` must be
        returned verbatim so the extractor saves the file and the processor can map
        fields downstream.
        """
        payload = {
            "daySummary": {
                "startDate": "2026-05-23",
                "dayInCycle": 3,
                "periodLength": 3,
                "currentPhase": 1,
                "lengthOfCurrentPhase": 3,
                "daysUntilNextPhase": 1,
                "predictedCycleLength": 28,
                "cycleType": "REGULAR",
                "predictedCycle": False,
            },
            "dayLog": {
                "userProfilePk": 15007510,
                "calendarDate": "2026-05-25",
                "symptoms": ["BACKACHE", "CRAMPS"],
                "moods": ["HAPPY"],
                "discharge": ["CREAMY"],
                "flow": "HEAVY",
                "sexDrive": "AVERAGE",
                "sexualActivity": "PROTECTED",
                "notes": "test",
                "reportTimestamp": "2026-05-25T18:34:49.9",
                "hasBabyMovement": False,
                "ovulationDay": True,
            },
        }
        client = MagicMock()
        client._connectapi.return_value = payload

        result = api.get_menstrual_data_for_date(client, "2026-05-25")

        assert result is payload
        client._connectapi.assert_called_once_with(
            f"{api.MENSTRUAL_DAYVIEW_URL}/2026-05-25"
        )

    def test_returns_payload_with_only_day_summary(self) -> None:
        """
        Garmin can return a daySummary without a dayLog when the day falls in a
        predicted cycle window the user hasn't logged.

        Pass it through; the processor will skip the dayLog scalars.
        """
        payload = {
            "daySummary": {
                "startDate": "2026-05-23",
                "dayInCycle": 5,
                "currentPhase": 2,
                "predictedCycle": True,
            }
        }
        client = MagicMock()
        client._connectapi.return_value = payload

        result = api.get_menstrual_data_for_date(client, "2026-05-27")

        assert result is payload

    def test_returns_none_when_empty_dict(self) -> None:
        """
        Unlogged days outside any cycle window return ``{}``.

        Must collapse to None.
        """
        client = MagicMock()
        client._connectapi.return_value = {}

        result = api.get_menstrual_data_for_date(client, "2026-05-25")

        assert result is None

    def test_returns_none_when_response_is_none(self) -> None:
        """
        Transient empty-body responses pass through as ``None``.
        """
        client = MagicMock()
        client._connectapi.return_value = None

        result = api.get_menstrual_data_for_date(client, "2026-05-25")

        assert result is None

    def test_returns_none_when_only_unknown_keys(self) -> None:
        """
        Defensive: if the endpoint returns a wrapper with neither daySummary nor dayLog,
        treat as no data rather than letting an unknown shape through.
        """
        client = MagicMock()
        client._connectapi.return_value = {"unexpected": "shape"}

        result = api.get_menstrual_data_for_date(client, "2026-05-25")

        assert result is None


class TestGetMenstrualCalendarData:
    """
    Tests for :func:`api.get_menstrual_calendar_data` covering both empty-response
    normalization and the 92-day pagination loop.
    """

    def test_returns_payload_for_single_chunk(self) -> None:
        """
        For a range within the 92-day API limit, the wrapper issues exactly one HTTP
        call and returns the merged payload.
        """
        cycle_payload = {
            "cycleSummaries": [
                {
                    "startDate": "2026-05-23",
                    "periodLength": 3,
                    "predictedCycle": False,
                }
            ],
            "loggedSymptomDays": ["2026-05-25"],
            "loggedOvulationDays": [],
            "loggedNoteDays": ["2026-05-25"],
        }
        client = MagicMock()
        client._connectapi.return_value = cycle_payload

        result = api.get_menstrual_calendar_data(client, "2026-04-01", "2026-05-25")

        assert client._connectapi.call_count == 1
        assert result is not None
        assert result["cycleSummaries"] == cycle_payload["cycleSummaries"]
        assert result["loggedSymptomDays"] == ["2026-05-25"]

    def test_paginates_when_range_exceeds_92_days(self) -> None:
        """
        Ranges longer than 92 days are split into contiguous sub-windows.

        Verify the wrapper issues multiple HTTP calls with non-overlapping date pairs
        that fully cover the requested range.
        """
        client = MagicMock()
        client._connectapi.return_value = {"cycleSummaries": []}

        api.get_menstrual_calendar_data(client, "2026-01-01", "2026-06-30")

        # Six months ~= 181 days, needs >=3 chunks at 92 days each.
        assert client._connectapi.call_count >= 2
        called_urls = [c.args[0] for c in client._connectapi.call_args_list]
        # First chunk starts at requested start.
        assert called_urls[0].endswith("/2026-01-01/2026-04-02")
        # Chunks are contiguous (each start == prior end + 1 day).
        # Last chunk ends at requested end.
        assert called_urls[-1].endswith("/2026-06-30")

    def test_merges_cycle_summaries_dedup_by_start_date(self) -> None:
        """
        A cycle that straddles a chunk boundary may appear in adjacent chunks.

        The wrapper must dedupe on startDate so the result contains one entry per cycle.
        """
        chunk_one = {
            "cycleSummaries": [
                {"startDate": "2026-03-01", "periodLength": 5, "predictedCycle": False}
            ],
            "loggedSymptomDays": [],
            "loggedOvulationDays": [],
            "loggedNoteDays": [],
        }
        chunk_two = {
            "cycleSummaries": [
                # Same cycle reappears in second chunk.
                {"startDate": "2026-03-01", "periodLength": 5, "predictedCycle": False},
                {"startDate": "2026-05-23", "periodLength": 3, "predictedCycle": False},
            ],
            "loggedSymptomDays": [],
            "loggedOvulationDays": [],
            "loggedNoteDays": [],
        }
        client = MagicMock()
        client._connectapi.side_effect = [chunk_one, chunk_two]

        result = api.get_menstrual_calendar_data(client, "2026-01-01", "2026-05-25")

        assert result is not None
        starts = [c["startDate"] for c in result["cycleSummaries"]]
        assert starts == ["2026-03-01", "2026-05-23"]

    def test_merges_logged_day_arrays_across_chunks(self) -> None:
        """
        Logged-day arrays merge into sorted unique lists across chunks.
        """
        chunk_one = {
            "cycleSummaries": [
                {"startDate": "2026-03-01", "periodLength": 5, "predictedCycle": False}
            ],
            "loggedSymptomDays": ["2026-03-02"],
            "loggedOvulationDays": [],
            "loggedNoteDays": ["2026-03-04"],
        }
        chunk_two = {
            "cycleSummaries": [
                {"startDate": "2026-05-23", "periodLength": 3, "predictedCycle": False}
            ],
            "loggedSymptomDays": ["2026-05-25", "2026-03-02"],
            "loggedOvulationDays": ["2026-05-25"],
            "loggedNoteDays": [],
        }
        client = MagicMock()
        client._connectapi.side_effect = [chunk_one, chunk_two]

        result = api.get_menstrual_calendar_data(client, "2026-01-01", "2026-05-25")

        assert result is not None
        assert result["loggedSymptomDays"] == ["2026-03-02", "2026-05-25"]
        assert result["loggedOvulationDays"] == ["2026-05-25"]
        assert result["loggedNoteDays"] == ["2026-03-04"]

    def test_returns_none_when_no_cycles(self) -> None:
        """
        All chunks empty -> None.
        """
        client = MagicMock()
        client._connectapi.return_value = {
            "cycleSummaries": [],
            "loggedSymptomDays": [],
            "loggedOvulationDays": [],
            "loggedNoteDays": [],
        }

        result = api.get_menstrual_calendar_data(client, "2026-04-01", "2026-05-25")

        assert result is None

    def test_returns_none_when_all_chunks_return_none(self) -> None:
        """
        Defensive: all-None responses across all chunks treated as no data.
        """
        client = MagicMock()
        client._connectapi.return_value = None

        result = api.get_menstrual_calendar_data(client, "2026-04-01", "2026-05-25")

        assert result is None

    def test_default_enddate_matches_startdate(self) -> None:
        """
        When ``enddate`` is omitted, ``startdate`` is used for both bounds.
        """
        client = MagicMock()
        client._connectapi.return_value = {"cycleSummaries": []}

        api.get_menstrual_calendar_data(client, "2026-05-25")

        client._connectapi.assert_called_once()
        url = client._connectapi.call_args.args[0]
        assert url.endswith("/2026-05-25/2026-05-25")

    def test_raises_on_backwards_range(self) -> None:
        """
        Enddate before startdate must raise rather than silently looping forever.
        """
        import pytest

        client = MagicMock()
        with pytest.raises(ValueError, match="on or after"):
            api.get_menstrual_calendar_data(client, "2026-05-25", "2026-05-01")

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

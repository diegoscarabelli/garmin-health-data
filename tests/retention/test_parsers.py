"""
Tests for ``garmin_health_data.retention.parsers``.

Covers the :class:`TimeGrain` and :class:`Duration` Click param types and the
:func:`resolve_range` helper that mirrors the extract command's date-range semantics.
"""

from datetime import date, datetime

import click
import pytest
from dateutil.relativedelta import relativedelta

from garmin_health_data.retention.parsers import (
    DURATION,
    TIME_GRAIN,
    Duration,
    TimeGrain,
    resolve_range,
)


# ---------------------------------------------------------------------------
# TimeGrain.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected_seconds",
    [
        ("1s", 1),
        ("30s", 30),
        ("60s", 60),
        ("1m", 60),
        ("2m", 120),
        ("5m", 300),
        ("15m", 900),
        ("60m", 3600),
    ],
)
def test_time_grain_accepts_valid_values(raw: str, expected_seconds: int) -> None:
    """
    Valid grain strings convert to the expected integer second count.
    """
    assert TIME_GRAIN.convert(raw, None, None) == expected_seconds


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "0s",
        "0m",
        "1h",
        "90",
        "1.5m",
        "05s",
        "-1s",
        "abc",
        "1ss",
    ],
)
def test_time_grain_rejects_invalid_values(raw: str) -> None:
    """
    Invalid grain strings raise :class:`click.BadParameter`.
    """
    with pytest.raises(click.BadParameter):
        TIME_GRAIN.convert(raw, None, None)


def test_time_grain_module_constant_is_time_grain_instance() -> None:
    """
    ``TIME_GRAIN`` is a ready-to-use :class:`TimeGrain` singleton.
    """
    assert isinstance(TIME_GRAIN, TimeGrain)
    assert TIME_GRAIN.name == "time_grain"


def test_time_grain_passes_through_pre_converted_int() -> None:
    """
    Pre-converted integers (e.g. programmatic defaults) round-trip unchanged.
    """
    assert TIME_GRAIN.convert(300, None, None) == 300


# ---------------------------------------------------------------------------
# Duration.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1d", relativedelta(days=1)),
        ("7d", relativedelta(days=7)),
        ("30d", relativedelta(days=30)),
        ("90d", relativedelta(days=90)),
        ("1m", relativedelta(months=1)),
        ("6m", relativedelta(months=6)),
        ("12m", relativedelta(months=12)),
        ("1y", relativedelta(years=1)),
        ("2y", relativedelta(years=2)),
    ],
)
def test_duration_accepts_valid_values(raw: str, expected: relativedelta) -> None:
    """
    Valid duration strings convert to the expected :class:`relativedelta`.
    """
    assert DURATION.convert(raw, None, None) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "0d",
        "0m",
        "0y",
        "1h",
        "90",
        "1.5m",
        "05d",
        "-1d",
        "abc",
        "1dd",
    ],
)
def test_duration_rejects_invalid_values(raw: str) -> None:
    """
    Invalid duration strings raise :class:`click.BadParameter`.
    """
    with pytest.raises(click.BadParameter):
        DURATION.convert(raw, None, None)


def test_duration_subtraction_day_arithmetic() -> None:
    """
    Subtracting a day-based duration produces the expected calendar date.
    """
    today = date(2026, 3, 15)
    assert today - DURATION.convert("1d", None, None) == date(2026, 3, 14)
    assert today - DURATION.convert("30d", None, None) == date(2026, 2, 13)


def test_duration_subtraction_month_wrap() -> None:
    """
    Subtracting a month-based duration walks the calendar correctly.
    """
    today = date(2026, 3, 15)
    assert today - DURATION.convert("1m", None, None) == date(2026, 2, 15)
    assert today - DURATION.convert("6m", None, None) == date(2025, 9, 15)
    assert today - DURATION.convert("12m", None, None) == date(2025, 3, 15)


def test_duration_subtraction_year_wrap() -> None:
    """
    Subtracting a year-based duration walks across the year boundary.
    """
    today = date(2026, 3, 15)
    assert today - DURATION.convert("1y", None, None) == date(2025, 3, 15)
    assert today - DURATION.convert("2y", None, None) == date(2024, 3, 15)


def test_duration_module_constant_is_duration_instance() -> None:
    """
    ``DURATION`` is a ready-to-use :class:`Duration` singleton.
    """
    assert isinstance(DURATION, Duration)
    assert DURATION.name == "duration"


def test_duration_passes_through_pre_converted_relativedelta() -> None:
    """
    Pre-converted :class:`relativedelta` values round-trip unchanged.
    """
    rd = relativedelta(days=42)
    assert DURATION.convert(rd, None, None) is rd


# ---------------------------------------------------------------------------
# resolve_range.
# ---------------------------------------------------------------------------


def test_resolve_range_regular_window() -> None:
    """
    A regular start/end window returns a half-open range of midnights.
    """
    start_dt, end_dt = resolve_range(date(2026, 1, 1), date(2026, 1, 10))
    assert start_dt == datetime(2026, 1, 1)
    assert end_dt == datetime(2026, 1, 10)


def test_resolve_range_same_day_special_case() -> None:
    """
    When ``start == end``, the single day is included in the range.
    """
    start_dt, end_dt = resolve_range(date(2026, 1, 5), date(2026, 1, 5))
    assert start_dt == datetime(2026, 1, 5)
    assert end_dt == datetime(2026, 1, 6)


def test_resolve_range_no_start() -> None:
    """
    When ``start`` is ``None``, the range begins at :attr:`datetime.min`.
    """
    start_dt, end_dt = resolve_range(None, date(2026, 1, 10))
    assert start_dt == datetime.min
    assert end_dt == datetime(2026, 1, 10)


def test_resolve_range_returns_naive_midnights() -> None:
    """
    Returned datetimes have no tzinfo and a midnight time component.
    """
    start_dt, end_dt = resolve_range(date(2026, 6, 1), date(2026, 6, 2))
    for value in (start_dt, end_dt):
        assert value.tzinfo is None
        assert value.hour == 0
        assert value.minute == 0
        assert value.second == 0
        assert value.microsecond == 0

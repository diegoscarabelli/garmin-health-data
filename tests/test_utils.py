"""
Tests for utility functions.
"""

from datetime import date


from garmin_health_data.utils import (
    format_count,
    format_date,
    format_duration,
    format_file_size,
    parse_date,
)


def test_parse_date_valid():
    """
    Test parsing valid date string.
    """
    result = parse_date("2024-01-15")
    assert result == date(2024, 1, 15)


def test_parse_date_none():
    """
    Test parsing None returns None.
    """
    result = parse_date(None)
    assert result is None


def test_format_date():
    """
    Test formatting date object.
    """
    d = date(2024, 1, 15)
    assert format_date(d) == "2024-01-15"


def test_format_file_size():
    """
    Test formatting file sizes.
    """
    assert format_file_size(500) == "500.0 B"
    assert format_file_size(1024) == "1.0 KB"
    assert format_file_size(1024 * 1024) == "1.0 MB"
    assert format_file_size(1024 * 1024 * 1024) == "1.0 GB"


def test_format_duration():
    """
    Test formatting duration.
    """
    assert format_duration(30) == "30s"
    assert format_duration(90) == "2m"
    assert format_duration(3600) == "1h 0m"
    assert format_duration(7200) == "2h 0m"


def test_format_count():
    """
    Test formatting numbers with separators.
    """
    assert format_count(1000) == "1,000"
    assert format_count(1000000) == "1,000,000"
    assert format_count(42) == "42"

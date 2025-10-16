"""
Shared utility functions for garmin-health-data.
"""

import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import click


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """
    Parse a date string in YYYY-MM-DD format.

    :param date_str: Date string to parse.
    :return: date object or None if date_str is None.
    """
    if date_str is None:
        return None

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise click.ClickException(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD format."
        )


def format_date(d: date) -> str:
    """
    Format a date object as YYYY-MM-DD string.

    :param d: Date object to format.
    :return: Formatted date string.
    """
    return d.strftime("%Y-%m-%d")


def get_temp_dir() -> Path:
    """
    Get a temporary directory for file extraction.

    :return: Path to temporary directory.
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="garmin_"))
    return temp_dir


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in bytes to human-readable string.

    :param size_bytes: Size in bytes.
    :return: Formatted size string (e.g., "1.5 MB").
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.

    :param seconds: Duration in seconds.
    :return: Formatted duration string (e.g., "2h 15m").
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.0f}m"
    else:
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        return f"{hours:.0f}h {minutes:.0f}m"


def format_count(count: int) -> str:
    """
    Format large numbers with thousands separators.

    :param count: Number to format.
    :return: Formatted number string (e.g., "1,234,567").
    """
    return f"{count:,}"

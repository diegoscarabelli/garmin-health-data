"""
Click param types and date-range helpers for retention CLI commands.

This module provides the input-parsing primitives shared by the prune,
downsample, and extract automation flags:

- :class:`TimeGrain` parses bucket grain strings (e.g. ``"5m"``, ``"60s"``)
  into integer seconds.
- :class:`Duration` parses age-of-data strings (e.g. ``"30d"``, ``"6m"``,
  ``"1y"``) into :class:`dateutil.relativedelta.relativedelta` instances.
- :func:`resolve_range` mirrors the ``extract`` command's date-range
  semantics so prune and downsample behave identically.
"""

import re
from datetime import date, datetime, timedelta
from typing import Optional

import click
from dateutil.relativedelta import relativedelta


# Format: integer (no leading zero) followed by a single-character unit.
_TIME_GRAIN_RE = re.compile(r"^([1-9][0-9]*)(s|m)$")
_DURATION_RE = re.compile(r"^([1-9][0-9]*)([dmy])$")


class TimeGrain(click.ParamType):
    """
    Parse a bucket grain string into integer seconds.

    Accepted formats are an integer (no leading zero) followed by a single
    unit character:

    - ``s`` for seconds (e.g. ``"30s"`` -> ``30``).
    - ``m`` for minutes (e.g. ``"5m"`` -> ``300``).

    Zero, decimals, hours, and any other suffix are rejected.
    """

    name = "time_grain"

    def convert(
        self,
        value: object,
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> int:
        """
        Convert a raw CLI value into integer seconds.

        :param value: Raw value supplied by Click (already an ``int`` if the caller
            passed a programmatic default).
        :param param: The Click parameter being parsed (forwarded to ``fail``).
        :param ctx: The active Click context (forwarded to ``fail``).
        :return: Bucket grain in seconds.
        """
        # Pass through pre-converted integers (Click reuses convert for
        # programmatic defaults).
        if isinstance(value, int) and not isinstance(value, bool):
            return value

        if not isinstance(value, str) or not value:
            self.fail(
                "time grain must be a non-empty string like '30s' or '5m'.",
                param,
                ctx,
            )

        match = _TIME_GRAIN_RE.match(value)
        if not match:
            self.fail(
                f"{value!r} is not a valid time grain. Use a positive "
                "integer followed by 's' (seconds) or 'm' (minutes), "
                "e.g. '30s' or '5m'.",
                param,
                ctx,
            )

        amount = int(match.group(1))
        unit = match.group(2)
        if unit == "s":
            return amount
        # unit == "m".
        return amount * 60


class Duration(click.ParamType):
    """
    Parse an age-of-data string into a :class:`relativedelta`.

    Accepted formats are an integer (no leading zero) followed by a single
    unit character:

    - ``d`` for days (e.g. ``"30d"``).
    - ``m`` for months (e.g. ``"6m"``).
    - ``y`` for years (e.g. ``"1y"``).

    The returned :class:`relativedelta` can be subtracted from a
    :class:`datetime.date` to compute a cutoff that respects calendar
    month and year boundaries.
    """

    name = "duration"

    def convert(
        self,
        value: object,
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> relativedelta:
        """
        Convert a raw CLI value into a :class:`relativedelta`.

        :param value: Raw value supplied by Click.
        :param param: The Click parameter being parsed (forwarded to ``fail``).
        :param ctx: The active Click context (forwarded to ``fail``).
        :return: A :class:`relativedelta` representing the requested age.
        """
        if isinstance(value, relativedelta):
            return value

        if not isinstance(value, str) or not value:
            self.fail(
                "duration must be a non-empty string like '30d', '6m', " "or '1y'.",
                param,
                ctx,
            )

        match = _DURATION_RE.match(value)
        if not match:
            self.fail(
                f"{value!r} is not a valid duration. Use a positive "
                "integer followed by 'd' (days), 'm' (months), or 'y' "
                "(years), e.g. '30d', '6m', '1y'.",
                param,
                ctx,
            )

        amount = int(match.group(1))
        unit = match.group(2)
        if unit == "d":
            return relativedelta(days=amount)
        if unit == "m":
            return relativedelta(months=amount)
        # unit == "y".
        return relativedelta(years=amount)


# Module-level singletons so callers can use ``type=TIME_GRAIN`` /
# ``type=DURATION`` directly in ``click.option`` declarations.
TIME_GRAIN = TimeGrain()
DURATION = Duration()


def resolve_range(start: Optional[date], end: date) -> tuple[datetime, datetime]:
    """
    Convert ``--start`` / ``--end`` dates into a half-open datetime range.

    Mirrors the semantics used by the ``extract`` command (see
    ``cli.py`` and ``extractor.py`` around the ``original_end_date`` /
    ``end_date`` handling):

    - ``end`` is required and treated as exclusive: data on this date is
      not in range.
    - ``start`` is optional and inclusive.
    - Same-day special case: when ``start == end``, the single day is
      included by returning ``[start_dt, start_dt + 1 day)``, which
      avoids an empty range and matches the extract command's
      "inclusive logic for same-day" branch.
    - When ``start`` is ``None``, returns ``(datetime.min, end_dt)`` so
      callers can scope to "everything strictly before ``end``".

    Returned datetimes are naive midnight values (no ``tzinfo``).

    :param start: Inclusive start date, or ``None`` to mean "no lower bound".
    :param end: Exclusive end date (with the same-day exception described
        above).
    :return: Tuple ``(start_dt, end_dt)`` defining the half-open range
        ``[start_dt, end_dt)``.
    """
    if start is None:
        return datetime.min, datetime.combine(end, datetime.min.time())

    start_dt = datetime.combine(start, datetime.min.time())

    if start == end:
        # Same-day special case: include the single requested day so the
        # range is non-empty, matching extract's inclusive same-day branch.
        end_dt = start_dt + timedelta(days=1)
    else:
        end_dt = datetime.combine(end, datetime.min.time())

    return start_dt, end_dt

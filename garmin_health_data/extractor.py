"""
Garmin Connect data extraction module for standalone use.

Extracts activity files and JSON Garmin data from Garmin Connect API and saves them to
the ingest directory. Designed for standalone applications without Apache Airflow
dependencies.
"""

import json
import socket
import time
import zipfile
import io

from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import click
import pendulum
import requests
from garmin_health_data.garmin_client import ActivityDownloadFormat, GarminClient
from garmin_health_data.garmin_client.exceptions import GarminConnectionError

from garmin_health_data.constants import (
    APIMethodTimeParam,
    GarminDataType,
    GARMIN_DATA_REGISTRY,
)

# Exception classes considered transient (worth retrying with backoff). All
# represent network or transport-level failures that typically self-heal in
# seconds. Application-level errors (parse failures, ValueError, etc.) are
# excluded because retries won't help.
_TRANSIENT_API_EXCEPTIONS: tuple = (
    GarminConnectionError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    socket.gaierror,
)

# Backoff schedule applied before each retry attempt. With three entries the
# call gets a total of four chances (1 initial + 3 retries) over ~40s in the
# worst case, comfortably absorbing typical DNS hiccups and brief outages.
_RETRY_BACKOFFS: tuple = (2.0, 8.0, 30.0)


def _with_retries(fn: Callable, *args, **kwargs):
    """
    Call ``fn(*args, **kwargs)`` with exponential-backoff retries on transient network
    errors.

    Retries only on the connection / DNS / timeout exception classes listed in
    ``_TRANSIENT_API_EXCEPTIONS``. Other exceptions propagate immediately so the
    caller's broader try/except (per-date isolation in
    :meth:`GarminExtractor._extract_day_by_day`, per-window isolation in
    :meth:`GarminExtractor._extract_range`, or per-activity isolation in
    :meth:`GarminExtractor.extract_fit_activities`) can record the failure once and move
    on.

    :param fn: Callable to invoke.
    :param args: Positional arguments forwarded to ``fn``.
    :param kwargs: Keyword arguments forwarded to ``fn``.
    :return: Whatever ``fn`` returns on success.
    :raises Exception: The last transient exception if all retries are exhausted, or any
        non-transient exception immediately.
    """
    total_attempts = 1 + len(_RETRY_BACKOFFS)
    last_exc: Optional[BaseException] = None
    for attempt in range(total_attempts):
        try:
            return fn(*args, **kwargs)
        except _TRANSIENT_API_EXCEPTIONS as e:
            last_exc = e
            if attempt < len(_RETRY_BACKOFFS):
                backoff = _RETRY_BACKOFFS[attempt]
                click.secho(
                    f"⏳ Transient error ({type(e).__name__}: {e}); "
                    f"retrying in {backoff:.0f}s "
                    f"(attempt {attempt + 2}/{total_attempts})...",
                    fg="yellow",
                )
                time.sleep(backoff)
    # All attempts exhausted; re-raise the last transient exception. The
    # explicit guard satisfies type checkers (last_exc is Optional in the
    # type system but is always set here because we only reach this line
    # after at least one except branch ran).
    if last_exc is None:
        raise RuntimeError(
            "_with_retries exhausted attempts without capturing an exception "
            "(should be unreachable)"
        )
    raise last_exc


@dataclass
class ExtractionFailure:
    """
    A single extraction failure recorded by the extractor for end-of-run reporting.

    :ivar data_type: Garmin data type name (e.g. ``"SLEEP"``, ``"ACTIVITY"``).
    :ivar date: Date context for the failure. Typically an ISO date string (``"YYYY-MM-
        DD"``) for per-day (DAILY-type) failures, a range string ``"<start>..<end>"``
        for any RANGE-type failure (the API call is per-window, so the failure label
        covers the whole window even when the response would have been split into per-
        day files on success), or ``""`` when no date context applies (per-data-type or
        per-activity failures).
    :ivar activity_id: Activity ID as a string for per-activity failures, or ``""``
        otherwise.
    :ivar error: Human-readable error description (typically ``"<ExceptionType>:
        <message>"``).
    """

    data_type: str
    date: str
    activity_id: str
    error: str


# Data types whose already-extracted past days can change retroactively when the user
# edits history in Garmin Connect (e.g. moving a menstrual period start recomputes
# ``dayInCycle`` for every following day). Such types must re-fetch a trailing window on
# every run so the upsert refreshes stale rows, not just the incremental slice. Maps the
# data type name to its look-back length in days. 90 days comfortably covers a full
# cycle's worth of cascade (a retroactive edit can reach back to the previous cycle
# start) with margin for irregular cycles.
_RETROACTIVE_LOOKBACK_DAYS: Dict[str, int] = {
    "MENSTRUAL_CYCLE_DAY": 90,
}


def _retroactive_lookback_start(
    data_type: GarminDataType, start_date: date, end_date: date
) -> date:
    """
    Extend ``start_date`` backward for data types that need a retroactive-edit refresh.

    For a data type registered in ``_RETROACTIVE_LOOKBACK_DAYS``, returns the earlier of
    the requested ``start_date`` and ``end_date - lookback`` so each run re-fetches a
    trailing window. The result only ever moves the start *earlier*, never later, so an
    explicit wider range (e.g. a full-history backfill) is left untouched. Data types not
    in the registry are returned unchanged.

    :param data_type: The data type being extracted.
    :param start_date: The requested (e.g. incremental) start date.
    :param end_date: The extraction end date.
    :return: The effective start date to extract from.
    """
    lookback = _RETROACTIVE_LOOKBACK_DAYS.get(data_type.name)
    if lookback is None:
        return start_date
    return min(start_date, end_date - timedelta(days=lookback))


def _weighin_has_numeric_timestamp(entry: Dict[str, Any]) -> bool:
    """
    Report whether a weigh-in entry has a numeric epoch-ms timestamp.

    Mirrors how ``_process_body_composition`` reads the timestamp (``timestampGMT``
    falling back to ``date``) and does arithmetic on it (``ts_ms / 1000``). An entry
    this rejects is exactly one the processor would crash on (TypeError / OverflowError
    / OSError), quarantining the whole per-day FileSet. ``bool`` is excluded because it
    is an ``int`` subclass but never a real timestamp.

    :param entry: A single ``allWeightMetrics`` weigh-in dict.
    :return:``True`` if the timestamp is a usable ``int``/``float``.
    """
    ts = entry.get("timestampGMT") or entry.get("date")
    return isinstance(ts, (int, float)) and not isinstance(ts, bool)


def _split_body_composition_by_day(
    payload: Dict[str, Any],
) -> Dict[date, Dict[str, Any]]:
    """
    Split a ``BODY_COMPOSITION`` per-range response into per-day payloads.

    The range endpoint (``/weight-service/weight/range/{start}/{end}?includeAll=true``)
    returns one ``dailyWeightSummaries`` entry per local calendar day that has data. Each
    summary carries *every* weigh-in for that day under ``allWeightMetrics`` (a user may
    weigh more than once per day) and the user-facing local day under ``summaryDate``.

    Groups by Garmin's own ``summaryDate`` rather than by each weigh-in's UTC
    ``timestampGMT`` so that two weigh-ins on the same local day land in one per-day file
    even when their UTC timestamps straddle midnight (e.g. a late-evening weigh-in in a
    timezone behind UTC). This mirrors the ``ACTIVITIES_LIST`` splitter, which groups by
    the user's local date for the same reason. Each per-day payload is re-wrapped under
    the ``dateWeightList`` key so the downstream body-composition processor parses it
    identically to the legacy per-day API responses; the ``allWeightMetrics`` entries
    carry the same field names (``timestampGMT``, ``weight``, ``bmi``, ...) the processor
    reads. The range-level wrapper fields (``totalAverage``, ``previousDateWeight``,
    ``nextDateWeight``) are intentionally dropped: they describe the full range and are
    not consumed by the processor.

    Malformed summaries are skipped rather than aborting the whole range split: a
    non-dict summary, a non-list ``allWeightMetrics``, or an unparseable
    ``summaryDate`` cannot anchor a per-day file and never reaches the processor.
    Weigh-in entries that are non-dict or lack a numeric timestamp are filtered out
    (the processor reads each via ``entry.get(...)`` and divides the timestamp); a
    summary is skipped once no usable entries remain.

    :param payload: Full range response from ``get_body_composition``.
    :return: Mapping ``{date: {"dateWeightList": [...that day's weigh-ins...]}}``.
    """
    by_day: Dict[date, List[Dict[str, Any]]] = {}
    for summary in payload.get("dailyWeightSummaries") or []:
        # Type-guard each summary so a single malformed entry (non-dict summary,
        # or ``allWeightMetrics`` that isn't a list) is skipped rather than
        # aborting the whole range split, per the "skipped" contract above.
        if not isinstance(summary, dict):
            continue
        metrics = summary.get("allWeightMetrics")
        if not isinstance(metrics, list):
            continue
        # Keep only dict weigh-in entries with a numeric timestamp: the processor
        # reads each via ``entry.get(...)`` and divides the timestamp, so a non-dict
        # element or a missing/non-numeric timestamp would crash it and quarantine the
        # whole FileSet. Skip the summary if nothing usable remains.
        metrics = [
            m
            for m in metrics
            if isinstance(m, dict) and _weighin_has_numeric_timestamp(m)
        ]
        if not metrics:
            continue
        summary_date = summary.get("summaryDate")
        if not isinstance(summary_date, str):
            continue
        try:
            # ``summaryDate`` is the local calendar day as ``YYYY-MM-DD``.
            day = date.fromisoformat(summary_date[:10])
        except ValueError:
            continue
        by_day.setdefault(day, []).extend(metrics)
    return {d: {"dateWeightList": entries} for d, entries in by_day.items()}


def _split_activities_list_by_day(
    payload: List[Dict[str, Any]],
) -> Dict[date, List[Dict[str, Any]]]:
    """
    Split an ``ACTIVITIES_LIST`` per-range response into per-day payloads.

    Each activity carries ``startTimeLocal`` (the wall clock the user perceives). Groups
    activities by that local date so each per-day file mirrors what the legacy per-day
    API response looked like for that user-facing day. Activities missing a parseable
    ``startTimeLocal`` are skipped.

    :param payload: Full range response from ``get_activities_by_date`` (a list).
    :return: Mapping ``{date: [...that day's activities...]}``.
    """
    by_day: Dict[date, List[Dict[str, Any]]] = {}
    for activity in payload or []:
        start_local = activity.get("startTimeLocal")
        if not isinstance(start_local, str):
            continue
        try:
            # Garmin returns e.g. "2026-05-15T10:00:00.0"; the date prefix is
            # all we need so we don't run the full fractional-seconds parser.
            activity_date = date.fromisoformat(start_local[:10])
        except ValueError:
            continue
        by_day.setdefault(activity_date, []).append(activity)
    return by_day


def _split_running_tolerance_by_day(
    payload: Optional[List[Dict[str, Any]]],
) -> Dict[date, List[Dict[str, Any]]]:
    """
    Split a ``RUNNING_TOLERANCE`` per-range response into per-day payloads.

    The ``aggregation=daily`` endpoint returns a list of per-day objects, each carrying
    a ``calendarDate``. Groups rows by that calendar date so each per-day file mirrors
    the per-day ``(user, day)`` FileSet abstraction the processor expects. Rows that are
    non-dict or lack a parseable ``calendarDate`` are skipped.

    :param payload: Full range response from ``get_running_tolerance`` (a list).
    :return: Mapping ``{date: [...that day's running-tolerance rows...]}``.
    """
    by_day: Dict[date, List[Dict[str, Any]]] = {}
    for row in payload or []:
        if not isinstance(row, dict):
            continue
        cal = row.get("calendarDate")
        if not isinstance(cal, str):
            continue
        try:
            row_date = date.fromisoformat(cal[:10])
        except ValueError:
            continue
        by_day.setdefault(row_date, []).append(row)
    return by_day


# Registry of per-day splitters: data types listed here have their per-range API
# responses split into one file per day before write. RANGE types not listed
# write one file stamped with end_date (current behavior — appropriate when the
# data is per-range rather than per-day, e.g. menstrual cycle summaries where
# the processor's wipe-and-replace policy needs atomicity across all cycles).
_PER_DAY_SPLITTERS: Dict[str, Callable[[Any], Dict[date, Any]]] = {
    "BODY_COMPOSITION": _split_body_composition_by_day,
    "ACTIVITIES_LIST": _split_activities_list_by_day,
    "RUNNING_TOLERANCE": _split_running_tolerance_by_day,
}


# File extensions that the downstream processor knows how to route.
# Used as a fallback when magic-byte detection is inconclusive.
_KNOWN_ACTIVITY_EXTENSIONS: frozenset[str] = frozenset({"fit", "tcx", "gpx", "kml"})


def _detect_format_from_magic(content: bytes) -> Optional[str]:
    """
    Detect activity file format from magic bytes.

    Uses format-specific byte signatures rather than filename extensions,
    which are not guaranteed to be accurate for Garmin's download service.
    Returns ``None`` for unrecognised content so the caller can apply a
    fallback strategy.

    Detection rules:
    - FIT: bytes 8–11 equal ``b'.FIT'`` (ANT+ FIT protocol header).
    - TCX: XML content containing ``<TrainingCenterDatabase``.
    - GPX: XML content containing ``<gpx``.
    - KML: XML content containing ``<kml``.

    :param content: Raw bytes of the activity file.
    :return: Lowercase file extension (``'fit'``, ``'tcx'``, ``'gpx'``,
        ``'kml'``), or ``None`` if the format cannot be identified.
    """
    # FIT: ANT+ FIT protocol magic bytes at offset 8–11.
    if len(content) >= 12 and content[8:12] == b".FIT":
        return "fit"

    # XML-based formats: inspect a small header prefix for the root element.
    text_head = content[:512].decode("utf-8", errors="ignore")
    if "<TrainingCenterDatabase" in text_head:
        return "tcx"
    if "<gpx" in text_head:
        return "gpx"
    if "<kml" in text_head:
        return "kml"

    return None


class GarminExtractor:
    """
    Handles Garmin Connect data extraction with shared state and methods.

    Downloads FIT activity files and JSON Garmin data from Garmin Connect for
    the specified date range. Files are saved with standardized naming
    conventions to the ingest directory for downstream processing.

    Authentication uses pre-existing tokens. If authentication fails, run
    refresh_garmin_tokens.py to obtain fresh tokens.

    The extraction includes:
    - FIT activity files (binary format).
    - Garmin data (JSON format): sleep, HRV, stress, body battery,
      respiration, SpO2, heart rate, resting heart rate, training metrics,
      steps, floors, etc.
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        ingest_dir: Path,
        data_types: Optional[List[str]] = None,
    ) -> None:
        """
        Initialize the Garmin extractor with date range and target directory.

        :param start_date: Start date for data extraction (inclusive).
        :param end_date: End date for data extraction (inclusive).
        :param ingest_dir: Directory to save extracted files.
        :param data_types: Optional list of data type names to extract (e.g., ['SLEEP',
            'HRV']). If None, extracts all available data types.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.ingest_dir = ingest_dir
        self.data_types = data_types
        self.garmin_client = None
        self.user_id = None
        self.failures: List[ExtractionFailure] = []

    def authenticate(self, token_store_dir: str = "~/.garminconnect") -> None:
        """
        Authenticate with Garmin Connect using pre-existing tokens.

        This function relies on OAuth tokens that have been previously saved
        by the ``garmin auth`` command. The library automatically handles
        token validation and session management once valid tokens are present.

        Sets both self.garmin_client and self.user_id upon successful
        authentication.

        Token Lifecycle:
        - Tokens are stored in ~/.garminconnect/<user_id>/ per account.
        - Access tokens (~18h) are auto-refreshed using the refresh token
          (30 days).
        - Refresh tokens rotate on each use; updated tokens are persisted
          to disk.
        - No credentials (email/password) required once valid tokens exist.

        When to run ``garmin auth``:
        - Initial setup (no tokens exist).
        - Idle for 30+ days (refresh token expired).
        - Authentication errors occur during extraction.

        :param token_store_dir: Per-account directory containing
            ``garmin_tokens.json`` (e.g. ``~/.garminconnect/12345678/``).
            Must point to the account-level subdirectory, not the root
            ``~/.garminconnect/`` directory. In normal usage this is always
            supplied by the caller; the default is a placeholder that will
            fail unless a ``garmin_tokens.json`` happens to exist there.
        :raises RuntimeError: If tokens are missing, expired, or invalid. Run
            ``garmin auth`` to resolve authentication issues.
        """
        token_store_path = Path(token_store_dir).expanduser()
        click.echo("Authenticating with Garmin Connect using saved tokens.")

        try:
            garmin = GarminClient.from_tokens(token_store_path)
            self.garmin_client = garmin
            click.secho(
                f"Authentication successful for {self.garmin_client.full_name}"
                f" using saved tokens.",
                fg="green",
            )
        except Exception as e:
            error_msg = (
                f"Garmin authentication failed: {str(e)}\n\n"
                "To resolve this issue, run:\n"
                "   garmin auth\n\n"
                "This will:\n"
                "   - Guide you through Garmin Connect login.\n"
                "   - Handle MFA if enabled on your account.\n"
                "   - Save fresh tokens for future use.\n\n"
                f"Expected token location: {token_store_path}."
            )
            click.secho(error_msg, fg="red")
            raise RuntimeError(error_msg) from e

        # Get user ID for later use.
        self.user_id = self.garmin_client.get_user_profile().get("id")

    def _get_data_types_to_extract(
        self, data_types: Optional[List[str]] = None
    ) -> List[GarminDataType]:
        """
        Get the list of data types to extract.

        :param data_types: Optional list of data type names to extract. If None, returns
            all registered data types. If empty list, returns empty list (no registered
            data type extraction).
        :return: List of GarminDataType objects to extract.
        :raises ValueError: If any requested data type names are not found in registry.
        """
        if data_types is None:
            return GARMIN_DATA_REGISTRY.all_data_types

        # Handle explicit empty list: user wants no Garmin data types.
        if len(data_types) == 0:
            click.echo("Empty data_types list provided.")
            return []

        # Validate and retrieve requested data types.
        filtered_data_types = []
        invalid_names = []

        for name in data_types:
            data_type = GARMIN_DATA_REGISTRY.get_by_name(name)
            if data_type is None:
                invalid_names.append(name)
            else:
                filtered_data_types.append(data_type)

        if invalid_names:
            available = [dt.name for dt in GARMIN_DATA_REGISTRY.all_data_types]
            raise ValueError(
                f"Invalid data type names: {invalid_names}. "
                f"Available data types: {sorted(available)}."
            )

        return filtered_data_types

    def extract_garmin_data(self) -> List[Path]:
        """
        Extract Garmin data from Garmin Connect using GARMIN_DATA_REGISTRY.

        Allows for flexible configuration of data types and API methods.

        This method always processes dates inclusively - both start_date and
        end_date are included in the extraction. The extract() function
        handles any exclusion logic before passing dates to the Extractor
        class.

        :return: List of saved JSON file paths.
        """
        # Get the data types to extract (all or filtered subset).
        data_types_to_extract = self._get_data_types_to_extract(self.data_types)

        # Early return if empty list (no data types to extract).
        if len(data_types_to_extract) == 0:
            click.echo("Skipping Garmin data extraction: no data types.")
            return []

        if self.data_types:
            data_type_names = [dt.name for dt in data_types_to_extract]
            click.echo(
                f"Fetching data from Garmin Connect for selected data types "
                f"from the GarminDataRegistry "
                f"(start: {self.start_date}, end: {self.end_date} inclusive): "
                f"{data_type_names}."
            )
        else:
            click.echo(
                f"Fetching from Garmin Connect for all data types from "
                f"the GarminDataRegistry "
                f"(start: {self.start_date}, end: {self.end_date} "
                f"inclusive)..."
            )

        # Extract Garmin data by iterating over selected data types.
        # Per-data-type try/except so one bad type (e.g. NO_DATE call that
        # raises, or a structural error escaping the inner per-date layer)
        # doesn't abort the rest of the account's extraction.
        saved_files = []

        for data_type in data_types_to_extract:
            try:
                files = self._extract_data_by_type(
                    data_type, self.start_date, self.end_date
                )
                saved_files.extend(files)
            except Exception as e:
                click.secho(
                    f"⚠️  {data_type.name} extraction failed entirely: "
                    f"{type(e).__name__}: {e}. Continuing with next data type.",
                    fg="red",
                )
                self.failures.append(
                    ExtractionFailure(
                        data_type=data_type.name,
                        date="",
                        activity_id="",
                        error=f"{type(e).__name__}: {e}",
                    )
                )

        return saved_files

    def _extract_day_by_day(
        self, data_type: GarminDataType, start_date: date, end_date: date
    ) -> List[Path]:
        """
        Extract a DAILY-typed Garmin data type one day at a time with per-date error
        isolation.

        A failure on one date is logged and recorded in :attr:`failures`; extraction
        continues with the next date so transient API hiccups never abort the full date
        range. RANGE-typed data types are extracted by :meth:`_extract_range` with a
        single API call covering the full window.

        :param data_type: GarminDataType with DAILY time parameter.
        :param start_date: Start date for data extraction (inclusive).
        :param end_date: End date for data extraction (inclusive).
        :return: List of saved file paths.
        """
        saved_files = []
        current_date = start_date

        while current_date <= end_date:  # Inclusive end_date.
            click.echo(
                f"Fetching {data_type.emoji} {data_type.name} data for "
                f"{current_date}."
            )
            date_str = current_date.strftime("%Y-%m-%d")

            try:
                api_method = getattr(self.garmin_client, data_type.api_method)
                # Wrap the API call in retries-with-backoff so a transient
                # network blip absorbs silently and only persistent failures
                # land in self.failures.
                data = _with_retries(api_method, date_str)

                if data:
                    saved_files.extend(
                        self._save_garmin_data(data, data_type, current_date)
                    )
                else:
                    click.secho(
                        f"{data_type.emoji} {data_type.name}: No data for "
                        f"{current_date}.",
                        fg="yellow",
                    )
            except Exception as e:
                click.secho(
                    f"⚠️  {data_type.name} {date_str} failed: "
                    f"{type(e).__name__}: {e}. Continuing.",
                    fg="red",
                )
                self.failures.append(
                    ExtractionFailure(
                        data_type=data_type.name,
                        date=date_str,
                        activity_id="",
                        error=f"{type(e).__name__}: {e}",
                    )
                )

            current_date += timedelta(days=1)
            time.sleep(0.1)  # Rate limiting.

        return saved_files

    def _extract_range(
        self, data_type: GarminDataType, start_date: date, end_date: date
    ) -> List[Path]:
        """
        Extract a RANGE-typed Garmin data type with a single API call covering the full
        window, instead of one call per day.

        The Garmin endpoints behind RANGE-typed wrappers each accept a native date
        range and return all matching rows in one response (the wrappers paginate
        internally where the endpoint imposes a max range, e.g. menstrual cycle
        calendar's 92-day chunk). Calling them once per day with ``start = end``
        wastes API quota, multiplies disk I/O, and (for the menstrual cycle summary
        path) re-fires the wipe-and-replace policy per file when one pass would
        suffice.

        After the single API call, the response is either:

        - **Split per day** for data types whose rows are naturally per-day
          (``BODY_COMPOSITION``, ``ACTIVITIES_LIST``; see ``_PER_DAY_SPLITTERS``).
          Each day with rows lands in its own file, preserving the per-day
          ``(user, day)`` FileSet abstraction the processor expects. Days with no
          rows write no file.
        - **Written as a single file stamped ``end_date``** for data types whose
          rows are per-range / per-cycle and need to be processed atomically
          (``MENSTRUAL_CYCLE_SUMMARY``: its wipe-and-replace policy for predicted
          cycles must see the full new set of cycles in one transaction, so
          splitting would re-introduce the redundant-write bug).

        Failure isolation is per-range (one API call), recorded as
        ``"{start}..{end}"`` in :attr:`failures`.

        :param data_type: GarminDataType with RANGE time parameter.
        :param start_date: Start date for data extraction (inclusive).
        :param end_date: End date for data extraction (inclusive).
        :return: List of saved file paths (zero, one, or many depending on the
            data type's splitter registration and how many days had data).
        """
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        click.echo(
            f"Fetching {data_type.emoji} {data_type.name} data for "
            f"{start_str}..{end_str}."
        )

        # Wrap the whole pipeline (API call + splitter + per-day saves) in one
        # try/except so any failure - HTTP error, splitter blowing up on an
        # unexpected payload shape, disk I/O on a save - records the same
        # per-window "<start>..<end>" failure label. Without this, a save error
        # would bubble to the outer per-data-type handler with an empty date
        # context, hiding which window was affected. We abort the rest of the
        # range on a save failure rather than recording per-day failures inside
        # the loop because the realistic causes (disk full, permission denied)
        # would just generate noise across every subsequent day too.
        try:
            api_method = getattr(self.garmin_client, data_type.api_method)
            data = _with_retries(api_method, start_str, end_str)

            if not data:
                click.secho(
                    f"{data_type.emoji} {data_type.name}: No data for "
                    f"{start_str}..{end_str}.",
                    fg="yellow",
                )
                return []

            splitter = _PER_DAY_SPLITTERS.get(data_type.name)
            if splitter is None:
                # Unsplittable: one file stamped end_date, body contains all rows.
                return self._save_garmin_data(data, data_type, end_date)

            per_day = splitter(data)
            if not per_day:
                click.secho(
                    f"{data_type.emoji} {data_type.name}: No data for "
                    f"{start_str}..{end_str}.",
                    fg="yellow",
                )
                return []

            saved_files: List[Path] = []
            for day in sorted(per_day):
                saved_files.extend(self._save_garmin_data(per_day[day], data_type, day))
            return saved_files
        except Exception as e:
            click.secho(
                f"⚠️  {data_type.name} {start_str}..{end_str} failed: "
                f"{type(e).__name__}: {e}. Continuing.",
                fg="red",
            )
            self.failures.append(
                ExtractionFailure(
                    data_type=data_type.name,
                    date=f"{start_str}..{end_str}",
                    activity_id="",
                    error=f"{type(e).__name__}: {e}",
                )
            )
            return []

    def _menstrual_days_have_data(self, start_date: date, end_date: date) -> bool:
        """
        Probe the menstrual calendar endpoint to decide whether the per-day
        MENSTRUAL_CYCLE_DAY fan-out is worth running for a window.

        The dayview endpoint is populated only for dates inside a menstrual cycle window
        (observed or predicted); dates outside all cycle windows return a bare payload
        the extractor drops. The calendar endpoint reports every cycle whose window
        overlaps the queried range (verified: a cycle is returned even when its start
        predates the window), so zero reported cycles means every dayview call in the
        window is empty. Accounts that do not track menstrual data (e.g. most male
        accounts) report no cycles, letting the extractor skip ~90 guaranteed-empty
        dayview calls per run.

        Gated on ``cycleSummaries`` only: logged symptom/ovulation/note days sit inside
        a cycle, so they never cover dates the cycle list misses.

        Fails open. The calendar wrapper returns ``None`` when every chunk fails
        transport and raises on any other error; this method catches both and returns
        ``True`` so a transient blip never suppresses a real extraction.

        :param start_date: Effective start of the dayview window (inclusive).
        :param end_date: End of the dayview window (inclusive).
        :return:``True`` to run the per-day extraction, ``False`` to skip it.
        """
        try:
            summary = self.garmin_client.get_menstrual_calendar_data(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )
        except Exception as e:
            click.secho(
                f"⚠️  MENSTRUAL_CYCLE_DAY summary probe failed "
                f"({type(e).__name__}: {e}); running the full window.",
                fg="yellow",
            )
            return True
        if summary is None:
            click.secho(
                "⚠️  MENSTRUAL_CYCLE_DAY summary probe returned no calendar "
                "response; running the full window.",
                fg="yellow",
            )
            return True
        return bool(summary.get("cycleSummaries"))

    def _extract_data_by_type(
        self, data_type: GarminDataType, start_date: date, end_date: date
    ) -> List[Path]:
        """
        Extract Garmin data for a specific type, dispatching by time-param taxonomy.

        :param data_type: GarminDataType defining the extraction parameters.
        :param start_date: Start date for data extraction (inclusive).
        :param end_date: End date for data extraction (inclusive).
        :return: List of saved file paths.
        :raises ValueError: If the data type's ``api_method_time_param`` is not one of
            the four supported variants.
        """
        if data_type.api_method_time_param == APIMethodTimeParam.DAILY:
            # One API call per day, per-date error isolation. Data types whose past days
            # can change retroactively (e.g. MENSTRUAL_CYCLE_DAY) extend the start back a
            # fixed window so those edits are re-fetched, not just the incremental slice.
            effective_start = _retroactive_lookback_start(
                data_type, start_date, end_date
            )
            # MENSTRUAL_CYCLE_DAY's dayview is populated only for dates inside a cycle
            # window, so gate its ~90-call fan-out on a cheap probe of the companion
            # calendar/summary endpoint: skip entirely when no cycle overlaps the window
            # (e.g. accounts that do not track menstrual data). See
            # _menstrual_days_have_data.
            if data_type.name == "MENSTRUAL_CYCLE_DAY":
                if not self._menstrual_days_have_data(effective_start, end_date):
                    click.secho(
                        f"{data_type.emoji} {data_type.name}: no cycles reported "
                        f"in {effective_start}..{end_date}; skipping per-day "
                        f"extraction.",
                        fg="yellow",
                    )
                    return []
            return self._extract_day_by_day(data_type, effective_start, end_date)

        if data_type.api_method_time_param == APIMethodTimeParam.RANGE:
            # One API call for the full window (wrappers paginate internally
            # where the endpoint has a max range). Per-range error isolation.
            return self._extract_range(data_type, start_date, end_date)

        if data_type.api_method_time_param == APIMethodTimeParam.NO_DATE:
            # Process no-date data. Wrap the call in _with_retries so
            # transient network failures absorb the same way they do for
            # DAILY (per-day) and RANGE (per-window) types; otherwise NO_DATE
            # types (USER_PROFILE, PERSONAL_RECORDS, RACE_PREDICTIONS) would
            # fail on the first DNS hiccup.
            click.echo(f"{data_type.emoji} Fetching {data_type.name.lower()} data.")
            api_method = getattr(self.garmin_client, data_type.api_method)
            data = _with_retries(api_method)

            if data:
                # Enhance USER_PROFILE data with client information.
                if data_type.name == "USER_PROFILE":
                    data["full_name"] = self.garmin_client.full_name

                return self._save_garmin_data(data, data_type, end_date)
            click.secho(
                f"{data_type.emoji} {data_type.name}: No data available.",
                fg="yellow",
            )
            return []

        if data_type.api_method_time_param == APIMethodTimeParam.PER_ACTIVITY:
            # PER_ACTIVITY types (ACTIVITY, EXERCISE_SETS) are iterated per
            # activity ID by extract_fit_activities, which sources the IDs from
            # the ACTIVITIES_LIST output. They don't fit the date-driven loop in
            # this dispatcher, so we no-op here and let the activities pipeline
            # handle them.
            click.echo(
                f"{data_type.emoji} {data_type.name} files will be handled "
                f"separately by extract_fit_activities()."
            )
            return []

        raise ValueError(
            f"Unsupported API method time parameter: "
            f"{data_type.api_method_time_param}."
        )

    def _save_garmin_data(
        self,
        data: Union[Dict[str, Any], List[Any]],
        data_type: GarminDataType,
        file_date: date,
    ) -> List[Path]:
        """
        Save Garmin data to JSON file with standardized naming.

        Generates filenames with user ID, data type, and ISO 8601 timestamp for
        consistent batching. Creates midday timestamp for date-based grouping.

        :param data: The data to save. Most data types return a dict; ACTIVITIES_LIST
            returns a top-level list, so the parameter accepts either.
        :param data_type: The data type.
        :param file_date: Date for timestamp generation used in filename.
        :return: List of saved file paths.
        """
        # Create midday timestamp for consistent grouping.
        midday_dt = datetime.combine(file_date, datetime.min.time()).replace(
            hour=12, minute=0, second=0
        )
        timestamp = pendulum.instance(midday_dt, tz="UTC").to_iso8601_string()

        # Generate filename: {user_id}_{DATA_TYPE}_{timestamp}.json.
        filename = f"{self.user_id}_{data_type.name}_{timestamp}.json".replace(":", "-")
        filepath = self.ingest_dir / filename

        # Save data.
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        click.echo(f"Saved {data_type.emoji} {data_type.name}: {filename}.")
        return [filepath]

    def _extract_activity_content(
        self, activity_id: int, raw_data: bytes
    ) -> Optional[tuple[str, bytes]]:
        """
        Extract and identify the content of a downloaded activity file.

        Garmin's ORIGINAL download format returns a ZIP archive whose inner
        file may be FIT, TCX, GPX, or another format depending on how the
        activity was originally recorded or uploaded. This method extracts
        the content and identifies the format using magic bytes, so the saved
        filename carries the correct extension regardless of what Garmin puts
        inside the ZIP.

        Fallback chain when magic bytes are inconclusive:
        1. Inner filename extension (if it is a known activity extension).
        2. ``'.bin'`` — file is preserved on disk but will not be processed.

        :param activity_id: Garmin activity ID (used in log messages).
        :param raw_data: Raw bytes returned by the download API.
        :return: Tuple of ``(file_extension, content_bytes)``, or ``None``
            if the archive is empty.
        """
        inner_name = ""

        try:
            with zipfile.ZipFile(io.BytesIO(raw_data), "r") as zip_ref:
                zip_files = zip_ref.namelist()
                if not zip_files:
                    click.secho(
                        f"⚠️  Empty ZIP archive for activity {activity_id}.",
                        fg="yellow",
                    )
                    return None

                if len(zip_files) > 1:
                    click.secho(
                        f"⚠️  ZIP for activity {activity_id} contains "
                        f"{len(zip_files)} files: {zip_files}. "
                        f"Using first: {zip_files[0]!r}.",
                        fg="yellow",
                    )

                inner_name = zip_files[0]
                content = zip_ref.read(inner_name)

        except zipfile.BadZipFile:
            # Not a ZIP — probe the raw bytes directly.
            content = raw_data

        file_ext = _detect_format_from_magic(content)

        if file_ext is not None:
            if file_ext not in ("fit", "tcx"):
                click.secho(
                    f"⚠️  Activity {activity_id}: detected '{file_ext}' format "
                    f"(inner file: {inner_name!r}). "
                    f"File will be saved but not processed.",
                    fg="yellow",
                )
            return file_ext, content

        # Magic bytes inconclusive — try the inner filename extension.
        inner_ext = Path(inner_name).suffix.lower().lstrip(".")
        if inner_ext in _KNOWN_ACTIVITY_EXTENSIONS:
            click.secho(
                f"⚠️  Activity {activity_id}: magic bytes inconclusive; "
                f"using inner filename extension '.{inner_ext}' "
                f"from {inner_name!r}.",
                fg="yellow",
            )
            return inner_ext, content

        # Completely unrecognised — preserve the file without processing it.
        click.secho(
            f"⚠️  Activity {activity_id}: unrecognised file format "
            f"(inner file: {inner_name!r}). Saving as '.bin' — "
            f"file will not be processed.",
            fg="yellow",
        )
        return "bin", content

    def _load_activities_list_from_disk(self) -> Optional[list]:
        """
        Read all saved ACTIVITIES_LIST JSON files from ``ingest_dir`` and merge them
        into a single deduplicated activities list.

        The registry-driven extract loop calls ``get_activities_by_date`` once for the
        full requested window inside ``_extract_range`` (RANGE-typed), then the per-day
        splitter (see ``_split_activities_list_by_day`` / ``_PER_DAY_SPLITTERS``) writes
        one ``<user_id>_ACTIVITIES_LIST_<day>T12-00-00Z.json`` per day with activities,
        each containing only that day's activities. Reading only the newest file would
        silently skip activities from earlier days; instead, parse every matching file
        and merge by ``activityId`` (last value wins for any duplicate). The dedupe also
        covers users upgrading from earlier versions with leftover per-range or per-day
        files of mixed shape in ``ingest/``.

        Falls back to ``None`` (caller hits the live API) on any read or parse error so
        a single corrupt file doesn't prevent extraction.

        :return: Merged + deduplicated activities list, or ``None`` if no files exist or
            any file cannot be parsed.
        """
        pattern = f"{self.user_id}_ACTIVITIES_LIST_*.json"
        matches = sorted(self.ingest_dir.glob(pattern))
        if not matches:
            return None

        merged: Dict = {}
        anonymous: list = []
        parsed_any = False
        for match in matches:
            try:
                with open(match, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                click.secho(
                    f"⚠️  Could not read {match.name}: {e}. "
                    f"Falling back to API call.",
                    fg="yellow",
                )
                return None
            if not isinstance(payload, list):
                click.secho(
                    f"⚠️  Could not use {match.name}: expected a JSON list. "
                    f"Falling back to API call.",
                    fg="yellow",
                )
                return None
            parsed_any = True
            for activity in payload:
                if isinstance(activity, dict) and "activityId" in activity:
                    merged[activity["activityId"]] = activity
                else:
                    # Activity without an ID — keep verbatim, don't dedupe.
                    anonymous.append(activity)

        # An empty list IS a valid result (the user has no activities in the
        # window). Only fall back to the API when no file was parseable.
        if not parsed_any:
            return None
        return list(merged.values()) + anonymous

    def extract_fit_activities(self) -> List[Path]:
        """
        Extract activity files from Garmin Connect.

        This method always processes dates inclusively — both start_date and end_date
        are included in the extraction. The extract() function handles any exclusion
        logic before passing dates to the Extractor class. Downloads activity files with
        user ID, activity ID, and activity start timestamp in filename. The file
        extension reflects the actual format detected from the downloaded content.

        :return: List of saved activity file paths.
        """
        click.echo(
            f"Fetching activities from Garmin Connect "
            f"(start: {self.start_date}, end: {self.end_date} inclusive)..."
        )

        # Get list of activities. The registry-driven extract loop has
        # already fetched and saved this same data as ACTIVITIES_LIST JSON
        # in ingest_dir (the API is RANGE-typed and called with the same
        # date window). Read it from disk to avoid a duplicate API call.
        # Fall back to a live API call if the file is missing (e.g.
        # ACTIVITIES_LIST extraction failed or this method is invoked
        # outside the normal pipeline).
        start_str = self.start_date.strftime("%Y-%m-%d")
        end_str = self.end_date.strftime("%Y-%m-%d")

        activities = self._load_activities_list_from_disk()
        if activities is None:
            try:
                activities = _with_retries(
                    self.garmin_client.get_activities_by_date,
                    start_str,
                    end_str,
                )
            except Exception as e:
                click.secho(
                    f"⚠️  Activity list fetch failed: {type(e).__name__}: {e}. "
                    f"No activity files will be downloaded for this account.",
                    fg="red",
                )
                self.failures.append(
                    ExtractionFailure(
                        data_type="ACTIVITIES_LIST",
                        date=f"{start_str}..{end_str}",
                        activity_id="",
                        error=f"{type(e).__name__}: {e}",
                    )
                )
                return []

        if not activities:
            click.secho(
                "No activities found in the specified date range.",
                fg="yellow",
            )
            return []

        click.echo(f"Found {len(activities)} activities.")

        downloaded_files = []

        # This per-activity loop backs three PER_ACTIVITY data types. Gate each sub-fetch
        # on whether its type was requested so, e.g., `--data-types MULTISPORT_CHILDREN`
        # only fetches legs (not every activity's FIT) rather than downloading everything.
        want_activity = self.data_types is None or "ACTIVITY" in self.data_types
        want_exercise_sets = (
            self.data_types is None or "EXERCISE_SETS" in self.data_types
        )
        want_multisport = (
            self.data_types is None or "MULTISPORT_CHILDREN" in self.data_types
        )

        for activity in activities:
            activity_id = activity["activityId"]

            # Generate timestamp with local timezone date at noon for
            # consistent batching with ACTIVITIES_LIST file. Uses same
            # midday timestamp approach as _save_garmin_data().
            activity_start = pendulum.parse(activity.get("startTimeLocal"))
            activity_date = activity_start.date()
            midday_dt = datetime.combine(activity_date, datetime.min.time()).replace(
                hour=12, minute=0, second=0
            )
            timestamp = pendulum.instance(midday_dt, tz="UTC").to_iso8601_string()

            # Download and save the activity file (ORIGINAL = ZIP archive), only when
            # ACTIVITY is requested. A 404 means the activity has no downloadable file
            # (manual entry, deleted upload, or an old activity Garmin no longer
            # retains); record it and fall through so the exercise-set / leg fetches
            # below still run for this activity.
            if want_activity:
                raw_data = None
                try:
                    raw_data = _with_retries(
                        self.garmin_client.download_activity,
                        activity_id,
                        dl_fmt=ActivityDownloadFormat.ORIGINAL,
                    )
                except Exception as e:
                    # Broaden from GarminConnectionError to Exception so unexpected
                    # errors (parse failures, transient SDK bugs) on one activity
                    # don't abort the loop. Record the failure for the summary.
                    click.secho(
                        f"⚠️  Skipping activity {activity_id}: "
                        f"{type(e).__name__}: {e}.",
                        fg="yellow",
                    )
                    self.failures.append(
                        ExtractionFailure(
                            data_type="ACTIVITY",
                            date="",
                            activity_id=str(activity_id),
                            error=f"{type(e).__name__}: {e}",
                        )
                    )

                # Detect the actual file format, extract content, and save.
                result = (
                    self._extract_activity_content(activity_id, raw_data)
                    if raw_data is not None
                    else None
                )
                if result is not None:
                    file_ext, file_content = result
                    filename = (
                        f"{self.user_id}_ACTIVITY_{activity_id}_"
                        f"{timestamp}.{file_ext}"
                    ).replace(":", "-")
                    filepath = self.ingest_dir / filename
                    with open(filepath, "wb") as f:
                        f.write(file_content)
                    file_size = filepath.stat().st_size / 1024  # KB.
                    click.echo(f"Saved: {filename} ({file_size:.1f} KB).")
                    downloaded_files.append(filepath)

            # Fetch exercise sets for strength training activities (when EXERCISE_SETS
            # or ACTIVITY is requested).
            activity_type_key = (
                activity.get("activityType", {}).get("typeKey", "").lower()
            )
            if (want_activity or want_exercise_sets) and activity_type_key in (
                "strength_training",
                "fitness_equipment",
            ):
                time.sleep(0.1)  # Rate limiting between API calls.
                exercise_sets_file = self._extract_exercise_sets(activity_id, timestamp)
                if exercise_sets_file:
                    downloaded_files.append(exercise_sets_file)

            # Fetch the leg (child) activities of a multi-sport parent (when
            # MULTISPORT_CHILDREN or ACTIVITY is requested). The flat `parent` flag
            # marks any activity that has child legs (structural signal, not a
            # hardcoded type name); the authoritative child list comes from the detail
            # endpoint's metadataDTO.childIds.
            if (want_activity or want_multisport) and activity.get("parent") is True:
                time.sleep(0.1)
                children_file = self._extract_multisport_children(
                    activity_id, timestamp
                )
                if children_file:
                    downloaded_files.append(children_file)

            # Rate limiting between activities.
            time.sleep(0.1)

        click.echo(
            f"Activity file extraction complete: {len(downloaded_files)} "
            f"files saved to {self.ingest_dir}."
        )
        return downloaded_files

    def _extract_exercise_sets(
        self, activity_id: int, timestamp: str
    ) -> Optional[Path]:
        """
        Fetch exercise sets data for a strength training activity.

        Calls the exercise sets API endpoint and saves the response as a JSON file.
        Returns None if the API returns no exercise sets data.

        :param activity_id: Garmin activity ID.
        :param timestamp: ISO 8601 timestamp for consistent filename batching.
        :return: Path to saved JSON file, or None if no data.
        """
        try:
            data = _with_retries(
                self.garmin_client.get_activity_exercise_sets, activity_id
            )
        except Exception as e:
            click.secho(
                f"Warning: Failed to fetch exercise sets for "
                f"activity {activity_id}: {e}.",
                fg="yellow",
            )
            return None

        # Skip if no exercise sets data.
        if not data or not data.get("exerciseSets"):
            click.echo(f"No exercise sets data for activity {activity_id}.")
            return None

        filename = (
            f"{self.user_id}_EXERCISE_SETS_{activity_id}_{timestamp}.json"
        ).replace(":", "-")
        filepath = self.ingest_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        file_size = filepath.stat().st_size / 1024  # KB.
        click.echo(f"Saved: {filename} ({file_size:.1f} KB).")
        return filepath

    def _extract_multisport_children(
        self, parent_activity_id: int, timestamp: str
    ) -> Optional[Path]:
        """
        Fetch and save the leg (child) activities of a multi-sport parent.

        Reads the parent's ``metadataDTO.childIds`` from the activity detail endpoint,
        then fetches each leg's own detail (which carries its real sport type and sport-
        specific ``summaryDTO`` metrics). The legs are hidden under the parent and do
        not appear in the activities list, so this is the only way to capture their per-
        leg data. Saves one file per parent, wrapping the leg details under ``children``
        with the ``parentActivityId`` so the processor can link them.

        :param parent_activity_id: Garmin activity ID of the multi-sport parent.
        :param timestamp: ISO 8601 timestamp for consistent filename batching.
        :return: Path to the saved JSON file, or None when there are no legs to save.
        """
        try:
            parent_detail = _with_retries(
                self.garmin_client.get_activity_details, parent_activity_id
            )
        except Exception as e:
            click.secho(
                f"Warning: Failed to fetch detail for multi-sport parent "
                f"{parent_activity_id}: {e}.",
                fg="yellow",
            )
            return None

        child_ids = ((parent_detail or {}).get("metadataDTO") or {}).get("childIds")
        # Only a non-empty list of ids is usable; a truthy non-list (dict/str) would be
        # iterated element-by-element and fetch garbage ids.
        if not isinstance(child_ids, list) or not child_ids:
            click.echo(f"No child legs for multi-sport activity {parent_activity_id}.")
            return None

        children = []
        for child_id in child_ids:
            try:
                child_detail = _with_retries(
                    self.garmin_client.get_activity_details, child_id
                )
            except Exception as e:
                click.secho(
                    f"Warning: Failed to fetch multi-sport leg {child_id} of "
                    f"activity {parent_activity_id}: {e}.",
                    fg="yellow",
                )
                continue
            if child_detail:
                children.append(child_detail)
            time.sleep(0.1)  # Rate limiting between legs.

        if not children:
            return None

        filename = (
            f"{self.user_id}_MULTISPORT_CHILDREN_"
            f"{parent_activity_id}_{timestamp}.json"
        ).replace(":", "-")
        filepath = self.ingest_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {"parentActivityId": parent_activity_id, "children": children},
                f,
                indent=2,
            )
        click.echo(
            f"Saved {len(children)} multi-sport leg(s) for activity "
            f"{parent_activity_id}: {filename}."
        )
        return filepath


def extract(
    ingest_dir: Path,
    data_interval_start: Union[str, pendulum.DateTime],
    data_interval_end: Union[str, pendulum.DateTime],
    data_types: Optional[List[str]] = None,
    accounts: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Download data from Garmin Connect for the specified date range.

    Supports multiple Garmin Connect accounts. Accounts are discovered automatically by
    scanning subdirectories in ~/.garminconnect/, where each subdirectory is named by
    the Garmin user ID and contains authentication tokens.

    If the resolved start date is after the end date (an empty extraction window, such
    as an already-current database whose auto-detected start lands past the end, or an
    inverted caller-supplied range), extraction is skipped and an all-zero result is
    returned without contacting Garmin Connect.

    :param ingest_dir: Directory path where extracted files will be saved.
    :param data_interval_start: Start date for data extraction (ISO string or datetime).
    :param data_interval_end: End date for data extraction (ISO string or datetime).
    :param data_types: Optional list of data type names to extract. If None, extracts
        all available data types including FIT activity files.
    :param accounts: Optional list of user_id strings to filter which accounts to
        extract. If None, extracts all discovered accounts.
    :param progress_callback: Optional callback function for progress updates.
    :return: Dictionary with the following keys:

        - ``garmin_files`` (``int``): count of saved JSON data files.
        - ``activity_files`` (``int``): count of saved activity files (FIT/TCX/GPX/KML).
        - ``failures`` (``List[ExtractionFailure]``): per-date / per-data-type /
          per-activity failures recorded by any per-account extractor.
        - ``failed_accounts`` (``List[str]``): user IDs whose entire run raised
          an unhandled exception (rare; see logs).

    :raises ValueError: If any requested data type names are not found in registry, or
        if accounts filter is not a list.
    """
    import logging

    logger = logging.getLogger(__name__)

    # Validate input parameters.
    if data_types is not None and len(data_types) == 0:
        error_msg = (
            "data_types is an empty list. Use None to extract all types "
            "or specify data types to extract. Extraction will be skipped."
        )
        click.echo(error_msg)
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    # Validate accounts filter.
    if accounts is not None and not isinstance(accounts, (list, tuple)):
        raise ValueError(
            f"accounts must be a list or tuple, got {type(accounts).__name__}. "
            "Example: ['12345678', '87654321']"
        )

    # Convert datetime objects or strings to date-only for Garmin API calls.
    if isinstance(data_interval_start, str):
        start_date = pendulum.parse(data_interval_start).date()
    else:
        start_date = data_interval_start.date()

    if isinstance(data_interval_end, str):
        original_end_date = pendulum.parse(data_interval_end).date()
    else:
        original_end_date = data_interval_end.date()

    # Apply end_date exclusion only if the start_date is different from the
    # original_end_date.
    if original_end_date > start_date:
        end_date = original_end_date - timedelta(days=1)  # Exclusive logic.
    else:
        end_date = original_end_date  # Inclusive logic for same-day.

    # An inverted resolved window (start after end) extracts nothing. Usually
    # the database is already current (auto-detected start is last-stored day
    # + 1, landing past the end), but it also covers an invalid caller-supplied
    # range, so the message stays neutral about the cause. Short-circuit before
    # any account discovery or API calls. This also deliberately skips the
    # per-type retroactive look-back (e.g. MENSTRUAL_CYCLE_DAY's 90-day
    # refresh): that window was already re-fetched on the run that advanced the
    # database, and the next advancing run re-covers it, so a no-op re-run needs
    # no calls.
    if start_date > end_date:
        click.secho(
            f"Nothing to extract (start {start_date.isoformat()} is after end "
            f"{end_date.isoformat()}).",
            fg="green",
        )
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    # Discover accounts.
    from garmin_health_data.auth import discover_accounts

    try:
        discovered = discover_accounts()
    except (FileNotFoundError, NotADirectoryError, RuntimeError) as e:
        click.secho(
            f"Account discovery failed: {e}\n"
            "Run 'garmin auth' to set up your Garmin account(s).",
            fg="red",
        )
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    # Apply account filter if provided.
    if accounts is not None:
        filter_set = set(accounts)
        discovered = [(uid, path) for uid, path in discovered if uid in filter_set]
        if not discovered:
            click.secho(
                f"No matching accounts found for filter: {accounts}",
                fg="yellow",
            )
            return {
                "garmin_files": 0,
                "activity_files": 0,
                "failures": [],
                "failed_accounts": [],
            }

    click.echo(f"Found {len(discovered)} account(s) to extract.")

    # Extract from each account with error isolation.
    all_garmin_files = []
    all_activity_files = []
    all_failures: List[ExtractionFailure] = []
    failed_accounts = []

    for user_id, token_dir in discovered:
        extractor: Optional[GarminExtractor] = None
        try:
            click.echo()
            click.echo(
                click.style(f"Extracting data for account {user_id}...", fg="cyan")
            )

            extractor = GarminExtractor(start_date, end_date, ingest_dir, data_types)
            extractor.authenticate(token_store_dir=str(token_dir))

            # Extract Garmin data.
            if progress_callback:
                progress_callback(f"Extracting Garmin data for account {user_id}...")
            garmin_files = extractor.extract_garmin_data()

            # Extract FIT activity files (if requested).
            activity_files = []
            if data_types is None or (
                data_types
                and {"ACTIVITY", "EXERCISE_SETS", "MULTISPORT_CHILDREN"}
                & set(data_types)
            ):
                if progress_callback:
                    progress_callback(
                        f"Extracting FIT activity files for account {user_id}..."
                    )
                activity_files = extractor.extract_fit_activities()

            all_garmin_files.extend(garmin_files)
            all_activity_files.extend(activity_files)

        except Exception:
            logger.exception(
                f"Account {user_id} failed. Continuing with remaining accounts."
            )
            click.secho(
                f"Account {user_id} failed. Continuing with remaining accounts.",
                fg="red",
            )
            failed_accounts.append(user_id)
        finally:
            # Always merge per-account ExtractionFailures so granular per-date /
            # per-data-type / per-activity failures recorded BEFORE an
            # account-level crash still appear in the end-of-run summary.
            # Without this, a partial run that captured 5 per-day failures and
            # then crashed in extract_fit_activities would lose those 5 from
            # the summary. `extractor` is None if the constructor itself
            # raised, in which case there's nothing to merge.
            if extractor is not None:
                all_failures.extend(extractor.failures)

    # Check if any data was extracted.
    if not all_garmin_files and not all_activity_files:
        click.echo(
            "No Garmin Connect data found for extraction. Skipping downstream tasks."
        )
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": all_failures,
            "failed_accounts": failed_accounts,
        }

    # Summary.
    activity_summary = (
        "\n".join([f"      - {file.name}" for file in all_activity_files])
        if all_activity_files
        else "      (none)"
    )
    garmin_summary = (
        "\n".join([f"      - {file.name}" for file in all_garmin_files])
        if all_garmin_files
        else "      (none)"
    )
    click.echo(
        f"\nExtraction Summary:\n"
        f"   Accounts processed: {len(discovered) - len(failed_accounts)}/{len(discovered)}\n"
        f"   Saved to: {ingest_dir}\n"
        f"   FIT activity files (total: {len(all_activity_files)}):\n"
        f"{activity_summary}\n"
        f"   Garmin data files (total: {len(all_garmin_files)}):\n"
        f"{garmin_summary}"
    )

    if failed_accounts:
        click.secho(f"   Failed accounts: {failed_accounts}", fg="red")

    return {
        "garmin_files": len(all_garmin_files),
        "activity_files": len(all_activity_files),
        "failures": all_failures,
        "failed_accounts": failed_accounts,
    }


def cli_extract(
    ingest_dir: str,
    start_date: str,
    end_date: str,
    data_types: List[str] = None,
    accounts: Optional[List[str]] = None,
) -> None:
    """
    CLI wrapper for extract function.

    :param ingest_dir: Directory path where extracted files will be saved.
    :param start_date: Start date in YYYY-MM-DD format.
    :param end_date: End date in YYYY-MM-DD format (exclusive).
    :param data_types: Optional list of data type names to extract.
    :param accounts: Optional list of user_id strings to filter accounts.
    """
    start_pendulum = pendulum.parse(start_date, tz="UTC")
    end_pendulum = pendulum.parse(end_date, tz="UTC")
    ingest_path = Path(ingest_dir)

    extract(
        ingest_dir=ingest_path,
        data_interval_start=start_pendulum,
        data_interval_end=end_pendulum,
        data_types=data_types,
        accounts=accounts,
    )

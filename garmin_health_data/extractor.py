"""
Garmin Connect data extraction module for standalone use.

Extracts activity files and JSON Garmin data from Garmin Connect API and saves them
to the ingest directory. Designed for standalone applications without Apache Airflow
dependencies.
"""

import json
import time
import zipfile
import io

from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Optional, Union, Callable, Dict

import click
import pendulum
from garmin_health_data.garmin_client import ActivityDownloadFormat, GarminClient
from garmin_health_data.garmin_client.exceptions import GarminConnectionError

from garmin_health_data.constants import (
    APIMethodTimeParam,
    GarminDataType,
    GARMIN_DATA_REGISTRY,
)

# File extensions that the downstream processor knows how to route.
# Used as a fallback when magic-byte detection is inconclusive.
_KNOWN_ACTIVITY_EXTENSIONS: frozenset = frozenset({"fit", "tcx", "gpx", "kml"})


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
        saved_files = []

        for data_type in data_types_to_extract:
            files = self._extract_data_by_type(
                data_type, self.start_date, self.end_date
            )
            saved_files.extend(files)

        return saved_files

    def _process_day_by_day(
        self, data_type: GarminDataType, start_date: date, end_date: date
    ) -> List[Path]:
        """
        Extract Garmin data type one day at a time with common loop logic.

        Handles both DAILY and RANGE API time parameter patterns by processing each day
        individually and calling the appropriate API method with the correct parameters.

        :param data_type: GarminDataType defining the extraction parameters.
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

            # Get API method dynamically.
            api_method = getattr(self.garmin_client, data_type.api_method)
            date_str = current_date.strftime("%Y-%m-%d")

            # Call API method with appropriate parameters based on type.
            if data_type.api_method_time_param == APIMethodTimeParam.DAILY:
                data = api_method(date_str)
            else:
                # Pass the same date to both date params for RANGE methods.
                data = api_method(date_str, date_str)

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

            current_date += timedelta(days=1)
            time.sleep(0.1)  # Rate limiting.

        return saved_files

    def _extract_data_by_type(
        self, data_type: GarminDataType, start_date: date, end_date: date
    ) -> List[Path]:
        """
        Extract Garmin data for a specific type.

        ACTIVITY files use different extraction logic.

        Uses the appropriate API method, handling the associated API time parameter
        pattern (DAILY, RANGE, NO_DATE) and generates consistent filenames.

        :param data_type: GarminDataType defining the extraction parameters.
        :param start_date: Start date for data extraction (inclusive).
        :param end_date: End date for data extraction (inclusive).
        :return: List of saved file paths.
        """

        # Special case: ACTIVITY and EXERCISE_SETS use different extraction logic.
        if data_type.name in ("ACTIVITY", "EXERCISE_SETS"):
            click.echo(
                f"{data_type.emoji} {data_type.name} files will be handled "
                f"separately by extract_fit_activities()."
            )
            return []  # Return empty list.

        if data_type.api_method_time_param in [
            APIMethodTimeParam.DAILY,
            APIMethodTimeParam.RANGE,
        ]:
            # Process each day individually using common helper method.
            return self._process_day_by_day(data_type, start_date, end_date)

        if data_type.api_method_time_param == APIMethodTimeParam.NO_DATE:
            # Process no-date data.
            click.echo(f"{data_type.emoji} Fetching {data_type.name.lower()} data.")
            api_method = getattr(self.garmin_client, data_type.api_method)
            data = api_method()

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

        raise ValueError(
            f"Unsupported API method time parameter: "
            f"{data_type.api_method_time_param}."
        )

    def _save_garmin_data(
        self, data: dict, data_type: GarminDataType, file_date: date
    ) -> List[Path]:
        """
        Save Garmin data to JSON file with standardized naming.

        Generates filenames with user ID, data type, and ISO 8601 timestamp for
        consistent batching. Creates midday timestamp for date-based grouping.

        :param data: The data to save.
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
    ) -> Optional[tuple]:
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
            if file_ext != "fit":
                # Non-FIT format: log so we can learn Garmin's conventions.
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

    def extract_fit_activities(self) -> List[Path]:
        """
        Extract activity files from Garmin Connect.

        This method always processes dates inclusively — both start_date and
        end_date are included in the extraction. The extract() function
        handles any exclusion logic before passing dates to the Extractor
        class. Downloads activity files with user ID, activity ID, and
        activity start timestamp in filename. The file extension reflects the
        actual format detected from the downloaded content.

        :return: List of saved activity file paths.
        """

        click.echo(
            f"Fetching activities from Garmin Connect "
            f"(start: {self.start_date}, end: {self.end_date} inclusive)..."
        )

        # Get list of activities, API is inclusive of both dates.
        # The API is designed to retrieve activities for entire days,
        # not specific time ranges within days.
        start_str = self.start_date.strftime("%Y-%m-%d")
        end_str = self.end_date.strftime("%Y-%m-%d")
        activities = self.garmin_client.get_activities_by_date(start_str, end_str)

        if not activities:
            click.secho(
                "No activities found in the specified date range.",
                fg="yellow",
            )
            return []

        click.echo(f"Found {len(activities)} activities.")

        downloaded_files = []

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

            # Download activity file (ORIGINAL format = ZIP archive).
            # A 404 means the activity exists in the list but has no
            # downloadable file (manually entered activity, deleted upload,
            # or a very old activity whose file is no longer retained by
            # Garmin). Skip and continue rather than aborting the run.
            try:
                raw_data = self.garmin_client.download_activity(
                    activity_id,
                    dl_fmt=ActivityDownloadFormat.ORIGINAL,
                )
            except GarminConnectionError as e:
                click.secho(
                    f"⚠️  Skipping activity {activity_id}: {e}.",
                    fg="yellow",
                )
                continue

            # Detect actual file format and extract content.
            result = self._extract_activity_content(activity_id, raw_data)
            if result is None:
                continue

            file_ext, file_content = result

            # Build filename using the detected extension.
            filename = (
                f"{self.user_id}_ACTIVITY_{activity_id}_{timestamp}.{file_ext}".replace(
                    ":", "-"
                )
            )
            filepath = self.ingest_dir / filename

            # Save to file.
            with open(filepath, "wb") as f:
                f.write(file_content)

            file_size = filepath.stat().st_size / 1024  # KB.
            click.echo(f"Saved: {filename} ({file_size:.1f} KB).")
            downloaded_files.append(filepath)

            # Fetch exercise sets for strength training activities.
            activity_type_key = (
                activity.get("activityType", {}).get("typeKey", "").lower()
            )
            if activity_type_key in (
                "strength_training",
                "fitness_equipment",
            ):
                time.sleep(0.1)  # Rate limiting between API calls.
                exercise_sets_file = self._extract_exercise_sets(activity_id, timestamp)
                if exercise_sets_file:
                    downloaded_files.append(exercise_sets_file)

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
            data = self.garmin_client.get_activity_exercise_sets(activity_id)
        except Exception as e:
            click.secho(
                f"Warning: Failed to fetch exercise sets for "
                f"activity {activity_id}: {e}.",
                fg="yellow",
            )
            return None

        # Skip if no exercise sets data.
        if not data or not data.get("exerciseSets"):
            click.echo(f"No exercise sets data for activity " f"{activity_id}.")
            return None

        filename = (
            f"{self.user_id}_EXERCISE_SETS_{activity_id}" f"_{timestamp}.json"
        ).replace(":", "-")
        filepath = self.ingest_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        file_size = filepath.stat().st_size / 1024  # KB.
        click.echo(f"Saved: {filename} ({file_size:.1f} KB).")
        return filepath


def extract(
    ingest_dir: Path,
    data_interval_start: Union[str, pendulum.DateTime],
    data_interval_end: Union[str, pendulum.DateTime],
    data_types: Optional[List[str]] = None,
    accounts: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """
    Download data from Garmin Connect for the specified date range.

    Supports multiple Garmin Connect accounts. Accounts are discovered automatically by
    scanning subdirectories in ~/.garminconnect/, where each subdirectory is named by
    the Garmin user ID and contains authentication tokens.

    :param ingest_dir: Directory path where extracted files will be saved.
    :param data_interval_start: Start date for data extraction (ISO string or datetime).
    :param data_interval_end: End date for data extraction (ISO string or datetime).
    :param data_types: Optional list of data type names to extract. If None, extracts
        all available data types including FIT activity files.
    :param accounts: Optional list of user_id strings to filter which accounts to
        extract. If None, extracts all discovered accounts.
    :param progress_callback: Optional callback function for progress updates.
    :return: Dictionary with counts of extracted files {'garmin_files': int,
        'activity_files': int}.
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
        return {"garmin_files": 0, "activity_files": 0}

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
        return {"garmin_files": 0, "activity_files": 0}

    # Apply account filter if provided.
    if accounts is not None:
        filter_set = set(accounts)
        discovered = [(uid, path) for uid, path in discovered if uid in filter_set]
        if not discovered:
            click.secho(
                f"No matching accounts found for filter: {accounts}",
                fg="yellow",
            )
            return {"garmin_files": 0, "activity_files": 0}

    click.echo(f"Found {len(discovered)} account(s) to extract.")

    # Extract from each account with error isolation.
    all_garmin_files = []
    all_activity_files = []
    failed_accounts = []

    for user_id, token_dir in discovered:
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
                data_types and {"ACTIVITY", "EXERCISE_SETS"} & set(data_types)
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

    # Check if any data was extracted.
    if not all_garmin_files and not all_activity_files:
        click.echo(
            "No Garmin Connect data found for extraction. Skipping downstream tasks."
        )
        return {"garmin_files": 0, "activity_files": 0}

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

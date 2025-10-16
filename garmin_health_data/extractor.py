"""
Garmin Connect data extraction module for standalone use.

Extracts FIT activity files and JSON Garmin data from Garmin Connect API and saves them
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
from garminconnect import Garmin

from garmin_health_data.constants import (
    APIMethodTimeParam,
    GarminDataType,
    GARMIN_DATA_REGISTRY,
)


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
        by the refresh_garmin_tokens.py script. The Garth library
        automatically handles token validation and session management once
        valid tokens are present.

        Sets both self.garmin_client and self.user_id upon successful
        authentication.

        Token Lifecycle:
        - Tokens are stored in ~/.garminconnect/ by default.
        - Valid for approximately 1 year from creation.
        - Garth attempts automatic token refresh, but may require manual
          refresh.
        - No credentials (email/password) required once valid tokens exist.

        When to run refresh_garmin_tokens.py:
        - Initial setup (no tokens exist).
        - Tokens have expired (approximately yearly).
        - MFA is required on your Garmin account.
        - Authentication errors occur in the pipeline.

        :param token_store_dir: Directory containing authentication tokens.
        :raises RuntimeError: If tokens are missing, expired, or invalid. Run
            refresh_garmin_tokens.py to resolve authentication issues.
        """

        token_store_path = Path(token_store_dir).expanduser()
        click.echo("Authenticating with Garmin Connect using saved tokens.")

        try:
            # Initialize Garmin client and load existing tokens.
            garmin = Garmin()
            garmin.login(str(token_store_path))
            self.garmin_client = garmin
            click.secho(
                f"Authentication successful for {self.garmin_client.full_name}"
                f" using saved tokens.",
                fg="green",
            )
        except Exception as e:
            error_msg = (
                f"Garmin authentication failed: {str(e)}\n\n"
                "To resolve this issue, run the token refresh script:\n"
                "   python refresh_garmin_tokens.py\n\n"
                "This script will:\n"
                "   - Guide you through Garmin Connect login.\n"
                "   - Handle MFA if enabled on your account.\n"
                "   - Save fresh tokens for pipeline use.\n\n"
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

        # Special case: ACTIVITY files use different extraction logic.
        if data_type.name == "ACTIVITY":
            click.echo(
                f"{data_type.emoji} ACTIVITY files will be handled "
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
        filename = f"{self.user_id}_{data_type.name}_{timestamp}.json"
        filepath = self.ingest_dir / filename

        # Save data.
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        click.echo(f"Saved {data_type.emoji} {data_type.name}: {filename}.")
        return [filepath]

    def extract_fit_activities(self) -> List[Path]:
        """
        Extract FIT activity files from Garmin Connect.

        This method always processes dates inclusively - both start_date and
        end_date are included in the extraction. The extract() function
        handles any exclusion logic before passing dates to the Extractor
        class. Downloads binary FIT files with user ID, activity ID, and
        activity start timestamp in filename.

        :return: List of saved FIT file paths.
        """

        click.echo(
            f"Fetching activities and associated FIT data from Garmin "
            f"Connect (start: {self.start_date}, end: {self.end_date} "
            f"inclusive)..."
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

            # Generate filename with local timezone date at noon for
            # consistent batching with ACTIVITIES_LIST file. Uses same
            # midday timestamp approach as _save_garmin_data().
            activity_start = pendulum.parse(activity.get("startTimeLocal"))
            activity_date = activity_start.date()
            midday_dt = datetime.combine(activity_date, datetime.min.time()).replace(
                hour=12, minute=0, second=0
            )
            timestamp = pendulum.instance(midday_dt, tz="UTC").to_iso8601_string()
            filename = f"{self.user_id}_ACTIVITY_{activity_id}_{timestamp}.fit"
            filepath = self.ingest_dir / filename

            # Download FIT file.
            fit_data = self.garmin_client.download_activity(
                activity_id,
                dl_fmt=self.garmin_client.ActivityDownloadFormat.ORIGINAL,
            )

            # Extract FIT file from ZIP archive.
            try:
                with zipfile.ZipFile(io.BytesIO(fit_data), "r") as zip_ref:
                    # Get the first (and typically only) file from the ZIP.
                    zip_files = zip_ref.namelist()
                    if not zip_files:
                        click.secho(
                            f"Empty ZIP archive for activity {activity_id}.",
                            fg="yellow",
                        )
                        continue

                    # Extract the FIT file content.
                    fit_file_content = zip_ref.read(zip_files[0])
            except zipfile.BadZipFile:
                # If it's not a ZIP file, use the data as-is (fallback).
                fit_file_content = fit_data

            # Save to file.
            with open(filepath, "wb") as f:
                f.write(fit_file_content)

            file_size = filepath.stat().st_size / 1024  # KB.
            click.echo(f"Saved: {filename} ({file_size:.1f} KB).")
            downloaded_files.append(filepath)

            # Rate limiting - be respectful to Garmin's servers.
            time.sleep(0.1)  # 100ms delay between downloads.

        click.echo(
            f"FIT activity extraction complete: {len(downloaded_files)} "
            f"files saved to {self.ingest_dir}."
        )
        return downloaded_files


def extract(
    ingest_dir: Path,
    data_interval_start: Union[str, pendulum.DateTime],
    data_interval_end: Union[str, pendulum.DateTime],
    data_types: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """
    Download data from Garmin Connect for the specified date range.

    Files are saved with standardized naming conventions to the specified
    directory for downstream processing.

    Authentication uses pre-existing tokens. If authentication fails, run
    refresh_garmin_tokens.py to obtain fresh tokens.

    Date ranges:
    Extract data with end date exclusion only if the start date is different
    from the end date. This ensures that when dates differ, we use exclusive
    end date logic, but when they are the same, we process the most recent
    data inclusively.

    The extraction includes:
    - Garmin data (JSON format) for specified data types or all available
      types from the GarminDataRegistry when `data_types` is None.
    - FIT activity files (binary format) when `data_types` is None or
      contains "ACTIVITY".

    :param ingest_dir: Directory path where extracted files will be saved.
    :param data_interval_start: Start date for data extraction (ISO string
        or datetime).
    :param data_interval_end: End date for data extraction (ISO string or
        datetime).
    :param data_types: Optional list of data type names to extract (e.g.,
        ['SLEEP', 'HRV', 'USER_PROFILE', 'ACTIVITY'], provided in
        constants.GarminDataRegistry). If None, extracts all available data
        types including FIT activity files. If empty list [], skip
        extraction.
    :param progress_callback: Optional callback function for progress
        updates.
    :return: Dictionary with counts of extracted files {'garmin_files': int,
        'activity_files': int}.
    :raises ValueError: If any requested data type names are not found in
        registry.
    """

    # Validate input parameters.
    if data_types is not None and len(data_types) == 0:
        error_msg = (
            "data_types is an empty list. Use None to extract all types "
            "or specify data types to extract. Extraction will be skipped."
        )
        click.echo(error_msg)
        return {"garmin_files": 0, "activity_files": 0}

    # Convert datetime objects or strings to date-only for Garmin API calls.
    # This ensures the API receives dates in YYYY-MM-DD format.
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

    # Initialize extractor and authenticate.
    extractor = GarminExtractor(start_date, end_date, ingest_dir, data_types)
    extractor.authenticate()

    # Extract Garmin data.
    if progress_callback:
        progress_callback("Extracting Garmin data...")
    garmin_files = extractor.extract_garmin_data()

    # Extract FIT activity files (if requested in data_types or data_types
    # is None).
    activity_files = []
    if data_types is None or (data_types and "ACTIVITY" in data_types):
        if progress_callback:
            progress_callback("Extracting FIT activity files...")
        activity_files = extractor.extract_fit_activities()

    # Check if any data was extracted.
    if not garmin_files and not activity_files:
        click.echo(
            "No Garmin Connect data found for extraction. " "Skipping downstream tasks."
        )
        return {"garmin_files": 0, "activity_files": 0}

    activity_summary = (
        "\n".join([f"      • {file.name}" for file in activity_files])
        if activity_files
        else "      (none)"
    )
    garmin_summary = (
        "\n".join([f"      • {file.name}" for file in garmin_files])
        if garmin_files
        else "      (none)"
    )
    click.echo(
        f"Extraction Summary:\n"
        f"   Saved to: {ingest_dir}\n"
        f"   FIT activity files (total: {len(activity_files)}):\n"
        f"{activity_summary}\n"
        f"   Garmin data files (total: {len(garmin_files)}):\n"
        f"{garmin_summary}"
    )

    return {
        "garmin_files": len(garmin_files),
        "activity_files": len(activity_files),
    }


def cli_extract(
    ingest_dir: str,
    start_date: str,
    end_date: str,
    data_types: List[str] = None,
) -> None:
    """
    CLI wrapper for extract function.

    :param ingest_dir: Directory path where extracted files will be saved.
    :param start_date: Start date for data extraction in YYYY-MM-DD format.
    :param end_date: End date for data extraction in YYYY-MM-DD format (exclusive).
    :param data_types: Optional list of data type names to extract (e.g., ['SLEEP',
        'HRV', 'USER_PROFILE', 'ACTIVITY'], provided in constants.GarminDataRegistry).
        If None, extracts all available data types including FIT activity files.
    """

    # Convert string dates to pendulum datetime objects.
    start_pendulum = pendulum.parse(start_date, tz="UTC")
    end_pendulum = pendulum.parse(end_date, tz="UTC")

    # Convert string path to Path object.
    ingest_path = Path(ingest_dir)

    # Direct call to extract function.
    extract(
        ingest_dir=ingest_path,
        data_interval_start=start_pendulum,
        data_interval_end=end_pendulum,
        data_types=data_types,
    )

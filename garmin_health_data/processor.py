"""
Garmin Connect data processor for ETL pipeline.

This module implements the GarminProcessor class that inherits from the base Processor
class to handle processing of Garmin Connect data files. It provides specialized
processing logic for different Garmin data types including user profiles, activities,
and health metrics.
"""

import json
import re
from collections import OrderedDict
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Any, Dict, Optional

import fitdecode
from sqlalchemy import and_, text
from sqlalchemy.orm import Session

from garmin_health_data.processor_helpers import Processor
from garmin_health_data.processor_helpers import FileSet
import click
from garmin_health_data.processor_helpers import upsert_model_instances
from garmin_health_data.constants import (
    GARMIN_DATA_REGISTRY,
    PR_TYPE_LABELS,
)
from garmin_health_data.models import (
    Acclimation,
    Activity,
    ActivityLapMetric,
    ActivitySplitMetric,
    ActivityTsMetric,
    BodyBattery,
    BreathingDisruption,
    CyclingAggMetrics,
    Floors,
    HeartRate,
    HRV,
    IntensityMinutes,
    PersonalRecord,
    RacePredictions,
    Respiration,
    RunningAggMetrics,
    Sleep,
    SleepMovement,
    SleepRestlessMoment,
    SpO2,
    Steps,
    Stress,
    SupplementalActivityMetric,
    SwimmingAggMetrics,
    TrainingLoad,
    TrainingReadiness,
    User,
    UserProfile,
    VO2Max,
)


class GarminProcessor(Processor):
    """
    Custom processor for Garmin Connect data files.

    Processes various types of Garmin data files including user profiles, activities,
    and health metrics. Handles data extraction, transformation, and loading into the
    appropriate database tables.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize GarminProcessor with additional instance attributes.
        """
        super().__init__(*args, **kwargs)
        self.user_id = None
        self.must_update_user = False

    def process_file_set(self, file_set: FileSet, session: Session):
        """
        Process all files in the given file set.

        Assumes all files in the file set belong to the same `user_id` and ensures that
        a user record exists before processing any files.

        :param file_set: FileSet containing Garmin data files to process.
        :param session: SQLAlchemy Session object.
        """

        # Extract `user_id` from the first file and set instance attribute.
        # All files in a file set have same `user_id` and `timestamp`.
        first_file = file_set.file_paths[0]
        filename_parts = self._parse_filename(first_file.name)
        self.user_id = filename_parts["user_id"]
        timestamp = filename_parts["timestamp"]

        click.echo(
            f"📁 Processing file set:\n"
            f"  • Number of files: {len(file_set.file_paths)}\n"
            f"  • Timestamp: {timestamp}\n"
            f"  • User ID: {self.user_id}"
        )

        # Ensure user exists in user table.
        self._ensure_user_exists(self.user_id, session)

        # Process JSON files using enum-based routing.
        # USER_PROFILE is processed first to update user demographics if needed.
        # ACTIVITIES_LIST is processed before PERSONAL_RECORDS and ACTIVITY FIT files
        #  to ensure the `activity.activity_id` is available for foreign key reference.
        file_processors = OrderedDict(
            [
                ("USER_PROFILE", self._process_user_profile),
                ("ACTIVITIES_LIST", self._process_activities),
                ("FLOORS", self._process_floors),
                ("HEART_RATE", self._process_heart_rate),
                ("INTENSITY_MINUTES", self._process_intensity_minutes),
                ("PERSONAL_RECORDS", self._process_personal_records),
                ("RACE_PREDICTIONS", self._process_race_predictions),
                ("RESPIRATION", self._process_respiration),
                ("SLEEP", self._process_sleep),
                ("STEPS", self._process_steps),
                ("STRESS", self._process_stress_body_battery),
                ("TRAINING_STATUS", self._process_training_status),
                ("TRAINING_READINESS", self._process_training_readiness),
                ("ACTIVITY", self._process_fit_file),
            ]
        )

        # Process all files in the order defined by `file_processors`.
        processed_enum_keys = set()

        for data_type_name, processor_func in file_processors.items():
            # Find the corresponding enum key in file_set for this data type.
            enum_key = next(
                (key for key in file_set.files.keys() if key.name == data_type_name),
                None,
            )

            # Process files if they exist for this data type.
            if enum_key:
                processed_enum_keys.add(enum_key)
                file_paths = file_set.files[enum_key]
                # Get emoji for this data type from registry.
                data_type = GARMIN_DATA_REGISTRY.get_by_name(enum_key.name)
                emoji = data_type.emoji
                for file_path in file_paths:
                    click.echo(
                        f"{emoji} Processing {enum_key.name} file: {file_path.name}."
                    )
                    # Call processor function.
                    processor_func(file_path, session)
                    click.echo(
                        f"✅ Successfully processed {enum_key.name} "
                        f"file {file_path.name}."
                    )

        # Check for any unprocessed files in the file set.
        unprocessed_keys = set(file_set.files.keys()) - processed_enum_keys
        for enum_key in unprocessed_keys:
            msg = f"⚠️ Processing Garmin data type {enum_key.name} not supported."
            click.secho(msg)

        click.echo("✅ Completed processing Garmin file set.")

    def _load_json_file(self, file_path: Path) -> dict:
        """
        Safely load and parse a JSON file.

        :param file_path: Path to the JSON file to load.
        :return: Parsed JSON data as a dictionary.
        """

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _parse_filename(self, filename: str) -> Dict[str, str]:
        """
        Parse a Garmin filename to extract `user_id`, `data_type`, and `timestamp`.

        Expected formats:
        - JSON files (based on extract._save_garmin_data()):
            {user_id}_{DATA_TYPE}_{timestamp}.json
        - FIT files (based on extract.extract_fit_activity()):
            {user_id}_ACTIVITY_{activity_id}_{timestamp}.fit

        :param filename: Name of the file to parse.
        :return: Dictionary with `user_id`, `data_type`, and `timestamp`.
        :raises ValueError: If filename doesn't match expected pattern.
        """

        pattern = r"^(\d+)_([A-Z_]+)(?:_\d+)?_([0-9T:\-Z\.]+)\.(json|fit)$"
        match = re.match(pattern, filename)

        if not match:
            raise ValueError(f"Filename does not match expected pattern: {filename}.")

        user_id, data_type, timestamp, file_extension = match.groups()

        return {
            "user_id": user_id,
            "data_type": data_type,
            "timestamp": timestamp,
            "file_extension": file_extension,
        }

    @staticmethod
    def _convert_field_name(field_name: str) -> str:
        """
        Convert camelCase field name to snake_case for database storage.

        :param field_name: Field name in camelCase.
        :return: Field name in snake_case.
        """

        # Insert underscore before capital letters and convert to lowercase.
        snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", field_name).lower()
        return snake_case

    @staticmethod
    def _parse_date_string(date_string: str) -> date:
        """
        Convert date string in YYYY-MM-DD format to Python date object.

        :param date_string: Date string in YYYY-MM-DD format.
        :return: Python date object.
        """
        return datetime.strptime(date_string, "%Y-%m-%d").date()

    def _ensure_user_exists(self, user_id: str, session: Session) -> None:
        """
        Ensure that a user record exists in the `user` table for the given `user_id`.

        If no user record exists, creates a minimal user record with only `user_id`.
        Sets `self.must_update_user` to True if the user record has null `full_name`.

        :param user_id: User ID to check and create if necessary.
        :param session: SQLAlchemy Session object.
        """

        # Check if user exists in user table.
        existing_user = session.query(User).filter(User.user_id == int(user_id)).first()

        if not existing_user:
            # Create minimal user record with conflict handling.
            session.execute(
                text(
                    """
                INSERT INTO user (user_id, full_name, birth_date) 
                VALUES (:user_id, NULL, NULL) 
                ON CONFLICT (user_id) DO NOTHING
            """
                ),
                {"user_id": int(user_id)},
            )
            session.flush()
            self.must_update_user = True
            click.echo(
                f"No existing user record found. "
                f"Created minimal user record for user {user_id}."
            )
        elif existing_user.full_name is None:
            # User exists but needs profile data update.
            self.must_update_user = True
            click.echo(f"User {user_id} exists but needs profile data update.")

    def _process_user_profile(self, file_path: Path, session: Session) -> None:
        """
        Process a user profile file and update `user` and `user_profile` tables.

        If `self.must_update_user` is True, updates the `user` table with `full_name`
        and `birth_date`. Creates/updates `user_profile` record with fitness metrics.

        :param file_path: Path to the user profile JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        data = self._load_json_file(file_path)
        profile_data = data["userData"]

        # Extract user demographics.
        full_name = data.get("full_name")
        birth_date = (
            datetime.strptime(profile_data["birthDate"], "%Y-%m-%d").date()
            if profile_data.get("birthDate")
            else None
        )

        # Update user table with demographics if needed.
        if self.must_update_user:
            user_record = (
                session.query(User).filter(User.user_id == int(self.user_id)).first()
            )
            if user_record:
                user_record.full_name = full_name
                user_record.birth_date = birth_date
                click.echo(f"Updated user demographics for user {self.user_id}.")

        # Get latest user profile.
        latest_profile = (
            session.query(UserProfile)
            .filter(and_(UserProfile.user_id == int(self.user_id), UserProfile.latest))
            .first()
        )

        # Prepare user profile data (fitness metrics only).
        profile_data_dict = {
            "user_id": int(self.user_id),
            "gender": (
                profile_data.get("gender").lower()
                if profile_data.get("gender")
                else None
            ),
            "weight": profile_data.get("weight"),
            "height": profile_data.get("height"),
            "vo2_max_running": profile_data.get("vo2MaxRunning"),
            "vo2_max_cycling": profile_data.get("vo2MaxCycling"),
            "lactate_threshold_speed": profile_data.get("lactateThresholdSpeed"),
            "lactate_threshold_heart_rate": profile_data.get(
                "lactateThresholdHeartRate"
            ),
            "moderate_intensity_minutes_hr_zone": profile_data.get(
                "moderateIntensityMinutesHrZone"
            ),
            "vigorous_intensity_minutes_hr_zone": profile_data.get(
                "vigorousIntensityMinutesHrZone"
            ),
            "latest": True,
        }

        # Set `latest`=False for the existing latest profile.
        if latest_profile:
            latest_profile.latest = False
            click.echo(
                f"Setting latest=False for previous latest user profile for user ID "
                f"{self.user_id}."
            )
            # Flush session to ensure the latest=False update is committed
            # before inserting the new record with latest=True.
            session.flush()

        # Create and insert the new user profile record.
        new_profile = UserProfile(**profile_data_dict)
        session.add(new_profile)
        session.flush()

    def _process_activities(self, file_path: Path, session: Session):
        """
        Process an activities list file and insert activity data into database tables.

        Processes each activity in the list, extracting data for the main activity
        table, sport-specific tables (running/cycling/swimming), and supplemental
        metrics table.

        :param file_path: Path to the activities list JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        activities_list = self._load_json_file(file_path)

        if not isinstance(activities_list, list):
            raise ValueError(f"Expected activities list to be an array: {file_path}.")

        if not activities_list:
            click.secho(f"⚠️ No activities found in {file_path}.", fg="yellow")
            return
        for activity in activities_list:
            self._process_single_activity(activity, session)

    def _process_single_activity(self, activity_data: Dict[str, Any], session: Session):
        """
        Process a single activity and upsert into appropriate database tables.

        :param activity_data: Activity data from JSON.
        :param session: SQLAlchemy Session object.
        """

        # Determine activity type for sport-specific processing.
        activity_type_key = (
            activity_data.get("activityType", {}).get("typeKey", "").lower()
        )
        click.echo(f"Processing {activity_type_key} activity data.")

        # Process main activity fields to database.
        activity_id = self._process_activity_base(activity_data, session)

        # Skip processing additional metrics if `activity_id` is None
        # (no valid activity data).
        if activity_id is None:
            return

        # Process sport-specific metrics.
        if activity_type_key == "running":
            self._process_running_metrics(activity_data, activity_id, session)
        elif "cycling" in activity_type_key or "biking" in activity_type_key:
            self._process_cycling_metrics(activity_data, activity_id, session)
        elif "swimming" in activity_type_key:
            self._process_swimming_metrics(activity_data, activity_id, session)

        # Process supplemental metrics from remaining fields.
        self._process_supplemental_metrics(activity_data, activity_id, session)

        click.echo(f"Processed {activity_type_key} activity with ID {activity_id}.")

    def _process_activity_base(
        self, activity_data: Dict[str, Any], session: Session
    ) -> int:
        """
        Extract activity fields and process to database using upsert with composite
        primary key.

        Uses pop() to remove processed fields from activity_data, enabling automatic
        supplemental metrics extraction without hardcoded exclusion lists.

        :param activity_data: Activity data from JSON (will be modified by pop()).
        :param session: SQLAlchemy Session object.
        :return: Activity ID from the processed record.
        """

        # Extract activity ID.
        activity_id = activity_data.pop("activityId")

        # Extract nested structures, all non-nullable, must be present.
        activity_type = activity_data.pop("activityType")
        event_type = activity_data.pop("eventType")

        # Extract timezone-naive timestamp strings.
        start_time_gmt_str = activity_data.pop("startTimeGMT")
        start_time_local_str = activity_data.pop("startTimeLocal")
        end_time_gmt_str = activity_data.pop("endTimeGMT", None)

        # Handle missing endTimeGMT in historical data by calculating from duration.
        if end_time_gmt_str is None:
            duration_seconds = activity_data.get("duration")
            if duration_seconds is not None:
                start_dt = datetime.fromisoformat(start_time_gmt_str)
                end_dt = start_dt + timedelta(seconds=duration_seconds)
                end_time_gmt_str = end_dt.isoformat()

        # Create timezone-aware datetimes and calculate offset.
        start_ts = datetime.fromisoformat(start_time_gmt_str).replace(
            tzinfo=timezone.utc
        )
        end_ts = datetime.fromisoformat(end_time_gmt_str).replace(tzinfo=timezone.utc)

        # Calculate timezone offset in hours (decimal precision for half-hour zones).
        utc_naive = datetime.fromisoformat(start_time_gmt_str)
        local_naive = datetime.fromisoformat(start_time_local_str)
        offset_seconds = (local_naive - utc_naive).total_seconds()
        timezone_offset_hours = offset_seconds / 3600

        # Create activity record - main fields.
        activity_record = {
            "activity_id": activity_id,
            "user_id": int(self.user_id),
            # Computed timestamp fields.
            "start_ts": start_ts,
            "end_ts": end_ts,
            "timezone_offset_hours": timezone_offset_hours,
        }

        # Non-nullable fields requiring custom mapping.
        activity_record.update(
            {
                "activity_type_id": activity_type.pop("typeId"),
                "activity_type_key": activity_type.pop("typeKey"),
                "event_type_id": event_type.pop("typeId"),
                "event_type_key": event_type.pop("typeKey"),
            }
        )

        # Auto-convert other non-nullable activity fields to snake_case.
        activity_fields_non_nullable = [
            "parent",
            "purposeful",
            "favorite",
            "pr",
            # Boolean flags (always present in historical data).
            "hasPolyline",
            "hasImages",
            "hasVideo",
            "hasHeatMap",
            "manualActivity",
            "autoCalcCalories",
        ]
        for field_name in activity_fields_non_nullable:
            snake_case_name = self._convert_field_name(field_name)
            activity_record[snake_case_name] = activity_data.pop(field_name)

        # Nullable fields requiring custom mapping.
        activity_record.update(
            {
                "average_hr": activity_data.pop("averageHR", None),
                "max_hr": activity_data.pop("maxHR", None),
            }
        )

        # Auto-convert other nullable activity fields to snake_case.
        activity_fields_nullable = [
            "duration",
            "distance",
            "calories",
            # Device and technical info (nullable for historical data).
            "activityName",
            "deviceId",
            "timeZoneId",
            "manufacturer",
            # Boolean flags (missing in historical data).
            "hasSplits",
            "elevationCorrected",
            "atpActivity",
            # Duration and timing.
            "elapsedDuration",
            "movingDuration",
            # Distance and speed.
            "lapCount",
            "averageSpeed",
            "maxSpeed",
            # Location.
            "startLatitude",
            "startLongitude",
            "endLatitude",
            "endLongitude",
            "locationName",
            # Training effects.
            "aerobicTrainingEffect",
            "aerobicTrainingEffectMessage",
            "anaerobicTrainingEffect",
            "anaerobicTrainingEffectMessage",
            "trainingEffectLabel",
            "activityTrainingLoad",
            # Body metrics.
            "differenceBodyBattery",
            "moderateIntensityMinutes",
            "vigorousIntensityMinutes",
            # Calories and hydration.
            "bmrCalories",
            "waterEstimated",
            # HR zones.
            "hrTimeInZone_1",
            "hrTimeInZone_2",
            "hrTimeInZone_3",
            "hrTimeInZone_4",
            "hrTimeInZone_5",
        ]
        for field_name in activity_fields_nullable:
            snake_case_name = self._convert_field_name(field_name)
            activity_record[snake_case_name] = activity_data.pop(field_name, None)

        # Process to database.
        if activity_record:
            activity = Activity(**activity_record)

            # Exclude `ts_data_available` from updates to preserve FIT file processing
            # flag. This prevents activities list reprocessing from overwriting the
            # flag set during FIT file processing.
            update_columns = [
                col.name
                for col in Activity.__table__.columns
                if col.name not in ["activity_id", "ts_data_available"]
            ]

            persisted_activity = upsert_model_instances(
                session=session,
                model_instances=[activity],
                conflict_columns=["activity_id"],
                update_columns=update_columns,
                on_conflict_update=True,
            )
            click.echo("Processed main activity metrics.")
            return persisted_activity[0].activity_id
        else:
            click.secho("⚠️ No main activity metrics found.", fg="yellow")
            return None

    def _process_swimming_metrics(
        self, activity_data: Dict[str, Any], activity_id: int, session: Session
    ):
        """
        Process swimming-specific metrics from activity data and insert into database.

        Uses pop() to remove processed fields from activity_data.

        :param activity_data: Activity data from JSON (will be modified by pop()).
        :param activity_id: Activity ID for foreign key reference.
        :param session: SQLAlchemy Session object.
        """

        # Extract fields requiring custom mapping first (all nullable).
        swimming_metrics = {
            "avg_swim_cadence": activity_data.pop(
                "averageSwimCadenceInStrokesPerMinute", None
            ),
            "avg_swolf": activity_data.pop("averageSwolf", None),
        }

        # Auto-convert other swimming-specific fields to snake_case.
        swimming_fields = [
            "poolLength",
            "activeLengths",
            "strokes",
            "avgStrokeDistance",
            "avgStrokes",
        ]
        for field_name in swimming_fields:
            snake_case_name = self._convert_field_name(field_name)
            swimming_metrics[snake_case_name] = activity_data.pop(field_name, None)

        # Insert swimming metrics into database if any metrics were found.
        if swimming_metrics:
            swimming_record = SwimmingAggMetrics(
                activity_id=activity_id, **swimming_metrics
            )
            # Adds the record to the session using .merge() semantics: SELECT to check
            # existence, then INSERT if new or UPDATE if existing (based on primary key
            # or unique constraints). If autoflush is True (default), the session
            # changes are persisted automatically to the database, unlike .add(). This
            # is because the merge() operation performs a query to the primary key and
            # determines its existence before deciding to insert or update.
            # https://docs.sqlalchemy.org/en/20/orm/session_basics.html#flushing
            session.merge(swimming_record)
            click.echo("Processed swimming metrics.")
        else:
            click.secho("⚠️ No swimming metrics found.", fg="yellow")

    def _process_cycling_metrics(
        self, activity_data: Dict[str, Any], activity_id: int, session: Session
    ):
        """
        Process cycling-specific metrics from activity data and insert into database.

        Uses pop() to remove processed fields from activity_data.

        :param activity_data: Activity data from JSON (will be modified by pop()).
        :param activity_id: Activity ID for foreign key reference.
        :param session: SQLAlchemy Session object.
        """

        # Extract fields requiring custom mapping first (all nullable).
        cycling_metrics = {
            "vo2_max_value": activity_data.pop("vO2MaxValue", None),
            "normalized_power": activity_data.pop("normPower", None),
            "avg_biking_cadence": activity_data.pop(
                "averageBikingCadenceInRevPerMinute", None
            ),
            "max_biking_cadence": activity_data.pop(
                "maxBikingCadenceInRevPerMinute", None
            ),
            "max_20min_power": activity_data.pop("max20MinPower", None),
        }

        # Auto-convert other cycling-specific fields to snake_case.
        cycling_fields = [
            "trainingStressScore",
            "intensityFactor",
            "avgPower",
            "maxPower",
            "avgLeftBalance",
            # Power curve fields.
            "maxAvgPower_1",
            "maxAvgPower_2",
            "maxAvgPower_5",
            "maxAvgPower_10",
            "maxAvgPower_20",
            "maxAvgPower_30",
            "maxAvgPower_60",
            "maxAvgPower_120",
            "maxAvgPower_300",
            "maxAvgPower_600",
            "maxAvgPower_1200",
            "maxAvgPower_1800",
            "maxAvgPower_3600",
            "maxAvgPower_7200",
            "maxAvgPower_18000",
            # Power zones.
            "powerTimeInZone_1",
            "powerTimeInZone_2",
            "powerTimeInZone_3",
            "powerTimeInZone_4",
            "powerTimeInZone_5",
            "powerTimeInZone_6",
            "powerTimeInZone_7",
            # Environmental conditions.
            "minTemperature",
            "maxTemperature",
            # Elevation metrics.
            "elevationGain",
            "elevationLoss",
            "minElevation",
            "maxElevation",
            # Respiration metrics.
            "minRespirationRate",
            "maxRespirationRate",
            "avgRespirationRate",
        ]
        for field_name in cycling_fields:
            snake_case_name = self._convert_field_name(field_name)
            cycling_metrics[snake_case_name] = activity_data.pop(field_name, None)

        # Insert cycling metrics into database if any metrics were found.
        if cycling_metrics:
            cycling_record = CyclingAggMetrics(
                activity_id=activity_id, **cycling_metrics
            )
            session.merge(cycling_record)
            click.echo("Processed cycling metrics.")
        else:
            click.secho("⚠️ No cycling metrics found.", fg="yellow")

    def _process_running_metrics(
        self, activity_data: Dict[str, Any], activity_id: int, session: Session
    ):
        """
        Process running-specific metrics from activity data and insert into database.

        Uses pop() to remove processed fields from activity_data.

        :param activity_data: Activity data from JSON (will be modified by pop()).
        :param activity_id: Activity ID for foreign key reference.
        :param session: SQLAlchemy Session object.
        """

        # Extract fields requiring custom mapping first (all nullable).
        running_metrics = {
            "vo2_max_value": activity_data.pop("vO2MaxValue", None),
            "normalized_power": activity_data.pop("normPower", None),
            "avg_running_cadence": activity_data.pop(
                "averageRunningCadenceInStepsPerMinute", None
            ),
            "max_running_cadence": activity_data.pop(
                "maxRunningCadenceInStepsPerMinute", None
            ),
        }

        # Auto-convert other running-specific fields to snake_case.
        running_fields = [
            "steps",
            "maxDoubleCadence",
            # Running dynamics.
            "avgVerticalOscillation",
            "avgGroundContactTime",
            "avgStrideLength",
            "avgVerticalRatio",
            "avgGroundContactBalance",
            # Power metrics.
            "avgPower",
            "maxPower",
            # Power zones.
            "powerTimeInZone_1",
            "powerTimeInZone_2",
            "powerTimeInZone_3",
            "powerTimeInZone_4",
            "powerTimeInZone_5",
            # Environmental conditions.
            "minTemperature",
            "maxTemperature",
            # Elevation metrics.
            "elevationGain",
            "elevationLoss",
            "minElevation",
            "maxElevation",
            # Physiological metrics.
            "minRespirationRate",
            "maxRespirationRate",
            "avgRespirationRate",
        ]
        for field_name in running_fields:
            snake_case_name = self._convert_field_name(field_name)
            running_metrics[snake_case_name] = activity_data.pop(field_name, None)

        # Insert running metrics into database if any metrics were found.
        if running_metrics:
            running_record = RunningAggMetrics(
                activity_id=activity_id, **running_metrics
            )
            session.merge(running_record)
            click.echo("Processed running metrics.")
        else:
            click.secho("⚠️ No running metrics found.", fg="yellow")

    def _process_supplemental_metrics(
        self, activity_data: Dict[str, Any], activity_id: int, session: Session
    ):
        """
        Process supplemental metrics from remaining fields and insert into database.

        This method processes fields that remain in activity_data after all other
        extraction methods have used pop() to remove their processed fields. This
        eliminates the need for hardcoded exclusion lists and ensures no fields are
        accidentally duplicated.

        :param activity_data: Activity data from JSON (remaining fields after pop()
            operations).
        :param activity_id: Activity ID for foreign key reference.
        :param session: SQLAlchemy Session object.
        """

        supplemental_metrics = {}

        for field_name, value in activity_data.items():
            # Skip dictionaries and lists (complex nested structures).
            if isinstance(value, (dict, list)):
                continue

            # Skip ownerProfileImageUrl fields (user-specific image URLs).
            if "ownerProfileImageUrl" in field_name:
                continue

            # Convert field name to snake_case and store numeric values as floats.
            # Include int, float, and bool (True=1.0, False=0.0) types only.
            if value is not None and isinstance(value, (int, float, bool)):
                snake_case_name = self._convert_field_name(field_name)
                supplemental_metrics[snake_case_name] = float(value)

        # Insert supplemental metrics into database if any metrics were found.
        if supplemental_metrics:
            supplemental_records = [
                SupplementalActivityMetric(
                    activity_id=activity_id,
                    metric=metric_name,
                    value=value,  # Already converted to float in extraction method.
                )
                for metric_name, value in supplemental_metrics.items()
            ]

            # Upsert supplemental metrics records in bulk.
            upsert_model_instances(
                session=session,
                model_instances=supplemental_records,
                conflict_columns=["activity_id", "metric"],
                on_conflict_update=True,
            )
            click.echo("Processed supplemental activity metrics.")
        else:
            click.secho("⚠️ No supplemental activity metrics found.", fg="yellow")

    def _process_sleep(self, file_path: Path, session: Session):
        """
        Process a SLEEP file and extract sleep session data.

        Extracts main sleep record and all timeseries data (movement, restless moments,
        SpO2, HRV, breathing disruption) according to prompt.md specifications.

        :param file_path: Path to the SLEEP JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        sleep_data = self._load_json_file(file_path)

        # Process main sleep record to database.
        sleep_id = self._process_sleep_base(sleep_data, session)

        # Skip timeseries processing if `sleep_id` is None
        # (no valid sleep data).
        if sleep_id is None:
            return

        # Extract and upsert timeseries data.
        self._process_sleep_movement(sleep_data, sleep_id, session)
        self._process_sleep_restless_moments(sleep_data, sleep_id, session)
        self._process_sleep_spo2_data(sleep_data, sleep_id, session)
        self._process_sleep_hrv_data(sleep_data, sleep_id, session)
        self._process_sleep_breathing_disruption(sleep_data, sleep_id, session)

    def _process_sleep_base(
        self, sleep_data: Dict[str, Any], session: Session
    ) -> Optional[int]:
        """
        Extract sleep fields and process to database using upsert with composite primary
        key.

        Uses pop() to remove processed fields from sleep_data, enabling time-series
        extraction without hardcoded exclusion lists.

        :param sleep_data: Sleep data from JSON (will be modified by pop()).
        :param session: SQLAlchemy Session object.
        :return: Sleep ID from the processed record, if the record was created.
        """

        # Extract dailySleepDTO section.
        daily_sleep_dto = sleep_data.pop("dailySleepDTO", {})

        # Skip processing if dailySleepDTO is empty.
        if not daily_sleep_dto:
            return None

        # Extract and convert timestamps: non-nullable fields, no defaults.
        sleep_start_gmt_ms = daily_sleep_dto.pop("sleepStartTimestampGMT")
        sleep_end_gmt_ms = daily_sleep_dto.pop("sleepEndTimestampGMT")
        sleep_start_local_ms = daily_sleep_dto.pop("sleepStartTimestampLocal")

        # Only skip if critical GMT timestamps are missing.
        if sleep_start_gmt_ms is None or sleep_end_gmt_ms is None:
            return None

        # Remove sleepEndTimestampLocal if present (not used, prevents auto-conversion).
        daily_sleep_dto.pop("sleepEndTimestampLocal", None)

        # Convert epoch milliseconds to datetime objects.
        start_ts = datetime.fromtimestamp(sleep_start_gmt_ms / 1000, tz=timezone.utc)
        end_ts = datetime.fromtimestamp(sleep_end_gmt_ms / 1000, tz=timezone.utc)
        sleep_start_local = datetime.fromtimestamp(
            sleep_start_local_ms / 1000, tz=timezone.utc
        )

        # Calculate timezone offset in hours (decimal precision for half-hour zones).
        offset_seconds = (sleep_start_local - start_ts).total_seconds()
        timezone_offset_hours = offset_seconds / 3600

        # Start building sleep record.
        sleep_record = {
            # Foreign key field.
            "user_id": int(self.user_id),
            # Non-nullable timestamps.
            "start_ts": start_ts,
            "end_ts": end_ts,
            "timezone_offset_hours": timezone_offset_hours,
        }

        # Remove the id field from JSON (not used: sleep_id is auto-generated).
        daily_sleep_dto.pop("id", None)

        # Nullable fields requiring custom mapping.
        sleep_record.update(
            {
                "sleep_quality_type_pk": daily_sleep_dto.pop(
                    "sleepQualityTypePK", None
                ),
                "sleep_result_type_pk": daily_sleep_dto.pop("sleepResultTypePK", None),
                "average_spo2": daily_sleep_dto.pop("averageSpO2Value", None),
                "lowest_spo2": daily_sleep_dto.pop("lowestSpO2Value", None),
                "highest_spo2": daily_sleep_dto.pop("highestSpO2Value", None),
                "average_spo2_hr_sleep": daily_sleep_dto.pop(
                    "averageSpO2HRSleep", None
                ),
                "average_respiration": daily_sleep_dto.pop(
                    "averageRespirationValue", None
                ),
                "lowest_respiration": daily_sleep_dto.pop(
                    "lowestRespirationValue", None
                ),
                "highest_respiration": daily_sleep_dto.pop(
                    "highestRespirationValue", None
                ),
            }
        )

        # Auto-convert other nullable sleep fields to snake_case.
        sleep_fields_nullable = [
            # Basic sleep data.
            "calendarDate",
            "sleepVersion",
            "ageGroup",
            "respirationVersion",
            # Sleep durations.
            "sleepTimeSeconds",
            "napTimeSeconds",
            "unmeasurableSleepSeconds",
            "deepSleepSeconds",
            "lightSleepSeconds",
            "remSleepSeconds",
            "awakeSleepSeconds",
            "awakeCount",
            # Sleep detection.
            "sleepWindowConfirmed",
            "sleepWindowConfirmationType",
            "retro",
            "sleepFromDevice",
            "deviceRemCapable",
            # Stress and breathing disruption.
            "avgSleepStress",
            "breathingDisruptionSeverity",
            # Sleep insights.
            "sleepScoreFeedback",
            "sleepScoreInsight",
            "sleepScorePersonalizedInsight",
        ]
        for field_name in sleep_fields_nullable:
            snake_case_name = self._convert_field_name(field_name)
            sleep_record[snake_case_name] = daily_sleep_dto.pop(field_name, None)

        # Extract sleep scores fields (only specified ones).
        sleep_scores = daily_sleep_dto.pop("sleepScores", {})

        # Extract nested score objects.
        total_duration = sleep_scores.pop("totalDuration", {})
        stress = sleep_scores.pop("stress", {})
        awake_count = sleep_scores.pop("awakeCount", {})
        restlessness = sleep_scores.pop("restlessness", {})
        overall = sleep_scores.pop("overall", {})
        light_percentage = sleep_scores.pop("lightPercentage", {})
        deep_percentage = sleep_scores.pop("deepPercentage", {})
        rem_percentage = sleep_scores.pop("remPercentage", {})

        sleep_record.update(
            {
                "total_duration_key": total_duration.pop("qualifierKey", None),
                "stress_key": stress.pop("qualifierKey", None),
                "awake_count_key": awake_count.pop("qualifierKey", None),
                "restlessness_key": restlessness.pop("qualifierKey", None),
                "score_overall_key": overall.pop("qualifierKey", None),
                "score_overall_value": overall.pop("value", None),
                "light_pct_key": light_percentage.pop("qualifierKey", None),
                "light_pct_value": light_percentage.pop("value", None),
                "deep_pct_key": deep_percentage.pop("qualifierKey", None),
                "deep_pct_value": deep_percentage.pop("value", None),
                "rem_pct_key": rem_percentage.pop("qualifierKey", None),
                "rem_pct_value": rem_percentage.pop("value", None),
            }
        )

        # Extract sleep need fields.
        sleep_need = daily_sleep_dto.pop("sleepNeed", {})
        sleep_record.update(
            {
                "sleep_need_baseline": sleep_need.pop("baseline", None),
                "sleep_need_actual": sleep_need.pop("actual", None),
                "sleep_need_feedback": sleep_need.pop("feedback", None),
                "sleep_need_training_feedback": sleep_need.pop(
                    "trainingFeedback", None
                ),
                "sleep_need_history_adj": sleep_need.pop(
                    "sleepHistoryAdjustment", None
                ),
                "sleep_need_hrv_adj": sleep_need.pop("hrvAdjustment", None),
                "sleep_need_nap_adj": sleep_need.pop("napAdjustment", None),
            }
        )

        # Extract next sleep need fields.
        next_sleep_need = daily_sleep_dto.pop("nextSleepNeed", {})
        sleep_record.update(
            {
                "next_sleep_need_baseline": next_sleep_need.pop("baseline", None),
                "next_sleep_need_actual": next_sleep_need.pop("actual", None),
                "next_sleep_need_feedback": next_sleep_need.pop("feedback", None),
                "next_sleep_need_training_feedback": next_sleep_need.pop(
                    "trainingFeedback", None
                ),
                "next_sleep_need_history_adj": next_sleep_need.pop(
                    "sleepHistoryAdjustment", None
                ),
                "next_sleep_need_hrv_adj": next_sleep_need.pop("hrvAdjustment", None),
                "next_sleep_need_nap_adj": next_sleep_need.pop("napAdjustment", None),
            }
        )

        # Extract wellnessSpO2SleepSummaryDTO fields.
        wellness_spo2 = sleep_data.pop("wellnessSpO2SleepSummaryDTO", {})
        sleep_record.update(
            {
                "number_of_events_below_threshold": wellness_spo2.pop(
                    "numberOfEventsBelowThreshold", None
                ),
                "duration_of_events_below_threshold": wellness_spo2.pop(
                    "durationOfEventsBelowThreshold", None
                ),
            }
        )

        # Auto-convert remaining root-level fields (all nullable).
        root_level_fields_nullable = [
            "restlessMomentsCount",
            "avgOvernightHrv",
            "hrvStatus",
            "bodyBatteryChange",
            "restingHeartRate",
            "skinTempDataExists",
            "remSleepData",
        ]
        for field_name in root_level_fields_nullable:
            snake_case_name = self._convert_field_name(field_name)
            sleep_record[snake_case_name] = sleep_data.pop(field_name, None)

        # Process to database.
        if sleep_record:
            sleep = Sleep(**sleep_record)
            # Exclude `sleep_id` (serial primary key) from updates to avoid FK
            # constraint issues.
            update_columns = [
                col.name
                for col in Sleep.__table__.columns
                if col.name not in ["user_id", "start_ts", "sleep_id"]
            ]
            persisted_sleep = upsert_model_instances(
                session=session,
                model_instances=[sleep],
                conflict_columns=["user_id", "start_ts"],
                update_columns=update_columns,
                on_conflict_update=True,
            )
            click.echo("Processed main sleep data.")
            return persisted_sleep[0].sleep_id
        else:
            click.secho("⚠️ No main sleep data found.", fg="yellow")
            return None

    def _process_sleep_movement(
        self, sleep_data: dict, sleep_id: int, session: Session
    ):
        """
        Process sleep movement data from sleepMovement array. Uses pop() to remove
        processed fields from sleep_data dictionary.

        :param sleep_data: Complete JSON sleep data.
        :param sleep_id: Sleep session ID.
        :param session: SQLAlchemy Session object.
        """

        sleep_movement = sleep_data.pop("sleepMovement", [])
        if not sleep_movement:
            return

        movement_records = []
        for movement in sleep_movement:
            timestamp_str = movement.pop("startGMT")
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str).replace(
                    tzinfo=timezone.utc
                )
                movement_records.append(
                    SleepMovement(
                        sleep_id=sleep_id,
                        timestamp=timestamp,
                        activity_level=movement.pop("activityLevel", None),
                    )
                )

        if movement_records:
            upsert_model_instances(
                session=session,
                model_instances=movement_records,
                conflict_columns=["sleep_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(movement_records)} sleep movement records.")
        else:
            click.secho("⚠️ No sleep movement data found.", fg="yellow")

    def _process_sleep_restless_moments(
        self, sleep_data: dict, sleep_id: int, session: Session
    ):
        """
        Process sleep restless moments from sleepRestlessMoments array. Uses pop() to
        remove processed fields from sleep_data dictionary.

        :param sleep_data: Complete JSON sleep data.
        :param sleep_id: Sleep session ID.
        :param session: SQLAlchemy Session object.
        """

        restless_moments = sleep_data.pop("sleepRestlessMoments", [])
        if not restless_moments:
            return

        restless_records = []
        for moment in restless_moments:
            start_gmt_ms = moment.pop("startGMT")
            if start_gmt_ms:
                timestamp = datetime.fromtimestamp(start_gmt_ms / 1000, tz=timezone.utc)
                restless_records.append(
                    SleepRestlessMoment(
                        sleep_id=sleep_id,
                        timestamp=timestamp,
                        value=moment.pop("value", None),
                    )
                )

        if restless_records:
            upsert_model_instances(
                session=session,
                model_instances=restless_records,
                conflict_columns=["sleep_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(
                f"Processed {len(restless_records)} sleep restless moment records."
            )
        else:
            click.secho("⚠️ No sleep restless moment data found.", fg="yellow")

    def _process_sleep_spo2_data(
        self, sleep_data: dict, sleep_id: int, session: Session
    ):
        """
        Process SpO2 data from wellnessEpochSPO2DataDTOList array. Uses pop() to remove
        processed fields from sleep_data dictionary.

        :param sleep_data: Complete JSON sleep data.
        :param sleep_id: Sleep session ID.
        :param session: SQLAlchemy Session object.
        """

        spo2_data = sleep_data.pop("wellnessEpochSPO2DataDTOList", [])
        if not spo2_data:
            click.secho("⚠️ No SpO2 data found.", fg="yellow")
            return

        spo2_records = []
        for spo2_reading in spo2_data:
            timestamp_str = spo2_reading.pop("epochTimestamp")
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str).replace(
                    tzinfo=timezone.utc
                )
                spo2_records.append(
                    SpO2(
                        sleep_id=sleep_id,
                        timestamp=timestamp,
                        value=spo2_reading.pop("spo2Reading", None),
                    )
                )

        if spo2_records:
            upsert_model_instances(
                session=session,
                model_instances=spo2_records,
                conflict_columns=["sleep_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(spo2_records)} SpO2 records.")
        else:
            click.secho("⚠️ No SpO2 data found.", fg="yellow")

    def _process_sleep_hrv_data(
        self, sleep_data: dict, sleep_id: int, session: Session
    ):
        """
        Process HRV data from hrvData array. Uses pop() to remove processed fields from
        sleep_data dictionary.

        :param sleep_data: Complete JSON sleep data.
        :param sleep_id: Sleep session ID.
        :param session: SQLAlchemy Session object.
        """

        hrv_data = sleep_data.pop("hrvData", [])
        if not hrv_data:
            click.secho("⚠️ No HRV data found.", fg="yellow")
            return

        hrv_records = []
        for hrv_reading in hrv_data:
            start_gmt_ms = hrv_reading.pop("startGMT")
            if start_gmt_ms:
                timestamp = datetime.fromtimestamp(start_gmt_ms / 1000, tz=timezone.utc)
                hrv_records.append(
                    HRV(
                        sleep_id=sleep_id,
                        timestamp=timestamp,
                        value=hrv_reading.pop("value", None),
                    )
                )

        if hrv_records:
            upsert_model_instances(
                session=session,
                model_instances=hrv_records,
                conflict_columns=["sleep_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(hrv_records)} HRV records.")
        else:
            click.secho("⚠️ No HRV data found.", fg="yellow")

    def _process_sleep_breathing_disruption(
        self, sleep_data: dict, sleep_id: int, session: Session
    ):
        """
        Process breathing disruption data from breathingDisruptionData array. Uses pop()
        to remove processed fields from sleep_data dictionary.

        :param sleep_data: Complete JSON sleep data.
        :param sleep_id: Sleep session ID.
        :param session: SQLAlchemy Session object.
        """

        breathing_data = sleep_data.pop("breathingDisruptionData", [])
        if not breathing_data:
            click.secho("⚠️ No breathing disruption data found.", fg="yellow")
            return

        breathing_records = []
        for breathing_event in breathing_data:
            start_gmt_ms = breathing_event.pop("startGMT")
            if start_gmt_ms:
                timestamp = datetime.fromtimestamp(start_gmt_ms / 1000, tz=timezone.utc)
                breathing_records.append(
                    BreathingDisruption(
                        sleep_id=sleep_id,
                        timestamp=timestamp,
                        value=breathing_event.pop("value", None),
                    )
                )

        if breathing_records:
            upsert_model_instances(
                session=session,
                model_instances=breathing_records,
                conflict_columns=["sleep_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(
                f"Processed {len(breathing_records)} breathing disruption records."
            )
        else:
            click.secho("⚠️ No breathing disruption data found.", fg="yellow")

    def _process_training_status(self, file_path: Path, session: Session):
        """
        Process a TRAINING_STATUS file. Extracts VO2 max data, acclimation metrics, and
        training load information.

        :param file_path: Path to the TRAINING_STATUS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        training_status_data = self._load_json_file(file_path)

        # Extract and process each data type.
        self._process_vo2_max_and_acclimation(training_status_data, session)
        self._process_training_load(training_status_data, session)

    def _process_vo2_max_and_acclimation(
        self, training_status_data: dict, session: Session
    ):
        """
        Extract and process VO2 max and acclimation data from mostRecentVO2Max section.
        Handles separate dates for generic and cycling VO2 max values, and processes
        heat/altitude acclimation data from the same section.

        :param training_status_data: Complete JSON training status data.
        :param session: SQLAlchemy Session object.
        """

        vo2_max_section = training_status_data.pop("mostRecentVO2Max", {})
        if not vo2_max_section:
            click.secho("⚠️ No VO2 max data found.", fg="yellow")
            return

        generic_record = None

        # Process generic VO2 max data if available.
        generic_data = vo2_max_section.pop("generic", {})
        if generic_data and generic_data.get("calendarDate"):
            generic_date = self._parse_date_string(generic_data.pop("calendarDate"))
            vo2_max_generic = generic_data.pop("vo2MaxPreciseValue", None)

            # Create VO2Max record with generic data.
            generic_record = VO2Max(
                user_id=int(self.user_id),
                date=generic_date,
                vo2_max_generic=vo2_max_generic,
            )

            # UPSERT generic record with only generic columns.
            upsert_model_instances(
                session=session,
                model_instances=[generic_record],
                conflict_columns=["user_id", "date"],
                update_columns=["vo2_max_generic"],
                on_conflict_update=True,
            )
            click.echo("Processed generic VO2 max data.")
        else:
            click.secho("⚠️ No generic VO2 max data found.", fg="yellow")

        # Process cycling VO2 max data if available.
        cycling_data = vo2_max_section.pop("cycling", {})
        if cycling_data and cycling_data.get("calendarDate"):
            cycling_date = self._parse_date_string(cycling_data.pop("calendarDate"))
            vo2_max_cycling = cycling_data.pop("vo2MaxPreciseValue", None)

            target_record = None

            # Check if we can reuse the generic record.
            if generic_record and generic_record.date == cycling_date:
                # Same date: add cycling field to existing generic record.
                target_record = generic_record
            else:
                # Different date: create new record for cycling data.
                target_record = VO2Max(
                    user_id=int(self.user_id),
                    date=cycling_date,
                )

            # Set cycling field on target record.
            target_record.vo2_max_cycling = vo2_max_cycling

            # UPSERT cycling record with only cycling columns.
            upsert_model_instances(
                session=session,
                model_instances=[target_record],
                conflict_columns=["user_id", "date"],
                update_columns=["vo2_max_cycling"],
                on_conflict_update=True,
            )
            click.echo("Processed cycling VO2 max data.")
        else:
            click.secho("⚠️ No cycling VO2 max data found.", fg="yellow")

        # Process acclimation data from the same section.
        acclimation_data = vo2_max_section.pop("heatAltitudeAcclimation", {})
        if acclimation_data and acclimation_data.get("calendarDate"):
            # Extract fields requiring custom mapping first.
            record_data = {
                "user_id": int(self.user_id),
                "date": self._parse_date_string(acclimation_data.pop("calendarDate")),
            }

            # Auto-convert all acclimation fields to snake_case.
            acclimation_fields = [
                "heatAcclimationPercentage",
                "altitudeAcclimation",
                "currentAltitude",
                "acclimationPercentage",
                "altitudeTrend",
                "heatTrend",
            ]
            for field_name in acclimation_fields:
                snake_case_name = self._convert_field_name(field_name)
                record_data[snake_case_name] = acclimation_data.pop(field_name, None)

            acclimation_record = Acclimation(**record_data)
            upsert_model_instances(
                session=session,
                model_instances=[acclimation_record],
                conflict_columns=["user_id", "date"],
                on_conflict_update=True,
            )
            click.echo("Processed acclimation data.")
        else:
            click.secho("⚠️ No acclimation data found.", fg="yellow")

    def _process_training_load(self, training_status_data: dict, session: Session):
        """
        Extract and process training load data from mostRecentTrainingLoadBalance and
        mostRecentTrainingStatus sections.

        :param training_status_data: Complete JSON training status data.
        :param session: SQLAlchemy Session object.
        """

        balance_record = None

        # Extract training load balance data.
        training_load_balance = training_status_data.pop(
            "mostRecentTrainingLoadBalance", {}
        )
        training_status = training_status_data.pop("mostRecentTrainingStatus", {})

        # Skip processing if both sections are empty.
        if not training_load_balance and not training_status:
            click.secho("⚠️ No training load data found.", fg="yellow")
            return

        # Process balance data if available.
        if training_load_balance:
            device_map = training_load_balance.pop(
                "metricsTrainingLoadBalanceDTOMap", {}
            )
            if device_map:
                # Get data from the first device.
                first_device_id = next(iter(device_map.keys()))
                balance_device_data = device_map[first_device_id]
                balance_date = balance_device_data.pop("calendarDate", None)
                if balance_date:
                    # Create TrainingLoad record with balance data.
                    balance_record = TrainingLoad(
                        user_id=int(self.user_id),
                        date=self._parse_date_string(balance_date),
                    )

                    # Auto-convert balance fields to snake_case and set on model.
                    balance_fields = [
                        "trainingBalanceFeedbackPhrase",
                        "monthlyLoadAerobicLow",
                        "monthlyLoadAerobicHigh",
                        "monthlyLoadAnaerobic",
                        "monthlyLoadAerobicLowTargetMin",
                        "monthlyLoadAerobicLowTargetMax",
                        "monthlyLoadAerobicHighTargetMin",
                        "monthlyLoadAerobicHighTargetMax",
                        "monthlyLoadAnaerobicTargetMin",
                        "monthlyLoadAnaerobicTargetMax",
                    ]
                    balance_update_columns = []
                    for field_name in balance_fields:
                        snake_case_name = self._convert_field_name(field_name)
                        field_value = balance_device_data.pop(field_name, None)
                        setattr(balance_record, snake_case_name, field_value)
                        balance_update_columns.append(snake_case_name)

                    # UPSERT balance record with only balance columns.
                    upsert_model_instances(
                        session=session,
                        model_instances=[balance_record],
                        conflict_columns=["user_id", "date"],
                        update_columns=balance_update_columns,
                        on_conflict_update=True,
                    )
                    click.echo("Processed training load balance data.")
                else:
                    click.secho("⚠️ No training load balance data found.", fg="yellow")

        # Process status data if available.
        if training_status:
            device_map = training_status.pop("latestTrainingStatusData", {})
            if device_map:
                # Get data from the first device.
                first_device_id = next(iter(device_map.keys()))
                status_device_data = device_map[first_device_id]
                status_date = status_device_data.pop("calendarDate", None)
                if status_date:
                    target_record = None

                    # Check if we can reuse the balance record.
                    if (
                        balance_record
                        and balance_record.date == self._parse_date_string(status_date)
                    ):
                        # Same date: add status fields to existing balance record.
                        target_record = balance_record
                    else:
                        # Different date: create new record for status data.
                        target_record = TrainingLoad(
                            user_id=int(self.user_id),
                            date=self._parse_date_string(status_date),
                        )

                    # Extract ACWR data.
                    acwr_data = status_device_data.pop("acuteTrainingLoadDTO", {})
                    if acwr_data is None:
                        acwr_data = {}

                    # Auto-convert ACWR fields to snake_case and set on model.
                    acwr_fields = [
                        "acwrPercent",
                        "acwrStatus",
                        "acwrStatusFeedback",
                        "dailyTrainingLoadAcute",
                        "maxTrainingLoadChronic",
                        "minTrainingLoadChronic",
                        "dailyTrainingLoadChronic",
                        "dailyAcuteChronicWorkloadRatio",
                    ]
                    status_update_columns = []
                    for field_name in acwr_fields:
                        snake_case_name = self._convert_field_name(field_name)
                        field_value = acwr_data.pop(field_name, None)
                        setattr(target_record, snake_case_name, field_value)
                        status_update_columns.append(snake_case_name)

                    # Auto-convert other status fields to snake_case and set on model.
                    status_fields = ["trainingStatus", "trainingStatusFeedbackPhrase"]
                    for field_name in status_fields:
                        snake_case_name = self._convert_field_name(field_name)
                        field_value = status_device_data.pop(field_name, None)
                        setattr(target_record, snake_case_name, field_value)
                        status_update_columns.append(snake_case_name)

                    # UPSERT status record with only status columns.
                    upsert_model_instances(
                        session=session,
                        model_instances=[target_record],
                        conflict_columns=["user_id", "date"],
                        update_columns=status_update_columns,
                        on_conflict_update=True,
                    )
                    click.echo("Processed acute/chronic training load data.")
                else:
                    click.secho(
                        "⚠️ No acute/chronic training load data found.", fg="yellow"
                    )

    def _process_training_readiness(self, file_path: Path, session: Session):
        """
        Process a TRAINING_READINESS file containing daily readiness scores and factors.

        Extracts training readiness records and loads them into the `training_readiness`
        table with upsert functionality based on `user_id` and `timestamp`.

        :param file_path: Path to the TRAINING_READINESS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        training_readiness_list = self._load_json_file(file_path)

        readiness_records = []
        for readiness_data in training_readiness_list:
            # Skip processing excluded fields per plan requirements.
            # Extract and exclude: userProfilePK, calendarDate, timestampLocal,
            # deviceId.
            readiness_data.pop("userProfilePK", None)
            readiness_data.pop("calendarDate", None)
            timestamp_local_str = readiness_data.pop("timestampLocal", None)
            readiness_data.pop("deviceId", None)

            # Extract timestamp and calculate timezone offset.
            timestamp_str = readiness_data.pop("timestamp", None)
            if not timestamp_str:
                continue

            # Parse timestamps and calculate timezone offset.
            timestamp_utc = datetime.fromisoformat(timestamp_str)
            timezone_offset_hours = 0.0

            if timestamp_local_str:
                timestamp_local = datetime.fromisoformat(timestamp_local_str)
                offset_seconds = (timestamp_local - timestamp_utc).total_seconds()
                timezone_offset_hours = offset_seconds / 3600

            # Start building the training readiness record.
            readiness_record = {
                "user_id": int(self.user_id),
                "timestamp": timestamp_utc.replace(tzinfo=timezone.utc),
                "timezone_offset_hours": timezone_offset_hours,
            }

            # Process remaining fields with snake_case conversion.
            for field_name, field_value in readiness_data.items():
                snake_case_name = self._convert_field_name(field_name)
                readiness_record[snake_case_name] = field_value

            readiness_records.append(TrainingReadiness(**readiness_record))

        if readiness_records:
            upsert_model_instances(
                session=session,
                model_instances=readiness_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=True,
            )
            click.echo(
                f"Processed {len(readiness_records)} training readiness" f" records."
            )
        else:
            click.secho("⚠️ No training readiness data found.", fg="yellow")

    def _process_stress_body_battery(self, file_path: Path, session: Session):
        """
        Process a STRESS file containing stress and body battery data.

        Extracts stress level and body battery level measurements from timeseries arrays
        and inserts them into the stress and body_battery tables with insert-only logic.

        :param file_path: Path to the STRESS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        stress_data = self._load_json_file(file_path)

        # Process stress values from stressValuesArray.
        stress_records = []
        stress_values_array = stress_data.pop("stressValuesArray", [])
        for stress_value in stress_values_array:
            if len(stress_value) >= 2:
                timestamp_ms, stress_level = stress_value[0], stress_value[1]
                # Skip negative values as they indicate unmeasurable periods.
                if stress_level >= 0:
                    timestamp = datetime.fromtimestamp(
                        timestamp_ms / 1000, tz=timezone.utc
                    )
                    stress_records.append(
                        Stress(
                            user_id=int(self.user_id),
                            timestamp=timestamp,
                            value=stress_level,
                        )
                    )

        # Bulk insert stress records with insert-only logic.
        if stress_records:
            upsert_model_instances(
                session=session,
                model_instances=stress_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo("Processed stress data.")
        else:
            click.secho("⚠️ No stress data found.", fg="yellow")

        # Process body battery values from bodyBatteryValuesArray.
        body_battery_records = []
        body_battery_values_array = stress_data.pop("bodyBatteryValuesArray", [])
        for battery_value in body_battery_values_array:
            if len(battery_value) >= 3:
                timestamp_ms, _, body_battery_level = (
                    battery_value[0],
                    battery_value[1],
                    battery_value[2],
                )
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                body_battery_records.append(
                    BodyBattery(
                        user_id=int(self.user_id),
                        timestamp=timestamp,
                        value=body_battery_level,
                    )
                )

        # Bulk insert body battery records with insert-only logic.
        if body_battery_records:
            upsert_model_instances(
                session=session,
                model_instances=body_battery_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(body_battery_records)} body battery records.")
        else:
            click.secho("⚠️ No body battery data found.", fg="yellow")

    def _process_heart_rate(self, file_path: Path, session: Session):
        """
        Process a HEART_RATE file containing heart rate measurements.

        Extracts heart rate values from timeseries arrays and inserts them into the
        heart_rate table with insert-only logic to prevent data corruption from
        duplicate processing.

        :param file_path: Path to the HEART_RATE JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        heart_rate_data = self._load_json_file(file_path)

        # Extract heart rate timeseries data.
        heart_rate_values = heart_rate_data.pop("heartRateValues", [])

        # Process heart rate measurements.
        heart_rate_records = []
        for timestamp_ms, heart_rate_value in (
            heart_rate_values if heart_rate_values else []
        ):
            if timestamp_ms and heart_rate_value is not None:
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                heart_rate_records.append(
                    HeartRate(
                        user_id=int(self.user_id),
                        timestamp=timestamp,
                        value=heart_rate_value,
                    )
                )

        # Bulk insert heart rate records.
        if heart_rate_records:
            upsert_model_instances(
                session=session,
                model_instances=heart_rate_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(heart_rate_records)} heart rate records.")
        else:
            click.secho("⚠️ No heart rate data found.", fg="yellow")

    def _process_steps(self, file_path: Path, session: Session):
        """
        Process a STEPS file containing step count measurements.

        Extracts step counts from 15-minute intervals and inserts them into the steps
        table with insert-only logic to prevent data corruption from duplicate
        processing.

        :param file_path: Path to the STEPS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        steps_data = self._load_json_file(file_path)

        # Process steps measurements.
        steps_records = []
        for record in steps_data if steps_data else []:
            end_gmt = record.pop("endGMT", None)
            steps = record.pop("steps", None)
            activity_level = record.pop("primaryActivityLevel", None)
            activity_level_constant = record.pop("activityLevelConstant", None)

            if end_gmt and steps is not None:
                timestamp = datetime.fromisoformat(end_gmt).replace(tzinfo=timezone.utc)
                steps_records.append(
                    Steps(
                        user_id=int(self.user_id),
                        timestamp=timestamp,
                        value=steps,
                        activity_level=activity_level,
                        activity_level_constant=activity_level_constant,
                    )
                )

        # Bulk insert steps records.
        if steps_records:
            upsert_model_instances(
                session=session,
                model_instances=steps_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(steps_records)} steps records.")
        else:
            click.secho("⚠️ No steps data found.", fg="yellow")

    def _process_respiration(self, file_path: Path, session: Session):
        """
        Process a RESPIRATION file containing respiration rate measurements.

        Extracts respiration rates from timeseries arrays and inserts them into the
        `respiration` table with insert-only logic to prevent data corruption from
        duplicate processing.

        :param file_path: Path to the RESPIRATION JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        respiration_data = self._load_json_file(file_path)

        # Extract respiration timeseries data.
        respiration_values = respiration_data.pop("respirationValuesArray", [])

        # Process respiration measurements.
        respiration_records = []
        for respiration_value in respiration_values if respiration_values else []:
            if len(respiration_value) >= 2:
                timestamp_ms, respiration_rate = (
                    respiration_value[0],
                    respiration_value[1],
                )
                # Skip negative values as they indicate unmeasurable periods.
                if respiration_rate >= 0:
                    timestamp = datetime.fromtimestamp(
                        timestamp_ms / 1000, tz=timezone.utc
                    )
                    respiration_records.append(
                        Respiration(
                            user_id=int(self.user_id),
                            timestamp=timestamp,
                            value=respiration_rate,
                        )
                    )

        # Bulk insert respiration records.
        if respiration_records:
            upsert_model_instances(
                session=session,
                model_instances=respiration_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(respiration_records)} respiration records.")
        else:
            click.secho("⚠️ No respiration data found.", fg="yellow")

    def _process_intensity_minutes(self, file_path: Path, session: Session):
        """
        Process an INTENSITY_MINUTES file containing intensity minute measurements.

        Extracts intensity minutes from timeseries arrays and inserts them into the
        `intensity_minutes` table with insert-only logic. Also extracts aggregate
        intensity data for the `training_load` table.

        :param file_path: Path to the INTENSITY_MINUTES JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        intensity_data = self._load_json_file(file_path)

        # Extract intensity minutes timeseries data.
        intensity_values = intensity_data.pop("imValuesArray", [])

        # Process intensity minute measurements.
        intensity_records = []
        for intensity_value in intensity_values if intensity_values else []:
            if len(intensity_value) >= 2:
                timestamp_ms, intensity_minutes = (
                    intensity_value[0],
                    intensity_value[1],
                )
                # Skip negative values as they indicate unmeasurable periods.
                if intensity_minutes >= 0:
                    timestamp = datetime.fromtimestamp(
                        timestamp_ms / 1000, tz=timezone.utc
                    )
                    intensity_records.append(
                        IntensityMinutes(
                            user_id=int(self.user_id),
                            timestamp=timestamp,
                            value=intensity_minutes,
                        )
                    )

        # Bulk insert intensity minutes records.
        if intensity_records:
            upsert_model_instances(
                session=session,
                model_instances=intensity_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(intensity_records)} intensity minutes records.")
        else:
            click.secho("⚠️ No intensity minutes data found.", fg="yellow")

        # Process aggregate intensity data for the `training_load` table.
        training_load_records = []
        calendar_date = intensity_data.pop("calendarDate", None)
        if calendar_date:
            calendar_date = self._parse_date_string(calendar_date)
            moderate_minutes = intensity_data.pop("moderateMinutes", None)
            vigorous_minutes = intensity_data.pop("vigorousMinutes", None)
            start_day_minutes = intensity_data.pop("startDayMinutes", None)
            end_day_minutes = intensity_data.pop("endDayMinutes", None)

            # Calculate total intensity minutes.
            total_intensity_minutes = None
            if start_day_minutes is not None and end_day_minutes is not None:
                total_intensity_minutes = end_day_minutes - start_day_minutes

            # Collect training_load record if we have data.
            if (
                moderate_minutes is not None
                or vigorous_minutes is not None
                or total_intensity_minutes is not None
            ):
                training_load = TrainingLoad(
                    user_id=int(self.user_id),
                    date=calendar_date,
                    moderate_minutes=moderate_minutes,
                    vigorous_minutes=vigorous_minutes,
                    total_intensity_minutes=total_intensity_minutes,
                )
                training_load_records.append(training_load)
        if training_load_records:
            upsert_model_instances(
                session=session,
                model_instances=training_load_records,
                conflict_columns=["user_id", "date"],
                update_columns=[
                    "moderate_minutes",
                    "vigorous_minutes",
                    "total_intensity_minutes",
                ],
                on_conflict_update=True,
            )
            click.echo(
                f"Updated training load with intensity minutes for "
                f"date {calendar_date}."
            )
        else:
            click.secho("⚠️ No aggregated intensity minutes data found.", fg="yellow")

    def _process_floors(self, file_path: Path, session: Session):
        """
        Process a FLOORS file containing floors ascended and descended measurements.

        Extracts floors data from timeseries arrays and inserts them into the
        `floors` table with insert-only logic.

        :param file_path: Path to the FLOORS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        floors_data = self._load_json_file(file_path)

        # Extract floors timeseries data.
        floor_values = floors_data.pop("floorValuesArray", [])

        # Process floors measurements.
        floors_records = []
        for floor_value in floor_values if floor_values else []:
            if len(floor_value) >= 4:
                end_time_str, ascended, descended = (
                    floor_value[1],
                    floor_value[2],
                    floor_value[3],
                )
                # Use endTimeGMT as timestamp.
                timestamp = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
                floors_records.append(
                    Floors(
                        user_id=int(self.user_id),
                        timestamp=timestamp,
                        ascended=ascended,
                        descended=descended,
                    )
                )

        # Bulk insert floors records.
        if floors_records:
            upsert_model_instances(
                session=session,
                model_instances=floors_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            click.echo(f"Processed {len(floors_records)} floors records.")
        else:
            click.secho("⚠️ No floors data found.", fg="yellow")

    def _process_personal_records(self, file_path: Path, session: Session):
        """
        Process a PERSONAL_RECORDS file containing personal record achievements.

        Extracts personal record data and upserts them into the `personal_record` table
        with update logic. Manages the `latest` flag by setting previous records to
        `latest`=false when new records with the same `user_id` and `type_id` are
        processed.

        :param file_path: Path to the PERSONAL_RECORDS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        personal_records_data = self._load_json_file(file_path)

        # Collect all existing latest PR records that need to be set to False.
        all_latest_prs = []
        personal_records = []

        for record in personal_records_data if personal_records_data else []:
            type_id = record.pop("typeId")
            activity_id = record.pop("activityId")
            pr_start_time_gmt = record.pop("prStartTimeGmt")
            value = record.pop("value", None)

            # Convert timestamp from milliseconds to datetime.
            timestamp = datetime.fromtimestamp(
                pr_start_time_gmt / 1000, tz=timezone.utc
            )

            # Get label from mapping or set to None if not found.
            label = PR_TYPE_LABELS.get(type_id)

            # Determine if this is a steps-based PR (typeId 12-16).
            # Steps PRs don't belong to activities and should have NULL `activity_id`.
            is_steps_record = type_id in [12, 13, 14, 15, 16]

            if is_steps_record:
                final_activity_id = None
            else:
                # Check if activity exists (warning only, not blocking).
                activity_exists = (
                    session.query(Activity)
                    .filter(Activity.activity_id == activity_id)
                    .first()
                    is not None
                )

                if not activity_exists:
                    click.secho(
                        f"⚠️ Activity {activity_id} not found for personal record "
                        f"(type_id: {type_id}, label: {label}). "
                        f"Processing anyway. This is expected when the database "
                        f"contains data for partial time ranges. "
                        f"Activity may be added later.",
                        fg="yellow",
                    )

                final_activity_id = activity_id

            # Find all PersonalRecord rows with `latest`=True and same `type_id` for
            # this user.
            latest_prs = (
                session.query(PersonalRecord)
                .filter(
                    and_(
                        PersonalRecord.user_id == int(self.user_id),
                        PersonalRecord.type_id == type_id,
                        PersonalRecord.latest,
                    )
                )
                .all()
            )

            # Collect existing records to update later.
            all_latest_prs.extend(latest_prs)

            personal_records.append(
                PersonalRecord(
                    user_id=int(self.user_id),
                    activity_id=final_activity_id,
                    timestamp=timestamp,
                    type_id=type_id,
                    label=label,
                    value=value,
                    latest=True,
                )
            )

        # Set `latest`=False for all previous latest personal records.
        if all_latest_prs:
            for record in all_latest_prs:
                record.latest = False
            click.echo(
                f"Setting `latest`=False for {len(all_latest_prs)} "
                f"previous latest personal record(s) for user ID {self.user_id}."
            )
            # Flush session to ensure the latest=False updates are committed
            # before inserting new records with latest=True.
            session.flush()

        # Then bulk insert personal records with update logic.
        if personal_records:
            upsert_model_instances(
                session=session,
                model_instances=personal_records,
                conflict_columns=[
                    "user_id",
                    "type_id",
                    "timestamp",
                ],
                on_conflict_update=True,
            )
            click.echo(f"Processed {len(personal_records)} personal records.")
        else:
            click.secho("⚠️ No personal records data found.", fg="yellow")

    def _process_race_predictions(self, file_path: Path, session: Session):
        """
        Process a RACE_PREDICTIONS file containing race time predictions.

        Extracts race prediction data and inserts them into the race_predictions table
        with upsert logic. Manages the `latest` flag by setting previous records to
        `latest`=false when new records with the same `user_id` are inserted.

        :param file_path: Path to the RACE_PREDICTIONS JSON file.
        :param session: SQLAlchemy Session object.
        """

        # Load and parse the JSON data.
        race_prediction_data = self._load_json_file(file_path)

        if not race_prediction_data:
            click.secho("⚠️ No race predictions data found.", fg="yellow")
            return

        # Find all race predictions with `latest`=True for this user.
        latest_race_predictions = (
            session.query(RacePredictions)
            .filter(
                and_(
                    RacePredictions.user_id == int(self.user_id),
                    RacePredictions.latest,
                )
            )
            .all()
        )

        # Extract race time predictions.
        race_prediction = RacePredictions(
            user_id=int(self.user_id),
            date=self._parse_date_string(race_prediction_data.pop("calendarDate")),
            time_5k=race_prediction_data.pop("time5K", None),
            time_10k=race_prediction_data.pop("time10K", None),
            time_half_marathon=race_prediction_data.pop("timeHalfMarathon", None),
            time_marathon=race_prediction_data.pop("timeMarathon", None),
            latest=True,
        )

        # Set `latest`=False for previous latest race predictions.
        if latest_race_predictions:
            for record in latest_race_predictions:
                record.latest = False
            click.echo(
                f"Setting `latest`=False for {len(latest_race_predictions)} previous "
                f"latest race prediction record(s) for user ID {self.user_id}."
            )
            # Flush session to ensure the latest=False updates are committed
            # before inserting the new record with latest=True.
            session.flush()

        # Then use insert-only logic with on conflict do nothing.
        upsert_model_instances(
            session=session,
            model_instances=[race_prediction],
            conflict_columns=["user_id", "date"],
            on_conflict_update=False,
        )

    def _process_fit_file(self, file_path: Path, session: Session):
        """
        Process a FIT file and extract time-series metrics.

        Processes FIT file using fitdecode library, extracts record frames with
        timestamp and sensor data, and stores metrics in `activity_ts_metric` table.
        Sets `ts_data_available` flag to True for the corresponding activity.

        :param file_path: Path to the FIT file.
        :param session: SQLAlchemy Session object.
        """

        # Extract `activity_id` from filename.
        # FIT files have format: {user_id}_ACTIVITY_{activity_id}_{timestamp}.fit
        # Use regex to extract activity_id directly from filename.
        pattern = r"^(\d+)_ACTIVITY_(\d+)_([0-9T:\-Z\.]+)\.fit$"
        match = re.match(pattern, file_path.name)

        if not match:
            raise ValueError(
                f"Cannot extract activity_id from filename: {file_path.name}"
            )

        activity_id = int(match.groups()[1])

        # Check if `ts_data_available` is already True for this activity.
        existing_activity = (
            session.query(Activity).filter(Activity.activity_id == activity_id).first()
        )

        if not existing_activity:
            raise ValueError(
                f"Activity {activity_id} not found in database. "
                f"FIT file processing requires existing activity record."
            )

        # Check if time-series data has already been processed for this activity.
        if existing_activity.ts_data_available:
            click.secho(
                f"⚠️ Time-series data already processed for "
                f"activity {activity_id}. Skipping: {file_path.name}"
            )
            return

        # Initialize metric lists and counters.
        ts_metrics = []
        split_metrics = []
        lap_metrics = []
        split_idx = 0
        lap_idx = 0

        with fitdecode.FitReader(file_path) as fit:
            for frame in fit:
                if frame.frame_type == fitdecode.FIT_FRAME_DATA:
                    # Process record frames for time-series data.
                    if frame.name == "record":
                        # Two-pass approach: first find timestamp, then process
                        # all fields.
                        timestamp = None

                        # First pass: find timestamp.
                        for field in frame.fields:
                            if field.name == "timestamp" and field.value:
                                timestamp = field.value.replace(tzinfo=timezone.utc)
                                break

                        # Second pass: process all fields if timestamp was found.
                        if timestamp is not None:
                            for field in frame.fields:
                                if (
                                    field.name is not None
                                    and field.name != "timestamp"
                                    and "unknown" not in field.name.lower()
                                    and field.value is not None
                                    and isinstance(field.value, (int, float, bool))
                                ):
                                    ts_metrics.append(
                                        ActivityTsMetric(
                                            activity_id=activity_id,
                                            timestamp=timestamp,
                                            name=field.name,
                                            value=float(field.value),
                                            units=field.units if field.units else None,
                                        )
                                    )

                    # Process split frames.
                    elif frame.name == "split":
                        split_idx += 1
                        split_type_value = None

                        # Extract split_type first.
                        for field in frame.fields:
                            if field.name == "split_type" and field.value is not None:
                                split_type_value = field.value
                                break

                        # Process all fields.
                        for field in frame.fields:
                            if (
                                field.name is not None
                                and "unknown" not in field.name.lower()
                                and field.value is not None
                            ):
                                try:
                                    # Handle split_type as string, others as float.
                                    if field.name == "split_type":
                                        continue  # Already captured above.

                                    # Handle array values with suffix.
                                    if isinstance(field.value, list):
                                        for i, array_val in enumerate(field.value, 1):
                                            if array_val is not None:
                                                split_metrics.append(
                                                    ActivitySplitMetric(
                                                        activity_id=activity_id,
                                                        split_idx=split_idx,
                                                        split_type=split_type_value,
                                                        name=f"{field.name}_{i}",
                                                        value=float(array_val),
                                                        units=(
                                                            field.units
                                                            if field.units
                                                            else None
                                                        ),
                                                    )
                                                )
                                    else:
                                        split_metrics.append(
                                            ActivitySplitMetric(
                                                activity_id=activity_id,
                                                split_idx=split_idx,
                                                split_type=split_type_value,
                                                name=field.name,
                                                value=float(field.value),
                                                units=(
                                                    field.units if field.units else None
                                                ),
                                            )
                                        )
                                except (ValueError, TypeError):
                                    # Skip fields that can't be converted to float.
                                    continue

                    # Process lap frames.
                    elif frame.name == "lap":
                        lap_idx += 1

                        # Process all fields.
                        for field in frame.fields:
                            if (
                                field.name is not None
                                and "unknown" not in field.name.lower()
                                and field.value is not None
                            ):
                                try:
                                    # Handle array values with suffix.
                                    if isinstance(field.value, list):
                                        for i, array_val in enumerate(field.value, 1):
                                            if array_val is not None:
                                                lap_metrics.append(
                                                    ActivityLapMetric(
                                                        activity_id=activity_id,
                                                        lap_idx=lap_idx,
                                                        name=f"{field.name}_{i}",
                                                        value=float(array_val),
                                                        units=(
                                                            field.units
                                                            if field.units
                                                            else None
                                                        ),
                                                    )
                                                )
                                    else:
                                        lap_metrics.append(
                                            ActivityLapMetric(
                                                activity_id=activity_id,
                                                lap_idx=lap_idx,
                                                name=field.name,
                                                value=float(field.value),
                                                units=(
                                                    field.units if field.units else None
                                                ),
                                            )
                                        )
                                except (ValueError, TypeError):
                                    # Skip fields that can't be converted to float.
                                    continue

        # Flush session to ensure foreign key relationships are resolved.
        session.flush()

        # Bulk insert all metrics if any were found.
        if ts_metrics:
            session.bulk_save_objects(ts_metrics)
            click.echo(f"Processed {len(ts_metrics)} time-series records.")
            existing_activity.ts_data_available = True
        else:
            click.secho("⚠️ No time-series data found.", fg="yellow")

        if split_metrics:
            session.bulk_save_objects(split_metrics)
            click.echo(f"Processed {len(split_metrics)} split records.")
        else:
            click.secho("⚠️ No split data found.", fg="yellow")

        if lap_metrics:
            session.bulk_save_objects(lap_metrics)
            click.echo(f"Processed {len(lap_metrics)} lap records.")
        else:
            click.secho("⚠️ No lap data found.", fg="yellow")

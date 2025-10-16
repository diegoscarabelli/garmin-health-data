"""
This module defines the data types and API methods used in the Garmin Connect data
pipeline.

It provides a registry for easy access to Garmin data types and their associated
metadata.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class APIMethodTimeParam(Enum):
    """
    Classification of API method time parameter patterns.
    """

    DAILY = "daily"  # Single date parameter: get_method(date_str).
    RANGE = "range"  # Date range parameters: get_method(start_str, end_str).
    NO_DATE = "no_date"  # No date parameters: get_method().


@dataclass
class GarminDataType:
    """
    Definition for a single Garmin Connect data type.

    Defines the API method, time parameter type, endpoint, and metadata for extracting a
    specific type of data from Garmin Connect.
    """

    name: str  # "SLEEP".
    api_method: str  # "get_sleep_data()".
    api_method_time_param: APIMethodTimeParam  # DAILY/RANGE/NO_DATE.
    api_endpoint: str  # API endpoint string.
    description: str  # Description of the data type.
    emoji: str  # Emoji for pretty logging.


class GarminDataRegistry:
    """
    Registry for Garmin Connect data types with fast lookup and filtering.

    Provides O(1) lookups by name and efficient filtering by API method time parameters.
    """

    def __init__(self):
        self._data_types_by_name: Dict[str, GarminDataType] = {}
        self._data_types_by_time_param: Dict[
            APIMethodTimeParam, List[GarminDataType]
        ] = {
            APIMethodTimeParam.DAILY: [],
            APIMethodTimeParam.RANGE: [],
            APIMethodTimeParam.NO_DATE: [],
        }
        self._all_data_types: List[GarminDataType] = []

        # Register all data types on initialization.
        self._register_all_types()

    def _register_all_types(self):
        """
        Register all Garmin Connect data types.
        """

        data_types = [
            # Daily Data - Single date parameter: get_method(date_str)
            GarminDataType(
                "SLEEP",
                "get_sleep_data",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/dailySleepData/{display_name}"
                "?date={date}&nonSleepBufferMinutes=60",
                "Sleep stage duration, movement, levels, restless moments, "
                "heart rate (redundant with Heart Rate dataset), stress levels "
                "(redundant with Stress dataset), respiration (redundant with "
                "Respiration dataset), body battery (redundant with Stress dataset), "
                "HRV (redundant with Sleep HRV dataset), breathing disruptions, "
                "scores, current need, next need.",
                "💤",
            ),
            GarminDataType(
                "STRESS",
                "get_stress_data",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/dailyStress/{date}",
                "Stress level and body battery measurements (3 mins interval "
                "time-series).",
                "🔋",
            ),
            GarminDataType(
                "RESPIRATION",
                "get_respiration_data",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/daily/respiration/{date}",
                "Breathing rate readings (2 mins interval and 1 hour aggregates "
                "time-series) and aggregated statistics.",
                "🫁",
            ),
            GarminDataType(
                "HEART_RATE",
                "get_heart_rates",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/dailyHeartRate/{display_name}?date={date}",
                "Heart rate readings (2 mins interval time-series).",
                "💓",
            ),
            GarminDataType(
                "TRAINING_READINESS",
                "get_training_readiness",
                APIMethodTimeParam.DAILY,
                "/metrics-service/metrics/trainingreadiness/{date}",
                "Daily training readiness scores (generated multiple times a day) and "
                "associated features.",
                "🏋️",
            ),
            GarminDataType(
                "TRAINING_STATUS",
                "get_training_status",
                APIMethodTimeParam.DAILY,
                "/metrics-service/metrics/trainingstatus/aggregated/{date}",
                "VO2 max (generic and cycling) including heat and altitude "
                "acclimation, training load balance (low and high aerobic, anaerobic) "
                "with targets, acute/chronic workload ratio (ACWR), and feedback.",
                "📊",
            ),
            GarminDataType(
                "STEPS",
                "get_steps_data",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/dailySummaryChart/{display_name}"
                "?date={date}",
                "Number of steps and activity level (sedentary, sleeping, active, "
                "etc.) (15 mins interval time-series).",
                "👣",
            ),
            GarminDataType(
                "FLOORS",
                "get_floors",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/floorsChartData/daily/{date}",
                "Floors climbed and descended (15 mins interval time-series).",
                "🪜",
            ),
            GarminDataType(
                "INTENSITY_MINUTES",
                "get_intensity_minutes_data",
                APIMethodTimeParam.DAILY,
                "/wellness-service/wellness/daily/im/{date}",
                "Weekly and daily moderate/vigorous intensity minutes with "
                "time-series data and goal tracking.",
                "⚡",
            ),
            # Range Data - Date range parameters: get_method(start_str, end_str)
            GarminDataType(
                "ACTIVITIES_LIST",
                "get_activities_by_date",
                APIMethodTimeParam.RANGE,
                "/activitylist-service/activities/search/activities",
                "Numerous aggregated metrics for user-recorded activities.",
                "📋",
            ),
            # No Date Data - No date parameters: get_method()
            # In case of backfilling, comment out PERSONAL_RECORD data type, since PRs
            # reference activity IDs that may not exist yet.
            GarminDataType(
                "PERSONAL_RECORDS",
                "get_personal_record",
                APIMethodTimeParam.NO_DATE,
                "/personalrecord-service/personalrecord/prs/{display_name}",
                "All-time personal bests steps, running, cycling, swimming, strength.",
                "🏆",
            ),
            GarminDataType(
                "RACE_PREDICTIONS",
                "get_race_predictions",
                APIMethodTimeParam.NO_DATE,
                "/metrics-service/metrics/racepredictions/latest/{display_name}",
                "Predicted running times based on current fitness level.",
                "🏁",
            ),
            GarminDataType(
                "USER_PROFILE",
                "get_user_profile",
                APIMethodTimeParam.NO_DATE,
                "/userprofile-service/userprofile/settings",
                "User profile settings including gender, weight, height, birthday, "
                "VO2 max (running and cycling), and lactate threshold (speed and heart "
                "rate).",
                "👤",
            ),
            GarminDataType(
                "ACTIVITY",
                "download_activity",
                APIMethodTimeParam.RANGE,
                "/download-service/files/activity/{activity_id}",
                "Binary FIT files containing detailed time-series activity data.",
                "🏃",
            ),
        ]

        for data_type in data_types:
            self.register(data_type)

    def register(self, data_type: GarminDataType):
        """
        Register a Garmin data type.

        :param data_type: GarminDataType to register.
        :raises ValueError: If a data type with the same name already exists.
        """

        if data_type.name in self._data_types_by_name:
            raise ValueError(f"Data type with name '{data_type.name}' already exists.")

        self._data_types_by_name[data_type.name] = data_type
        self._data_types_by_time_param[data_type.api_method_time_param].append(
            data_type
        )
        self._all_data_types.append(data_type)

    def get_by_name(self, name: str) -> Optional[GarminDataType]:
        """
        Get data type by name.

        :param name: Name of the data type to retrieve.
        :return: GarminDataType if found, None otherwise.
        """

        return self._data_types_by_name.get(name)

    def get_by_time_param(
        self, api_method_time_param: APIMethodTimeParam
    ) -> List[GarminDataType]:
        """
        Get all data types of a specific API method time parameter.

        :param api_method_time_param: API method time parameter.
        :return: List of GarminDataType data types for the specified time param.
        """

        return self._data_types_by_time_param[api_method_time_param].copy()

    @property
    def all_data_types(self) -> List[GarminDataType]:
        """
        Get all registered data types.

        :return: Copy of all registered data types.
        """

        return self._all_data_types.copy()

    @property
    def daily_data_types(self) -> List[GarminDataType]:
        """
        Get all daily data types (shorthand).

        :return: List of data types with DAILY time parameter.
        """
        return self.get_by_time_param(APIMethodTimeParam.DAILY)

    @property
    def range_data_types(self) -> List[GarminDataType]:
        """
        Get all range data types (shorthand).

        :return: List of data types with RANGE time parameter.
        """

        return self.get_by_time_param(APIMethodTimeParam.RANGE)

    @property
    def no_date_data_types(self) -> List[GarminDataType]:
        """
        Get all no-date data types (shorthand).

        :return: List of data types with NO_DATE time parameter.
        """

        return self.get_by_time_param(APIMethodTimeParam.NO_DATE)


def _create_garmin_file_types() -> type:
    """
    Dynamically create GarminFileTypes enum from GarminDataRegistry, following the class
    signature specified in the lib.filesystem_utils.DefaultFileTypes class.

    This ensures that file type patterns stay synchronized with registered
    data types without manual maintenance. Each data type gets a corresponding
    file pattern with the appropriate extension based on its data format.

    Pattern format: .*_{data_type.name}_.*\\.{extension}$
    Extensions:
    - JSON files: .json (for most data types)
    - FIT files: .fit (for ACTIVITY data type)

    :return: Enum class with file type patterns for Garmin Connect data pipeline.
    """

    patterns = {}

    # Add patterns for each data type in registry.
    for data_type in GARMIN_DATA_REGISTRY.all_data_types:
        if data_type.name == "ACTIVITY":
            file_extension = "fit"
        else:
            file_extension = "json"

        pattern = re.compile(rf".*_{data_type.name}_.*\.{file_extension}$")
        patterns[data_type.name] = pattern

    # Create dynamic enum class.
    return Enum("GarminFileTypes", patterns)


# Global registry instance.
GARMIN_DATA_REGISTRY = GarminDataRegistry()

# Dynamically created file types enum based on registered data types.
GARMIN_FILE_TYPES = _create_garmin_file_types()

PR_TYPE_LABELS = {
    1: "Run: 1 km",
    2: "Run: 1 mile",
    3: "Run: 5 km",
    4: "Run: 10 km",
    7: "Run: Longest",
    8: "Bike: Longest",
    9: "Bike: Max Total Ascent",
    10: "Bike: 20 min Avg Power",
    11: "Bike: 40 km",
    12: "Steps: Most in a Day",
    13: "Steps: Most in a Week",
    14: "Steps: Most in a Month",
    15: "Steps: Longest Goal Streak",
    16: "Steps: Unknown Type",
    17: "Swim: Longest",
    18: "Swim: 100 m",
    19: "Swim: 100 yd",
    20: "Swim: 400 m",
    21: "Swim: 500 yd",
    22: "Swim: 750 m",
    23: "Swim: 1000 m",
    24: "Swim: 1000 yd",
    25: "Swim: 1500 m",
    26: "Swim: 1650 yd",
}

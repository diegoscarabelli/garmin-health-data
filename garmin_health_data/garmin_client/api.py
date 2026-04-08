"""
API method implementations for the vendored Garmin Connect client.

Each function here corresponds to one of the 15 Garmin Connect endpoints the
openetl pipeline consumes. The functions are written as plain functions taking
the ``GarminClient`` instance as their first argument so that the client class
can stay slim, and so that the file is testable in isolation.

Method-to-endpoint mapping is inherited from the upstream ``python-garminconnect``
library; URL templates live in :mod:`.constants`.

The 15 supported endpoints:

- Daily wellness:        sleep, stress, respiration, heart_rates, training_readiness,
                         training_status, steps, floors, intensity_minutes
- Range activities:      activities_by_date (paginated), activity_exercise_sets
- No-date metadata:      personal_records, race_predictions, user_profile
- Binary download:       download_activity (FIT/TCX/GPX/KML/CSV)
"""

import re
from datetime import datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from .constants import (
    ACTIVITIES_URL,
    ACTIVITY_URL,
    CSV_DOWNLOAD_URL,
    DAILY_INTENSITY_MINUTES_URL,
    DAILY_RESPIRATION_URL,
    DAILY_SLEEP_URL,
    DAILY_STRESS_URL,
    FIT_DOWNLOAD_URL,
    FLOORS_CHART_DAILY_URL,
    GPX_DOWNLOAD_URL,
    HEART_RATES_DAILY_URL,
    KML_DOWNLOAD_URL,
    PERSONAL_RECORD_URL,
    RACE_PREDICTOR_URL,
    TCX_DOWNLOAD_URL,
    TRAINING_READINESS_URL,
    TRAINING_STATUS_URL,
    USER_SETTINGS_URL,
    USER_SUMMARY_CHART_URL,
)

if TYPE_CHECKING:
    from .client import GarminClient


_DATE_FORMAT_STR = "%Y-%m-%d"
_DATE_FORMAT_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ActivityDownloadFormat(Enum):
    """
    Supported binary download formats for ``download_activity``.

    The openetl pipeline only ever uses ``ORIGINAL`` (zipped FIT). The other
    members exist for signature parity with upstream ``python-garminconnect``.
    """

    ORIGINAL = auto()
    TCX = auto()
    GPX = auto()
    KML = auto()
    CSV = auto()


def _validate_date_format(date_str: str, param_name: str = "date") -> str:
    """
    Validate that a date string is in ``YYYY-MM-DD`` format and represents a real date.

    :param date_str: Date string to validate.
    :param param_name: Name of the calling parameter, for error messages.
    :return: The validated, whitespace-stripped date string.
    :raises ValueError: If the input is not a string, has the wrong shape, or does not
        represent a real calendar date.
    """

    if not isinstance(date_str, str):
        raise ValueError(f"{param_name} must be a string")

    date_str = date_str.strip()

    if not _DATE_FORMAT_REGEX.fullmatch(date_str):
        raise ValueError(
            f"{param_name} must be in format 'YYYY-MM-DD', got: {date_str}"
        )

    try:
        datetime.strptime(date_str, _DATE_FORMAT_STR)
    except ValueError as e:
        raise ValueError(f"invalid {param_name}: {e}") from e

    return date_str


# ----------------------------------------------------------------------------------------
# DAILY WELLNESS METHODS
# ----------------------------------------------------------------------------------------


def get_sleep_data(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch sleep data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Sleep data dictionary including sleep stages, scores, HRV, and
        breathing disruptions.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{DAILY_SLEEP_URL}/{client.display_name}"
    params = {"date": cdate, "nonSleepBufferMinutes": 60}
    return client._connectapi(url, params=params)


def get_stress_data(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch stress and body battery data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Stress data dictionary with 3-minute interval time series.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{DAILY_STRESS_URL}/{cdate}"
    return client._connectapi(url)


def get_respiration_data(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch respiration data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Respiration dictionary with 2-minute interval and 1-hour aggregate
        readings.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{DAILY_RESPIRATION_URL}/{cdate}"
    return client._connectapi(url)


def get_heart_rates(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch heart rate data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Heart rate dictionary with 2-minute interval time series.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{HEART_RATES_DAILY_URL}/{client.display_name}"
    params = {"date": cdate}
    return client._connectapi(url, params=params)


def get_training_readiness(client: "GarminClient", cdate: str) -> List[Dict[str, Any]]:
    """
    Fetch training readiness scores for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: List of training readiness score dictionaries for the day.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{TRAINING_READINESS_URL}/{cdate}"
    return client._connectapi(url)


def get_training_status(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch training status data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Training status dictionary including VO2 max, training load, and
        ACWR.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{TRAINING_STATUS_URL}/{cdate}"
    return client._connectapi(url)


def get_steps_data(client: "GarminClient", cdate: str) -> List[Dict[str, Any]]:
    """
    Fetch steps data for the given date.

    Returns an empty list when the API returns ``None`` (e.g. for dates with no
    sync history). This matches upstream ``python-garminconnect`` behavior so the
    openetl pipeline's downstream filtering is unchanged.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: List of 15-minute steps interval dictionaries, or an empty list.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{USER_SUMMARY_CHART_URL}/{client.display_name}"
    params = {"date": cdate}
    response = client._connectapi(url, params=params)
    if response is None:
        return []
    return response


def get_floors(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch floors climbed and descended data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Floors dictionary with 15-minute interval time series.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{FLOORS_CHART_DAILY_URL}/{cdate}"
    return client._connectapi(url)


def get_intensity_minutes_data(client: "GarminClient", cdate: str) -> Dict[str, Any]:
    """
    Fetch intensity minutes data for the given date.

    :param client: GarminClient instance.
    :param cdate: Date in ``YYYY-MM-DD`` format.
    :return: Intensity minutes dictionary with weekly and daily moderate/vigorous
        breakdown.
    """

    cdate = _validate_date_format(cdate, "cdate")
    url = f"{DAILY_INTENSITY_MINUTES_URL}/{cdate}"
    return client._connectapi(url)


# ----------------------------------------------------------------------------------------
# RANGE ACTIVITY METHODS
# ----------------------------------------------------------------------------------------


def get_activities_by_date(
    client: "GarminClient",
    startdate: str,
    enddate: Optional[str] = None,
    activitytype: Optional[str] = None,
    sortorder: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch activities recorded between two dates, paginated 20 at a time.

    Mimics the Garmin Connect web app's infinite-scroll behavior: keep fetching
    20-activity pages until the API returns an empty list.

    :param client: GarminClient instance.
    :param startdate: Start date in ``YYYY-MM-DD`` format.
    :param enddate: Optional end date in ``YYYY-MM-DD`` format.
    :param activitytype: Optional activity type filter (``cycling``, ``running``,
        ``swimming``, ``multi_sport``, ``fitness_equipment``, ``hiking``,
        ``walking``, ``other``).
    :param sortorder: Optional sort direction (``asc`` to override the default
        descending order).
    :return: List of activity dictionaries.
    """

    activities: List[Dict[str, Any]] = []
    start = 0
    limit = 20

    startdate = _validate_date_format(startdate, "startdate")
    if enddate is not None:
        enddate = _validate_date_format(enddate, "enddate")

    params: Dict[str, str] = {
        "startDate": startdate,
        "start": str(start),
        "limit": str(limit),
    }
    if enddate:
        params["endDate"] = enddate
    if activitytype:
        params["activityType"] = str(activitytype)
    if sortorder:
        params["sortOrder"] = str(sortorder)

    while True:
        params["start"] = str(start)
        page = client._connectapi(ACTIVITIES_URL, params=params)
        if page:
            activities.extend(page)
            start += limit
        else:
            break

    return activities


def get_activity_exercise_sets(
    client: "GarminClient", activity_id: Any
) -> Dict[str, Any]:
    """
    Fetch per-set strength training data for a given activity.

    :param client: GarminClient instance.
    :param activity_id: Garmin activity ID (numeric or string-encoded numeric).
    :return: Exercise sets dictionary with ML-classified exercises, reps,
        weights, and set types.
    :raises ValueError: If ``activity_id`` is not a positive integer.
    """

    aid = int(activity_id)
    if aid <= 0:
        raise ValueError(f"activity_id must be a positive integer, got {activity_id}")
    url = f"{ACTIVITY_URL}/{aid}/exerciseSets"
    return client._connectapi(url)


# ----------------------------------------------------------------------------------------
# NO-DATE METADATA METHODS
# ----------------------------------------------------------------------------------------


def get_personal_record(client: "GarminClient") -> Dict[str, Any]:
    """
    Fetch all-time personal records for the authenticated user.

    :param client: GarminClient instance.
    :return: Personal records dictionary covering steps, running, cycling, swimming, and
        strength.
    """

    url = f"{PERSONAL_RECORD_URL}/{client.display_name}"
    return client._connectapi(url)


def get_race_predictions(
    client: "GarminClient",
    startdate: Optional[str] = None,
    enddate: Optional[str] = None,
    _type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch race time predictions for the authenticated user.

    Two call modes:

    - All three parameters omitted (the openetl hot path): returns the latest
      predictions for the current date.
    - All three parameters provided: returns predictions for each day or month
      in the range, depending on ``_type``.

    :param client: GarminClient instance.
    :param startdate: Optional range start (``YYYY-MM-DD``).
    :param enddate: Optional range end (``YYYY-MM-DD``).
    :param _type: Optional aggregation: ``daily`` or ``monthly``.
    :return: Race predictions dictionary.
    :raises ValueError: For invalid ``_type``, partial parameter sets, or ranges
        longer than one year.
    """

    valid = {"daily", "monthly", None}
    if _type not in valid:
        raise ValueError(f"_type must be one of {valid!r}")

    if _type is None and startdate is None and enddate is None:
        url = f"{RACE_PREDICTOR_URL}/latest/{client.display_name}"
        return client._connectapi(url)

    if _type is not None and startdate is not None and enddate is not None:
        startdate = _validate_date_format(startdate, "startdate")
        enddate = _validate_date_format(enddate, "enddate")
        if (
            datetime.strptime(enddate, _DATE_FORMAT_STR).date()
            - datetime.strptime(startdate, _DATE_FORMAT_STR).date()
        ).days > 366:
            raise ValueError("Startdate cannot be more than one year before enddate")
        url = f"{RACE_PREDICTOR_URL}/{_type}/{client.display_name}"
        params = {"fromCalendarDate": startdate, "toCalendarDate": enddate}
        return client._connectapi(url, params=params)

    raise ValueError("you must either provide all parameters or no parameters")


def get_user_profile(client: "GarminClient") -> Dict[str, Any]:
    """
    Fetch the authenticated user's profile settings.

    Note: this hits ``/user-settings``, not the ``/profile`` endpoint used by
    :meth:`GarminClient._load_profile` to populate ``display_name``. The two
    return distinct payloads.

    :param client: GarminClient instance.
    :return: User settings dictionary including ``id``, gender, weight, height,
        birthday, and threshold metrics.
    """

    return client._connectapi(USER_SETTINGS_URL)


# ----------------------------------------------------------------------------------------
# BINARY DOWNLOAD METHODS
# ----------------------------------------------------------------------------------------


def download_activity(
    client: "GarminClient",
    activity_id: Any,
    dl_fmt: ActivityDownloadFormat = ActivityDownloadFormat.ORIGINAL,
) -> bytes:
    """
    Download an activity file in the requested binary format.

    The ``ORIGINAL`` format returns a zipped FIT file (the openetl pipeline
    extracts the FIT inside). The other formats return their respective
    serialized representations.

    :param client: GarminClient instance.
    :param activity_id: Garmin activity ID.
    :param dl_fmt: Download format (defaults to ``ORIGINAL`` for FIT).
    :return: Raw bytes of the downloaded file.
    :raises ValueError: If ``dl_fmt`` is not a supported format.
    """

    aid = str(activity_id)
    urls = {
        ActivityDownloadFormat.ORIGINAL: f"{FIT_DOWNLOAD_URL}/{aid}",
        ActivityDownloadFormat.TCX: f"{TCX_DOWNLOAD_URL}/{aid}",
        ActivityDownloadFormat.GPX: f"{GPX_DOWNLOAD_URL}/{aid}",
        ActivityDownloadFormat.KML: f"{KML_DOWNLOAD_URL}/{aid}",
        ActivityDownloadFormat.CSV: f"{CSV_DOWNLOAD_URL}/{aid}",
    }
    if dl_fmt not in urls:
        raise ValueError(f"unexpected value {dl_fmt} for dl_fmt")
    return client._download(urls[dl_fmt])

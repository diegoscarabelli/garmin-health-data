"""
Constants and helper functions for the vendored Garmin Connect client.

Holds:
- SSO endpoints, client IDs, and User-Agent strings used by the five login
  strategies.
- DI OAuth2 token exchange URL and the rolling list of accepted client IDs.
- API URL templates for the 15 endpoints the openetl pipeline consumes.
- Cloudflare WAF anti-rate-limit delay bounds applied between SSO page GET and
  credential POST.

Helper functions ``_random_browser_headers``, ``_build_basic_auth``, and
``_native_headers`` build the HTTP headers used by the strategies and DI token
endpoints. They live here so the strategy and client modules can share them
without forming a circular import.
"""

import base64
from typing import Dict, Optional

try:
    from ua_generator import generate as _generate_ua

    HAS_UA_GEN = True
except ImportError:
    HAS_UA_GEN = False


# ----------------------------------------------------------------------------------------
# SSO ENDPOINTS AND CLIENT IDENTIFIERS
# ----------------------------------------------------------------------------------------

# Mobile SSO (Android Garmin Connect Mobile app flow).
MOBILE_SSO_CLIENT_ID = "GCM_ANDROID_DARK"
MOBILE_SSO_SERVICE_URL = "https://mobile.integration.garmin.com/gcm/android"
MOBILE_SSO_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; sdk_gphone64_arm64 Build/TE1A.220922.025; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.0.0 "
    "Mobile Safari/537.36"
)

# Web portal (desktop browser flow that connect.garmin.com itself uses).
PORTAL_SSO_CLIENT_ID = "GarminConnect"
PORTAL_SSO_SERVICE_URL = "https://connect.garmin.com/app"
DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


# ----------------------------------------------------------------------------------------
# NATIVE API HEADERS (used for DI token exchange and authenticated API calls)
# ----------------------------------------------------------------------------------------

NATIVE_API_USER_AGENT = "GCM-Android-5.23"
NATIVE_X_GARMIN_USER_AGENT = (
    "com.garmin.android.apps.connectmobile/5.23; ; Google/sdk_gphone64_arm64/google; "
    "Android/33; Dalvik/2.1.0"
)


# ----------------------------------------------------------------------------------------
# DI OAUTH2 TOKEN EXCHANGE
# ----------------------------------------------------------------------------------------

DI_TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token"  # noqa: S105
DI_GRANT_TYPE = (
    "https://connectapi.garmin.com/di-oauth2-service/oauth/grant/service_ticket"
)

# Garmin rotates accepted DI client IDs each quarter. The exchange flow tries each one
# in order until one succeeds, so this list also serves as a "newest accepted first"
# fallback chain.
DI_CLIENT_IDS = (
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2025Q2",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2024Q4",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI",
)


# ----------------------------------------------------------------------------------------
# CLOUDFLARE WAF ANTI-RATE-LIMIT DELAY
# ----------------------------------------------------------------------------------------

# Garmin's Cloudflare WAF rate-limits requests that go directly from the SSO page GET
# to the credential POST without intervening activity. A random 30-45s delay mimics
# natural browser behavior and consistently avoids the 429 block.
LOGIN_DELAY_MIN_S = 30.0
LOGIN_DELAY_MAX_S = 45.0


# ----------------------------------------------------------------------------------------
# API URL TEMPLATES
# ----------------------------------------------------------------------------------------

# Profile endpoints.
# ``SOCIAL_PROFILE_URL`` returns the dict containing ``displayName`` / ``fullName``
# that we interpolate into per-user URL paths (sleep, steps, heart rate, etc).
# Note: ``/userprofile-service/userprofile/profile`` returns 404 with DI Bearer
# auth; the social profile endpoint is the working alternative.
SOCIAL_PROFILE_URL = "/userprofile-service/socialProfile"
USER_SETTINGS_URL = "/userprofile-service/userprofile/user-settings"

# Wellness endpoints.
DAILY_SLEEP_URL = "/wellness-service/wellness/dailySleepData"
DAILY_STRESS_URL = "/wellness-service/wellness/dailyStress"
DAILY_RESPIRATION_URL = "/wellness-service/wellness/daily/respiration"
HEART_RATES_DAILY_URL = "/wellness-service/wellness/dailyHeartRate"
USER_SUMMARY_CHART_URL = "/wellness-service/wellness/dailySummaryChart"
FLOORS_CHART_DAILY_URL = "/wellness-service/wellness/floorsChartData/daily"
DAILY_INTENSITY_MINUTES_URL = "/wellness-service/wellness/daily/im"

# Metrics endpoints.
TRAINING_READINESS_URL = "/metrics-service/metrics/trainingreadiness"
TRAINING_STATUS_URL = "/metrics-service/metrics/trainingstatus/aggregated"
RACE_PREDICTOR_URL = "/metrics-service/metrics/racepredictions"

# Personal records.
PERSONAL_RECORD_URL = "/personalrecord-service/personalrecord/prs"

# Activities.
ACTIVITIES_URL = "/activitylist-service/activities/search/activities"
ACTIVITY_URL = "/activity-service/activity"
FIT_DOWNLOAD_URL = "/download-service/files/activity"
TCX_DOWNLOAD_URL = "/download-service/export/tcx/activity"
GPX_DOWNLOAD_URL = "/download-service/export/gpx/activity"
KML_DOWNLOAD_URL = "/download-service/export/kml/activity"
CSV_DOWNLOAD_URL = "/download-service/export/csv/activity"


# ----------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------------------


def _random_browser_headers() -> Dict[str, str]:
    """
    Generate a random browser User-Agent + sec-ch-ua headers.

    Falls back to a static desktop Chrome User-Agent if ``ua_generator`` is not
    installed.

    :return: Dictionary of browser-style HTTP headers.
    """

    if HAS_UA_GEN:
        ua = _generate_ua()
        return dict(ua.headers.get())
    return {"User-Agent": DESKTOP_USER_AGENT}


def _build_basic_auth(client_id: str) -> str:
    """
    Build a Basic auth header value for the DI OAuth2 token endpoint.

    The DI auth server expects ``Basic <base64(client_id:)>`` (no client secret).

    :param client_id: DI OAuth2 client identifier.
    :return: Basic auth header value (including the ``Basic `` prefix).
    """

    return "Basic " + base64.b64encode(f"{client_id}:".encode()).decode()


def _native_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Build the headers used for native (Android app) API calls and DI token exchange.

    :param extra: Optional extra headers to merge on top of the native defaults.
    :return: Dictionary of HTTP headers.
    """

    headers: Dict[str, str] = {
        "User-Agent": NATIVE_API_USER_AGENT,
        "X-Garmin-User-Agent": NATIVE_X_GARMIN_USER_AGENT,
        "X-Garmin-Paired-App-Version": "10861",
        "X-Garmin-Client-Platform": "Android",
        "X-App-Ver": "10861",
        "X-Lang": "en",
        "X-GCExperience": "GC5",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if extra:
        headers.update(extra)
    return headers

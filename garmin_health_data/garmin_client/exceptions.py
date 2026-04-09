"""
Exceptions raised by the vendored Garmin Connect client.

These exceptions mirror the upstream ``python-garminconnect`` library's
``GarminConnect*Error`` types but use shorter names to make the vendored origin
explicit.
"""


class GarminAuthenticationError(Exception):
    """
    Authentication failed.

    Raised for bad credentials, expired tokens, missing MFA prompts, MFA failures, or DI
    token exchange failures.
    """


class GarminConnectionError(Exception):
    """
    Network, HTTP, or local token-store error.

    Raised when the Garmin API returns a non-success status code that does not indicate
    an authentication or rate-limit problem, when a transport-level error prevents the
    request from completing, or when the local token-store helpers in ``tokens.py``
    cannot read/write ``garmin_tokens.json`` (missing file, malformed JSON, unreadable
    path). Local-I/O failures share this exception class so callers that wrap the whole
    ``from_tokens`` bootstrap in a single ``except GarminConnectionError`` still catch
    filesystem problems.
    """


class GarminTooManyRequestsError(GarminConnectionError):
    """
    Garmin or Cloudflare returned HTTP 429.

    Subclass of ``GarminConnectionError`` so callers that want to handle all
    connection problems uniformly can catch the parent class while callers that
    care about rate limiting specifically can catch this subclass.
    """

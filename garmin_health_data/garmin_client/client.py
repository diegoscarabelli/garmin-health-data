"""
GarminClient: vendored Garmin Connect client.

This is the public class of the ``garmin_client`` module. It owns:

- The five-strategy login fallback chain (delegated to :mod:`.strategies`).
- The DI OAuth2 token exchange and refresh (the only auth scheme this client
  supports; the upstream library's ``JWT_WEB`` cookie fallback is intentionally
  not vendored).
- Profile state (``display_name``, ``full_name``) populated from
  ``/userprofile-service/socialProfile`` after authentication.
- The 15 API methods used by the openetl pipeline (delegated to :mod:`.api`).
- An authenticated request helper that auto-refreshes the access token when it
  is within 15 minutes of expiry, and once on HTTP 401.
- Token persistence via :mod:`.tokens`.

Construction modes:

- ``GarminClient.from_tokens(token_dir)``: production hot path. Loads
  ``garmin_tokens.json`` from disk, calls ``/userprofile-service/socialProfile``
  to populate ``display_name``, returns a client ready for API calls.
- ``GarminClient()`` + ``client.login(email, password)``: bootstrap path used by
  ``refresh_garmin_tokens.py``. Runs the five-strategy login chain, optionally
  pauses for MFA, then calls ``client.dump(path)`` to persist the resulting
  tokens.
"""

import base64
import contextlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import requests

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException as _CffiRequestException

    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False
    _CffiRequestException = None  # type: ignore[assignment,misc]

# Transport-level exception classes raised by the underlying HTTP libraries.
# ``_http_post`` routes to curl_cffi when available, so we must catch
# ``curl_cffi.requests.exceptions.RequestException`` in addition to
# ``requests.RequestException``: the two hierarchies are unrelated
# (curl_cffi's RequestException inherits from ``OSError`` via ``CurlError``,
# not from ``requests.RequestException``).
if HAS_CFFI:
    _TRANSPORT_EXCEPTIONS: Tuple[type, ...] = (
        requests.RequestException,
        _CffiRequestException,
    )
else:
    _TRANSPORT_EXCEPTIONS = (requests.RequestException,)

from . import api, strategies, tokens
from .api import ActivityDownloadFormat
from .constants import (
    DI_CLIENT_IDS,
    DI_GRANT_TYPE,
    DI_TOKEN_URL,
    MOBILE_SSO_SERVICE_URL,
    SOCIAL_PROFILE_URL,
    _build_basic_auth,
    _native_headers,
)
from .exceptions import (
    GarminAuthenticationError,
    GarminConnectionError,
    GarminTooManyRequestsError,
)

_LOGGER = logging.getLogger(__name__)


class GarminClient:
    """
    Garmin Connect client: authentication + API access.

    Two construction modes are supported:

    1. From saved tokens (production):

       .. code-block:: python

           client = GarminClient.from_tokens("~/.garminconnect/12345678/")

       Loads DI tokens from disk, calls ``/userprofile-service/socialProfile``
       to populate ``display_name`` and ``full_name``, and returns a client ready
       to make API calls.

    2. From credentials (bootstrap):

       .. code-block:: python

           client = GarminClient()
           result = client.login(email, password, return_on_mfa=True)
           if result and result[0] == "needs_mfa":
               client.resume_login(result[1], mfa_code)
           client.dump("~/.garminconnect/12345678/")
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, domain: str = "garmin.com", **kwargs: Any) -> None:
        """
        :param domain: Garmin domain (use ``garmin.cn`` for China).
        :param kwargs: Optional ``pool_connections`` and ``pool_maxsize`` for the
            requests adapter when curl_cffi is unavailable.
        """

        self.domain = domain
        self._sso = f"https://sso.{domain}"
        self._connect = f"https://connect.{domain}"
        # URL base for connectapi.garmin.com. Named with a ``_url`` suffix to
        # avoid colliding with the ``_connectapi`` request helper method.
        self._connectapi_url = f"https://connectapi.{domain}"

        # DI Bearer tokens (the only auth scheme this client supports).
        self.di_token: Optional[str] = None
        self.di_refresh_token: Optional[str] = None
        self.di_client_id: Optional[str] = None

        # Profile state, populated by _load_profile after auth.
        self.display_name: Optional[str] = None
        self.full_name: Optional[str] = None

        # Path on disk where tokens were loaded from (or are to be saved to). Used
        # by _refresh_session to persist refreshed tokens back to disk so the
        # rotating refresh chain stays alive across pipeline runs.
        self._tokenstore_path: Optional[str] = None

        # Lazily-initialized requests.Session reused across all authenticated API
        # calls so that connection pooling and keep-alive amortize TLS handshakes
        # across the many per-day endpoint calls in the extract pipeline. Login
        # strategies build their own short-lived sessions in strategies.py because
        # they need TLS impersonation; this session is only for connectapi calls.
        self._api_session: Optional[requests.Session] = None
        self._pool_connections: int = kwargs.get("pool_connections", 20)
        self._pool_maxsize: int = kwargs.get("pool_maxsize", 20)

    @classmethod
    def from_tokens(cls, token_dir: Union[str, Path]) -> "GarminClient":
        """
        Load tokens from disk, populate profile state, and return a ready client.

        :param token_dir: Directory containing ``garmin_tokens.json``, or a
            direct path to that file.
        :return: Authenticated GarminClient ready to make API calls.
        :raises GarminConnectionError: If the token file is missing or
            unreadable.
        :raises GarminAuthenticationError: If the token file contains no token,
            or if the profile fetch fails.
        """

        client = cls()
        client.load(token_dir)
        client._load_profile()
        return client

    # ------------------------------------------------------------------
    # Authentication state
    # ------------------------------------------------------------------

    @property
    def is_authenticated(self) -> bool:
        """
        :return: True if a DI access token is currently held.
        """

        return bool(self.di_token)

    def get_api_headers(self) -> Dict[str, str]:
        """
        Build the headers used for authenticated API calls.

        :return: Dictionary including the Bearer token and native UA headers.
        :raises GarminAuthenticationError: If no token is held.
        """

        if not self.is_authenticated:
            raise GarminAuthenticationError("Not authenticated")
        return _native_headers(
            {
                "Authorization": f"Bearer {self.di_token}",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Login (delegates to strategies module)
    # ------------------------------------------------------------------

    def login(
        self,
        email: str,
        password: str,
        prompt_mfa: Optional[Callable[[], str]] = None,
        return_on_mfa: bool = False,
    ) -> Tuple[Optional[str], Any]:
        """
        Authenticate with Garmin Connect using email and password.

        Tries five login strategies in order until one succeeds. Each strategy
        targets a different Garmin SSO endpoint, with different TLS fingerprints
        and User-Agent strings, to maximize the chance of slipping past
        Cloudflare's bot detection on any given day:

        1. ``portal+cffi``: ``/portal/api/login`` with curl_cffi (5 browser TLS
           fingerprints). Currently the only strategy that succeeds reliably
           in production, so it leads the chain.
        2. ``portal+requests``: same endpoint with plain requests.
        3. ``mobile+cffi``: ``/mobile/api/login`` with curl_cffi (Safari).
        4. ``mobile+requests``: same endpoint with plain requests.
        5. ``widget+cffi``: SSO embed widget HTML form (no clientId, not subject
           to per-client rate limiting). Kept as a last-resort insurance: it
           has 429'd reliably for months but is preserved in case Cloudflare
           configuration changes restore it.

        :param email: Garmin Connect email.
        :param password: Garmin Connect password.
        :param prompt_mfa: Optional callable returning a 6-digit MFA code,
            invoked when MFA is required and ``return_on_mfa`` is False.
        :param return_on_mfa: If True, return ``("needs_mfa", session)`` early
            when MFA is required so the caller can complete it via
            :meth:`resume_login`.
        :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on
            MFA challenge with ``return_on_mfa=True``.
        :raises GarminAuthenticationError: On invalid credentials.
        :raises GarminTooManyRequestsError: When all strategies are 429'd.
        :raises GarminConnectionError: When all strategies fail with non-auth
            errors.
        """

        # Clear any leftover MFA state from a prior abandoned login attempt so
        # resume_login doesn't incorrectly route to a stale strategy.
        for attr in (
            "_widget_session",
            "_widget_signin_params",
            "_widget_last_resp",
            "_mfa_portal_web_session",
            "_mfa_portal_web_params",
            "_mfa_portal_web_headers",
            "_mfa_cffi_session",
            "_mfa_cffi_params",
            "_mfa_cffi_headers",
            "_mfa_session",
        ):
            if hasattr(self, attr):
                delattr(self, attr)

        strategy_chain: List[Tuple[str, Callable[..., Tuple[Optional[str], Any]]]] = []

        if HAS_CFFI:
            strategy_chain.append(
                (
                    "portal+cffi",
                    lambda *a, **k: strategies.portal_web_login_cffi(self, *a, **k),
                )
            )
        strategy_chain.append(
            (
                "portal+requests",
                lambda *a, **k: strategies.portal_web_login_requests(self, *a, **k),
            )
        )
        if HAS_CFFI:
            strategy_chain.append(
                ("mobile+cffi", lambda *a, **k: strategies.portal_login(self, *a, **k))
            )
        strategy_chain.append(
            ("mobile+requests", lambda *a, **k: strategies.mobile_login(self, *a, **k))
        )
        # widget+cffi is kept as a last-resort fallback: it has 429'd reliably
        # for months in production, so it's tried only after all other strategies
        # have failed. Kept in the chain in case Cloudflare configuration changes
        # restore it.
        if HAS_CFFI:
            strategy_chain.append(
                (
                    "widget+cffi",
                    lambda *a, **k: strategies.widget_login_cffi(self, *a, **k),
                )
            )

        last_err: Optional[Exception] = None
        for name, method in strategy_chain:
            try:
                _LOGGER.info("Trying login strategy: %s", name)
                result = method(
                    email,
                    password,
                    prompt_mfa=prompt_mfa,
                    return_on_mfa=return_on_mfa,
                )
                # If we got here without an exception, populate the profile so
                # display_name is ready for the API methods that need it.
                if not (isinstance(result, tuple) and result[0] == "needs_mfa"):
                    self._load_profile()
                return result
            except GarminAuthenticationError:
                # Bad credentials / invalid MFA — no point trying other strategies.
                raise
            except (GarminTooManyRequestsError, GarminConnectionError) as e:
                _LOGGER.warning("Login strategy %s failed: %s", name, e)
                last_err = e
                continue
            except Exception as e:
                _LOGGER.warning("Login strategy %s failed: %s", name, e)
                last_err = e
                continue

        if isinstance(last_err, GarminTooManyRequestsError):
            raise last_err
        raise GarminConnectionError(
            f"All login strategies failed. Last error: {last_err}"
        )

    def resume_login(
        self, _client_state: Any, mfa_code: str
    ) -> Tuple[Optional[str], Any]:
        """
        Complete an MFA challenge that was paused via ``return_on_mfa=True``.

        Routes to the appropriate MFA completion function based on which login
        strategy stashed its session state on the client.

        :param _client_state: Opaque session token returned by ``login()`` (kept
            for signature parity with the upstream library; resume routes via
            attribute presence on the client instance, not via this argument).
        :param mfa_code: 6-digit MFA code.
        :return: ``(None, None)`` on success.
        :raises GarminAuthenticationError: On verification failure.
        """

        if hasattr(self, "_widget_session"):
            ticket = strategies.complete_mfa_widget(self, mfa_code)
            sso_embed = f"{self._sso}/sso/embed"
            self._establish_session(
                ticket, sess=self._widget_session, service_url=sso_embed
            )
            del self._widget_session
            del self._widget_signin_params
            del self._widget_last_resp
        elif hasattr(self, "_mfa_portal_web_session"):
            strategies.complete_mfa_portal_web(self, mfa_code)
        elif hasattr(self, "_mfa_cffi_session"):
            strategies.complete_mfa_portal(self, mfa_code)
        elif hasattr(self, "_mfa_session"):
            strategies.complete_mfa(self, mfa_code)
        else:
            raise GarminAuthenticationError(
                "No pending MFA challenge to resume. resume_login() must be "
                "called after a login() call that returned ('needs_mfa', ...)"
            )

        self._load_profile()
        return None, None

    # ------------------------------------------------------------------
    # Session establishment + DI token exchange
    # ------------------------------------------------------------------

    def _establish_session(
        self,
        ticket: str,
        sess: Any = None,
        service_url: Optional[str] = None,
    ) -> None:
        """
        Consume a CAS service ticket by exchanging it for DI OAuth2 tokens.

        Unlike the upstream library, this client does not fall back to
        ``JWT_WEB`` cookie auth: DI Bearer tokens have worked exclusively for
        months and the JWT_WEB path is dead code we don't want to maintain.

        :param ticket: CAS service ticket from the SSO login flow.
        :param sess: Optional session that originated the ticket (kept for
            interface parity; the DI token exchange uses its own HTTP client).
        :param service_url: SSO service URL the ticket was issued for.
        :raises GarminAuthenticationError: If the DI exchange fails on all
            client IDs.
        """

        # sess is intentionally accepted but unused: the DI token exchange
        # speaks a different protocol (OAuth2 over diauth.garmin.com) and does
        # not consume CAS cookies from the SSO login session.
        del sess
        self._exchange_service_ticket(ticket, service_url=service_url)

    @staticmethod
    def _http_post(url: str, **kwargs: Any) -> Any:
        """
        POST helper using curl_cffi if installed, plain requests otherwise.

        Used by the DI token exchange and refresh, both of which post directly
        to ``diauth.garmin.com`` (no SSO cookies required).
        """

        if HAS_CFFI:
            return cffi_requests.post(url, impersonate="chrome", **kwargs)
        return requests.post(url, **kwargs)  # noqa: S113

    def _exchange_service_ticket(
        self, ticket: str, service_url: Optional[str] = None
    ) -> None:
        """
        Exchange a CAS service ticket for a DI OAuth2 access + refresh token pair.

        Tries the rolling list of accepted DI client IDs in order until one succeeds.
        Garmin rotates these quarterly; the newest is tried first so production traffic
        uses the most current credential.

        :param ticket: CAS service ticket.
        :param service_url: SSO service URL associated with the ticket. Must match the
            URL used during the originating login flow.
        :raises GarminTooManyRequestsError: On HTTP 429.
        :raises GarminConnectionError: If every client ID fails due to transport errors
            (connection, timeout, SSL), or if all HTTP failures are 5xx with no 4xx
            responses (pure server-side outage).
        :raises GarminAuthenticationError: If any client ID returns a 4xx, or if all
            fail for non-transport reasons (malformed response, missing token).
        """

        svc_url = service_url or MOBILE_SSO_SERVICE_URL

        di_token = None
        di_refresh = None
        di_client_id = None
        last_transport_error: Optional[Exception] = None
        last_server_error: Optional[tuple] = None
        had_auth_failure = False  # True if any non-429 4xx response seen

        for client_id in DI_CLIENT_IDS:
            try:
                r = self._http_post(
                    DI_TOKEN_URL,
                    headers=_native_headers(
                        {
                            "Authorization": _build_basic_auth(client_id),
                            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Cache-Control": "no-cache",
                        }
                    ),
                    data={
                        "client_id": client_id,
                        "service_ticket": ticket,
                        "grant_type": DI_GRANT_TYPE,
                        "service_url": svc_url,
                    },
                    timeout=30,
                )
            except _TRANSPORT_EXCEPTIONS as exc:
                # Transport-level failure (connection, timeout, SSL). Try the
                # next client ID; if all fail this way, raise GarminConnectionError
                # so callers see a typed transport error rather than a leaked
                # requests/curl_cffi exception.
                _LOGGER.debug("DI exchange transport error for %s: %s", client_id, exc)
                last_transport_error = exc
                continue
            if r.status_code == 429:
                raise GarminTooManyRequestsError("DI token exchange rate limited")
            if not r.ok:
                _LOGGER.debug(
                    "DI exchange failed for %s: %s %s",
                    client_id,
                    r.status_code,
                    r.text[:200],
                )
                if r.status_code >= 500:
                    last_server_error = (r.status_code, r.text[:200])
                else:
                    had_auth_failure = True
                continue
            try:
                data = r.json()
                new_token = data["access_token"]
                new_refresh = data.get("refresh_token")
                if not new_refresh:
                    # Reject responses missing the refresh token. Without it the
                    # client can never refresh the access token after expiry, so
                    # treating an "incomplete" response as success would leave
                    # the pipeline in a half-broken state.
                    raise ValueError("response missing refresh_token")
                di_token = new_token
                di_refresh = new_refresh
                di_client_id = self._extract_client_id_from_jwt(di_token) or client_id
                break
            except Exception as e:
                _LOGGER.debug("DI token parse failed for %s: %s", client_id, e)
                continue

        if not di_token:
            if last_transport_error is not None:
                raise GarminConnectionError(
                    f"DI token exchange transport error on all client IDs: "
                    f"{last_transport_error}"
                ) from last_transport_error
            if last_server_error is not None and not had_auth_failure:
                # Only treat as a server-side failure if every HTTP response
                # was a 5xx: a mix of 5xx and 4xx suggests the ticket itself
                # was invalid (auth problem), not a Garmin outage.
                raise GarminConnectionError(
                    f"DI token exchange server error on all client IDs: "
                    f"HTTP {last_server_error[0]}: {last_server_error[1]}"
                )
            raise GarminAuthenticationError(
                "DI token exchange failed for all client IDs"
            )

        self.di_token = di_token
        self.di_refresh_token = di_refresh
        self.di_client_id = di_client_id

    def _refresh_di_token(self) -> None:
        """
        Refresh the DI access token using the stored refresh token.

        Garmin rotates the refresh token on each use, so the response includes
        a new refresh token that replaces the old one. The caller is responsible
        for persisting the new pair to disk via :meth:`_refresh_session`.

        :raises GarminAuthenticationError: If no refresh token is held, or the
            server rejects it.
        :raises GarminTooManyRequestsError: If the DI token endpoint returns
            HTTP 429.
        :raises GarminConnectionError: On transport errors (connection,
            timeout, SSL) or non-JSON responses.
        """

        if not self.di_refresh_token or not self.di_client_id:
            raise GarminAuthenticationError("No DI refresh token available")
        try:
            r = self._http_post(
                DI_TOKEN_URL,
                headers=_native_headers(
                    {
                        "Authorization": _build_basic_auth(self.di_client_id),
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Cache-Control": "no-cache",
                    }
                ),
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.di_client_id,
                    "refresh_token": self.di_refresh_token,
                },
                timeout=30,
            )
        except _TRANSPORT_EXCEPTIONS as exc:
            raise GarminConnectionError(
                f"DI token refresh transport error: {exc}"
            ) from exc
        if r.status_code == 429:
            raise GarminTooManyRequestsError(
                f"DI token refresh rate limited: {r.text[:200]}"
            )
        if not r.ok:
            raise GarminAuthenticationError(
                f"DI token refresh failed: {r.status_code} {r.text[:200]}"
            )
        try:
            data = r.json()
        except (json.JSONDecodeError, ValueError) as err:
            preview = " ".join(r.text.split())[:200]
            raise GarminConnectionError(
                f"DI token refresh returned non-JSON (status {r.status_code}): "
                f"{preview!r}"
            ) from err
        # A 2xx JSON response still has to contain an ``access_token``; otherwise
        # a bare ``data["access_token"]`` would leak an untyped ``KeyError`` up
        # the stack and look like a bug in the client rather than a malformed
        # auth server response. ``refresh_token`` is intentionally optional here
        # per OAuth2 RFC 6749 section 6: an omitted refresh_token means the
        # existing one remains valid, so we fall back to it.
        new_token = data.get("access_token")
        if not new_token:
            raise GarminAuthenticationError(
                "DI token refresh response malformed: expected access_token, "
                f"got keys {sorted(data.keys())}"
            )
        self.di_token = new_token
        self.di_refresh_token = data.get("refresh_token", self.di_refresh_token)
        self.di_client_id = (
            self._extract_client_id_from_jwt(self.di_token) or self.di_client_id
        )

    @staticmethod
    def _extract_client_id_from_jwt(token: str) -> Optional[str]:
        """
        Extract the ``client_id`` claim from a JWT access token's payload.

        Garmin's DI tokens advertise the client ID that minted them in the JWT payload,
        which is the most reliable way to know which client ID to use when refreshing
        the token (rather than relying on the one we used during the initial exchange).

        :param token: JWT access token.
        :return: Client ID string, or None if the token cannot be parsed.
        """

        try:
            parts = token.split(".")
            if len(parts) < 2:
                return None
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode())
            value = payload.get("client_id")
            return str(value) if value else None
        except Exception:
            return None

    def _token_expires_soon(self) -> bool:
        """
        :return: True if the current access token expires within 15 minutes.
        """

        token = self.di_token
        if not token:
            return False
        try:
            parts = str(token).split(".")
            if len(parts) >= 2:
                payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
                payload = json.loads(
                    base64.urlsafe_b64decode(payload_b64.encode()).decode()
                )
                exp = payload.get("exp")
                if exp and time.time() > (int(exp) - 900):
                    return True
        except Exception:
            _LOGGER.debug("Failed to check token expiry")
        return False

    def _refresh_session(self) -> None:
        """
        Refresh the DI access token and persist the new pair to disk.

        Called automatically by :meth:`_request` when the token is near expiry
        or when an API call returns 401. The persistence step is best-effort:
        a failure to write the new tokens to disk does not block the in-memory
        refresh from succeeding.
        """

        if not self.di_token:
            return
        try:
            self._refresh_di_token()
            if self._tokenstore_path:
                with contextlib.suppress(Exception):
                    self.dump(self._tokenstore_path)
        except Exception as err:
            _LOGGER.debug("DI token refresh failed: %s", err)

    # ------------------------------------------------------------------
    # Profile loading
    # ------------------------------------------------------------------

    def _load_profile(self) -> None:
        """
        Populate ``display_name`` and ``full_name`` from the social profile endpoint.

        Five of the 15 API methods (sleep, steps, heart_rates, personal_records,
        race_predictions) interpolate ``display_name`` into the URL path, so we
        cannot make any of those calls until profile is loaded.

        Note: the ``userprofile/profile`` endpoint returns 404 against DI Bearer
        authentication. The social profile endpoint (see ``SOCIAL_PROFILE_URL``)
        exposes the same ``displayName`` and ``fullName`` fields and is what the
        upstream library uses (see ``garminconnect/__init__.py``
        ``_init_garmin_connect``).

        :raises GarminAuthenticationError: If the profile response is missing
            ``displayName``.
        """

        profile = self._connectapi(SOCIAL_PROFILE_URL)
        if not profile or "displayName" not in profile:
            raise GarminAuthenticationError("Profile response missing displayName")
        self.display_name = profile["displayName"]
        self.full_name = profile.get("fullName")

    # ------------------------------------------------------------------
    # Authenticated request helpers
    # ------------------------------------------------------------------

    def _connectapi(self, path: str, **kwargs: Any) -> Any:
        """
        GET against ``connectapi.garmin.com`` and return the parsed JSON body.

        :param path: API path (e.g. ``/wellness-service/wellness/dailyStress/2024-01-01``).
        :param kwargs: Additional kwargs forwarded to :meth:`_request` (e.g.
            ``params``).
        :return: Parsed JSON, or an empty dict on HTTP 204.
        :raises GarminConnectionError: If the response body is not valid JSON
            (e.g., a Cloudflare HTML edge page returned with a 200 status).
        """

        resp = self._request("GET", path, **kwargs)
        if resp.status_code == 204:
            return {}
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as err:
            preview = " ".join(resp.text.split())[:200]
            raise GarminConnectionError(
                f"Invalid JSON response from Garmin (status {resp.status_code}): "
                f"{preview!r}"
            ) from err

    def _download(self, path: str, **kwargs: Any) -> bytes:
        """
        GET against ``connectapi.garmin.com`` and return raw bytes.

        Used by :func:`api.download_activity` to fetch binary FIT/TCX/etc files.

        :param path: API path.
        :return: Raw response bytes.
        """

        headers = kwargs.pop("headers", {})
        headers["Accept"] = "*/*"
        return self._request("GET", path, headers=headers, **kwargs).content

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """
        Issue an authenticated HTTP request, refreshing the access token as needed and
        mapping error responses to typed exceptions.

        Refresh logic:

        - If the access token is within 15 minutes of expiry, refresh first.
        - If the response is HTTP 401, refresh once and retry.

        Error mapping:

        - 429 -> :class:`GarminTooManyRequestsError`
        - 401 (after retry) -> :class:`GarminAuthenticationError`
        - other 4xx/5xx -> :class:`GarminConnectionError`
        - transport errors (connection, timeout, SSL) ->
          :class:`GarminConnectionError`

        :param method: HTTP method (``GET``, ``POST``, etc).
        :param path: API path. Leading slash optional.
        :param kwargs: Forwarded to ``requests.Session.request``. ``timeout``
            defaults to 15s.
        :return: The successful response object.
        """

        if self.is_authenticated and self._token_expires_soon():
            self._refresh_session()

        url = f"{self._connectapi_url}/{path.lstrip('/')}"

        if "timeout" not in kwargs:
            kwargs["timeout"] = 15

        custom_headers = kwargs.pop("headers", {}) or {}

        def _build_headers() -> Dict[str, str]:
            merged = self.get_api_headers()
            if custom_headers:
                merged.update(custom_headers)
            return merged

        if self._api_session is None:
            self._api_session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self._pool_connections,
                pool_maxsize=self._pool_maxsize,
            )
            self._api_session.mount("https://", adapter)

        sess = self._api_session
        try:
            resp = sess.request(method, url, headers=_build_headers(), **kwargs)
        except requests.RequestException as exc:
            raise GarminConnectionError(f"API request failed: {exc}") from exc

        if resp.status_code == 401:
            self._refresh_session()
            try:
                resp = sess.request(method, url, headers=_build_headers(), **kwargs)
            except requests.RequestException as exc:
                raise GarminConnectionError(
                    f"API request failed after token refresh: {exc}"
                ) from exc
            if resp.status_code == 401:
                raise GarminAuthenticationError(
                    "API request unauthorized after token refresh"
                )

        if resp.status_code == 429:
            raise GarminTooManyRequestsError(
                f"API rate limited (HTTP 429): {resp.text[:200]}"
            )

        if resp.status_code == 204:
            return resp

        if resp.status_code >= 400:
            error_msg = f"API Error {resp.status_code}"
            try:
                error_data = resp.json()
                if isinstance(error_data, dict):
                    msg = error_data.get("message") or error_data.get("content") or ""
                    if msg:
                        error_msg += f" - {msg}"
                    else:
                        error_msg += f" - {error_data}"
            except Exception:
                if len(resp.text) < 500:
                    error_msg += f" - {resp.text}"
            raise GarminConnectionError(error_msg)

        return resp

    # ------------------------------------------------------------------
    # Token persistence (delegates to tokens module)
    # ------------------------------------------------------------------

    def dumps(self) -> str:
        """:return: JSON string of the current DI token state."""

        return tokens.dumps(self)

    def dump(self, path: Union[str, Path]) -> None:
        """
        Write current DI tokens to disk.

        :param path: Directory or ``.json`` file path.
        """

        tokens.dump(self, path)

    def loads(self, tokenstore: str) -> None:
        """
        Replace current DI tokens with the ones in a JSON string.

        :param tokenstore: JSON string with ``di_token``, ``di_refresh_token``,
            ``di_client_id``.
        """

        tokens.loads(self, tokenstore)

    def load(self, path: Union[str, Path]) -> None:
        """
        Replace current DI tokens with the ones loaded from disk.

        Records the resolved path so that subsequent token refreshes persist
        back to the same file.

        :param path: Directory or ``.json`` file path.
        """

        tokens.load(self, path)

    # ------------------------------------------------------------------
    # API method bindings (delegate to api module)
    # ------------------------------------------------------------------

    # Defined as bound methods (not lambdas) so they show up correctly in
    # introspection / autocomplete and so getattr-based dispatch in
    # extract.py keeps working.

    def get_sleep_data(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_sleep_data`.
        """
        return api.get_sleep_data(self, cdate)

    def get_stress_data(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_stress_data`.
        """
        return api.get_stress_data(self, cdate)

    def get_respiration_data(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_respiration_data`.
        """
        return api.get_respiration_data(self, cdate)

    def get_heart_rates(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_heart_rates`.
        """
        return api.get_heart_rates(self, cdate)

    def get_training_readiness(self, cdate: str) -> List[Dict[str, Any]]:
        """
        See :func:`api.get_training_readiness`.
        """
        return api.get_training_readiness(self, cdate)

    def get_training_status(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_training_status`.
        """
        return api.get_training_status(self, cdate)

    def get_steps_data(self, cdate: str) -> List[Dict[str, Any]]:
        """
        See :func:`api.get_steps_data`.
        """
        return api.get_steps_data(self, cdate)

    def get_floors(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_floors`.
        """
        return api.get_floors(self, cdate)

    def get_intensity_minutes_data(self, cdate: str) -> Dict[str, Any]:
        """
        See :func:`api.get_intensity_minutes_data`.
        """
        return api.get_intensity_minutes_data(self, cdate)

    def get_activities_by_date(
        self,
        startdate: str,
        enddate: Optional[str] = None,
        activitytype: Optional[str] = None,
        sortorder: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        See :func:`api.get_activities_by_date`.
        """
        return api.get_activities_by_date(
            self, startdate, enddate, activitytype, sortorder
        )

    def get_activity_exercise_sets(self, activity_id: Any) -> Dict[str, Any]:
        """
        See :func:`api.get_activity_exercise_sets`.
        """
        return api.get_activity_exercise_sets(self, activity_id)

    def get_personal_record(self) -> Dict[str, Any]:
        """
        See :func:`api.get_personal_record`.
        """
        return api.get_personal_record(self)

    def get_race_predictions(
        self,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
        _type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        See :func:`api.get_race_predictions`.
        """
        return api.get_race_predictions(self, startdate, enddate, _type)

    def get_user_profile(self) -> Dict[str, Any]:
        """
        See :func:`api.get_user_profile`.
        """
        return api.get_user_profile(self)

    def download_activity(
        self,
        activity_id: Any,
        dl_fmt: ActivityDownloadFormat = ActivityDownloadFormat.ORIGINAL,
    ) -> bytes:
        """
        See :func:`api.download_activity`.
        """
        return api.download_activity(self, activity_id, dl_fmt)

    # Class-level alias so existing call sites that reach for
    # ``self.garmin_client.ActivityDownloadFormat.ORIGINAL`` keep working without
    # an explicit import (matches upstream python-garminconnect's nested-enum
    # convention).
    ActivityDownloadFormat = ActivityDownloadFormat

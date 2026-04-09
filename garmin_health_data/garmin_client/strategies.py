"""
Login strategy implementations for the vendored Garmin Connect client.

Garmin's Cloudflare WAF aggressively rate-limits the SSO login endpoints, and the
exact endpoint that gets blocked rotates over time. To stay reliable, the client
tries five fallback strategies in order until one succeeds:

1. ``portal_web_login_cffi``: ``/portal/api/login`` (the endpoint
   connect.garmin.com itself uses) with curl_cffi, trying five browser TLS
   fingerprints.
2. ``portal_web_login_requests``: same endpoint with plain ``requests`` and a
   random browser User-Agent.
3. ``portal_login``: mobile ``/mobile/api/login`` with curl_cffi (Safari TLS).
4. ``mobile_login``: mobile ``/mobile/api/login`` with plain ``requests``.
5. ``widget_login_cffi``: SSO embed widget HTML form flow with curl_cffi TLS
   impersonation. Last-resort fallback: empirically the most rate-limited path,
   but kept in the chain because it occasionally succeeds when all four JSON
   endpoints have been blocked.

The portal and mobile flows (the internal ``_portal_web_login`` helper used by
``portal_web_login_cffi`` / ``portal_web_login_requests``, plus ``portal_login``
and ``mobile_login``) sleep 30-45 seconds between the SSO page GET and the
credential POST. Without this delay Garmin's Cloudflare WAF returns 429
immediately, treating the back-to-back requests as bot-like.

These functions are written as plain functions taking ``client`` as the first
argument so the file stays decoupled from the ``GarminClient`` class definition.
``GarminClient.login`` invokes them via lambdas (no class-level binding).
"""

import json
import logging
import random
import re
import time
from typing import Any, Callable, Optional, Tuple

import requests

try:
    from curl_cffi import requests as cffi_requests

    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

from .constants import (
    LOGIN_DELAY_MAX_S,
    LOGIN_DELAY_MIN_S,
    MOBILE_SSO_CLIENT_ID,
    MOBILE_SSO_SERVICE_URL,
    MOBILE_SSO_USER_AGENT,
    PORTAL_SSO_CLIENT_ID,
    PORTAL_SSO_SERVICE_URL,
    _random_browser_headers,
)
from .exceptions import (
    GarminAuthenticationError,
    GarminConnectionError,
    GarminTooManyRequestsError,
)

_LOGGER = logging.getLogger(__name__)


# Compiled at module level to amortize regex compilation cost across many login attempts.
_CSRF_RE = re.compile(r'name="_csrf"\s+value="(.+?)"')
_TITLE_RE = re.compile(r"<title>(.+?)</title>")
_TICKET_RE = re.compile(r'embed\?ticket=([^"]+)"')


# ----------------------------------------------------------------------------------------
# SSO EMBED WIDGET LOGIN (HTML form flow, no clientId)
# ----------------------------------------------------------------------------------------


def widget_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Log in via the SSO embed widget using curl_cffi TLS impersonation.

    This is the classic HTML form-based flow used by ``garth`` for years. Kept
    as the last-resort fallback in the strategy chain: empirically Cloudflare
    rate-limits this endpoint aggressively (the chain reorder was driven by
    repeated 429s on this path), but it occasionally still succeeds when all
    four JSON-API strategies have been blocked on the current IP.

    Requires ``curl_cffi`` for TLS fingerprint impersonation to pass Cloudflare bot
    detection. Cannot run otherwise.

    :param client: GarminClient instance.
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code, used when MFA is
        required and ``return_on_mfa`` is False.
    :param return_on_mfa: If True and MFA is required, return early with
        ``("needs_mfa", session)`` so the caller can complete MFA later via
        ``resume_login``.
    :return: ``(None, None)`` on full success, or ``("needs_mfa", session)`` if
        MFA was required and ``return_on_mfa`` is True.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminAuthenticationError: On invalid credentials or missing MFA prompt.
    :raises GarminConnectionError: On HTTP errors or unexpected page contents.
    """

    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; widget+cffi unavailable")

    sess: Any = cffi_requests.Session(impersonate="chrome", timeout=30)

    sso_base = f"{client._sso}/sso"
    sso_embed = f"{sso_base}/embed"
    embed_params = {
        "id": "gauth-widget",
        "embedWidget": "true",
        "gauthHost": sso_base,
    }
    signin_params = {
        **embed_params,
        "gauthHost": sso_embed,
        "service": sso_embed,
        "source": sso_embed,
        "redirectAfterAccountLoginUrl": sso_embed,
        "redirectAfterAccountCreationUrl": sso_embed,
    }

    # Step 1: GET /sso/embed to establish session cookies.
    r = sess.get(sso_embed, params=embed_params)
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429 on embed page")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget login: embed page returned HTTP {r.status_code}"
        )

    # Step 2: GET /sso/signin to obtain CSRF token.
    r = sess.get(
        f"{sso_base}/signin",
        params=signin_params,
        headers={"Referer": sso_embed},
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429 on sign-in page")
    csrf_match = _CSRF_RE.search(r.text)
    if not csrf_match:
        raise GarminConnectionError(
            "Widget login: could not find CSRF token in sign-in page"
        )

    # Step 3: POST credentials via HTML form.
    r = sess.post(
        f"{sso_base}/signin",
        params=signin_params,
        headers={"Referer": r.url},
        data={
            "username": email,
            "password": password,
            "embed": "true",
            "_csrf": csrf_match.group(1),
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget login returned 429")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget login: credential POST returned HTTP {r.status_code}"
        )

    title_match = _TITLE_RE.search(r.text)
    title = title_match.group(1) if title_match else ""

    # Step 4: Handle MFA.
    if "MFA" in title:
        client._widget_session = sess
        client._widget_signin_params = signin_params
        client._widget_last_resp = r

        if return_on_mfa:
            return "needs_mfa", sess

        if prompt_mfa:
            mfa_code = prompt_mfa()
            ticket = complete_mfa_widget(client, mfa_code)
            client._establish_session(ticket, sess=sess, service_url=sso_embed)
            del client._widget_session
            del client._widget_signin_params
            del client._widget_last_resp
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if title != "Success":
        # Detect credential failures explicitly so we don't fall through to other
        # strategies with bad credentials and waste their attempts.
        title_lower = title.lower()
        if any(
            hint in title_lower for hint in ("locked", "invalid", "error", "incorrect")
        ):
            raise GarminAuthenticationError(
                f"Widget login: authentication failed ('{title}')"
            )
        raise GarminConnectionError(f"Widget login: unexpected title '{title}'")

    # Step 5: Extract service ticket from success page.
    ticket_match = _TICKET_RE.search(r.text)
    if not ticket_match:
        raise GarminConnectionError(
            "Widget login: could not find service ticket in response"
        )
    client._establish_session(ticket_match.group(1), sess=sess, service_url=sso_embed)
    return None, None


def complete_mfa_widget(client: Any, mfa_code: str) -> str:
    """
    Complete MFA verification for the widget flow and return the service ticket.

    Uses the session, signin params, and CSRF state stashed by ``widget_login_cffi``
    when it detected an MFA challenge.

    :param client: GarminClient instance with widget MFA state.
    :param mfa_code: 6-digit MFA code from email/SMS/authenticator app.
    :return: Service ticket extracted from the success page.
    :raises GarminAuthenticationError: On verification failure.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminConnectionError: On HTTP errors.
    """

    sess = client._widget_session
    r = client._widget_last_resp

    csrf_match = _CSRF_RE.search(r.text)
    if not csrf_match:
        raise GarminAuthenticationError("Widget MFA: could not find CSRF token")

    r = sess.post(
        f"{client._sso}/sso/verifyMFA/loginEnterMfaCode",
        params=client._widget_signin_params,
        headers={"Referer": r.url},
        data={
            "mfa-code": mfa_code,
            "embed": "true",
            "_csrf": csrf_match.group(1),
            "fromPage": "setupEnterMfaCode",
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError("Widget MFA returned 429")
    if not r.ok:
        raise GarminConnectionError(
            f"Widget MFA: verify endpoint returned HTTP {r.status_code}"
        )

    title_match = _TITLE_RE.search(r.text)
    title = title_match.group(1) if title_match else ""

    if title != "Success":
        raise GarminAuthenticationError(f"Widget MFA verification failed: '{title}'")

    ticket_match = _TICKET_RE.search(r.text)
    if not ticket_match:
        raise GarminAuthenticationError("Widget MFA: could not find service ticket")
    return ticket_match.group(1)


# ----------------------------------------------------------------------------------------
# PORTAL WEB LOGIN (desktop browser flow used by connect.garmin.com)
# ----------------------------------------------------------------------------------------


def portal_web_login_cffi(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Log in via the portal web endpoint using curl_cffi TLS impersonation.

    Tries five browser TLS fingerprints in order. Safari is less likely to be
    blocked by Cloudflare than Chrome.

    :param client: GarminClient instance.
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code (see widget flow).
    :param return_on_mfa: If True, return early on MFA challenge.
    :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on MFA.
    :raises GarminConnectionError: If all five impersonations fail.
    """

    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; portal+cffi unavailable")

    impersonations = ["safari", "safari_ios", "chrome120", "edge101", "chrome"]
    last_err: Optional[Exception] = None
    last_429: Optional[GarminTooManyRequestsError] = None
    rate_limited_count = 0
    for imp in impersonations:
        try:
            _LOGGER.debug("Trying portal+cffi with impersonation=%s", imp)
            sess: Any = cffi_requests.Session(impersonate=imp)  # type: ignore[arg-type]
            return _portal_web_login(
                client,
                sess,
                email,
                password,
                prompt_mfa=prompt_mfa,
                return_on_mfa=return_on_mfa,
            )
        except GarminAuthenticationError:
            raise
        except GarminTooManyRequestsError as e:
            # Different TLS fingerprints can be rate-limited independently by
            # Cloudflare, so try the next impersonation before giving up.
            _LOGGER.debug("portal+cffi(%s) 429: %s", imp, e)
            last_err = e
            last_429 = e
            rate_limited_count += 1
            continue
        except GarminConnectionError as e:
            _LOGGER.debug("portal+cffi(%s) transient failure: %s", imp, e)
            last_err = e
            continue
        except Exception as e:
            _LOGGER.debug("portal+cffi(%s) failed: %s", imp, e)
            last_err = e
            continue
    # Classify the aggregate rather than trusting ``last_err`` alone: if every
    # impersonation was rate-limited, surface ``GarminTooManyRequestsError`` so
    # the outer ``Client.login()`` strategy chain can detect the all-429 case
    # and apply backoff. A mix of 429 and non-429 failures collapses to
    # ``GarminConnectionError`` because the rate limiter wasn't the sole cause.
    if rate_limited_count == len(impersonations) and last_429 is not None:
        raise last_429
    if last_err is not None:
        raise GarminConnectionError("All cffi impersonations failed") from last_err
    raise GarminConnectionError("All cffi impersonations failed")


def portal_web_login_requests(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Log in via the portal web endpoint using plain ``requests``.

    Acts as a no-curl_cffi fallback for the portal flow. Browser-style headers
    (random User-Agent + sec-ch-ua) are generated inside ``_portal_web_login``
    and passed explicitly on each request, so no session-level headers are set
    here.

    :param client: GarminClient instance.
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code.
    :param return_on_mfa: If True, return early on MFA challenge.
    :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on MFA.
    """

    sess = requests.Session()
    return _portal_web_login(
        client,
        sess,
        email,
        password,
        prompt_mfa=prompt_mfa,
        return_on_mfa=return_on_mfa,
    )


def _portal_web_login(
    client: Any,
    sess: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Shared portal web login implementation used by the cffi and requests variants.

    Hits ``/portal/api/login``, which is the same endpoint the Garmin Connect
    React app uses. Cloudflare cannot block it without breaking the website
    itself, but it does rate-limit consecutive GET+POST without intervening
    delay. The 30-45s sleep between Step 1 and Step 2 mimics natural browser
    behavior and consistently avoids the 429 block.

    :param client: GarminClient instance.
    :param sess: Pre-built session (cffi or plain requests).
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code.
    :param return_on_mfa: If True, return early on MFA challenge.
    :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on MFA.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminAuthenticationError: On invalid credentials or missing MFA prompt.
    :raises GarminConnectionError: On HTTP errors or unexpected response shape.
    """

    signin_url = f"{client._sso}/portal/sso/en-US/sign-in"

    # Generate a consistent random browser identity for this login attempt.
    browser_hdrs = _random_browser_headers()

    # Step 1: GET the sign-in page to establish session cookies.
    get_headers = {
        **browser_hdrs,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    get_resp = sess.get(
        signin_url,
        params={
            "clientId": PORTAL_SSO_CLIENT_ID,
            "service": PORTAL_SSO_SERVICE_URL,
        },
        headers=get_headers,
        timeout=30,
    )
    if get_resp.status_code == 429:
        raise GarminTooManyRequestsError(
            "Portal login GET returned 429. Cloudflare is blocking this request."
        )
    if not get_resp.ok:
        raise GarminConnectionError(
            f"Portal login GET failed: HTTP {get_resp.status_code}"
        )

    # Garmin's Cloudflare WAF rate-limits requests that go directly from the SSO
    # page GET to the login POST without intervening activity.
    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info(
        "Portal login: waiting %.0fs to avoid Cloudflare rate limiting...", delay_s
    )
    time.sleep(delay_s)

    # Step 2: POST credentials to the portal login API.
    login_url = f"{client._sso}/portal/api/login"
    login_params = {
        "clientId": PORTAL_SSO_CLIENT_ID,
        "locale": "en-US",
        "service": PORTAL_SSO_SERVICE_URL,
    }
    post_headers = {
        **browser_hdrs,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": client._sso,
        "Referer": (
            f"{signin_url}?clientId={PORTAL_SSO_CLIENT_ID}"
            f"&service={PORTAL_SSO_SERVICE_URL}"
        ),
    }

    r = sess.post(
        login_url,
        params=login_params,
        headers=post_headers,
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError(
            "Portal login returned 429. Cloudflare is blocking this request."
        )
    if not r.ok:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"Portal login POST failed: HTTP {r.status_code}: {body_preview!r}"
        )

    try:
        res = r.json()
    except (json.JSONDecodeError, ValueError) as err:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"Portal login returned non-JSON (status {r.status_code}): "
            f"{body_preview!r}"
        ) from err

    resp_type = res.get("responseStatus", {}).get("type")

    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_portal_web_session = sess
        client._mfa_portal_web_params = login_params
        client._mfa_portal_web_headers = post_headers

        if return_on_mfa:
            return "needs_mfa", sess

        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa_portal_web(client, mfa_code)
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket, sess=sess, service_url=PORTAL_SSO_SERVICE_URL)
        return None, None

    if resp_type == "INVALID_USERNAME_PASSWORD":
        raise GarminAuthenticationError(
            "401 Unauthorized (Invalid Username or Password)"
        )

    raise GarminConnectionError(f"Portal web login failed: {res}")


def complete_mfa_portal_web(client: Any, mfa_code: str) -> None:
    """
    Complete MFA via the portal web flow.

    Tries ``/portal/api/mfa/verifyCode`` first, then ``/mobile/api/mfa/verifyCode``
    as a fallback. Both share the SSO session cookies, but Garmin occasionally
    routes one or the other through a different rate limit bucket, so trying
    both gives the best success rate.

    :param client: GarminClient instance with portal web MFA state.
    :param mfa_code: 6-digit MFA code.
    :raises GarminTooManyRequestsError: If every attempted endpoint returned HTTP 429
        or a 429 in the JSON body (pure rate-limit aggregate).
    :raises GarminConnectionError: If every attempted endpoint failed with a
        transport/non-JSON error (pure infrastructure aggregate).
    :raises GarminAuthenticationError: If at least one endpoint returned a parseable
        non-SUCCESSFUL response (definitive verification failure).
    """

    sess = client._mfa_portal_web_session
    mfa_json = {
        "mfaMethod": getattr(client, "_mfa_method", "email"),
        "mfaVerificationCode": mfa_code,
        "rememberMyBrowser": True,
        "reconsentList": [],
        "mfaSetup": False,
    }

    mfa_endpoints = [
        (
            f"{client._sso}/portal/api/mfa/verifyCode",
            client._mfa_portal_web_params,
            client._mfa_portal_web_headers,
        ),
        (
            f"{client._sso}/mobile/api/mfa/verifyCode",
            {
                "clientId": MOBILE_SSO_CLIENT_ID,
                "locale": "en-US",
                "service": MOBILE_SSO_SERVICE_URL,
            },
            client._mfa_portal_web_headers,
        ),
    ]

    # Track the failure type per endpoint so the aggregate exception can match
    # the underlying cause (rate limit vs transport vs verification failure).
    # A pure all-rate-limit aggregate should raise GarminTooManyRequestsError so
    # callers can apply backoff; a pure all-transport aggregate should raise
    # GarminConnectionError so callers don't mistake infrastructure problems for
    # bad credentials. Only actual verification responses (parsed JSON that was
    # not SUCCESSFUL) count as auth failures.
    failures = []
    rate_limited_count = 0
    transport_error_count = 0
    for mfa_url, params, headers in mfa_endpoints:
        _LOGGER.debug("Trying MFA endpoint: %s", mfa_url)
        try:
            r = sess.post(
                mfa_url,
                params=params,
                headers=headers,
                json=mfa_json,
                timeout=30,
            )
        except Exception as e:
            failures.append(f"{mfa_url}: connection error {e}")
            transport_error_count += 1
            continue

        if r.status_code == 429:
            failures.append(f"{mfa_url}: HTTP 429")
            rate_limited_count += 1
            continue

        # A non-2xx with a JSON error body (e.g. HTTP 500 returning {"error":...})
        # is still an infrastructure failure, not a verification failure: Garmin
        # never had a chance to judge the MFA code. Classify as transport before
        # the JSON parse so the aggregate classifier doesn't mis-credit it as an
        # auth failure.
        if not r.ok:
            body_preview = r.text[:200] if r.text else "(empty)"
            failures.append(f"{mfa_url}: HTTP {r.status_code}: {body_preview}")
            transport_error_count += 1
            continue

        try:
            res = r.json()
        except Exception:
            body_preview = r.text[:200] if r.text else "(empty)"
            failures.append(f"{mfa_url}: HTTP {r.status_code} non-JSON: {body_preview}")
            # Non-JSON responses are almost always Cloudflare HTML challenges,
            # which are effectively rate limiting at the infrastructure layer.
            # Count as transport so the aggregate is classified consistently.
            transport_error_count += 1
            continue

        if res.get("error", {}).get("status-code") == "429":
            failures.append(f"{mfa_url}: 429 in JSON body")
            rate_limited_count += 1
            continue

        if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
            ticket = res["serviceTicketId"]
            svc_url = (
                PORTAL_SSO_SERVICE_URL
                if "/portal/" in mfa_url
                else MOBILE_SSO_SERVICE_URL
            )
            client._establish_session(ticket, sess=sess, service_url=svc_url)
            return

        failures.append(f"{mfa_url}: HTTP {r.status_code} => {res}")

    # All endpoints exhausted. Classify the aggregate by precedence: any real
    # verification failure wins (auth); then any rate-limit result (throttle);
    # otherwise the aggregate is purely transport-level.
    aggregate_msg = f"MFA Verification failed on all endpoints: {'; '.join(failures)}"
    total = len(mfa_endpoints)
    auth_failure_count = total - rate_limited_count - transport_error_count
    if auth_failure_count > 0:
        raise GarminAuthenticationError(aggregate_msg)
    if rate_limited_count > 0:
        raise GarminTooManyRequestsError(aggregate_msg)
    raise GarminConnectionError(aggregate_msg)


# ----------------------------------------------------------------------------------------
# MOBILE SSO LOGIN (Android app flow)
# ----------------------------------------------------------------------------------------


def portal_login(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Log in via the mobile SSO API using curl_cffi for TLS impersonation.

    This is the Android Garmin Connect Mobile app flow. Used as a fallback when
    the web portal flows are blocked.

    :param client: GarminClient instance.
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code.
    :param return_on_mfa: If True, return early on MFA challenge.
    :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on MFA.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminAuthenticationError: On bad credentials or unexpected status.
    :raises GarminConnectionError: If curl_cffi is unavailable, on non-OK
        responses, or on non-JSON response bodies.
    """

    if not HAS_CFFI:
        raise GarminConnectionError("curl_cffi not installed; mobile+cffi unavailable")

    sess: Any = cffi_requests.Session(impersonate="safari")

    # Step 1: GET mobile sign-in page (sets SESSION cookies).
    signin_url = f"{client._sso}/mobile/sso/en_US/sign-in"
    get_resp = sess.get(
        signin_url,
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "service": MOBILE_SSO_SERVICE_URL,
        },
        headers={
            "User-Agent": MOBILE_SSO_USER_AGENT,
            "accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "accept-language": "en-US,en;q=0.9",
        },
        timeout=30,
    )
    if get_resp.status_code == 429:
        raise GarminTooManyRequestsError("Mobile portal login GET returned 429")
    if not get_resp.ok:
        raise GarminConnectionError(
            f"Mobile portal login GET failed: HTTP {get_resp.status_code}"
        )

    # Same Cloudflare WAF rate-limiting protection as the browser portal flow.
    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info(
        "Mobile portal login: waiting %.0fs to avoid Cloudflare rate limiting...",
        delay_s,
    )
    time.sleep(delay_s)

    # Step 2: POST credentials.
    login_params = {
        "clientId": MOBILE_SSO_CLIENT_ID,
        "locale": "en-US",
        "service": MOBILE_SSO_SERVICE_URL,
    }
    post_headers = {
        "User-Agent": MOBILE_SSO_USER_AGENT,
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": client._sso,
        "referer": (
            f"{signin_url}?clientId={MOBILE_SSO_CLIENT_ID}"
            f"&service={MOBILE_SSO_SERVICE_URL}"
        ),
    }
    r = sess.post(
        f"{client._sso}/mobile/api/login",
        params=login_params,
        headers=post_headers,
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError("Too many requests during mobile portal login")
    if not r.ok:
        raise GarminConnectionError(
            f"Mobile portal login POST failed: HTTP {r.status_code}"
        )
    try:
        res = r.json()
    except (json.JSONDecodeError, ValueError) as err:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"Mobile portal login returned non-JSON (status {r.status_code}): "
            f"{body_preview!r}"
        ) from err
    resp_type = res.get("responseStatus", {}).get("type")

    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_cffi_session = sess
        client._mfa_cffi_params = login_params
        client._mfa_cffi_headers = post_headers

        if return_on_mfa:
            return "needs_mfa", sess

        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa_portal(client, mfa_code)
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket, sess=sess)
        return None, None

    if resp_type == "INVALID_USERNAME_PASSWORD":
        raise GarminAuthenticationError(
            "401 Unauthorized (Invalid Username or Password)"
        )

    raise GarminAuthenticationError(f"Portal login failed: {res}")


def complete_mfa_portal(client: Any, mfa_code: str) -> None:
    """
    Complete MFA verification for the mobile portal cffi flow.

    :param client: GarminClient instance with cffi MFA state.
    :param mfa_code: 6-digit MFA code.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminConnectionError: On non-2xx response or non-JSON body
        (server/transport error, not a verification failure).
    :raises GarminAuthenticationError: On a parsed non-SUCCESSFUL verification response.
    """

    sess = client._mfa_cffi_session
    r = sess.post(
        f"{client._sso}/mobile/api/mfa/verifyCode",
        params=client._mfa_cffi_params,
        headers=client._mfa_cffi_headers,
        json={
            "mfaMethod": getattr(client, "_mfa_method", "email"),
            "mfaVerificationCode": mfa_code,
            "rememberMyBrowser": True,
            "reconsentList": [],
            "mfaSetup": False,
        },
        timeout=30,
    )
    if r.status_code == 429:
        raise GarminTooManyRequestsError(
            "MFA Verification failed: HTTP 429 Too Many Requests"
        )
    # Non-2xx with any body is a server/transport error; Garmin never had a
    # chance to evaluate the MFA code, so GarminConnectionError is correct here.
    if not r.ok:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"MFA Verification failed: HTTP {r.status_code}: {body_preview}"
        )
    try:
        res = r.json()
    except (json.JSONDecodeError, ValueError) as err:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"MFA Verification failed: invalid JSON response: {body_preview}"
        ) from err
    if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket, sess=sess)
        return
    raise GarminAuthenticationError(f"MFA Verification failed: {res}")


def mobile_login(
    client: Any,
    email: str,
    password: str,
    prompt_mfa: Optional[Callable[[], str]] = None,
    return_on_mfa: bool = False,
) -> Tuple[Optional[str], Any]:
    """
    Log in via the mobile SSO API using plain ``requests``.

    No TLS impersonation. Position 4 of 5 in the strategy chain, ahead of only
    the widget+cffi last-resort. Likely to be 429'd by Cloudflare without TLS
    impersonation, but occasionally succeeds when the cffi-based portal and
    mobile flows have been blocked on the current IP.

    :param client: GarminClient instance.
    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param prompt_mfa: Callable returning a 6-digit MFA code.
    :param return_on_mfa: If True, return early on MFA challenge.
    :return: ``(None, None)`` on success, or ``("needs_mfa", session)`` on MFA.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminAuthenticationError: On bad credentials.
    :raises GarminConnectionError: On non-JSON response.
    """

    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": MOBILE_SSO_USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    get_resp = sess.get(
        f"{client._sso}/mobile/sso/en_US/sign-in",
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "service": MOBILE_SSO_SERVICE_URL,
        },
        timeout=30,
    )
    if get_resp.status_code == 429:
        raise GarminTooManyRequestsError("Mobile login GET returned 429")
    if not get_resp.ok:
        raise GarminConnectionError(
            f"Mobile login GET failed: HTTP {get_resp.status_code}"
        )

    # Same Cloudflare WAF rate-limiting protection as the browser portal flow.
    delay_s = random.uniform(LOGIN_DELAY_MIN_S, LOGIN_DELAY_MAX_S)
    _LOGGER.info(
        "Mobile login: waiting %.0fs to avoid Cloudflare rate limiting...", delay_s
    )
    time.sleep(delay_s)

    r = sess.post(
        f"{client._sso}/mobile/api/login",
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "locale": "en-US",
            "service": MOBILE_SSO_SERVICE_URL,
        },
        json={
            "username": email,
            "password": password,
            "rememberMe": True,
            "captchaToken": "",
        },
        timeout=30,
    )

    if r.status_code == 429:
        raise GarminTooManyRequestsError(
            "Login failed (429 Rate Limit). Try again later."
        )

    # Non-2xx with a parseable JSON error body still means the auth server
    # never validated our credentials, so classify it as infrastructure rather
    # than letting it fall through to the JSON-based "not SUCCESSFUL / not
    # MFA_REQUIRED" branch where it would look like an auth failure.
    if not r.ok:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"Login failed: HTTP {r.status_code}: {body_preview!r}"
        )

    try:
        res = r.json()
    except Exception as err:
        raise GarminConnectionError(
            f"Login failed (Not JSON): HTTP {r.status_code}"
        ) from err

    resp_type = res.get("responseStatus", {}).get("type")

    if resp_type == "MFA_REQUIRED":
        client._mfa_method = res.get("customerMfaInfo", {}).get(
            "mfaLastMethodUsed", "email"
        )
        client._mfa_session = sess

        if return_on_mfa:
            return "needs_mfa", client._mfa_session

        if prompt_mfa:
            mfa_code = prompt_mfa()
            complete_mfa(client, mfa_code)
            return None, None
        raise GarminAuthenticationError(
            "MFA Required but no prompt_mfa mechanism supplied"
        )

    if resp_type == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return None, None

    if "status-code" in res.get("error", {}) and res["error"]["status-code"] == "429":
        raise GarminTooManyRequestsError("429 Rate Limit")

    if resp_type == "INVALID_USERNAME_PASSWORD":
        raise GarminAuthenticationError(
            "401 Unauthorized (Invalid Username or Password)"
        )

    raise GarminAuthenticationError(f"Unhandled Garmin Login JSON, Login failed: {res}")


def complete_mfa(client: Any, mfa_code: str) -> None:
    """
    Complete MFA verification for the plain-requests mobile flow.

    :param client: GarminClient instance with mobile MFA state.
    :param mfa_code: 6-digit MFA code.
    :raises GarminTooManyRequestsError: On HTTP 429.
    :raises GarminConnectionError: On non-2xx response or non-JSON body
        (server/transport error, not a verification failure).
    :raises GarminAuthenticationError: On a parsed non-SUCCESSFUL verification response.
    """

    r = client._mfa_session.post(
        f"{client._sso}/mobile/api/mfa/verifyCode",
        params={
            "clientId": MOBILE_SSO_CLIENT_ID,
            "locale": "en-US",
            "service": MOBILE_SSO_SERVICE_URL,
        },
        json={
            "mfaMethod": getattr(client, "_mfa_method", "email"),
            "mfaVerificationCode": mfa_code,
            "rememberMyBrowser": True,
            "reconsentList": [],
            "mfaSetup": False,
        },
        timeout=30,
    )
    if r.status_code == requests.codes.too_many_requests:
        raise GarminTooManyRequestsError(
            "MFA Verification failed: HTTP 429 Too Many Requests"
        )
    # Non-2xx with any body is a server/transport error; Garmin never had a
    # chance to evaluate the MFA code, so GarminConnectionError is correct here.
    if not r.ok:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"MFA Verification failed: HTTP {r.status_code}: {body_preview}"
        )
    try:
        res = r.json()
    except (json.JSONDecodeError, ValueError) as err:
        body_preview = " ".join((r.text or "").split())[:200]
        raise GarminConnectionError(
            f"MFA Verification failed: invalid JSON response: {body_preview}"
        ) from err
    if res.get("responseStatus", {}).get("type") == "SUCCESSFUL":
        ticket = res["serviceTicketId"]
        client._establish_session(ticket)
        return
    raise GarminAuthenticationError(f"MFA Verification failed: {res}")

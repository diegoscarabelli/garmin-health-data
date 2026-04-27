"""
Optional check for a newer version of garmin-health-data on PyPI.

Called once per CLI invocation. Result is cached for 24h in a small JSON file
under ``~/.cache/garmin-health-data/`` so the network is hit at most once per
day per machine. The check is deliberately defensive: any failure (no network,
PyPI down, malformed response, unreadable cache, version-parse error) is
swallowed silently — a version-check failure must never abort the user's
``garmin`` command.

Users who want to skip the network call entirely can set the environment
variable ``GARMIN_NO_VERSION_CHECK=1`` (e.g. for offline runs or to avoid the
~2s timeout on a slow connection).
"""

import json
import os
import time
from pathlib import Path
from typing import Optional

import click
import requests
from packaging.version import InvalidVersion, Version

from garmin_health_data.__version__ import __version__

PYPI_URL = "https://pypi.org/pypi/garmin-health-data/json"
CACHE_PATH = Path("~/.cache/garmin-health-data/version-check.json").expanduser()
CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours
HTTP_TIMEOUT_SECONDS = 2.0
ENV_DISABLE = "GARMIN_NO_VERSION_CHECK"


def check_for_newer_version() -> None:
    """
    Print an upgrade hint if a newer version is on PyPI; otherwise no-op.

    Safe to call from any CLI entry point: never raises, never blocks for more
    than ``HTTP_TIMEOUT_SECONDS``.
    """
    if os.environ.get(ENV_DISABLE):
        return
    try:
        latest = _get_latest_version()
    except Exception:
        return
    if latest is None:
        return
    try:
        if Version(latest) > Version(__version__):
            click.secho(
                f"💡 garmin-health-data {latest} is available "
                f"(installed: {__version__}). Run "
                f"'pip install --upgrade garmin-health-data' to update.",
                fg="cyan",
            )
    except InvalidVersion:
        # Cached or fetched value is not a parseable PEP 440 version; ignore.
        return


def _get_latest_version() -> Optional[str]:
    """
    Return the latest version string, preferring a fresh cache, falling back to a live
    PyPI fetch (which then refreshes the cache).
    """
    cached = _read_cached()
    if cached is not None:
        return cached
    fetched = _fetch_from_pypi()
    if fetched is not None:
        _write_cache(fetched)
    return fetched


def _read_cached() -> Optional[str]:
    """
    Return the cached latest-version string if the cache is fresh, else None.
    """
    if not CACHE_PATH.exists():
        return None
    try:
        age = time.time() - CACHE_PATH.stat().st_mtime
    except OSError:
        return None
    if age > CACHE_TTL_SECONDS:
        return None
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        latest = payload.get("latest")
        if isinstance(latest, str) and latest:
            return latest
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _write_cache(latest: str) -> None:
    """
    Persist the fetched latest-version string for future invocations.

    Failures are swallowed so a non-writable cache directory never blocks the CLI.
    """
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({"latest": latest, "checked_at": time.time()}, f)
    except OSError:
        return


def _fetch_from_pypi() -> Optional[str]:
    """
    Fetch the latest version from PyPI's JSON API.

    Returns ``None`` on any network or parse error so the caller can no-op
    silently.
    """
    try:
        response = requests.get(PYPI_URL, timeout=HTTP_TIMEOUT_SECONDS)
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        info = response.json().get("info") or {}
        latest = info.get("version")
        if isinstance(latest, str) and latest:
            return latest
    except ValueError:
        return None
    return None

"""
Tests for the optional PyPI version-check helper.
"""

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from garmin_health_data import version_check
from garmin_health_data.__version__ import __version__


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """
    Redirect the cache file to a fresh path under tmp_path so each test starts with no
    cached state.
    """
    cache_path = tmp_path / "version-check.json"
    monkeypatch.setattr(version_check, "CACHE_PATH", cache_path)
    monkeypatch.delenv(version_check.ENV_DISABLE, raising=False)
    return cache_path


def _mock_pypi_response(version: str, status: int = 200) -> MagicMock:
    """
    Build a fake requests Response returning the given PyPI version.
    """
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {"info": {"version": version}}
    return resp


def test_prints_hint_when_newer_version_available(isolated_cache, capsys):
    """
    A newer PyPI version triggers the upgrade hint.
    """
    higher = "999.0.0"
    with patch.object(
        version_check.requests, "get", return_value=_mock_pypi_response(higher)
    ):
        version_check.check_for_newer_version()

    out = capsys.readouterr().out
    assert higher in out
    assert __version__ in out
    assert "pip install --upgrade garmin-health-data" in out


def test_silent_when_installed_is_latest(isolated_cache, capsys):
    """
    No hint when the installed version equals the PyPI latest.
    """
    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response(__version__),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""


def test_silent_when_installed_is_newer_than_pypi(isolated_cache, capsys):
    """
    No hint when running an unreleased dev build newer than PyPI.
    """
    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response("0.0.1"),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""


def test_silent_on_network_error(isolated_cache, capsys):
    """
    A request exception is swallowed without output.
    """
    with patch.object(
        version_check.requests,
        "get",
        side_effect=version_check.requests.ConnectionError("dns down"),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""


def test_silent_on_non_200_response(isolated_cache, capsys):
    """
    A non-200 PyPI response is swallowed without output.
    """
    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response("999.0.0", status=503),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""


def test_disabled_via_env_var(isolated_cache, monkeypatch, capsys):
    """
    Setting GARMIN_NO_VERSION_CHECK=1 skips the network entirely.
    """
    monkeypatch.setenv(version_check.ENV_DISABLE, "1")
    mock_get = MagicMock()
    with patch.object(version_check.requests, "get", mock_get):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""
    mock_get.assert_not_called()


def test_uses_cached_version_when_fresh(isolated_cache, capsys):
    """
    A fresh cache file is used; no network call.
    """
    isolated_cache.write_text(json.dumps({"latest": "999.0.0"}))
    mock_get = MagicMock()
    with patch.object(version_check.requests, "get", mock_get):
        version_check.check_for_newer_version()

    out = capsys.readouterr().out
    assert "999.0.0" in out
    mock_get.assert_not_called()


def test_refreshes_stale_cache(isolated_cache, capsys, monkeypatch):
    """
    A cache older than TTL triggers a fresh PyPI fetch.
    """
    isolated_cache.write_text(json.dumps({"latest": "0.0.1"}))
    # Force the cache to look ancient by patching the TTL to 0.
    monkeypatch.setattr(version_check, "CACHE_TTL_SECONDS", 0)

    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response("999.0.0"),
    ):
        version_check.check_for_newer_version()

    out = capsys.readouterr().out
    assert "999.0.0" in out
    # Cache was rewritten with the fresh value.
    assert json.loads(isolated_cache.read_text())["latest"] == "999.0.0"


def test_refreshes_when_mtime_is_in_the_future(isolated_cache, capsys, monkeypatch):
    """
    On Windows, NTFS mtime resolution is finer than ``time.time()`` so a file written
    immediately before the check can have an mtime slightly *after* the current clock.

    The cache must still be considered stale at TTL=0 rather than returning the cached
    value because age is negative.
    """
    isolated_cache.write_text(json.dumps({"latest": "0.0.1"}))
    # Force the cache file's mtime 1 second into the future to deterministically
    # simulate the Windows clock-resolution race.
    future = time.time() + 1.0
    os.utime(isolated_cache, (future, future))
    monkeypatch.setattr(version_check, "CACHE_TTL_SECONDS", 0)

    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response("999.0.0"),
    ):
        version_check.check_for_newer_version()

    assert "999.0.0" in capsys.readouterr().out


def test_silent_on_malformed_cache(isolated_cache, capsys):
    """
    A corrupted cache file falls back to a live PyPI fetch.
    """
    isolated_cache.write_text("not valid json {{{")
    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response(__version__),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""


def test_silent_on_invalid_pypi_version_string(isolated_cache, capsys):
    """
    A garbage version string from PyPI is rejected silently.
    """
    with patch.object(
        version_check.requests,
        "get",
        return_value=_mock_pypi_response("not-a-version"),
    ):
        version_check.check_for_newer_version()

    assert capsys.readouterr().out == ""

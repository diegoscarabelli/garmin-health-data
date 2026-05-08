"""
Version information for garmin-health-data.

Single source of truth: ``[project].version`` in ``pyproject.toml``.
Read back at runtime via ``importlib.metadata`` so the value can never
drift from what was packaged into the installed distribution.

Falls back to a clearly-broken sentinel when the package isn't installed
(e.g. a source checkout run without ``pip install -e .``), making the
"version unknown" case obvious instead of silently reporting a stale
literal.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("garmin-health-data")
except PackageNotFoundError:
    __version__ = "0.0.0+uninstalled"

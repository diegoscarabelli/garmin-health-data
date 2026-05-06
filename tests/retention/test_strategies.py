"""
Tests for ``garmin_health_data.retention.strategies``.
"""

import pytest

from garmin_health_data.retention import strategies
from garmin_health_data.retention.strategies import (
    EXPLICIT_OVERRIDES,
    Strategy,
    format_strategy_table,
    strategy_for,
)


# --------------------------------------------------------------------------- #
# strategy_for: per-branch resolution.
# --------------------------------------------------------------------------- #


def test_strategy_for_distance_registry():
    """
    ``distance`` is in the explicit registry and resolves to ``LAST``.
    """
    assert strategy_for("distance") is Strategy.LAST


def test_strategy_for_accumulated_power_registry():
    """
    ``accumulated_power`` is in the explicit registry and resolves to ``LAST``.
    """
    assert strategy_for("accumulated_power") is Strategy.LAST


def test_strategy_for_position_lat_heuristic():
    """
    ``position_lat`` matches the ``position_*`` heuristic and resolves to ``SKIP``.
    """
    assert strategy_for("position_lat") is Strategy.SKIP


def test_strategy_for_position_long_heuristic():
    """
    ``position_long`` matches the ``position_*`` heuristic and resolves to ``SKIP``.
    """
    assert strategy_for("position_long") is Strategy.SKIP


def test_strategy_for_accumulated_speed_heuristic():
    """
    ``accumulated_speed`` is not in the registry but matches the ``accumulated_*``
    heuristic and resolves to ``LAST``.
    """
    assert "accumulated_speed" not in EXPLICIT_OVERRIDES
    assert strategy_for("accumulated_speed") is Strategy.LAST


def test_strategy_for_total_calories_heuristic():
    """
    ``total_calories`` matches the ``total_*`` heuristic and resolves to ``LAST``.
    """
    assert strategy_for("total_calories") is Strategy.LAST


def test_strategy_for_heart_rate_default():
    """
    ``heart_rate`` falls through to the default ``AGGREGATE``.
    """
    assert strategy_for("heart_rate") is Strategy.AGGREGATE


def test_strategy_for_power_default():
    """
    ``power`` falls through to the default ``AGGREGATE``.
    """
    assert strategy_for("power") is Strategy.AGGREGATE


def test_strategy_for_future_unknown_metric_default():
    """
    An unknown metric name falls through to the default ``AGGREGATE``.
    """
    assert strategy_for("future_unknown_metric") is Strategy.AGGREGATE


def test_strategy_for_empty_string_default():
    """
    The empty string matches no registry entry or heuristic and resolves to the default
    ``AGGREGATE``.
    """
    assert strategy_for("") is Strategy.AGGREGATE


# --------------------------------------------------------------------------- #
# Registry-vs-heuristic precedence.
# --------------------------------------------------------------------------- #


def test_registry_beats_heuristic(monkeypatch):
    """
    When a metric name is present in both the registry and would match a prefix
    heuristic, the registry wins.

    Uses ``position_lat`` as the synthetic case: by default it resolves via the
    ``position_*`` heuristic to ``SKIP``, but if the registry maps it to ``AGGREGATE``
    the registry must take precedence.
    """
    # Sanity check: heuristic path resolves to SKIP without the override.
    assert strategy_for("position_lat") is Strategy.SKIP

    patched = dict(EXPLICIT_OVERRIDES)
    patched["position_lat"] = Strategy.AGGREGATE
    monkeypatch.setattr(strategies, "EXPLICIT_OVERRIDES", patched)

    assert strategy_for("position_lat") is Strategy.AGGREGATE


# --------------------------------------------------------------------------- #
# format_strategy_table.
# --------------------------------------------------------------------------- #


def test_format_strategy_table_empty_returns_empty_string():
    """
    An empty input yields an empty string (no header).
    """
    assert format_strategy_table([]) == ""


def test_format_strategy_table_mixed_sorted_output():
    """
    Mixed metric names produce sorted rows with the expected header, separator, and per-
    row source labels.
    """
    metrics = [
        "heart_rate",
        "distance",
        "position_lat",
        "position_long",
        "accumulated_power",
        "total_strokes",
        "cadence",
    ]
    output = format_strategy_table(metrics)
    lines = output.splitlines()

    # Header + separator + one row per unique metric.
    assert len(lines) == 2 + len(set(metrics))

    # Header column labels are present.
    assert "Metric" in lines[0]
    assert "Strategy" in lines[0]
    assert "Source" in lines[0]

    # Separator row consists only of dashes and spaces.
    assert set(lines[1]) <= {"-", " "}

    # Data rows are sorted by metric name.
    data_rows = lines[2:]
    metric_first_tokens = [row.split()[0] for row in data_rows]
    assert metric_first_tokens == sorted(set(metrics))


def test_format_strategy_table_source_labels():
    """
    Each known source label appears exactly where expected.
    """
    metrics = [
        "distance",  # registry.
        "position_lat",  # heuristic: position_*.
        "accumulated_speed",  # heuristic: accumulated_*.
        "total_calories",  # heuristic: total_*.
        "heart_rate",  # default.
    ]
    output = format_strategy_table(metrics)
    lines = output.splitlines()
    rows_by_metric = {row.split()[0]: row for row in lines[2:]}

    assert rows_by_metric["distance"].endswith("registry")
    assert "LAST" in rows_by_metric["distance"]

    assert rows_by_metric["position_lat"].endswith("heuristic: position_*")
    assert "SKIP" in rows_by_metric["position_lat"]

    assert rows_by_metric["accumulated_speed"].endswith("heuristic: accumulated_*")
    assert "LAST" in rows_by_metric["accumulated_speed"]

    assert rows_by_metric["total_calories"].endswith("heuristic: total_*")
    assert "LAST" in rows_by_metric["total_calories"]

    assert rows_by_metric["heart_rate"].endswith("default")
    assert "AGGREGATE" in rows_by_metric["heart_rate"]


def test_format_strategy_table_deduplicates_input():
    """
    Repeated metric names in the input collapse to a single row.
    """
    output = format_strategy_table(["heart_rate", "heart_rate", "heart_rate"])
    lines = output.splitlines()
    # Header + separator + a single data row.
    assert len(lines) == 3
    assert lines[2].split()[0] == "heart_rate"


@pytest.mark.parametrize(
    "name, expected_strategy",
    [
        ("distance", Strategy.LAST),
        ("accumulated_power", Strategy.LAST),
        ("position_lat", Strategy.SKIP),
        ("accumulated_speed", Strategy.LAST),
        ("total_calories", Strategy.LAST),
        ("heart_rate", Strategy.AGGREGATE),
        ("", Strategy.AGGREGATE),
    ],
)
def test_strategy_for_parametrized(name, expected_strategy):
    """
    Parametrized smoke check covering one example per resolution branch.
    """
    assert strategy_for(name) is expected_strategy

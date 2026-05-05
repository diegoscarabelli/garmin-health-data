"""
Per-metric downsample strategy resolution for the ``activity_ts_metric`` table.

Each metric name extracted from FIT files is classified into one of three
downsample strategies that the retention ``downsample`` command uses to build
the per-metric aggregation SQL:

- ``AGGREGATE``: emit average, minimum, and maximum within each bucket.
- ``LAST``: keep the last sample within each bucket (cumulative metrics).
- ``SKIP``: do not downsample (e.g., GPS coordinates that cannot be averaged).

Resolution proceeds in three steps: explicit registry overrides first, then a
prefix heuristic for known cumulative or non-aggregatable name patterns, then
``AGGREGATE`` as the default fallback.
"""

import enum
from typing import Iterable, Tuple


class Strategy(enum.Enum):
    """
    Downsample strategy applied to an ``activity_ts_metric`` series.
    """

    AGGREGATE = "aggregate"
    LAST = "last"
    SKIP = "skip"


# Explicit per-metric overrides observed in real databases. Any metric listed
# here bypasses the prefix heuristic and uses the mapped strategy directly.
EXPLICIT_OVERRIDES: dict[str, Strategy] = {
    "distance": Strategy.LAST,
    "accumulated_power": Strategy.LAST,
}


# Prefix heuristic, evaluated in order. Each entry is ``(prefix, strategy)``.
# The first matching prefix wins. Order matters only if prefixes overlap, which
# they currently do not.
_PREFIX_HEURISTIC: Tuple[Tuple[str, Strategy], ...] = (
    ("position_", Strategy.SKIP),
    ("accumulated_", Strategy.LAST),
    ("total_", Strategy.LAST),
)


# Cap for the metric name column width in :func:`format_strategy_table`. Names
# longer than this are still rendered (the column simply widens for that row),
# but the default padding never expands beyond this value.
_METRIC_COLUMN_CAP: int = 40


def strategy_for(name: str) -> Strategy:
    """
    Resolve the downsample strategy for a given metric name.

    Resolution order:

    1. Explicit overrides from :data:`EXPLICIT_OVERRIDES`.
    2. Prefix heuristic (``position_*`` -> ``SKIP``, ``accumulated_*`` ->
       ``LAST``, ``total_*`` -> ``LAST``).
    3. Default: :attr:`Strategy.AGGREGATE`.

    :param name: Metric name as stored in ``activity_ts_metric.metric_name``.
    :return: The resolved :class:`Strategy` for the metric.
    """
    if name in EXPLICIT_OVERRIDES:
        return EXPLICIT_OVERRIDES[name]
    for prefix, strategy in _PREFIX_HEURISTIC:
        if name.startswith(prefix):
            return strategy
    return Strategy.AGGREGATE


def _source_for(name: str) -> str:
    """
    Describe how :func:`strategy_for` would resolve ``name``.

    Returns a short string suitable for the ``Source`` column in
    :func:`format_strategy_table`: ``"registry"`` for explicit overrides, ``"heuristic:
    <prefix>*"`` for prefix matches, and ``"default"`` otherwise.

    :param name: Metric name to classify.
    :return: Source label describing the resolution path.
    """
    if name in EXPLICIT_OVERRIDES:
        return "registry"
    for prefix, _strategy in _PREFIX_HEURISTIC:
        if name.startswith(prefix):
            return f"heuristic: {prefix}*"
    return "default"


def format_strategy_table(metric_names: Iterable[str]) -> str:
    """
    Build a printable table mapping metric name to strategy and source.

    Rows are sorted by metric name for stable output. The ``Source`` column
    indicates which branch resolved the strategy: ``registry`` for an
    :data:`EXPLICIT_OVERRIDES` hit, ``heuristic: <prefix>*`` for a prefix
    match, or ``default`` for the :attr:`Strategy.AGGREGATE` fallback.

    Returns the empty string when ``metric_names`` is empty so callers can
    short-circuit "nothing to print" cases without rendering a bare header.

    :param metric_names: Iterable of metric names to classify.
    :return: Multi-line string suitable for ``click.echo``.
    """
    names = sorted(set(metric_names))
    if not names:
        return ""

    metric_header = "Metric"
    strategy_header = "Strategy"
    source_header = "Source"

    # Compute column widths. The metric column is capped at
    # ``_METRIC_COLUMN_CAP`` for the default padding; longer names still
    # render in full and naturally widen that row.
    metric_width = min(
        max(len(metric_header), max(len(n) for n in names)),
        _METRIC_COLUMN_CAP,
    )
    strategy_width = max(
        len(strategy_header),
        max(len(s.name) for s in Strategy),
    )

    lines = []
    header = (
        f"{metric_header:<{metric_width}} "
        f"{strategy_header:<{strategy_width}} "
        f"{source_header}"
    )
    separator = (
        f"{'-' * metric_width} "
        f"{'-' * strategy_width} "
        f"{'-' * len(source_header)}"
    )
    lines.append(header)
    lines.append(separator)
    for name in names:
        strategy = strategy_for(name)
        source = _source_for(name)
        lines.append(
            f"{name:<{metric_width}} " f"{strategy.name:<{strategy_width}} " f"{source}"
        )
    return "\n".join(lines)

"""
Retention helpers for the activity_ts_metric table.

Submodules:

- ``parsers``: Click param types (``TimeGrain``, ``Duration``) and the
  ``resolve_range`` helper that mirrors the ``extract`` command's date-range
  semantics (start inclusive, end exclusive, with the same-day special case).
- ``strategies``: per-metric downsample strategy registry plus the prefix-based
  heuristic used for unknown metric names.
- ``operations``: ``prune_ts_metrics``, ``downsample_activities``, and
  ``migrate_cascade`` SQL helpers.
"""

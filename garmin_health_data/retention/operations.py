"""
Retention operations for the garmin-health-data SQLite database.

Public entry points:

- :func:`migrate_cascade` retrofits ``ON DELETE CASCADE`` onto the 16 child foreign
  keys that were declared without an explicit FK action in older versions of
  ``tables.ddl``. SQLite has no ``ALTER TABLE`` mechanism for changing FK actions,
  so each affected child table is rebuilt via the standard recreate dance.
- :func:`prune_ts_metrics` deletes rows from ``activity_ts_metric`` whose parent
  activity ``start_ts`` falls in a half-open ``[start, end)`` range matching the
  ``extract`` command's date conventions (with the same-day special case).
- :func:`downsample_activities` aggregates ``activity_ts_metric`` rows into
  per-bucket records in ``activity_ts_metric_downsampled``, with activity-level
  replace semantics (target activities have their existing downsampled rows wiped
  and rewritten in a single transaction; activities with no source rows are
  excluded from the replace set so their pre-existing buckets are preserved).
"""

import re
import shutil
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from garmin_health_data.retention.parsers import resolve_range
from garmin_health_data.retention.strategies import Strategy, strategy_for


def _ensure_schema_current(db_path: Path) -> None:
    """
    Create any 2.8-new tables on a database that predates them.

    Only the ``activity_ts_metric_downsampled`` table is new in 2.8; older tables and
    indexes already exist in any database that has been touched by this codebase. We
    deliberately do NOT re-run the full DDL here: indexes in the full DDL reference
    columns on parent tables, and pre-2.8 fixtures that strip those parent tables down
    (e.g., in unit tests or hand-edited databases) would fail with ``no such column``.
    Materializing only the genuinely-new table keeps this helper safe to call from any
    retention entry point on any vintage of database.

    The CREATE statement is sourced from ``tables.ddl`` (single source of truth) and
    runs idempotently via ``CREATE TABLE IF NOT EXISTS``.

    :param db_path: Path to the SQLite database file.
    :raises RuntimeError: when the schema source is missing the new table definition
        (would indicate a packaging bug).
    """
    new_table_ddl = _extract_table_ddl(
        _load_ddl_text(), "activity_ts_metric_downsampled"
    )
    if new_table_ddl is None:
        raise RuntimeError(
            "tables.ddl does not contain CREATE TABLE for "
            "activity_ts_metric_downsampled; schema source is broken."
        )
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(new_table_ddl)
        conn.commit()
    finally:
        conn.close()


# Handle importlib.resources for different Python versions, mirroring the
# pattern in :mod:`garmin_health_data.db`.
if sys.version_info >= (3, 9):
    from importlib.resources import files
else:
    from importlib_resources import files


# Tables whose FK to the parent gained ``ON DELETE CASCADE`` in this
# release, paired with the parent column they reference. Order is purely
# cosmetic: it controls the order entries appear in the summary lists.
_MIGRATION_TARGETS: Tuple[Tuple[str, str], ...] = (
    # Activity-children referencing activity.activity_id.
    ("swimming_agg_metrics", "activity"),
    ("cycling_agg_metrics", "activity"),
    ("running_agg_metrics", "activity"),
    ("supplemental_activity_metric", "activity"),
    ("activity_ts_metric", "activity"),
    ("activity_split_metric", "activity"),
    ("activity_lap_metric", "activity"),
    ("activity_path", "activity"),
    ("strength_exercise", "activity"),
    ("strength_set", "activity"),
    # Sleep-children referencing sleep.sleep_id.
    ("sleep_level", "sleep"),
    ("sleep_movement", "sleep"),
    ("sleep_restless_moment", "sleep"),
    ("spo2", "sleep"),
    ("hrv", "sleep"),
    ("breathing_disruption", "sleep"),
)


def _load_ddl_text() -> str:
    """
    Load the packaged ``tables.ddl`` resource as text.

    Mirrors the resource-loading fallback used by
    :func:`garmin_health_data.db.create_tables` so the same code path works for
    installed packages and editable/development checkouts.

    :return: Full DDL file contents.
    :raises FileNotFoundError: when the DDL resource cannot be located.
    """
    try:
        return files("garmin_health_data").joinpath("tables.ddl").read_text()
    except (FileNotFoundError, TypeError):
        ddl_file = Path(__file__).resolve().parent.parent / "tables.ddl"
        if not ddl_file.exists():
            raise FileNotFoundError(f"Schema DDL file not found: {ddl_file}")
        return ddl_file.read_text()


def _extract_table_ddl(ddl_text: str, table: str) -> Optional[str]:
    """
    Extract the ``CREATE TABLE IF NOT EXISTS <table>`` block from the DDL text.

    The match is non-greedy and terminates at the first line that ends with ``);`` after
    the opening parenthesis, which matches the convention used in ``tables.ddl``.

    :param ddl_text: Full text of ``tables.ddl``.
    :param table: Bare table name (no schema prefix).
    :return: The full ``CREATE TABLE`` statement including the trailing semicolon, or
        ``None`` if the table is not declared in the DDL.
    """
    pattern = rf"CREATE TABLE IF NOT EXISTS {re.escape(table)} \(" r"(?:.|\n)*?\n\);"
    match = re.search(pattern, ddl_text)
    return match.group(0) if match else None


def _extract_indexes_ddl(ddl_text: str, table: str) -> List[str]:
    """
    Extract every ``CREATE INDEX`` statement that targets ``<table>``.

    Both unique and non-unique indexes are captured. ``WHERE`` clauses on partial
    indexes are preserved, as is multi-line formatting.

    :param ddl_text: Full text of ``tables.ddl``.
    :param table: Bare table name to filter on.
    :return: List of full index statements (each ending with ``;``) in the order they
        appear in the DDL. Empty list when no indexes exist.
    """
    pattern = (
        r"CREATE(?: UNIQUE)? INDEX IF NOT EXISTS [a-zA-Z0-9_]+\s+"
        rf"ON {re.escape(table)}\s*\((?:.|\n)*?\)"
        r"(?:\s+WHERE[^;]*)?;"
    )
    return re.findall(pattern, ddl_text)


def _table_has_cascade(conn: sqlite3.Connection, table: str, parent: str) -> bool:
    """
    Return ``True`` when the existing CREATE TABLE includes cascade for ``parent``.

    Reads ``sqlite_master.sql`` for the table and looks for an FK clause that references
    ``<parent>`` and includes ``ON DELETE CASCADE``. The check is case-insensitive and
    tolerates whitespace variations.

    :param conn: Open SQLite connection.
    :param table: Child table to inspect.
    :param parent: Parent table referenced by the FK we care about.
    :return: True when cascade is already declared, False otherwise. Also returns False
        when the table does not exist (nothing to migrate).
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if row is None or row[0] is None:
        return False
    sql = row[0]

    # Find the FK clause referencing ``parent`` and check whether it
    # includes ``ON DELETE CASCADE`` before the next clause boundary.
    fk_pattern = (
        r"FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+"
        rf"{re.escape(parent)}\s*\([^)]+\)([^,)]*)"
    )
    for match in re.finditer(fk_pattern, sql, flags=re.IGNORECASE):
        tail = match.group(1)
        if re.search(r"ON\s+DELETE\s+CASCADE", tail, flags=re.IGNORECASE):
            return True
    return False


def _orphan_check(conn: sqlite3.Connection) -> List[Tuple]:
    """
    Run ``PRAGMA foreign_key_check`` and return offending rows.

    The pragma yields one row per orphan ``(table, rowid, parent, fkid)``. A successful
    check returns an empty list.

    :param conn: Open SQLite connection.
    :return: List of orphan tuples; empty when the database is clean.
    """
    return list(conn.execute("PRAGMA foreign_key_check").fetchall())


def _backup_db(db_path: Path) -> Path:
    """
    Copy the database file to ``<db_path>.bak.<ISO timestamp>``.

    Uses :func:`shutil.copy2` to preserve metadata (mtime/atime, mode). The timestamp is
    UTC and includes microseconds so concurrent calls in the same second remain
    distinct.

    :param db_path: Path to the live database file.
    :return: Path to the freshly created backup file.
    """
    # Colons are illegal in filenames on some platforms, so swap them out
    # of the ISO timestamp. Microseconds keep adjacent calls disjoint.
    stamp = (
        datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(":", "-")
    )
    backup = db_path.with_name(f"{db_path.name}.bak.{stamp}")
    shutil.copy2(db_path, backup)
    return backup


def _migrate_one_table(
    conn: sqlite3.Connection,
    table: str,
    new_create: str,
    indexes: List[str],
) -> None:
    """
    Recreate one child table with cascade via the SQLite 12-step dance.

    The operation runs inside a single transaction with foreign-key
    enforcement temporarily disabled (per SQLite's recommended recipe):

    1. ``ALTER TABLE <table> RENAME TO <table>__migrate_old``.
    2. Execute the new ``CREATE TABLE`` statement.
    3. ``INSERT INTO <table> SELECT * FROM <table>__migrate_old``.
    4. ``DROP TABLE <table>__migrate_old`` (also drops its indexes).
    5. Recreate every index from the new DDL.

    :param conn: Open SQLite connection. Must already have foreign-key
        enforcement disabled by the caller.
    :param table: Child table to rebuild.
    :param new_create: Full ``CREATE TABLE`` statement from the new DDL.
    :param indexes: Index statements to recreate after the table swap.
    """
    old_name = f"{table}__migrate_old"
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        cur.execute(f"ALTER TABLE {table} RENAME TO {old_name}")
        # NEVER use cur.executescript() here. executescript() issues an
        # implicit COMMIT before running and ignores the surrounding BEGIN,
        # which would leave the rename committed even when the subsequent
        # CREATE/INSERT/DROP fails. We use cur.execute() per statement so
        # the explicit BEGIN/COMMIT actually wraps the whole 12-step dance.
        # The DDL extracted from tables.ddl is a single CREATE TABLE
        # statement and the index extracts are single CREATE INDEX
        # statements, so cur.execute() suffices for all of them; if the
        # extractor ever returned multi-statement strings, the assertion
        # below would surface that mismatch loudly rather than silently
        # break atomicity again.
        assert ";" not in new_create.rstrip().rstrip(";"), (
            "_extract_table_ddl returned a multi-statement string; "
            "_migrate_one_table would silently lose transactional safety. "
            "See operations.py for the executescript footgun explanation."
        )
        cur.execute(new_create.rstrip().rstrip(";"))
        # Column lists are identical between old and new, so a positional
        # ``SELECT *`` copy is safe.
        cur.execute(f"INSERT INTO {table} SELECT * FROM {old_name}")
        cur.execute(f"DROP TABLE {old_name}")
        for index_sql in indexes:
            cur.execute(index_sql.rstrip().rstrip(";"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def migrate_cascade(
    db_path: str,
    *,
    dry_run: bool = False,
    backup: bool = True,
) -> dict[str, Any]:
    """
    Retrofit ON DELETE CASCADE onto child FKs in an existing database.

    SQLite has no ALTER TABLE for changing FK actions, so each child table
    that is missing cascade is recreated via the standard 12-step dance:
    rename -> CREATE NEW with cascade -> INSERT SELECT -> DROP OLD ->
    recreate indexes. The whole sequence runs inside a transaction per
    table.

    :param db_path: Path to the SQLite database file.
    :param dry_run: When True, plan and report without modifying the
        database.
    :param backup: When True (default), copy the database to
        ``<db_path>.bak.<ISO timestamp>`` before any writes.
    :return: Summary dict with keys:

        - ``"migrated"``: list of table names that were rebuilt with
          cascade.
        - ``"skipped"``: list of table names that already had cascade
          (idempotent).
        - ``"backup_path"``: str path to backup file or None when
          ``backup=False`` or ``dry_run=True``.
        - ``"dry_run"``: bool echoing the input.
    :raises RuntimeError: when pre-flight ``PRAGMA foreign_key_check``
        finds existing orphan rows (refusing to migrate a corrupted DB).
    :raises FileNotFoundError: when ``db_path`` does not exist.
    """
    db_file = Path(db_path).expanduser().resolve()
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")

    ddl_text = _load_ddl_text()

    # First pass: open with FK enforcement enabled, run the orphan check,
    # and classify each target as migrate/skip. We do not modify anything
    # in this pass.
    migrated: List[str] = []
    skipped: List[str] = []
    plans: List[Tuple[str, str, List[str]]] = []

    conn = sqlite3.connect(str(db_file))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        orphans = _orphan_check(conn)
        if orphans:
            raise RuntimeError(
                "PRAGMA foreign_key_check reported orphan rows; "
                "refusing to migrate a database with existing FK "
                f"violations. Offending rows: {orphans!r}."
            )

        for table, parent in _MIGRATION_TARGETS:
            # Tables missing entirely from this database are simply
            # skipped; ``create_tables`` will add them on the next init.
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if exists is None:
                skipped.append(table)
                continue

            if _table_has_cascade(conn, table, parent):
                skipped.append(table)
                continue

            new_create = _extract_table_ddl(ddl_text, table)
            if new_create is None:
                # Should never happen for our 16 hard-coded targets, but
                # be defensive: treat as skipped rather than crash.
                skipped.append(table)
                continue
            indexes = _extract_indexes_ddl(ddl_text, table)
            plans.append((table, new_create, indexes))
            migrated.append(table)
    finally:
        conn.close()

    backup_path: Optional[str] = None

    if dry_run or not plans:
        # Dry-run never writes, and a no-op (everything already cascade)
        # also skips backup creation.
        return {
            "migrated": migrated if not dry_run else migrated,
            "skipped": skipped,
            "backup_path": backup_path,
            "dry_run": dry_run,
        }

    if backup:
        backup_path = str(_backup_db(db_file))

    # Second pass: actually rebuild each planned table. Open a fresh
    # connection with FK enforcement disabled so the transient
    # ``__migrate_old`` rename does not trigger constraint checks during
    # the swap.
    conn = sqlite3.connect(str(db_file))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        for table, new_create, indexes in plans:
            _migrate_one_table(conn, table, new_create, indexes)
        # Re-enable enforcement on this connection before closing so a
        # follow-up ``foreign_key_check`` sees the post-migration state.
        conn.execute("PRAGMA foreign_keys = ON")
        post_orphans = _orphan_check(conn)
        if post_orphans:
            # The migration itself should never produce orphans (we copy
            # rows verbatim), but a hard sanity check is cheap and gives
            # the operator a clear signal if something went wrong.
            raise RuntimeError(
                "Post-migration foreign_key_check reported orphans: "
                f"{post_orphans!r}."
            )
    finally:
        conn.close()

    # Bring the schema fully up to date so any new-in-this-release tables
    # (e.g. activity_ts_metric_downsampled) exist after the migration. This
    # only runs on a real (non-dry-run) migration; the dry-run path returns
    # earlier and leaves the database untouched. Without this, a user could
    # follow `garmin migrate-cascade` with `garmin downsample` and hit
    # `no such table` until some unrelated extract bootstrapped the schema.
    _ensure_schema_current(db_file)

    return {
        "migrated": migrated,
        "skipped": skipped,
        "backup_path": backup_path,
        "dry_run": dry_run,
    }


def _open_with_fk(db_path: Path) -> sqlite3.Connection:
    """
    Open a SQLite connection with foreign-key enforcement enabled.

    The connection is opened with ``isolation_level=None`` (autocommit mode) so
    that DML executed for setup (creating/populating the temp scoping table)
    does not start an implicit transaction that would collide with the explicit
    ``BEGIN`` we issue around the destructive DELETE/INSERT block.

    :param db_path: Path to the SQLite database file.
    :return: Open connection in autocommit mode with foreign keys enabled.
    """
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _populate_target_activities(
    conn: sqlite3.Connection,
    start_dt: datetime,
    end_dt: datetime,
    user_ids: Optional[Sequence[int]],
    *,
    require_source: bool,
) -> int:
    """
    Build a temp table ``_tmp_target_activities(activity_id)`` with activities in scope.

    Using a temp table sidesteps SQLite's parameter limit on large ``IN (...)`` lists
    and lets every downstream query share a clean ``JOIN _tmp_target_activities``.

    :param conn: Open SQLite connection. The caller owns the transaction lifecycle.
    :param start_dt: Inclusive start datetime (UTC, naive midnight).
    :param end_dt: Exclusive end datetime (UTC, naive midnight).
    :param user_ids: Optional iterable of user_ids to scope to. ``None`` means all users
        in range.
    :param require_source: When True, restrict to activities with at least one row in
        ``activity_ts_metric`` (the downsample replace-set rule). When False, include
        every in-range activity (the prune scope).
    :return: Number of rows inserted into the temp table.
    """
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _tmp_target_activities ("
        "activity_id INTEGER PRIMARY KEY)"
    )
    conn.execute("DELETE FROM _tmp_target_activities")

    user_clause = ""
    params: List[Any] = [start_dt.isoformat(sep=" "), end_dt.isoformat(sep=" ")]
    if user_ids:
        user_ids = list(user_ids)
        placeholders = ",".join("?" * len(user_ids))
        user_clause = f" AND a.user_id IN ({placeholders})"
        params.extend(user_ids)

    if require_source:
        # Intersection with activity_ts_metric: only activities that still have
        # source rows are downsampled. Activities whose source was pruned are
        # excluded entirely so their pre-existing downsampled rows survive.
        select_sql = (
            "INSERT INTO _tmp_target_activities (activity_id) "
            "SELECT DISTINCT a.activity_id FROM activity a "
            "WHERE a.start_ts >= ? AND a.start_ts < ?" + user_clause + " AND EXISTS ("
            "SELECT 1 FROM activity_ts_metric m WHERE m.activity_id = a.activity_id"
            ")"
        )
    else:
        select_sql = (
            "INSERT INTO _tmp_target_activities (activity_id) "
            "SELECT a.activity_id FROM activity a "
            "WHERE a.start_ts >= ? AND a.start_ts < ?" + user_clause
        )

    cur = conn.execute(select_sql, params)
    return cur.rowcount


def _drop_target_temp(conn: sqlite3.Connection) -> None:
    """
    Drop the temp table used for activity scoping if it exists.

    :param conn: Open SQLite connection.
    """
    conn.execute("DROP TABLE IF EXISTS _tmp_target_activities")


def prune_ts_metrics(
    db_path: str,
    *,
    end: date,
    start: Optional[date] = None,
    user_ids: Optional[Iterable[int]] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Delete rows from ``activity_ts_metric`` for activities whose ``start_ts`` is in
    range.

    Range semantics match the ``extract`` command exactly: ``end`` is required and
    exclusive, ``start`` is optional (``None`` means ``-infinity``) and inclusive,
    with the same-day special case (``start == end`` includes that single day).
    See :func:`garmin_health_data.retention.parsers.resolve_range`.

    Does NOT touch ``activity``, ``activity_ts_metric_downsampled``, splits, laps,
    paths, sport-specific aggregates, or any other table. Only the high-volume
    per-second source rows are removed.

    :param db_path: Path to the SQLite database file.
    :param end: Required exclusive end date.
    :param start: Optional inclusive start date. ``None`` means everything before
        ``end``.
    :param user_ids: Optional iterable of user_ids to scope to. ``None`` means
        every user.
    :param dry_run: When True, count matching rows without deleting.
    :return: Summary dict with keys ``"activity_count"`` (number of in-range
        activities), ``"rows_affected"`` (rows deleted, or counted when dry_run),
        and ``"dry_run"`` (echo of the input).
    :raises FileNotFoundError: when ``db_path`` does not exist.
    """
    db_file = Path(db_path).expanduser().resolve()
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")

    _ensure_schema_current(db_file)

    start_dt, end_dt = resolve_range(start, end)
    user_ids_list: Optional[List[int]] = list(user_ids) if user_ids else None

    conn = _open_with_fk(db_file)
    try:
        activity_count = _populate_target_activities(
            conn,
            start_dt,
            end_dt,
            user_ids_list,
            require_source=False,
        )

        if activity_count == 0:
            return {
                "activity_count": 0,
                "rows_affected": 0,
                "dry_run": dry_run,
            }

        if dry_run:
            count_sql = (
                "SELECT COUNT(*) FROM activity_ts_metric m "
                "WHERE m.activity_id IN ("
                "SELECT activity_id FROM _tmp_target_activities)"
            )
            rows_affected = conn.execute(count_sql).fetchone()[0]
        else:
            cur = conn.cursor()
            cur.execute("BEGIN")
            try:
                delete_sql = (
                    "DELETE FROM activity_ts_metric "
                    "WHERE activity_id IN ("
                    "SELECT activity_id FROM _tmp_target_activities)"
                )
                cur.execute(delete_sql)
                rows_affected = cur.rowcount
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        _drop_target_temp(conn)
        conn.close()

    return {
        "activity_count": activity_count,
        "rows_affected": rows_affected,
        "dry_run": dry_run,
    }


def _bucket_ts_expr() -> str:
    """
    Build the SQL fragment that aligns a source timestamp to its bucket start.

    Uses ``strftime('%s', ...)`` to convert each timestamp to integer Unix seconds
    (truncating any sub-second component, which is irrelevant for bucket grains coarser
    than 1s) and does the bucket math purely in integers, then converts back via
    ``datetime(N, 'unixepoch')``. The julianday-based alternative drifts at minute
    boundaries due to IEEE 754 rounding when multiplying day fractions by 86400, which
    causes off-by-one bucket assignments at exact minute marks. ``strftime`` and
    ``unixepoch`` are available on every SQLite version Python 3.10+ ships with.

    The bucket boundary is anchored to ``a.start_ts`` so buckets never span activity
    boundaries.

    :return: SQL fragment producing a ``DATETIME`` value as ``bucket_ts``.
    """
    # Implicit concatenation, never a triple-quoted string (docformatter would
    # treat that as a docstring and append a period, corrupting the SQL).
    return (
        "datetime("
        "CAST(strftime('%s', a.start_ts) AS INTEGER) + "
        "((CAST(strftime('%s', m.timestamp) AS INTEGER) - "
        "CAST(strftime('%s', a.start_ts) AS INTEGER)) / :grain) * :grain, "
        "'unixepoch')"
    )


def _aggregate_insert_sql(grain: int) -> str:
    """
    Build the INSERT...SELECT for the AGGREGATE strategy (avg + min + max).

    :param grain: Bucket width in seconds; embedded as a literal so the same SQL can be
        reused with named parameters.
    :return: Full SQL statement to ``execute``.
    """
    bucket_ts = _bucket_ts_expr()
    return (
        "INSERT INTO activity_ts_metric_downsampled "
        "(activity_id, bucket_ts, name, bucket_seconds, "
        "value, min_value, max_value, sample_count, units) "
        "SELECT a.activity_id, "
        f"{bucket_ts} AS bucket_ts, "
        "m.name, :grain, "
        "AVG(m.value), MIN(m.value), MAX(m.value), COUNT(*), MIN(m.units) "
        "FROM activity_ts_metric m "
        "JOIN activity a ON a.activity_id = m.activity_id "
        "JOIN _tmp_target_activities t ON t.activity_id = a.activity_id "
        "WHERE m.name = :name "
        "GROUP BY a.activity_id, bucket_ts, m.name"
    )


def _last_insert_sql(grain: int) -> str:
    """
    Build the INSERT...SELECT for the LAST strategy (last-in-bucket).

    Uses a CTE plus ``ROW_NUMBER()`` partitioned by ``(activity_id, name, bucket_ts)``
    ordered by source ``timestamp`` descending, then keeps only rank 1 per bucket.
    ``min_value`` and ``max_value`` are NULL for LAST since a single representative
    value is the whole point of the strategy.

    :param grain: Bucket width in seconds.
    :return: Full SQL statement to ``execute``.
    """
    bucket_ts = _bucket_ts_expr()
    return (
        "WITH bucketed AS ("
        "SELECT a.activity_id, "
        f"{bucket_ts} AS bucket_ts, "
        "m.name, m.value, m.units, m.timestamp, "
        "ROW_NUMBER() OVER ("
        f"PARTITION BY a.activity_id, m.name, {bucket_ts} "
        "ORDER BY m.timestamp DESC) AS rn, "
        "COUNT(*) OVER ("
        f"PARTITION BY a.activity_id, m.name, {bucket_ts}) AS sample_count "
        "FROM activity_ts_metric m "
        "JOIN activity a ON a.activity_id = m.activity_id "
        "JOIN _tmp_target_activities t ON t.activity_id = a.activity_id "
        "WHERE m.name = :name"
        ") "
        "INSERT INTO activity_ts_metric_downsampled "
        "(activity_id, bucket_ts, name, bucket_seconds, "
        "value, min_value, max_value, sample_count, units) "
        "SELECT activity_id, bucket_ts, name, :grain, "
        "value, NULL, NULL, sample_count, units "
        "FROM bucketed WHERE rn = 1"
    )


def downsample_activities(
    db_path: str,
    *,
    time_grain_seconds: int,
    end: date,
    start: Optional[date] = None,
    user_ids: Optional[Iterable[int]] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Aggregate ``activity_ts_metric`` rows into per-bucket records.

    Activity-level replace semantics: the set of activities affected is the
    intersection of (a) activities whose ``start_ts`` is in range and (b)
    activities with at least one source row in ``activity_ts_metric``. For each
    such activity, all of its existing rows in ``activity_ts_metric_downsampled``
    are deleted before the freshly computed buckets are inserted; activities
    whose source was previously pruned are excluded from the replace set so
    their pre-existing downsampled buckets survive untouched.

    The whole DELETE + INSERT(s) sequence runs inside a single transaction.
    Bucket alignment is activity-start-relative, so buckets never span activity
    boundaries.

    :param db_path: Path to the SQLite database file.
    :param time_grain_seconds: Bucket width in seconds (must be > 0). Embedded
        as ``bucket_seconds`` on every emitted row.
    :param end: Required exclusive end date.
    :param start: Optional inclusive start date. ``None`` means everything
        before ``end``.
    :param user_ids: Optional iterable of user_ids to scope to. ``None`` means
        every user.
    :param dry_run: When True, classify metrics and report counts without
        modifying any tables.
    :return: Summary dict with keys:

        - ``"activity_count"``: number of activities in the replace set.
        - ``"rows_deleted"``: number of pre-existing downsampled rows removed
          (0 in dry-run; the count of rows that would have been removed).
        - ``"rows_inserted"``: number of bucket rows inserted (0 in dry-run).
        - ``"metric_strategies"``: list of ``(name, Strategy)`` tuples sorted
          by name, covering the distinct metric names present in the source
          rows for the replace set.
        - ``"dry_run"``: bool echoing the input.
    :raises FileNotFoundError: when ``db_path`` does not exist.
    :raises ValueError: when ``time_grain_seconds`` is not positive.
    """
    if time_grain_seconds <= 0:
        raise ValueError(
            f"time_grain_seconds must be positive; got {time_grain_seconds}"
        )
    db_file = Path(db_path).expanduser().resolve()
    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")

    _ensure_schema_current(db_file)

    start_dt, end_dt = resolve_range(start, end)
    user_ids_list: Optional[List[int]] = list(user_ids) if user_ids else None

    conn = _open_with_fk(db_file)
    try:
        activity_count = _populate_target_activities(
            conn,
            start_dt,
            end_dt,
            user_ids_list,
            require_source=True,
        )

        # Distinct metric names in source for the replace set; classification
        # is computed even on an empty result so callers can still print an
        # informative empty-strategy table.
        names_sql = (
            "SELECT DISTINCT m.name FROM activity_ts_metric m "
            "JOIN _tmp_target_activities t ON t.activity_id = m.activity_id"
        )
        metric_names = [row[0] for row in conn.execute(names_sql).fetchall()]
        metric_strategies: List[Tuple[str, Strategy]] = sorted(
            ((n, strategy_for(n)) for n in metric_names),
            key=lambda pair: pair[0],
        )

        if activity_count == 0:
            return {
                "activity_count": 0,
                "rows_deleted": 0,
                "rows_inserted": 0,
                "metric_strategies": metric_strategies,
                "dry_run": dry_run,
            }

        existing_count_sql = (
            "SELECT COUNT(*) FROM activity_ts_metric_downsampled d "
            "JOIN _tmp_target_activities t ON t.activity_id = d.activity_id"
        )

        if dry_run:
            rows_deleted = conn.execute(existing_count_sql).fetchone()[0]
            return {
                "activity_count": activity_count,
                "rows_deleted": rows_deleted,
                "rows_inserted": 0,
                "metric_strategies": metric_strategies,
                "dry_run": True,
            }

        rows_deleted = 0
        rows_inserted = 0
        cur = conn.cursor()
        cur.execute("BEGIN")
        try:
            cur.execute(
                "DELETE FROM activity_ts_metric_downsampled "
                "WHERE activity_id IN ("
                "SELECT activity_id FROM _tmp_target_activities)"
            )
            rows_deleted = cur.rowcount

            for name, strategy in metric_strategies:
                if strategy is Strategy.SKIP:
                    continue
                if strategy is Strategy.AGGREGATE:
                    sql = _aggregate_insert_sql(time_grain_seconds)
                elif strategy is Strategy.LAST:
                    sql = _last_insert_sql(time_grain_seconds)
                else:
                    # Defensive: future enum value with no handler should not
                    # silently emit no rows.
                    raise RuntimeError(f"Unhandled downsample strategy: {strategy!r}")
                cur.execute(sql, {"grain": time_grain_seconds, "name": name})
            # Count freshly inserted rows by querying the destination table.
            # `cur.rowcount` is unreliable for `WITH ... INSERT INTO ...` in
            # Python's sqlite3 binding (the LAST-strategy query uses a CTE
            # and undercounts), so we compute the count from the truth on
            # disk inside the same transaction.
            rows_inserted = cur.execute(
                "SELECT COUNT(*) FROM activity_ts_metric_downsampled "
                "WHERE activity_id IN ("
                "SELECT activity_id FROM _tmp_target_activities)"
            ).fetchone()[0]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        _drop_target_temp(conn)
        conn.close()

    return {
        "activity_count": activity_count,
        "rows_deleted": rows_deleted,
        "rows_inserted": rows_inserted,
        "metric_strategies": metric_strategies,
        "dry_run": False,
    }

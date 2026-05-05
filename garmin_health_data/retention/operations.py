"""
Retention operations for the garmin-health-data SQLite database.

This module currently exposes a single public entry point, :func:`migrate_cascade`,
which retrofits ``ON DELETE CASCADE`` onto the 16 child foreign keys that were declared
without an explicit FK action in older versions of ``tables.ddl``. SQLite has no ``ALTER
TABLE`` mechanism for changing FK actions, so each affected child table is rebuilt via
the standard recreate dance: rename, ``CREATE TABLE`` with the new DDL, ``INSERT ...
SELECT``, drop the old table, and recreate any indexes.

The ``prune_ts_metrics`` and ``downsample_activities`` operations land in follow-up
commits.
"""

import re
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Tuple

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
        cur.executescript(new_create)
        # Column lists are identical between old and new, so a positional
        # ``SELECT *`` copy is safe.
        cur.execute(f"INSERT INTO {table} SELECT * FROM {old_name}")
        cur.execute(f"DROP TABLE {old_name}")
        for index_sql in indexes:
            cur.executescript(index_sql)
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

    return {
        "migrated": migrated,
        "skipped": skipped,
        "backup_path": backup_path,
        "dry_run": dry_run,
    }

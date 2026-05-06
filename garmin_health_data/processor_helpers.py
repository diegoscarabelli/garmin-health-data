"""
Helper classes and functions for the Garmin data processor.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from garmin_health_data.db import check_sqlite_version

# Lowest SQLITE_MAX_VARIABLE_NUMBER across supported builds.
# Pre-3.32.0 defaulted to 999; 3.32.0+ raised it to 32 766.
# Using the floor guarantees safety on all platforms.
_SQLITE_MAX_PARAMS = 999


@dataclass
class FileSet:
    """
    Represents a set of files to process together.
    """

    file_paths: List[Path]
    files: Dict[Any, List[Path]]  # Maps data type enum to file paths


class Processor:
    """
    Base processor class for handling file sets.
    """

    def __init__(self, file_set: FileSet, session: Session):
        """
        Initialize processor.

        :param file_set: FileSet to process.
        :param session: SQLAlchemy session.
        """
        self.file_set = file_set
        self.session = session

    def process_file_set(self, file_set: FileSet, session: Session):
        """
        Process a file set.

        Override in subclasses.
        :param file_set: FileSet to process.
        :param session: SQLAlchemy session.
        """
        raise NotImplementedError("Subclasses must implement process_file_set")


def upsert_model_instances(
    session: Session,
    model_instances: List[Any],
    conflict_columns: List[str],
    on_conflict_update: bool = True,
    update_columns: Optional[List[str]] = None,
    returning_columns: Optional[List[str]] = None,
) -> List[Any]:
    """
    Bulk upsert SQLAlchemy ORM model instances into SQLite database tables.

    Uses SQLite's ``INSERT ... ON CONFLICT`` syntax to perform efficient bulk upsert
    operations, matching the implementation pattern used in OpenETL for PostgreSQL.
    Large batches are automatically split into chunks so the total parameter count stays
    within SQLite's ``SQLITE_MAX_VARIABLE_NUMBER`` limit.

    When ``returning_columns`` is omitted (default), the input list is returned
    unchanged. When provided, the listed columns are read back from the database via
    ``RETURNING``, so callers can recover database-assigned columns such as auto-
    increment primary keys or server-defaulted timestamps. The returned list contains
    one row per input row, in input order (position-aligned), regardless of whether
    ``on_conflict_update`` is True or False. Requires SQLite >= 3.35.

    For the ``on_conflict_update=False`` + ``returning_columns`` combination, the helper
    internally rewrites the statement using a no-op ``DO UPDATE`` (assigning a conflict
    column to itself) so ``RETURNING`` fires for both newly-inserted and conflicted
    rows. See the inline comment in the chunked execution loop for the full rationale.

    :param session: SQLAlchemy session.
    :param model_instances: List of model instances to upsert.
    :param conflict_columns: Column **names** (database column names, matching
        ``Column.name``) forming the unique conflict target. SQLAlchemy's
        ``index_elements`` parameter expects names, which is why this list
        uses the names namespace; ``update_columns`` and ``returning_columns``
        use the keys namespace instead. For the common case
        ``Column('foo', ...)`` the two are identical and the distinction does
        not matter; it only diverges when callers do
        ``Column('db_name', key='attr_name')``.
    :param on_conflict_update: If True, update on conflict; if False, ignore.
    :param update_columns: Column **keys** (Python attribute names, matching
        ``Column.key``) to update on conflict. Defaults to all columns except
        the conflict columns, primary-key columns, ``create_ts``, and
        ``update_ts``.
    :param returning_columns: Column **keys** (Python attribute names, matching
        ``Column.key``) to populate on the returned instances. If None, the
        input ``model_instances`` list is returned unchanged.
    :return: List of model instances. If ``returning_columns`` is None, this is the
        input list. Otherwise, it is a fresh list of model instances populated with only
        the requested columns from the database.
    """
    if not model_instances:
        return []

    model_class = type(model_instances[0])
    model_columns = model_class.__table__.columns.keys()  # KEYS namespace.
    column_names = {col.name for col in model_class.__table__.columns}  # NAMES.
    name_to_key = {col.name: col.key for col in model_class.__table__.columns}

    # SQLAlchemy uses two namespaces for the same column:
    # - col.name = the database column name (used by SQL emitted via
    #   `index_elements=conflict_columns` in `on_conflict_do_update`).
    # - col.key  = the Python attribute name (used by `excluded[col]` and
    #   `getattr(model_class, col)`).
    # They are equal for the common case `Column('foo', ...)`, but diverge
    # when callers do `Column('db_name', key='attr_name')`. The contract
    # this helper enforces:
    #   - `conflict_columns` entries are NAMES (matching SQLAlchemy's
    #     `index_elements`).
    #   - `update_columns` and `returning_columns` are KEYS (matching
    #     `excluded[...]` and `getattr(model_class, ...)`).
    # Validate up front so a mismatch fails with a clear error here rather
    # than an opaque AttributeError / KeyError deep in execution.
    def _duplicates(seq: List[str]) -> List[str]:
        seen: set = set()
        dups: List[str] = []
        for item in seq:
            if item in seen and item not in dups:
                dups.append(item)
            seen.add(item)
        return dups

    if not conflict_columns:
        raise ValueError(
            "`conflict_columns` must be a non-empty list. An empty list "
            "produces invalid `ON CONFLICT` SQL in both DO UPDATE and DO "
            "NOTHING modes."
        )
    unknown_conflict = [c for c in conflict_columns if c not in column_names]
    if unknown_conflict:
        raise ValueError(
            f"`conflict_columns` references column name(s) not present on "
            f"{model_class.__name__}: {unknown_conflict}. Valid column "
            f"names: {sorted(column_names)}"
        )
    dup_conflict = _duplicates(conflict_columns)
    if dup_conflict:
        raise ValueError(
            f"`conflict_columns` contains duplicate entries: {dup_conflict}."
        )
    if update_columns is not None:
        unknown_update = [c for c in update_columns if c not in model_columns]
        if unknown_update:
            raise ValueError(
                f"`update_columns` references column key(s) not present on "
                f"{model_class.__name__}: {unknown_update}. Valid column "
                f"keys: {sorted(model_columns)}"
            )
        dup_update = _duplicates(update_columns)
        if dup_update:
            raise ValueError(
                f"`update_columns` contains duplicate entries: {dup_update}."
            )
    if returning_columns is not None:
        if not returning_columns:
            raise ValueError(
                "`returning_columns` must be a non-empty list when provided. "
                "Pass None to opt out of the RETURNING path."
            )
        unknown_returning = [c for c in returning_columns if c not in model_columns]
        if unknown_returning:
            raise ValueError(
                f"`returning_columns` references column key(s) not present "
                f"on {model_class.__name__}: {unknown_returning}. Valid "
                f"column keys: {sorted(model_columns)}"
            )
        dup_returning = _duplicates(returning_columns)
        if dup_returning:
            raise ValueError(
                f"`returning_columns` contains duplicate entries: "
                f"{dup_returning}. Duplicate column labels in RETURNING "
                f"would silently collide in the result dict."
            )
        check_sqlite_version()

    # Convert all instances to dictionaries (bulk preparation).
    values = []
    for instance in model_instances:
        instance_dict = {}
        for key, value in instance.__dict__.items():
            if key in model_columns:
                instance_dict[key] = value
        values.append(instance_dict)

    # Determine which columns to update on conflict. Excludes:
    # - conflict columns (used to identify the row, must not change),
    # - primary-key columns (immutable; for an auto-increment PK like sleep_id,
    #   not present on the input instance, leaving it would generate
    #   `SET sleep_id = NULL` and trigger a new-rowid assignment in SQLite),
    # - create_ts (audit column, should reflect the original insert time),
    # - update_ts (set explicitly below to current_timestamp if present).
    #
    # All comparisons happen in the KEYS namespace. We translate
    # `conflict_columns` (names per the public contract) to keys via
    # `name_to_key` so the set membership actually matches; PK columns use
    # `col.key` directly.
    pk_columns = {col.key for col in model_class.__table__.primary_key.columns}
    if update_columns is None:
        conflict_keys = {name_to_key[c] for c in conflict_columns}
        excluded_cols = conflict_keys | pk_columns | {"create_ts", "update_ts"}
        update_columns = [col for col in model_columns if col not in excluded_cols]

    # Clamp chunk size so total parameters stay within the SQLite
    # limit. Use the full model column count because SQLAlchemy
    # fills in columns with Python-side defaults even when omitted
    # from the values dicts.
    num_cols = len(model_class.__table__.columns)
    max_rows = max(1, _SQLITE_MAX_PARAMS // num_cols)

    returned_rows: List[Dict[str, Any]] = []

    for chunk_start in range(0, len(values), max_rows):
        chunk = values[chunk_start : chunk_start + max_rows]

        insert_stmt = sqlite_insert(model_class).values(chunk)

        if on_conflict_update:
            # Build update dictionary for ON CONFLICT DO UPDATE. Both the SET
            # keys and `excluded[...]` lookups use the KEYS namespace, which
            # matches `update_columns` per the public contract.
            update_dict = {col: insert_stmt.excluded[col] for col in update_columns}

            # Automatically refresh update_ts on every conflict update.
            # SQLite's DEFAULT CURRENT_TIMESTAMP only applies on INSERT, not
            # UPDATE. We unconditionally overwrite any update_ts entry
            # already in update_dict (e.g. when callers pass
            # `update_columns` derived from `__table__.columns`, which
            # includes update_ts and would otherwise resolve to
            # `excluded.update_ts`, propagating whatever was on the input
            # row instead of refreshing it). Audit semantics > caller
            # control here: the row was just updated, so its update_ts
            # should reflect that.
            if hasattr(model_class, "update_ts"):
                update_dict["update_ts"] = func.current_timestamp()

            # Defensive fallback: if update_dict is empty (e.g. a table with
            # only PK + conflict + audit columns and no `update_ts`), the
            # default `update_columns` list is empty and we'd otherwise emit
            # `ON CONFLICT (...) DO UPDATE SET` with no SET targets, which is
            # invalid SQL. Use the no-op DO UPDATE trick (assign a conflict
            # column to itself) so the conflict path still fires and RETURNING
            # works if requested. `name_to_key` translates the name namespace
            # (per the conflict_columns contract) to keys (what `excluded[...]`
            # expects).
            if not update_dict:
                key_col = name_to_key[conflict_columns[0]]
                update_dict[key_col] = insert_stmt.excluded[key_col]

            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_columns, set_=update_dict
            )
        elif returning_columns:
            # "No-op DO UPDATE" trick to keep do-nothing semantics while still
            # getting one RETURNING row per input row in input order.
            #
            # Why: `ON CONFLICT DO NOTHING` does not emit RETURNING rows for
            # ignored conflicts. With N input rows where K conflict, RETURNING
            # would emit only N - K rows, so we'd lose IDs for the K
            # pre-existing rows. A follow-up `SELECT ... WHERE or_(conflict
            # keys)` recovers them but returns rows in undefined order and
            # de-duplicates by conflict key, breaking position-alignment.
            #
            # Trick: rewrite as `DO UPDATE SET <conflict_col> = excluded.
            # <conflict_col>`. The conflict column's existing value is by
            # definition equal to the incoming value (that's what triggered
            # the conflict), so assigning it to itself is a provable no-op at
            # the value level. But the conflict path still *fires*, which
            # makes RETURNING emit one row per input row in input order, for
            # both fresh inserts and conflicts.
            #
            # The only observable difference vs. pure DO NOTHING: a row-level
            # write happens at the storage layer (journal entry, no value
            # change). We deliberately do NOT include `update_ts` in the SET
            # clause here, so the do-nothing contract is preserved at the
            # audit-column level too.
            #
            # The pure DO NOTHING path is preserved below for callers that
            # don't request returning_columns and want to skip the write
            # entirely on conflict.
            #
            # NOTE: `conflict_columns` is in the NAMES namespace per the
            # public contract, but `set_` keys and `excluded[...]` are in the
            # KEYS namespace. Translate via `name_to_key` so the trick works
            # for models with `Column('db_name', key='attr_name')` (where the
            # two diverge). For the common case the translation is a no-op.
            key_col = name_to_key[conflict_columns[0]]
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={key_col: insert_stmt.excluded[key_col]},
            )
        else:
            # Pure DO NOTHING: no row-write on conflict.
            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=conflict_columns
            )

        if returning_columns:
            return_cols = [getattr(model_class, col) for col in returning_columns]
            # Both branches above resolve to ON CONFLICT DO UPDATE (real or
            # no-op), so RETURNING always emits exactly one row per input row
            # in input order.
            upsert_stmt = upsert_stmt.returning(*return_cols)
            result = session.execute(upsert_stmt)
            returned_rows.extend(row._asdict() for row in result.fetchall())
        else:
            session.execute(upsert_stmt)

    if returning_columns is None:
        return model_instances
    return [model_class(**row) for row in returned_rows]

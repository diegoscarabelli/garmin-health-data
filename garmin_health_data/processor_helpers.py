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
    :param conflict_columns: Columns that define uniqueness.
    :param on_conflict_update: If True, update on conflict; if False, ignore.
    :param update_columns: Columns to update (if None, update all non-conflict cols).
    :param returning_columns: Columns to populate on the returned instances. If None,
        the input ``model_instances`` list is returned unchanged.
    :return: List of model instances. If ``returning_columns`` is None, this is the
        input list. Otherwise, it is a fresh list of model instances populated with only
        the requested columns from the database.
    """
    if not model_instances:
        return []

    # Validate `returning_columns` and surface the SQLite version requirement
    # at the call boundary so callers that build a Session outside `get_engine`
    # (and therefore skip its version gate) still get a clear error instead of
    # an opaque SQL syntax failure on `RETURNING`.
    if returning_columns is not None:
        if not returning_columns:
            raise ValueError(
                "`returning_columns` must be a non-empty list when provided. "
                "Pass None to opt out of the RETURNING path."
            )
        check_sqlite_version()

    model_class = type(model_instances[0])
    model_columns = model_class.__table__.columns.keys()

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
    pk_columns = {col.name for col in model_class.__table__.primary_key.columns}
    if update_columns is None:
        excluded_cols = set(conflict_columns) | pk_columns | {"create_ts", "update_ts"}
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
            # Build update dictionary for ON CONFLICT DO UPDATE.
            update_dict = {col: insert_stmt.excluded[col] for col in update_columns}

            # Automatically update update_ts column if it exists in the
            # model. SQLite's DEFAULT CURRENT_TIMESTAMP only applies on
            # INSERT, not UPDATE. We must explicitly set update_ts to the
            # current timestamp on updates.
            if hasattr(model_class, "update_ts"):
                update_dict["update_ts"] = func.current_timestamp()

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
            key_col = conflict_columns[0]
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

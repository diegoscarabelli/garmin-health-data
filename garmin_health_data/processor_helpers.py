"""
Helper classes and functions for the Garmin data processor.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

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
    ``RETURNING`` (or, for the insert-ignore path, a follow-up ``SELECT`` over the
    conflict keys), so callers can recover database-assigned columns such as auto-
    increment primary keys or server-defaulted timestamps. Requires SQLite >= 3.35.

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
        else:
            # Ignore conflicts (insert-only).
            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=conflict_columns
            )

        if returning_columns:
            return_cols = [getattr(model_class, col) for col in returning_columns]
            if on_conflict_update:
                # ON CONFLICT DO UPDATE always emits a row per input, so
                # RETURNING gives us one row per input row.
                upsert_stmt = upsert_stmt.returning(*return_cols)
                result = session.execute(upsert_stmt)
                returned_rows.extend(row._asdict() for row in result.fetchall())
            else:
                # ON CONFLICT DO NOTHING does not emit RETURNING rows for
                # ignored conflicts. Execute without RETURNING, then SELECT
                # the conflict keys of this chunk to recover IDs for both
                # newly-inserted and pre-existing rows.
                session.execute(upsert_stmt)
                conflict_conditions = [
                    and_(
                        *[
                            (
                                getattr(model_class, col) == value[col]
                                if value[col] is not None
                                else getattr(model_class, col).is_(None)
                            )
                            for col in conflict_columns
                        ]
                    )
                    for value in chunk
                ]
                stmt = select(*return_cols).where(or_(*conflict_conditions))
                returned_rows.extend(
                    row._asdict() for row in session.execute(stmt).all()
                )
        else:
            session.execute(upsert_stmt)

    if returning_columns is None:
        return model_instances
    return [model_class(**row) for row in returned_rows]

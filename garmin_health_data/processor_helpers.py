"""
Helper classes and functions for the Garmin data processor.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session


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
        Process a file set. Override in subclasses.

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
) -> List[Any]:
    """
    Bulk upsert SQLAlchemy ORM model instances into SQLite database tables.

    This function uses SQLite's INSERT ... ON CONFLICT syntax to perform
    efficient bulk upsert operations in a single SQL statement, matching
    the implementation pattern used in OpenETL for PostgreSQL.

    :param session: SQLAlchemy session.
    :param model_instances: List of model instances to upsert.
    :param conflict_columns: Columns that define uniqueness.
    :param on_conflict_update: If True, update on conflict; if False, ignore.
    :param update_columns: Columns to update (if None, update all non-conflict cols).
    :return: List of persisted instances.
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

    # Determine which columns to update on conflict.
    if update_columns is None:
        # Update all columns except conflict columns, create_ts, and update_ts.
        # Exclude create_ts (should never change on update).
        # Exclude update_ts (will be set explicitly below if it exists).
        excluded_cols = set(conflict_columns) | {"create_ts", "update_ts"}
        update_columns = [col for col in model_columns if col not in excluded_cols]

    # Create bulk insert statement with all values.
    insert_stmt = sqlite_insert(model_class).values(values)

    if on_conflict_update:
        # Build update dictionary for ON CONFLICT DO UPDATE.
        update_dict = {col: insert_stmt.excluded[col] for col in update_columns}

        # Automatically update update_ts column if it exists in the model.
        # SQLite's DEFAULT CURRENT_TIMESTAMP only applies on INSERT, not UPDATE.
        # We must explicitly set update_ts to the current timestamp on updates.
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

    # Execute the bulk upsert statement (single SQL statement for all rows).
    session.execute(upsert_stmt)

    # Flush session to force immediate resolution of foreign key relationships.
    session.flush()

    return model_instances

"""
Helper classes and functions for the Garmin data processor.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    SQLite-compatible upsert for model instances.

    Uses SQLite's INSERT ... ON CONFLICT syntax to perform upsert operations.

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

    for instance in model_instances:
        # Convert instance to dict, excluding columns with None values
        # that have server defaults so those defaults can apply.
        instance_dict = {}
        for c in instance.__table__.columns:
            value = getattr(instance, c.name, None)
            # Skip columns with None if they have server defaults
            # Check both server_default and nullable to determine if we should skip
            if value is None and (c.server_default is not None or not c.nullable):
                # For columns with server defaults or NOT NULL, skip None values
                # so the database can apply the default
                if c.server_default is not None:
                    continue
            instance_dict[c.name] = value

        # Create insert statement.
        stmt = sqlite_insert(model_class).values(**instance_dict)

        if on_conflict_update:
            # Determine which columns to update.
            if update_columns:
                update_dict = {col: stmt.excluded[col] for col in update_columns}
            else:
                # Update all columns except conflict columns.
                update_dict = {
                    c.name: stmt.excluded[c.name]
                    for c in model_class.__table__.columns
                    if c.name not in conflict_columns
                }

            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns, set_=update_dict
            )
        else:
            # Ignore conflicts (insert-only).
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)

        session.execute(stmt)

    session.flush()
    return model_instances

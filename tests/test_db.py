"""
Tests for database module.
"""

from garmin_health_data.db import (
    create_tables,
    database_exists,
    get_database_size,
)


def test_database_exists_false(tmp_path):
    """
    Test database_exists returns False for non-existent database.
    """
    db_path = tmp_path / "test.db"
    assert not database_exists(str(db_path))


def test_database_exists_true(tmp_path):
    """
    Test database_exists returns True for existing database.
    """
    db_path = tmp_path / "test.db"
    db_path.touch()
    assert database_exists(str(db_path))


def test_create_tables(tmp_path):
    """
    Test creating database tables.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))
    assert db_path.exists()


def test_get_database_size_nonexistent(tmp_path):
    """
    Test getting size of non-existent database.
    """
    db_path = tmp_path / "test.db"
    assert get_database_size(str(db_path)) == 0


def test_get_database_size(tmp_path):
    """
    Test getting size of existing database.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))
    size = get_database_size(str(db_path))
    assert size > 0

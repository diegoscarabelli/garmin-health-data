"""
Tests for database module.
"""

from sqlalchemy import text

from garmin_health_data.db import (
    create_tables,
    database_exists,
    get_database_size,
    get_engine,
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


def test_get_engine_enables_foreign_keys(tmp_path):
    """
    Verify every connection from get_engine has `PRAGMA foreign_keys` set to 1.

    SQLite's default is 0 (off) per connection; the connect listener must turn it on so
    `ON DELETE CASCADE` clauses defined in the schema actually fire.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))
    engine = get_engine(str(db_path))

    # Open two independent connections and assert each has FKs enabled.
    for _ in range(2):
        with engine.connect() as conn:
            value = conn.execute(text("PRAGMA foreign_keys")).scalar()
            assert value == 1, (
                "PRAGMA foreign_keys is OFF on a connection from get_engine; "
                "ON DELETE CASCADE will silently not fire."
            )

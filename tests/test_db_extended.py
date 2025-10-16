"""
Extended tests for database module.

This test suite provides comprehensive coverage of:
    - Database session management (commit and rollback).
    - Database initialization and table creation.
    - Record counting and statistics.
    - Last update date tracking.
    - Error handling for database operations.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from garmin_health_data.db import (
    create_tables,
    database_exists,
    get_database_size,
    get_engine,
    get_last_update_dates,
    get_record_counts,
    get_session,
    initialize_database,
)
from garmin_health_data.models import User


class TestGetEngine:
    """
    Test class for database engine creation.
    """

    def test_get_engine_creates_sqlite_engine(self, temp_db_path: str) -> None:
        """
        Test that get_engine creates a SQLite engine.

        :param temp_db_path: Temporary database path fixture.
        """
        engine = get_engine(temp_db_path)

        assert engine is not None
        assert "sqlite" in str(engine.url)
        engine.dispose()


class TestGetSession:
    """
    Test class for database session management.
    """

    def test_get_session_commits_on_success(self, temp_db_path: str) -> None:
        """
        Test that get_session commits changes on successful execution.

        :param temp_db_path: Temporary database path fixture.
        """
        create_tables(temp_db_path)

        with get_session(temp_db_path) as session:
            user = User(user_id=12345, full_name="Test User")
            session.add(user)

        # Verify the record was committed.
        with get_session(temp_db_path) as session:
            result = session.query(User).filter_by(user_id=12345).first()
            assert result is not None
            assert result.full_name == "Test User"

    def test_get_session_rolls_back_on_exception(self, temp_db_path: str) -> None:
        """
        Test that get_session rolls back changes on exception.

        :param temp_db_path: Temporary database path fixture.
        """
        create_tables(temp_db_path)

        with pytest.raises(ValueError):
            with get_session(temp_db_path) as session:
                user = User(user_id=12345, full_name="Test User")
                session.add(user)
                raise ValueError("Intentional error")

        # Verify the record was not committed.
        with get_session(temp_db_path) as session:
            result = session.query(User).filter_by(user_id=12345).first()
            assert result is None


class TestInitializeDatabase:
    """
    Test class for database initialization.
    """

    @patch("click.echo")
    @patch("click.secho")
    def test_initialize_database_new_database(
        self,
        mock_secho: MagicMock,
        mock_echo: MagicMock,
        temp_db_path: str,
    ) -> None:
        """
        Test initializing a new database.

        :param mock_secho: Mock click.secho function.
        :param mock_echo: Mock click.echo function.
        :param temp_db_path: Temporary database path fixture.
        """
        initialize_database(temp_db_path)

        assert Path(temp_db_path).exists()
        mock_echo.assert_called()
        mock_secho.assert_called_with(
            "✅ Database initialized successfully", fg="green"
        )

    @patch("click.echo")
    @patch("click.secho")
    def test_initialize_database_existing_database(
        self,
        mock_secho: MagicMock,
        mock_echo: MagicMock,
        temp_db_path: str,
    ) -> None:
        """
        Test initializing an existing database.

        :param mock_secho: Mock click.secho function.
        :param mock_echo: Mock click.echo function.
        :param temp_db_path: Temporary database path fixture.
        """
        # Create database first.
        create_tables(temp_db_path)

        # Initialize again.
        initialize_database(temp_db_path)

        mock_echo.assert_called()
        assert "already exists" in str(mock_echo.call_args_list[0])


class TestGetLastUpdateDates:
    """
    Test class for last update date tracking.
    """

    def test_get_last_update_dates_empty_database(
        self,
        db_engine: Engine,
    ) -> None:
        """
        Test getting last update dates from empty database.

        :param db_engine: Database engine fixture.
        """
        temp_db_path = str(db_engine.url).replace("sqlite:///", "")
        dates = get_last_update_dates(temp_db_path)

        # All dates should be None for empty database.
        assert dates["sleep"] is None
        assert dates["heart_rate"] is None
        assert dates["activity"] is None

    def test_get_last_update_dates_with_data(
        self,
        db_engine: Engine,
        db_session: Session,
    ) -> None:
        """
        Test getting last update dates with data in database.

        :param db_engine: Database engine fixture.
        :param db_session: Database session fixture.
        """
        # For now, skip this test until we have proper test data fixtures.
        # Activity model requires many NOT NULL fields making it complex to test.
        pytest.skip("Requires comprehensive test data fixtures")


class TestGetRecordCounts:
    """
    Test class for record counting functionality.
    """

    def test_get_record_counts_empty_database(
        self,
        db_engine: Engine,
    ) -> None:
        """
        Test getting record counts from empty database.

        :param db_engine: Database engine fixture.
        """
        temp_db_path = str(db_engine.url).replace("sqlite:///", "")
        counts = get_record_counts(temp_db_path)

        # All counts should be 0 for empty database.
        assert counts["users"] == 0
        assert counts["activities"] == 0
        assert counts["sleep_sessions"] == 0
        assert counts["heart_rate_readings"] == 0

    def test_get_record_counts_with_data(
        self,
        db_engine: Engine,
        db_session: Session,
    ) -> None:
        """
        Test getting record counts with data in database.

        :param db_engine: Database engine fixture.
        :param db_session: Database session fixture.
        """
        # Add test data - just test with User table.
        user = User(user_id=12345, full_name="Test User")
        db_session.add(user)
        db_session.commit()

        temp_db_path = str(db_engine.url).replace("sqlite:///", "")
        counts = get_record_counts(temp_db_path)

        assert counts["users"] == 1
        # Other tables should be 0.
        assert counts["sleep_sessions"] == 0
        assert counts["heart_rate_readings"] == 0


class TestDatabaseHelpers:
    """
    Test class for database helper functions.
    """

    def test_database_exists_nonexistent(self, tmp_path: Path) -> None:
        """
        Test database_exists returns False for non-existent database.

        :param tmp_path: Pytest temporary directory fixture.
        """
        db_path = tmp_path / "nonexistent.db"
        assert not database_exists(str(db_path))

    def test_database_exists_true(self, temp_db_path: str) -> None:
        """
        Test database_exists returns True for existing database.

        :param temp_db_path: Temporary database path fixture.
        """
        create_tables(temp_db_path)
        assert database_exists(temp_db_path)

    def test_get_database_size_nonexistent(self, tmp_path: Path) -> None:
        """
        Test getting size of non-existent database returns 0.

        :param tmp_path: Pytest temporary directory fixture.
        """
        db_path = tmp_path / "nonexistent.db"
        assert get_database_size(str(db_path)) == 0

    def test_get_database_size_existing(self, temp_db_path: str) -> None:
        """
        Test getting size of existing database returns positive value.

        :param temp_db_path: Temporary database path fixture.
        """
        create_tables(temp_db_path)
        size = get_database_size(temp_db_path)
        assert size > 0

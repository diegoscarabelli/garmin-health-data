"""
Pytest configuration and fixtures for garmin_health_data tests.

Provides reusable fixtures for database testing, mock Garmin API clients, and temporary
directories.
"""

from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from garmin_health_data.models import Base


@pytest.fixture
def temp_db_path(tmp_path: Path) -> str:
    """
    Create a temporary database path for testing.

    :param tmp_path: Pytest temporary directory fixture.
    :return: String path to temporary database file.
    """
    db_path = tmp_path / "test_garmin.db"
    return str(db_path)


@pytest.fixture
def db_engine(temp_db_path: str) -> Generator[Engine, None, None]:
    """
    Create a test database engine with all tables.

    :param temp_db_path: Path to temporary database.
    :return: SQLAlchemy engine instance.
    """
    engine = create_engine(f"sqlite:///{temp_db_path}")
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session(db_engine: Engine) -> Generator[Session, None, None]:
    """
    Create a database session for testing.

    :param db_engine: SQLAlchemy engine fixture.
    :return: SQLAlchemy session instance.
    """
    SessionLocal = sessionmaker(bind=db_engine)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def mock_garmin_client() -> MagicMock:
    """
    Create a mock Garmin Connect client for testing.

    :return: Mock Garmin client instance.
    """
    mock_client = MagicMock()
    mock_client.login.return_value = None
    mock_client.garth = MagicMock()
    return mock_client


@pytest.fixture
def mock_garmin_class(
    mock_garmin_client: MagicMock,
) -> Generator[MagicMock, None, None]:
    """
    Create a mock Garmin class that returns a mock client instance.

    :param mock_garmin_client: Mock Garmin client fixture.
    :return: Mock Garmin class.
    """
    from unittest.mock import patch

    with patch("garmin_health_data.auth.Garmin") as mock_class:
        mock_class.return_value = mock_garmin_client
        yield mock_class


@pytest.fixture
def token_dir(tmp_path: Path) -> Path:
    """
    Create a temporary token directory for authentication tests.

    :param tmp_path: Pytest temporary directory fixture.
    :return: Path to token directory.
    """
    tokens = tmp_path / ".garminconnect"
    tokens.mkdir()
    return tokens

"""
Tests for CLI commands.
"""

import sqlalchemy
import pytest
from click.testing import CliRunner
from sqlalchemy.exc import ArgumentError

from garmin_health_data.cli import verify
from garmin_health_data.db import create_tables, get_session

SQLALCHEMY_2 = sqlalchemy.__version__.startswith("2.")


@pytest.mark.skipif(not SQLALCHEMY_2, reason="SQLAlchemy 2.x required.")
def test_raw_sql_string_raises_error(tmp_path):
    """
    Test that passing a raw SQL string to session.execute() raises an error.

    SQLAlchemy 2.x requires raw SQL to be wrapped with text(). This test captures the
    original bug where PRAGMA integrity_check was passed as a plain string, causing
    ArgumentError.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    with get_session(str(db_path)) as session:
        with pytest.raises(
            ArgumentError, match="should be explicitly declared as text"
        ):
            session.execute("PRAGMA integrity_check")


def test_verify_runs_integrity_check(tmp_path):
    """
    Test that verify command executes PRAGMA integrity_check successfully.

    Ensures raw SQL is wrapped with text() for SQLAlchemy 2.x compatibility.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    result = runner.invoke(verify, ["--db-path", str(db_path)])

    assert result.exit_code == 0
    assert "Database integrity check passed" in result.output


def test_verify_nonexistent_db(tmp_path):
    """
    Test that verify command rejects non-existent database path.
    """
    db_path = tmp_path / "nonexistent.db"

    runner = CliRunner()
    result = runner.invoke(verify, ["--db-path", str(db_path)])

    assert result.exit_code != 0
    assert "does not exist" in result.output

"""
Tests for CLI commands.
"""

from click.testing import CliRunner

from garmin_health_data.cli import verify
from garmin_health_data.db import create_tables


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

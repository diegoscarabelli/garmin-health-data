"""
Tests for CLI commands.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from sqlalchemy.exc import ArgumentError

from garmin_health_data.cli import extract, verify
from garmin_health_data.db import create_tables, get_session


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


def _stub_extract_no_files(*args, **kwargs):
    """
    Stub for extract_data that returns no files so the CLI exits early after the cleanup
    branch runs.
    """
    return {"garmin_files": 0, "activity_files": 0}


def test_extract_default_uses_temp_dir_and_cleans_up(tmp_path):
    """
    Without --keep-files, the CLI uses the system temp directory for extraction and
    removes it after processing.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    expected_temp = Path(tempfile.gettempdir()) / "garmin_extraction"

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ) as mock_extract,
    ):
        result = runner.invoke(
            extract,
            [
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-02",
            ],
        )

    assert result.exit_code == 0, result.output
    assert mock_extract.call_args.kwargs["ingest_dir"] == expected_temp
    assert not expected_temp.exists()


def test_extract_keep_files_writes_next_to_db_and_persists(tmp_path):
    """
    With --keep-files, the CLI uses a 'garmin_files/' directory beside the database file
    and does not remove it after processing.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    expected_dir = tmp_path / "garmin_files"

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ) as mock_extract,
    ):
        result = runner.invoke(
            extract,
            [
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-02",
                "--keep-files",
            ],
        )

    assert result.exit_code == 0, result.output
    assert mock_extract.call_args.kwargs["ingest_dir"] == expected_dir
    assert expected_dir.exists()
    assert expected_dir.is_dir()
    assert "Keeping extracted files at" in result.output


def test_extract_keep_files_preserves_existing_files(tmp_path):
    """
    With --keep-files, files already present in the target directory are preserved
    across runs (the directory itself is not wiped).
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    files_dir = tmp_path / "garmin_files"
    files_dir.mkdir()
    sentinel = files_dir / "previous_run.json"
    sentinel.write_text('{"existing": true}')

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ),
    ):
        result = runner.invoke(
            extract,
            [
                "--db-path",
                str(db_path),
                "--start-date",
                "2025-01-01",
                "--end-date",
                "2025-01-02",
                "--keep-files",
            ],
        )

    assert result.exit_code == 0, result.output
    assert sentinel.exists()
    assert sentinel.read_text() == '{"existing": true}'

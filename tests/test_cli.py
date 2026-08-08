"""
Tests for CLI commands.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from sqlalchemy.exc import ArgumentError

from garmin_health_data.cli import extract, verify
from garmin_health_data.db import create_tables, get_session
from garmin_health_data.lifecycle import acquire_lock, setup_lifecycle_dirs


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
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    result = runner.invoke(verify, ["--db-path", str(db_path)])

    assert result.exit_code == 0
    assert "Database integrity check passed" in result.output


def test_verify_nonexistent_db(tmp_path):
    """
    Test that verify command rejects non-existent database path with a friendly message
    (rather than Click's raw "does not exist" path-validator error).
    """
    db_path = tmp_path / "nonexistent.db"

    runner = CliRunner()
    result = runner.invoke(verify, ["--db-path", str(db_path)])

    assert result.exit_code == 1
    assert "Database not found" in result.output
    assert "garmin extract" in result.output


# --------------------------------------------------------------------------------------
# Lifecycle integration tests for the extract command
# --------------------------------------------------------------------------------------


def _stub_extract_no_files(*args, **kwargs):
    """
    Return an empty extraction result; ingest_dir untouched.
    """
    return {
        "garmin_files": 0,
        "activity_files": 0,
        "failures": [],
        "failed_accounts": [],
    }


def _common_invoke(runner, db_path, *extra_args):
    """
    Invoke the extract command with a default narrow date range.
    """
    return runner.invoke(
        extract,
        [
            "--db-path",
            str(db_path),
            "--start-date",
            "2025-01-01",
            "--end-date",
            "2025-01-02",
            *extra_args,
        ],
    )


def test_extract_creates_lifecycle_dirs_next_to_db(tmp_path):
    """
    Extract command creates garmin_files/{ingest,process,storage,quarantine} next to the
    database file before extraction runs.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    base = tmp_path / "garmin_files"
    for name in ("ingest", "process", "storage", "quarantine"):
        assert (base / name).is_dir()


def test_extract_recovers_stale_process_files(tmp_path):
    """
    Files left in process/ from a previously crashed run are moved back to ingest/ at
    the start of the next extract run.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    stale = base / "process" / "stale.json"
    stale.write_text('{"old": true}')

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    # Recovery message printed.
    assert "Recovered 1 file(s)" in result.output
    # The stale file's content is preserved. With no GARMIN_FILE_TYPES
    # pattern matching its name, the pre-routing step archived it directly
    # to storage/ as backup-only — no longer left in process/ to loop on
    # subsequent runs.
    assert (base / "storage" / "stale.json").read_text() == '{"old": true}'
    assert not (base / "process" / "stale.json").exists()


def test_extract_uses_ingest_dir_for_extract_data(tmp_path):
    """
    extract_data is invoked with ingest_dir = garmin_files/ingest.
    """

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    captured = {}

    def capture_ingest(*args, **kwargs):
        captured["ingest_dir"] = kwargs["ingest_dir"]
        return _stub_extract_no_files()

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=capture_ingest),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    assert captured["ingest_dir"] == tmp_path / "garmin_files" / "ingest"


# --------------------------------------------------------------------------------------
# Per-FileSet processing routes to storage / quarantine
# --------------------------------------------------------------------------------------


def test_process_loop_routes_success_to_storage_failure_to_quarantine(tmp_path):
    """
    With two FileSets in ingest/ where one will succeed and one will raise, the
    successful FileSet's files end up in storage/ and the failed FileSet's files end up
    in quarantine/.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    good = base / "ingest" / "user1_SLEEP_2025-01-01T12-00-00+00-00.json"
    bad = base / "ingest" / "user1_SLEEP_2025-01-02T12-00-00+00-00.json"
    good.write_text('{"ok": true}')
    bad.write_text('{"corrupt": true}')

    def stub_extract(*args, **kwargs):
        return {
            "garmin_files": 2,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    def stub_process(self, file_set, session):
        names = [p.name for p in file_set.file_paths]
        if any("2025-01-02" in n for n in names):
            raise RuntimeError("boom")

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
        patch(
            "garmin_health_data.processor.GarminProcessor.process_file_set",
            new=stub_process,
        ),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    assert (base / "storage" / good.name).exists()
    assert (base / "quarantine" / bad.name).exists()
    assert not good.exists()
    assert not bad.exists()


# --------------------------------------------------------------------------------------
# Failure summary printing
# --------------------------------------------------------------------------------------


def test_extract_prints_failure_summary(tmp_path):
    """
    End-of-run summary lists per-data-type failures from the extractor.
    """
    from garmin_health_data.extractor import ExtractionFailure

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    def stub_extract(*args, **kwargs):
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": [
                ExtractionFailure(
                    data_type="SLEEP",
                    date="2025-01-02",
                    activity_id="",
                    error="RuntimeError: hiccup",
                ),
                ExtractionFailure(
                    data_type="SLEEP",
                    date="2025-01-03",
                    activity_id="",
                    error="RuntimeError: again",
                ),
            ],
            "failed_accounts": [],
        }

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    assert "Extraction failures" in result.output
    assert "SLEEP" in result.output
    assert "2025-01-02" in result.output


# --------------------------------------------------------------------------------------
# --extract-only and --process-only flags
# --------------------------------------------------------------------------------------


def test_extract_only_skips_processing(tmp_path):
    """
    --extract-only writes files into ingest/ and stops; no move to process/
    or storage/.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    def stub_extract(*args, **kwargs):
        ingest = kwargs["ingest_dir"]
        (ingest / "user1_SLEEP_2025-01-01T12-00-00+00-00.json").write_text("{}")
        return {
            "garmin_files": 1,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
    ):
        result = _common_invoke(runner, db_path, "--extract-only")

    assert result.exit_code == 0, result.output
    base = tmp_path / "garmin_files"
    assert list((base / "ingest").iterdir())
    assert list((base / "process").iterdir()) == []
    assert list((base / "storage").iterdir()) == []
    assert "Extraction-only mode" in result.output


def test_process_only_skips_extraction(tmp_path):
    """
    --process-only does not call the extract API; it only processes whatever
    is in ingest/.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "user1_SLEEP_2025-01-01T12-00-00+00-00.json").write_text("{}")

    runner = CliRunner()
    mock_extract = MagicMock()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", mock_extract),
        patch("garmin_health_data.processor.GarminProcessor.process_file_set"),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path), "--process-only"],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_not_called()


def test_extract_only_and_process_only_are_mutually_exclusive(tmp_path):
    """
    Passing both flags exits non-zero with a clear error message.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    result = _common_invoke(runner, db_path, "--extract-only", "--process-only")

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


# --------------------------------------------------------------------------------------
# Concurrency lock
# --------------------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="fcntl-based lock is a no-op on Windows; concurrency test does not apply.",
)
def test_extract_aborts_when_lock_held(tmp_path):
    """
    A second concurrent extract aborts immediately with a clear message when the
    lifecycle lock is already held.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)

    runner = CliRunner()
    with (
        acquire_lock(base),
        patch("garmin_health_data.cli.ensure_authenticated"),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code != 0
    assert "Another garmin extract run is in progress" in result.output


# --------------------------------------------------------------------------------------
# Process-only does not require authentication
# --------------------------------------------------------------------------------------


def test_process_only_does_not_require_authentication(tmp_path):
    """
    Authentication is only required when extracting; --process-only must work without
    invoking ensure_authenticated.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)

    runner = CliRunner()
    auth_mock = MagicMock()
    with (
        patch("garmin_health_data.cli.ensure_authenticated", auth_mock),
        patch("garmin_health_data.processor.GarminProcessor.process_file_set"),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path), "--process-only"],
        )

    assert result.exit_code == 0, result.output
    auth_mock.assert_not_called()


def test_unmatched_files_routed_to_storage_as_backup(tmp_path):
    """
    Files that don't match any GARMIN_FILE_TYPES processor pattern (e.g. an unknown .xyz
    extension, or TCX/GPX/KML activity formats) are real Garmin data the user wanted
    extracted, just not data we can load.

    They go directly to storage/ as backup-only — NOT to quarantine, which is for
    genuine processing failures. Mirrors openetl's `store_format` skip-to-storage
    behavior.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    # Filename with timestamp so it groups into a FileSet, but with an
    # extension that no GARMIN_FILE_TYPES pattern matches.
    extra = base / "ingest" / "user1_UNKNOWN_2025-01-01T12-00-00+00-00.xyz"
    extra.write_text("backup-only data")

    def stub_extract(*args, **kwargs):
        return {
            "garmin_files": 1,
            "activity_files": 0,
            "failures": [],
            "failed_accounts": [],
        }

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    # Backup-only file landed in storage, not quarantine.
    assert (base / "storage" / extra.name).exists()
    assert not (base / "quarantine" / extra.name).exists()
    assert not extra.exists()
    assert list((base / "process").iterdir()) == []


def test_extract_suppresses_date_range_line_when_window_empty(tmp_path):
    """
    On an up-to-date database the auto-detected start (last stored day + 1) lands after
    the default end (today), an empty window.

    The CLI must not print its "Date range" line for that no-op, while still delegating
    to extract_data, which owns the authoritative short-circuit.
    """
    from datetime import date

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    # A far-future "last stored day" guarantees start > today's end regardless of the
    # wall clock, reproducing the up-to-date-database inverted window deterministically.
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.get_latest_date", return_value=date(2999, 1, 1)),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ) as mock_extract,
    ):
        result = runner.invoke(extract, ["--db-path", str(db_path)])

    assert result.exit_code == 0, result.output
    # Auto-detect path ran, so execution reached the range-printing region...
    assert "Auto-detected start date:" in result.output
    # ...and the misleading backwards range line was suppressed.
    assert "Date range:" not in result.output
    # Delegation still happens; extract_data performs the real no-op short-circuit.
    mock_extract.assert_called_once()


def test_extract_prints_date_range_line_for_normal_window(tmp_path):
    """
    A normal forward window (start before end) still prints the "Date range" line.

    Guards the empty-window suppression from regressing into always-on.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ),
    ):
        result = _common_invoke(runner, db_path)

    assert result.exit_code == 0, result.output
    assert "Date range:" in result.output


def test_extract_labels_same_day_window_inclusive(tmp_path):
    """
    A same-day window (start == end) is extracted inclusively.

    The CLI labels it "single day, inclusive" rather than the multi-day "(exclusive)"
    wording, matching the documented same-day behavior in extract().
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

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
                "2025-01-01",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "single day, inclusive" in result.output
    assert "(exclusive)" not in result.output


def test_extract_exclude_data_types_extracts_all_but_excluded(tmp_path):
    """
    --exclude-data-types resolves to every registry type except the excluded one(s),
    handed to extract_data as an explicit list (forward-compatible: types added later
    are still included).
    """
    from garmin_health_data.constants import GARMIN_DATA_REGISTRY

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    captured = {}

    def capture(*args, **kwargs):
        captured["data_types"] = kwargs.get("data_types")
        return _stub_extract_no_files()

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=capture),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path), "--exclude-data-types", "MENSTRUAL_CYCLE_DAY"],
        )

    assert result.exit_code == 0, result.output
    expected = [
        d.name
        for d in GARMIN_DATA_REGISTRY.all_data_types
        if d.name != "MENSTRUAL_CYCLE_DAY"
    ]
    assert captured["data_types"] == expected


def test_extract_rejects_data_types_with_exclude_data_types(tmp_path):
    """
    --data-types and --exclude-data-types are mutually exclusive; supplying both aborts
    before any extraction.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

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
                "--data-types",
                "SLEEP",
                "--exclude-data-types",
                "STRESS",
            ],
        )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output
    mock_extract.assert_not_called()


def test_extract_rejects_unknown_exclude_data_type(tmp_path):
    """
    An unknown --exclude-data-types name aborts with an error naming the offender.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

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
            ["--db-path", str(db_path), "--exclude-data-types", "NOTAREALTYPE"],
        )

    assert result.exit_code != 0
    assert "NOTAREALTYPE" in result.output
    mock_extract.assert_not_called()


def test_extract_rejects_excluding_all_data_types(tmp_path):
    """
    Excluding every registry type leaves nothing to extract, which aborts.
    """
    from garmin_health_data.constants import GARMIN_DATA_REGISTRY

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    exclude_args = []
    for data_type in GARMIN_DATA_REGISTRY.all_data_types:
        exclude_args += ["--exclude-data-types", data_type.name]

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch(
            "garmin_health_data.cli.extract_data",
            side_effect=_stub_extract_no_files,
        ) as mock_extract,
    ):
        result = runner.invoke(extract, ["--db-path", str(db_path), *exclude_args])

    assert result.exit_code != 0
    assert "nothing left to extract" in result.output
    mock_extract.assert_not_called()


def test_extract_exclude_data_types_prints_exclusion_summary(tmp_path):
    """
    In exclude mode the CLI names the excluded types rather than dumping the full
    resolved allowlist of every other type.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

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
            ["--db-path", str(db_path), "--exclude-data-types", "MENSTRUAL_CYCLE_DAY"],
        )

    assert result.exit_code == 0, result.output
    assert "Extracting all data types except: MENSTRUAL_CYCLE_DAY" in result.output

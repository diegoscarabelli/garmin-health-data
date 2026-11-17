"""
Smoke tests for the retention CLI commands (prune, downsample, vacuum, migrate-cascade)
and the extract auto-flag validation.

The SQL semantics are exhaustively covered by
``tests/retention/test_prune_and_downsample.py`` and
``tests/retention/test_migrate_cascade.py``. These tests assert only the Click surface:
parameter parsing, prompt flow, output formatting, and flag-pairing rules.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner
from sqlalchemy import insert
from sqlalchemy.orm import Session

from garmin_health_data.cli import cli
from garmin_health_data.db import create_tables, get_engine
from garmin_health_data.models import (
    Activity,
    ActivityTsMetric,
    ActivityTsMetricDownsampled,
    User,
)


def _seed_minimal_db(db_path: str) -> None:
    """
    Build a tiny database with one user, one activity, and a few ts-metric rows.

    Used by tests that need a non-empty database to exercise prune/downsample but don't
    care about the specific values; SQL behavior is tested elsewhere.

    :param db_path: Path to the SQLite database file.
    """
    create_tables(db_path)
    engine = get_engine(db_path)
    with Session(engine) as session:
        session.add(User(user_id=42))
        session.commit()
        start = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
        end = datetime(2026, 1, 5, 12, 10, tzinfo=timezone.utc)
        session.add(
            Activity(
                activity_id=1,
                user_id=42,
                activity_type_id=1,
                activity_type_key="running",
                event_type_id=1,
                event_type_key="other",
                start_ts=start,
                end_ts=end,
                timezone_offset_hours=0.0,
            )
        )
        session.commit()
        session.execute(
            insert(ActivityTsMetric),
            [
                {
                    "activity_id": 1,
                    "timestamp": datetime(2026, 1, 5, 12, 0, sec, tzinfo=timezone.utc),
                    "name": "heart_rate",
                    "value": 140.0 + sec,
                    "units": "bpm",
                }
                for sec in (5, 25, 55)
            ],
        )
        session.commit()
    engine.dispose()


# ---------------------------------------------------------------------------
# garmin prune
# ---------------------------------------------------------------------------


def test_prune_dry_run_reports_counts(tmp_path: Path):
    """
    --dry-run prints the row count and does not modify the database.
    """
    db = tmp_path / "garmin.db"
    _seed_minimal_db(str(db))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prune",
            "--db-path",
            str(db),
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-31",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output
    # Three ts-metric rows seeded across one activity.
    assert "3" in result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetric).count() == 3
    engine.dispose()


def test_prune_real_run_with_yes_flag(tmp_path: Path):
    """
    --yes skips the confirmation and deletes the matching rows.
    """
    db = tmp_path / "garmin.db"
    _seed_minimal_db(str(db))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prune",
            "--db-path",
            str(db),
            "--end-date",
            "2026-01-31",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetric).count() == 0
        # Activity itself preserved.
        assert session.query(Activity).count() == 1
    engine.dispose()


def test_prune_prompt_aborts_without_yes(tmp_path: Path):
    """
    Without --yes and with a 'no' response, the command aborts and writes nothing.
    """
    db = tmp_path / "garmin.db"
    _seed_minimal_db(str(db))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prune",
            "--db-path",
            str(db),
            "--end-date",
            "2026-01-31",
        ],
        input="n\n",
    )

    assert result.exit_code == 0, result.output
    assert "Aborted" in result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetric).count() == 3
    engine.dispose()


def test_prune_requires_end_date():
    """
    --end-date is required; omitting it must produce a Click usage error.
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["prune"])
    assert result.exit_code != 0
    assert "end-date" in result.output.lower()


def test_prune_missing_db_path_aborts_cleanly(tmp_path: Path):
    """
    Regression: a non-existent --db-path must produce a clean Click abort with a "run
    `garmin extract`" hint, not an unhandled FileNotFoundError traceback.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "prune",
            "--db-path",
            str(tmp_path / "does-not-exist.db"),
            "--end-date",
            "2026-01-31",
            "--yes",
        ],
    )
    assert result.exit_code != 0
    assert "Database not found" in result.output
    assert "garmin extract" in result.output


def test_downsample_missing_db_path_aborts_cleanly(tmp_path: Path):
    """
    Regression: same as the prune test for the downsample command.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "downsample",
            "--db-path",
            str(tmp_path / "does-not-exist.db"),
            "--end-date",
            "2026-01-31",
            "--time-grain",
            "60s",
            "--yes",
        ],
    )
    assert result.exit_code != 0
    assert "Database not found" in result.output


# ---------------------------------------------------------------------------
# garmin downsample
# ---------------------------------------------------------------------------


def test_downsample_dry_run_prints_strategy_table(tmp_path: Path):
    """
    Dry run emits the strategy table and does not insert any buckets.
    """
    db = tmp_path / "garmin.db"
    _seed_minimal_db(str(db))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "downsample",
            "--db-path",
            str(db),
            "--end-date",
            "2026-01-31",
            "--time-grain",
            "60s",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Strategy" in result.output
    assert "heart_rate" in result.output
    assert "Dry run" in result.output


def test_downsample_invalid_time_grain():
    """
    --time-grain rejects malformed values via the TimeGrain Click param type.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "downsample",
            "--end-date",
            "2026-01-31",
            "--time-grain",
            "1h",
        ],
    )
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# garmin migrate-cascade
# ---------------------------------------------------------------------------


def test_migrate_cascade_dry_run_on_fresh_db(tmp_path: Path):
    """
    On a freshly created DB (already has cascade everywhere), dry run reports nothing
    migrated and prints the deprecation banner.
    """
    db = tmp_path / "garmin.db"
    _seed_minimal_db(str(db))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "migrate-cascade",
            "--db-path",
            str(db),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "migrate-cascade is intended for one-time migration" in result.output
    assert "Dry run" in result.output


# ---------------------------------------------------------------------------
# extract auto flags
# ---------------------------------------------------------------------------


def test_extract_rejects_downsample_older_than_without_grain():
    """
    --downsample-older-than requires --downsample-grain; missing one is a hard
    error before any extraction work happens.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--downsample-older-than",
            "90d",
        ],
    )
    assert result.exit_code != 0
    assert "must be supplied together" in result.output


def test_extract_rejects_downsample_grain_without_older_than():
    """
    --downsample-grain requires --downsample-older-than; the inverse pairing
    error.
    """
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--downsample-grain",
            "60s",
        ],
    )
    assert result.exit_code != 0
    assert "must be supplied together" in result.output


def _seed_old_activity_db(db_path: Path) -> datetime:
    """
    Build a DB with one user and one activity dated ~400 days in the past plus a handful
    of ts-metric rows. Returns the activity's start_ts.

    :param db_path: Path to the SQLite database file.
    :return: The activity start_ts for assertions.
    """
    create_tables(str(db_path))
    engine = get_engine(str(db_path))
    old_start = datetime.now(timezone.utc) - timedelta(days=400)
    with Session(engine) as session:
        session.add(User(user_id=99))
        session.commit()
        session.add(
            Activity(
                activity_id=999,
                user_id=99,
                activity_type_id=1,
                activity_type_key="running",
                event_type_id=1,
                event_type_key="other",
                start_ts=old_start,
                end_ts=old_start + timedelta(minutes=10),
                timezone_offset_hours=0.0,
            )
        )
        session.commit()
        session.execute(
            insert(ActivityTsMetric),
            [
                {
                    "activity_id": 999,
                    "timestamp": old_start + timedelta(seconds=sec),
                    "name": "heart_rate",
                    "value": 140.0 + sec,
                    "units": "bpm",
                }
                for sec in (5, 25, 55)
            ],
        )
        session.commit()
    engine.dispose()
    return old_start


def test_extract_auto_prune_actually_runs(tmp_path: Path):
    """
    `extract --process-only --prune-older-than` deletes activity_ts_metric rows for
    activities older than today minus DURATION before any extraction work happens.

    End-to-end coverage of the auto-flag wiring (the validation tests above only
    exercise the error paths).
    """
    db = tmp_path / "garmin.db"
    _seed_old_activity_db(db)
    # Lifecycle dirs so --process-only does not crash on missing folders;
    # ingest/ stays empty so the actual extraction step is a no-op.
    (tmp_path / "garmin_files" / "ingest").mkdir(parents=True)
    (tmp_path / "garmin_files" / "process").mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--db-path",
            str(db),
            "--prune-older-than",
            "30d",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Auto prune" in result.output
    assert "Deleted" in result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetric).count() == 0
        # Activity itself is preserved.
        assert session.query(Activity).count() == 1
    engine.dispose()


def test_extract_rejects_extract_only_with_retention_flags(tmp_path: Path):
    """
    `--extract-only` promises "download files, no DB writes".

    Combining it with `--prune-older-than` or `--downsample-older-than` is incoherent
    and would silently mutate the database; reject the combination up front.
    """
    db = tmp_path / "garmin.db"
    create_tables(str(db))
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--db-path",
            str(db),
            "--extract-only",
            "--prune-older-than",
            "30d",
        ],
    )
    assert result.exit_code != 0
    assert "extract-only is incompatible" in result.output


def test_extract_auto_prune_scopes_to_accounts(tmp_path: Path):
    """
    `--accounts X --prune-older-than ...` must scope the auto-prune to only the listed
    users, not all users in the database.
    """
    db = tmp_path / "garmin.db"
    create_tables(str(db))
    engine = get_engine(str(db))
    old_start = datetime.now(timezone.utc) - timedelta(days=400)
    with Session(engine) as session:
        session.add(User(user_id=10))
        session.add(User(user_id=20))
        session.commit()
        # Two activities in the past, one per user.
        for activity_id, user_id in ((1, 10), (2, 20)):
            session.add(
                Activity(
                    activity_id=activity_id,
                    user_id=user_id,
                    activity_type_id=1,
                    activity_type_key="running",
                    event_type_id=1,
                    event_type_key="other",
                    start_ts=old_start,
                    end_ts=old_start + timedelta(minutes=10),
                    timezone_offset_hours=0.0,
                )
            )
        session.commit()
        for activity_id in (1, 2):
            session.execute(
                insert(ActivityTsMetric),
                [
                    {
                        "activity_id": activity_id,
                        "timestamp": old_start,
                        "name": "heart_rate",
                        "value": 140.0,
                        "units": "bpm",
                    }
                ],
            )
        session.commit()
    engine.dispose()

    (tmp_path / "garmin_files" / "ingest").mkdir(parents=True)
    (tmp_path / "garmin_files" / "process").mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--db-path",
            str(db),
            "--accounts",
            "10",
            "--prune-older-than",
            "30d",
        ],
    )
    assert result.exit_code == 0, result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        # Only user 10's activity should have its ts-metric rows pruned.
        assert session.query(ActivityTsMetric).filter_by(activity_id=1).count() == 0
        assert session.query(ActivityTsMetric).filter_by(activity_id=2).count() == 1
    engine.dispose()


def test_extract_auto_downsample_actually_runs(tmp_path: Path):
    """
    `extract --process-only --downsample-older-than --downsample-grain` runs the
    downsample before extraction and writes buckets to the new
    activity_ts_metric_downsampled table.
    """
    db = tmp_path / "garmin.db"
    _seed_old_activity_db(db)
    (tmp_path / "garmin_files" / "ingest").mkdir(parents=True)
    (tmp_path / "garmin_files" / "process").mkdir(parents=True)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--db-path",
            str(db),
            "--downsample-older-than",
            "30d",
            "--downsample-grain",
            "60s",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Auto downsample" in result.output

    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetricDownsampled).count() == 1
        bucket = session.query(ActivityTsMetricDownsampled).one()
        assert bucket.name == "heart_rate"
        assert bucket.bucket_seconds == 60
        assert bucket.sample_count == 3
    engine.dispose()


# ---------------------------------------------------------------------------
# Vacuum after prune, and garmin vacuum
# ---------------------------------------------------------------------------


def _seed_bulky_db(
    db_path: Path, start: datetime, rows: int = 5000, activity_id: int = 999
) -> None:
    """
    Add one activity holding enough ts-metric rows to span many pages, so a prune frees
    space that a vacuum can measurably reclaim. Safe to call repeatedly on the same
    database with distinct activity ids.

    :param db_path: Path to the SQLite database file.
    :param start: Activity start_ts.
    :param rows: Number of per-second heart-rate rows to insert.
    :param activity_id: Activity primary key.
    """
    create_tables(str(db_path))
    engine = get_engine(str(db_path))
    with Session(engine) as session:
        session.merge(User(user_id=99))
        session.commit()
        session.add(
            Activity(
                activity_id=activity_id,
                user_id=99,
                activity_type_id=1,
                activity_type_key="running",
                event_type_id=1,
                event_type_key="other",
                start_ts=start,
                end_ts=start + timedelta(seconds=rows),
                timezone_offset_hours=0.0,
            )
        )
        session.commit()
        session.execute(
            insert(ActivityTsMetric),
            [
                {
                    "activity_id": activity_id,
                    "timestamp": start + timedelta(seconds=sec),
                    "name": "heart_rate",
                    "value": 140.0,
                    "units": "bpm",
                }
                for sec in range(rows)
            ],
        )
        session.commit()
    engine.dispose()


_JAN_START = datetime(2026, 1, 5, 12, 0, tzinfo=timezone.utc)
_PRUNE_ARGS = ["prune", "--end-date", "2026-01-31", "--yes"]
_NO_DISK = SimpleNamespace(total=1, used=1, free=0)


def test_prune_vacuums_by_default(tmp_path: Path):
    """
    A real prune that deletes rows vacuums afterwards and reports the reclaimed size.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    size_before = db.stat().st_size

    result = CliRunner().invoke(cli, _PRUNE_ARGS + ["--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    assert "Reclaimed" in result.output
    assert db.stat().st_size < size_before


def test_prune_no_vacuum_leaves_size_unchanged(tmp_path: Path):
    """
    --no-vacuum deletes the rows but skips the vacuum, so the file keeps its size.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    size_before = db.stat().st_size

    result = CliRunner().invoke(
        cli, _PRUNE_ARGS + ["--db-path", str(db), "--no-vacuum"]
    )

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    assert "Vacuuming" not in result.output
    assert db.stat().st_size == size_before


def test_prune_dry_run_does_not_vacuum(tmp_path: Path):
    """
    --dry-run mentions the vacuum a real run would do but does not run it.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = CliRunner().invoke(
            cli,
            [
                "prune",
                "--db-path",
                str(db),
                "--end-date",
                "2026-01-31",
                "--dry-run",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "would then vacuum" in result.output
    vacuum.assert_not_called()


def test_prune_nothing_to_prune_does_not_vacuum(tmp_path: Path):
    """
    When no rows are in range, prune returns early without vacuuming.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = CliRunner().invoke(
            cli,
            ["prune", "--db-path", str(db), "--end-date", "2025-01-01", "--yes"],
        )

    assert result.exit_code == 0, result.output
    assert "Nothing to prune" in result.output
    vacuum.assert_not_called()


def test_prune_insufficient_disk_skips_vacuum(tmp_path: Path):
    """
    Short free disk space skips the vacuum with a hint, and prune still succeeds.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    size_before = db.stat().st_size

    with patch(
        "garmin_health_data.retention.operations.shutil.disk_usage",
        return_value=_NO_DISK,
    ):
        result = CliRunner().invoke(cli, _PRUNE_ARGS + ["--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    assert "Skipped vacuum: not enough free disk space" in result.output
    assert "garmin vacuum" in result.output
    assert db.stat().st_size == size_before
    engine = get_engine(str(db))
    with Session(engine) as session:
        assert session.query(ActivityTsMetric).count() == 0
    engine.dispose()


def test_prune_vacuum_failure_is_a_warning(tmp_path: Path):
    """
    A SQLite error during the vacuum is reported as a warning; the committed prune
    stands and the command exits 0.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)

    with patch(
        "garmin_health_data.cli.vacuum_database",
        side_effect=sqlite3.OperationalError("database is locked"),
    ):
        result = CliRunner().invoke(cli, _PRUNE_ARGS + ["--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    assert "Vacuum failed (database is locked)" in result.output
    assert "garmin vacuum" in result.output


def test_vacuum_command_reclaims_space(tmp_path: Path):
    """
    `garmin vacuum` shrinks a database that was pruned without vacuuming.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    runner = CliRunner()
    pruned = runner.invoke(cli, _PRUNE_ARGS + ["--db-path", str(db), "--no-vacuum"])
    assert pruned.exit_code == 0, pruned.output
    size_before = db.stat().st_size

    result = runner.invoke(cli, ["vacuum", "--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Reclaimed" in result.output
    assert db.stat().st_size < size_before


def test_vacuum_command_missing_db_aborts_cleanly(tmp_path: Path):
    """
    A non-existent --db-path aborts with the same hint as the other retention commands.
    """
    result = CliRunner().invoke(
        cli, ["vacuum", "--db-path", str(tmp_path / "does-not-exist.db")]
    )
    assert result.exit_code != 0
    assert "Database not found" in result.output
    assert "garmin extract" in result.output


def test_vacuum_command_insufficient_disk_aborts(tmp_path: Path):
    """
    `garmin vacuum` with short free disk space exits non-zero without writing.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START)
    size_before = db.stat().st_size

    with patch(
        "garmin_health_data.retention.operations.shutil.disk_usage",
        return_value=_NO_DISK,
    ):
        result = CliRunner().invoke(cli, ["vacuum", "--db-path", str(db)])

    assert result.exit_code != 0
    assert "Not enough free disk space" in result.output
    assert db.stat().st_size == size_before


def test_vacuum_command_sqlite_error_aborts(tmp_path: Path):
    """
    `garmin vacuum` exits non-zero with a clear message when SQLite fails.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, _JAN_START, rows=10)

    with patch(
        "garmin_health_data.cli.vacuum_database",
        side_effect=sqlite3.OperationalError("database is locked"),
    ):
        result = CliRunner().invoke(cli, ["vacuum", "--db-path", str(db)])

    assert result.exit_code != 0
    assert "Vacuum failed (database is locked)" in result.output


def _invoke_extract_prune(tmp_path: Path, *extra: str):
    """
    Run `extract --process-only --prune-older-than 30d` against an empty ingest/.

    :param tmp_path: Directory holding the database and lifecycle folders.
    :param extra: Additional CLI arguments.
    :return: Click result.
    """
    (tmp_path / "garmin_files" / "ingest").mkdir(parents=True)
    (tmp_path / "garmin_files" / "process").mkdir(parents=True)
    return CliRunner().invoke(
        cli,
        [
            "extract",
            "--process-only",
            "--db-path",
            str(tmp_path / "garmin.db"),
            "--prune-older-than",
            "30d",
            *extra,
        ],
    )


def test_extract_auto_prune_vacuums(tmp_path: Path):
    """
    `extract --prune-older-than` vacuums after deleting rows.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, datetime.now(timezone.utc) - timedelta(days=400))
    size_before = db.stat().st_size

    result = _invoke_extract_prune(tmp_path)

    assert result.exit_code == 0, result.output
    assert "Reclaimed" in result.output
    assert db.stat().st_size < size_before


def test_extract_auto_prune_no_vacuum(tmp_path: Path):
    """
    `extract --prune-older-than --no-vacuum` deletes rows without vacuuming.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, datetime.now(timezone.utc) - timedelta(days=400))

    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = _invoke_extract_prune(tmp_path, "--no-vacuum")

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    vacuum.assert_not_called()


def test_extract_auto_prune_nothing_deleted_does_not_vacuum(tmp_path: Path):
    """
    When the auto prune deletes nothing, extract does not vacuum.
    """
    db = tmp_path / "garmin.db"
    _seed_bulky_db(db, datetime.now(timezone.utc) - timedelta(days=1), rows=10)

    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = _invoke_extract_prune(tmp_path)

    assert result.exit_code == 0, result.output
    vacuum.assert_not_called()


def _seed_small_prune_db(db_path: Path, small_start: datetime) -> None:
    """
    Seed a small activity to prune next to a large recent one that stays, so the prune
    frees well under the auto-vacuum threshold.

    :param db_path: Path to the SQLite database file.
    :param small_start: Start of the small activity that the prune will hit.
    """
    _seed_bulky_db(db_path, small_start, rows=500, activity_id=1)
    _seed_bulky_db(
        db_path,
        datetime.now(timezone.utc) - timedelta(days=1),
        rows=20000,
        activity_id=2,
    )


def _freelist_count(db_path: Path) -> int:
    """
    Read the database's free-page count.

    :param db_path: Path to the SQLite database file.
    :return: Number of pages on the free list.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute("PRAGMA freelist_count").fetchone()[0]
    finally:
        conn.close()


def test_prune_below_threshold_skips_vacuum(tmp_path: Path):
    """
    When the prune frees under the threshold, the vacuum is skipped with a note.
    """
    db = tmp_path / "garmin.db"
    _seed_small_prune_db(db, _JAN_START)

    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = CliRunner().invoke(cli, _PRUNE_ARGS + ["--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output
    assert "Skipped vacuum: only" in result.output
    assert "garmin vacuum" in result.output
    vacuum.assert_not_called()


def test_extract_auto_prune_below_threshold_skips_vacuum(tmp_path: Path):
    """
    `extract --prune-older-than` also skips the vacuum below the threshold.
    """
    db = tmp_path / "garmin.db"
    _seed_small_prune_db(db, datetime.now(timezone.utc) - timedelta(days=400))

    with patch("garmin_health_data.cli.vacuum_database") as vacuum:
        result = _invoke_extract_prune(tmp_path)

    assert result.exit_code == 0, result.output
    assert "Skipped vacuum: only" in result.output
    vacuum.assert_not_called()


def test_vacuum_command_ignores_threshold(tmp_path: Path):
    """
    `garmin vacuum` runs even when free pages are under the auto-vacuum threshold.
    """
    db = tmp_path / "garmin.db"
    _seed_small_prune_db(db, _JAN_START)
    runner = CliRunner()
    pruned = runner.invoke(cli, _PRUNE_ARGS + ["--db-path", str(db), "--no-vacuum"])
    assert pruned.exit_code == 0, pruned.output
    assert _freelist_count(db) > 0

    result = runner.invoke(cli, ["vacuum", "--db-path", str(db)])

    assert result.exit_code == 0, result.output
    assert "Reclaimed" in result.output
    assert _freelist_count(db) == 0

"""
Smoke tests for the retention CLI commands (prune, downsample, migrate-cascade) and the
extract auto-flag validation.

The SQL semantics are exhaustively covered by
``tests/retention/test_prune_and_downsample.py`` and
``tests/retention/test_migrate_cascade.py``. These tests assert only the Click surface:
parameter parsing, prompt flow, output formatting, and flag-pairing rules.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

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

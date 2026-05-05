"""
Command-line interface for garmin-health-data.
"""

import logging
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import click
from sqlalchemy import text

from garmin_health_data.__version__ import __version__
from garmin_health_data.auth import (
    ensure_authenticated,
    get_credentials,
    refresh_tokens,
)
from garmin_health_data.constants import GARMIN_FILE_TYPES
from garmin_health_data.db import (
    create_tables,
    database_exists,
    get_database_size,
    get_last_update_dates,
    get_latest_date,
    get_record_counts,
    get_session,
    initialize_database,
)
from garmin_health_data.extractor import extract as extract_data
from garmin_health_data.lifecycle import (
    LockHeldError,
    acquire_lock,
    move_files_to_quarantine,
    move_files_to_storage,
    move_ingest_to_process,
    recover_stale_process,
    setup_lifecycle_dirs,
)
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet
from garmin_health_data.retention.operations import (
    downsample_activities,
    migrate_cascade,
    prune_ts_metrics,
)
from garmin_health_data.retention.parsers import DURATION, TIME_GRAIN
from garmin_health_data.retention.strategies import format_strategy_table
from garmin_health_data.utils import format_count, format_date, format_file_size
from garmin_health_data.version_check import check_for_newer_version

# Filename timestamp pattern shared by all extracted JSON / FIT / TCX / GPX
# / KML files. Used to group files into per-(user_id, timestamp) FileSets.
_TIMESTAMP_REGEX = (
    r"\d{4}-\d{2}-\d{2}T\d{2}[:\-]\d{2}[:\-]\d{2}"
    r"(?:\.\d{1,6})?(?:[+-]\d{2}[:\-]\d{2}|Z)?"
)


@click.group(invoke_without_command=True)
@click.version_option(version=__version__)
@click.pass_context
def cli(ctx: click.Context):
    """
    Garmin Connect health data extraction tool.

    Extract your complete Garmin Connect health data to a local SQLite database.
    """
    # Show INFO-level messages from our own code (e.g. login delay warnings)
    # without exposing noisy INFO output from third-party libraries.
    # Guard against duplicate handlers when the CLI entrypoint is invoked
    # multiple times in-process (e.g. in tests). propagate=False prevents
    # double-printing via the root logger.
    _log = logging.getLogger("garmin_health_data")
    if not any(isinstance(h, logging.StreamHandler) for h in _log.handlers):
        _handler = logging.StreamHandler()
        _handler.setFormatter(logging.Formatter("%(message)s"))
        _log.addHandler(_handler)
    _log.setLevel(logging.INFO)
    _log.propagate = False

    # Hint when a newer version is available on PyPI. Cached for 24h, opt-out
    # via GARMIN_NO_VERSION_CHECK=1, never blocks more than ~2s, never aborts
    # the user's command.
    check_for_newer_version()

    # invoke_without_command=True is set so the version check above fires on
    # bare `garmin` too. Click no longer prints help automatically in that
    # case, so render it here and exit.
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        ctx.exit(0)


@cli.command()
@click.option(
    "--email",
    envvar="GARMIN_EMAIL",
    help="Garmin Connect email (or set GARMIN_EMAIL env var)",
)
@click.option(
    "--password",
    envvar="GARMIN_PASSWORD",
    help="Garmin Connect password (or set GARMIN_PASSWORD env var)",
)
def auth(email: Optional[str], password: Optional[str]):
    """
    Authenticate with Garmin Connect and save tokens.
    """
    if email and password:
        # Use provided credentials.
        click.echo("Using provided credentials...")
    else:
        # Prompt for credentials.
        email, password = get_credentials()

    refresh_tokens(email, password)


@cli.command()
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD). Auto-detected if not provided.",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD), EXCLUSIVE — data from this date is not "
    "included (except when start and end are the same day, in which case "
    "that single day is extracted). Defaults to today.",
)
@click.option(
    "--data-types",
    multiple=True,
    help="Specific data types to extract (can specify multiple times). "
    "Extracts all if not specified.",
)
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.option(
    "--accounts",
    multiple=True,
    help="Garmin user IDs to extract (comma-separated or repeated). "
    "Examples: --accounts 123,456 or --accounts 123 --accounts 456. "
    "Extracts all discovered accounts if not specified.",
)
@click.option(
    "--extract-only",
    is_flag=True,
    default=False,
    help="Download files into ingest/ and stop. Do not move to process/ "
    "or load into the database.",
)
@click.option(
    "--process-only",
    is_flag=True,
    default=False,
    help="Skip extraction. Process whatever files are currently in ingest/.",
)
@click.option(
    "--downsample-older-than",
    "downsample_older_than",
    type=DURATION,
    help="Before extracting, downsample activity_ts_metric rows for activities "
    "older than DURATION (e.g., 90d, 6m, 1y). Requires --downsample-grain. The "
    "cutoff date is computed as `today - DURATION` and used as an exclusive "
    "end date for the downsample run.",
)
@click.option(
    "--downsample-grain",
    "downsample_grain",
    type=TIME_GRAIN,
    help="Bucket grain used by --downsample-older-than. Same format as the "
    "standalone `garmin downsample` command (e.g., 60s, 5m, 15m).",
)
@click.option(
    "--prune-older-than",
    "prune_older_than",
    type=DURATION,
    help="Before extracting, delete activity_ts_metric rows for activities "
    "older than DURATION (e.g., 90d, 6m, 1y). Runs after --downsample-older-than "
    "if both are given, so today's prune does not strand a bucket aggregation.",
)
def extract(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    data_types: tuple,
    db_path: str,
    accounts: tuple,
    extract_only: bool,
    process_only: bool,
    downsample_older_than,
    downsample_grain: Optional[int],
    prune_older_than,
):
    """
    Extract Garmin Connect data and save to SQLite database.

    Files flow through a four-folder lifecycle next to the database:
    ingest/ -> process/ -> storage/ (success) or quarantine/ (failure).
    Files are preserved on disk by default for offline backup and
    post-mortem inspection.
    """
    if extract_only and process_only:
        click.secho(
            "❌ --extract-only and --process-only are mutually exclusive.",
            fg="red",
        )
        raise click.Abort()

    # Validate auto retention flag pairings: downsample requires both flags.
    # Single missing flag is a user error and aborts before any work.
    if (downsample_older_than is None) != (downsample_grain is None):
        click.secho(
            "❌ --downsample-older-than and --downsample-grain must be "
            "supplied together (or both omitted).",
            fg="red",
        )
        raise click.Abort()

    # --extract-only promises "download files and stop, no DB writes". Auto
    # retention is a DB write, so combining the two is incoherent and would
    # silently mutate the database the user just asked us to leave alone.
    # Reject the combination up front.
    if extract_only and (
        downsample_older_than is not None or prune_older_than is not None
    ):
        click.secho(
            "❌ --extract-only is incompatible with --prune-older-than and "
            "--downsample-older-than (those flags write to the database; "
            "--extract-only is for download-only runs).",
            fg="red",
        )
        raise click.Abort()

    # Authentication is only required when we will hit the Garmin API.
    if not process_only:
        ensure_authenticated()

    # Initialize or migrate database schema.
    if not database_exists(db_path):
        click.echo()
        click.echo(click.style("🗄️  Initializing new database...", fg="cyan"))
        initialize_database(db_path)
        click.echo()
    else:
        # Idempotent migration: creates new tables if schema was
        # updated, existing tables are untouched.
        create_tables(db_path)

    # Date auto-detection and extract-only logging are skipped when running
    # in --process-only mode (no API calls, dates would be unused).
    data_types_list = list(data_types) if data_types else None
    accounts_list = (
        [a.strip() for raw in accounts for a in raw.split(",") if a.strip()]
        if accounts
        else None
    )
    # Auto retention scopes to the same accounts the extract run targets.
    # The retention helpers expect integer user_ids while the extractor
    # surface uses strings, so do the conversion here. Invalid values are
    # surfaced as a Click usage error rather than silently dropped.
    retention_user_ids: Optional[list] = None
    if accounts_list:
        try:
            retention_user_ids = [int(a) for a in accounts_list]
        except ValueError as exc:
            raise click.BadParameter(
                f"--accounts contains a non-integer user_id: {exc}"
            ) from exc

    if not process_only:
        # Auto-detect start date if not provided.
        if start_date is None:
            latest = get_latest_date(db_path)
            if latest:
                # Start from day after last update.
                start_date = datetime.combine(
                    latest + timedelta(days=1), datetime.min.time()
                )
                click.echo(
                    click.style(
                        f"📅 Auto-detected start date: "
                        f"{format_date(start_date.date())} "
                        f"(day after last update)",
                        fg="cyan",
                    )
                )
            else:
                # Default to 30 days ago for new database.
                start_date = datetime.now() - timedelta(days=30)
                click.echo(
                    click.style(
                        f"📅 Using default start date: "
                        f"{format_date(start_date.date())} (30 days ago)",
                        fg="cyan",
                    )
                )

        # Default end date to today.
        if end_date is None:
            end_date = datetime.now()

        if data_types_list:
            click.echo(f"📊 Extracting data types: {', '.join(data_types_list)}")
        else:
            click.echo("📊 Extracting all available data types")

        if accounts_list:
            click.echo(f"👤 Filtering accounts: {', '.join(accounts_list)}")
        else:
            click.echo("👤 Extracting all discovered accounts")

        click.echo(
            f"📆 Date range: {format_date(start_date.date())} (inclusive) "
            f"to {format_date(end_date.date())} (exclusive)"
        )
        click.echo()
    else:
        click.echo("📦 Process-only mode: loading existing files in ingest/.")
        click.echo()

    # Set up the four-folder lifecycle next to the database.
    files_root = Path(db_path).expanduser().resolve().parent / "garmin_files"
    setup_lifecycle_dirs(files_root)
    ingest_dir = files_root / "ingest"
    process_dir = files_root / "process"
    click.echo(f"💾 Files directory: {files_root}")

    # Acquire the lifecycle lock so a second concurrent run aborts cleanly.
    # The entire run lives inside the 'with' block so the lock is
    # released even if the body raises (no manual __enter__/__exit__).
    try:
        with acquire_lock(files_root):
            # Recover any files left in process/ from a previously crashed run.
            recovered = recover_stale_process(files_root)
            if recovered:
                click.secho(
                    f"♻️  Recovered {recovered} file(s) from a previous run "
                    f"(process/ → ingest/).",
                    fg="cyan",
                )

            # Auto retention runs INSIDE the lifecycle lock so two concurrent
            # `garmin extract` runs cannot both start mutating the database
            # before one of them loses the lock. Downsample runs first: if the
            # user asked for both, today's prune must not strand the bucket
            # aggregation that needed the source rows.
            today = datetime.now().date()
            if downsample_older_than is not None:
                cutoff = today - downsample_older_than
                click.secho(
                    f"📉 Auto downsample: activities with start_ts < "
                    f"{format_date(cutoff)} at {downsample_grain}s grain.",
                    fg="cyan",
                )
                ds_result = downsample_activities(
                    db_path,
                    time_grain_seconds=downsample_grain,
                    end=cutoff,
                    user_ids=retention_user_ids,
                )
                # Print the strategy table so users see the per-metric
                # classification a cron run is about to commit. A misclassified
                # future metric can be patched in a follow-up release; the
                # source rows are preserved until prune, so re-running
                # downsample with the corrected registry recovers cleanly.
                if ds_result["metric_strategies"]:
                    click.echo(
                        format_strategy_table(
                            [name for name, _ in ds_result["metric_strategies"]]
                        )
                    )
                click.echo(
                    f"   {ds_result['activity_count']} activit"
                    f"{'y' if ds_result['activity_count'] == 1 else 'ies'} "
                    f"processed; replaced "
                    f"{format_count(ds_result['rows_deleted'])} prior rows "
                    f"with {format_count(ds_result['rows_inserted'])} new "
                    f"bucket rows."
                )

            if prune_older_than is not None:
                cutoff = today - prune_older_than
                click.secho(
                    f"🗑️  Auto prune: activity_ts_metric with start_ts < "
                    f"{format_date(cutoff)}.",
                    fg="cyan",
                )
                pr_result = prune_ts_metrics(
                    db_path, end=cutoff, user_ids=retention_user_ids
                )
                click.echo(
                    f"   Deleted {format_count(pr_result['rows_affected'])} "
                    f"rows across {pr_result['activity_count']} activit"
                    f"{'y' if pr_result['activity_count'] == 1 else 'ies'}."
                )

            # ---------------------------------------------------------------- Step 1: Extract.
            result = {
                "garmin_files": 0,
                "activity_files": 0,
                "failures": [],
                "failed_accounts": [],
            }
            if not process_only:
                click.echo(
                    click.style(
                        "🔄 Step 1/3: Extracting data from Garmin Connect...",
                        fg="cyan",
                        bold=True,
                    )
                )
                click.echo()

                result = extract_data(
                    ingest_dir=ingest_dir,
                    data_interval_start=format_date(start_date.date()),
                    data_interval_end=format_date(end_date.date()),
                    data_types=data_types_list,
                    accounts=accounts_list,
                )

                garmin_files = result.get("garmin_files", 0)
                activity_files = result.get("activity_files", 0)
                total_files = garmin_files + activity_files

                click.echo()
                click.secho(
                    f"✅ Extracted {format_count(total_files)} files", fg="green"
                )
                click.echo(f"   • Garmin data files: {format_count(garmin_files)}")
                click.echo(f"   • Activity files: {format_count(activity_files)}")
                click.echo()

            if extract_only:
                _print_extraction_failures(result.get("failures", []))
                click.echo()
                click.secho(
                    "✅ Extraction-only mode: files left in ingest/. "
                    "Run 'garmin extract --process-only' to load them into "
                    "the database.",
                    fg="green",
                )
                return

            # ---------------------------------------------------------------- Step 2: Process.
            click.echo(
                click.style(
                    "🔄 Step 2/3: Processing data and loading into database...",
                    fg="cyan",
                    bold=True,
                )
            )
            click.echo()

            # Move every file from ingest/ to process/ before parsing.
            moved = move_ingest_to_process(files_root)
            click.echo(f"📦 Moved {format_count(moved)} file(s) ingest/ → process/.")

            # Pre-routing (mirrors openetl's `ingest` task): files that
            # match no known processor type (e.g. TCX/GPX/KML activity
            # formats, anything unrecognised) are still real Garmin data
            # the user wanted preserved. Route them straight to storage/
            # before any FileSet grouping. Without this they would loop
            # ingest <-> process forever via the next run's recovery +
            # bulk-move; with it they reach a terminal state immediately.
            all_in_process = [p for p in process_dir.iterdir() if p.is_file()]
            processable, backup_only = _partition_processable_and_backup(all_in_process)
            total_backup_only = 0
            if backup_only:
                click.secho(
                    f"💾 Archiving {format_count(len(backup_only))} "
                    f"backup-only file(s) to storage (no processor type "
                    f"matched): "
                    f"{', '.join(p.name for p in backup_only[:5])}"
                    + (
                        f" (+{len(backup_only) - 5} more)"
                        if len(backup_only) > 5
                        else ""
                    ),
                    fg="cyan",
                )
                # Same warn-and-continue treatment as the per-FileSet
                # storage move: an IO failure here must not abort the
                # whole run. Files stay in process/; the next run's
                # recovery + bulk-move will surface them again.
                try:
                    move_files_to_storage(backup_only, files_root)
                    total_backup_only = len(backup_only)
                except OSError as e:
                    click.secho(
                        f"⚠️  Move-to-storage for backup-only files "
                        f"failed: {type(e).__name__}: {e}. Files remain "
                        f"in process/; the next run will recover them.",
                        fg="yellow",
                    )
            total_processed = 0
            total_quarantined = 0
            if processable:
                files_by_key = _group_files_by_user_and_timestamp(processable)

                num_filesets = len(files_by_key)
                click.echo()
                plural = "s" if num_filesets != 1 else ""
                click.secho(
                    f"📦 Processing {format_count(num_filesets)} file set{plural} "
                    f"(grouped by account and timestamp)",
                    fg="cyan",
                    bold=True,
                )
                click.echo()

                # Per-FileSet: own session, try/except, route to storage/quarantine.
                for (uid, timestamp_str), timestamp_files in files_by_key.items():
                    files_by_type = _classify_files_by_type(timestamp_files)
                    if not files_by_type:
                        # Should be unreachable since we pre-filtered above;
                        # belt-and-suspenders guard.
                        continue

                    matched_paths = [
                        p for paths in files_by_type.values() for p in paths
                    ]
                    file_set = FileSet(file_paths=matched_paths, files=files_by_type)

                    # Phase A — DB load. get_session() commits on clean
                    # exit and rolls back on exception, so no explicit
                    # commit/rollback is needed here. A processing failure
                    # routes the FileSet to quarantine.
                    db_load_failed = False
                    try:
                        with get_session(db_path) as session:
                            processor = GarminProcessor(file_set, session)
                            processor.process_file_set(file_set, session)
                    except Exception as e:
                        click.secho(
                            f"❌ FileSet {uid}/{timestamp_str} DB load "
                            f"failed: {type(e).__name__}: {e}. "
                            f"Moving to quarantine.",
                            fg="red",
                        )
                        move_files_to_quarantine(matched_paths, files_root)
                        total_quarantined += len(matched_paths)
                        db_load_failed = True

                    if db_load_failed:
                        continue

                    # Phase B — file move. The DB transaction has already
                    # committed; if the move fails (disk full, permission,
                    # etc.) we must NOT quarantine, because the data is
                    # already loaded. Leave the files in process/ and warn:
                    # the next run's recovery + bulk-move will surface them
                    # again, and the upserts will be idempotent no-ops.
                    try:
                        move_files_to_storage(matched_paths, files_root)
                        total_processed += len(matched_paths)
                    except OSError as e:
                        click.secho(
                            f"⚠️  FileSet {uid}/{timestamp_str}: DB load "
                            f"succeeded but move-to-storage failed: "
                            f"{type(e).__name__}: {e}. Files remain in "
                            f"process/; the next run will recover and "
                            f"re-upsert (no-op).",
                            fg="yellow",
                        )

                click.echo()
                click.secho(
                    f"✅ Processed {format_count(total_processed)} file(s); "
                    f"❌ quarantined {format_count(total_quarantined)} file(s).",
                    fg="green" if total_quarantined == 0 else "yellow",
                )
            else:
                click.secho("⚠️  No files to process", fg="yellow")

            click.echo()

            # ---------------------------------------------------------------- Step 3: Summary.
            click.echo(click.style("📊 Step 3/3: Summary", fg="cyan", bold=True))
            click.echo()

            _print_extraction_failures(result.get("failures", []))

            failed_accounts = result.get("failed_accounts", [])
            if failed_accounts:
                click.secho(
                    f"❌ Account-level extraction failures "
                    f"({len(failed_accounts)}): "
                    f"{', '.join(failed_accounts)}. "
                    f"These accounts were skipped entirely; check the "
                    f"per-account logs above for details.",
                    fg="red",
                    bold=True,
                )
                click.echo()

            click.echo("File lifecycle this run:")
            click.echo(f"   • Loaded into DB: {format_count(total_processed)}")
            click.echo(
                f"   • Archived as backup-only "
                f"(no processor type): {format_count(total_backup_only)}"
            )
            click.echo(
                f"   • Quarantined (processing failed): {format_count(total_quarantined)}"
            )
            click.echo()

            counts = get_record_counts(db_path)
            db_size = get_database_size(db_path)

            click.echo("Database statistics:")
            click.echo(f"   • Database size: {format_file_size(db_size)}")
            click.echo(f"   • Activities: {format_count(counts.get('activities', 0))}")
            click.echo(
                f"   • Sleep sessions: {format_count(counts.get('sleep_sessions', 0))}"
            )
            hr_count = format_count(counts.get("heart_rate_readings", 0))
            click.echo(f"   • Heart rate readings: {hr_count}")
            click.echo(
                f"   • Stress readings: {format_count(counts.get('stress_readings', 0))}"
            )
            click.echo()

            click.secho("🎉 Extraction complete!", fg="green", bold=True)
            click.echo(f"   Your data has been saved to: {db_path}")
            click.echo(f"   Original files preserved at: {files_root}")
            click.echo()
            click.echo("💡 Next steps:")
            click.echo("   • Run 'garmin info' to see detailed statistics")
            click.echo("   • Query the database with your favorite SQLite tool")
            click.echo("   • Run 'garmin extract' again later to update with new data")
            click.echo(
                "   • Inspect 'garmin_files/quarantine/' for any files that failed "
                "to process"
            )

    except LockHeldError as e:
        click.secho(f"❌ {e}", fg="red")
        raise click.Abort()


def _group_files_by_user_and_timestamp(
    file_paths: list,
) -> "OrderedDict[tuple, list[Path]]":
    """
    Group files into per-(user_id, timestamp) FileSets.

    Each Garmin extracted file is named ``<user_id>_<DATA_TYPE>_<timestamp>.<ext>``.
    Files sharing the same ``(user_id, timestamp)`` represent one day's worth of data
    for one account and are processed together as a FileSet.

    :param file_paths: Iterable of file Paths in process/.
    :return: OrderedDict mapping ``(user_id, timestamp_str)`` to a list of Paths, sorted
        by key for deterministic processing order.
    """
    files_by_key: "OrderedDict[tuple, list]" = OrderedDict()
    for file_path in file_paths:
        parts = file_path.name.split("_", maxsplit=1)
        user_id_prefix = parts[0] if len(parts) > 1 else "unknown"
        match = re.search(_TIMESTAMP_REGEX, file_path.name)
        if match:
            key = (user_id_prefix, match.group(0))
            files_by_key.setdefault(key, []).append(file_path)
        else:
            click.secho(
                f"No timestamp found in filename: {file_path.name}",
                fg="yellow",
            )
    return OrderedDict(sorted(files_by_key.items()))


def _classify_files_by_type(file_paths: list) -> dict:
    """
    Map files to their ``GARMIN_FILE_TYPES`` enum value via filename pattern.

    Callers should have already filtered out backup-only (no-pattern-match) files via
    :func:`_partition_processable_and_backup` before calling this.

    :param file_paths: Iterable of file Paths within a single FileSet.
    :return: Dict mapping ``GarminFileTypes`` enum to a list of matching Paths.
    """
    files_by_type: dict = {}
    for file_path in file_paths:
        for file_type_enum in GARMIN_FILE_TYPES:
            if file_type_enum.value.match(file_path.name):
                files_by_type.setdefault(file_type_enum, []).append(file_path)
                break  # Each file matches at most one pattern.
    return files_by_type


def _partition_processable_and_backup(file_paths: list) -> tuple:
    """
    Split files into (processable, backup-only) lists.

    Processable files match at least one ``GARMIN_FILE_TYPES`` pattern. Backup-only
    files (e.g. TCX / GPX / KML activity formats we have no processor for, or any other
    unrecognised filename) are real Garmin data the user wanted preserved on disk; the
    caller is expected to move them straight to ``storage/`` rather than feed them to
    the processor.

    :param file_paths: Iterable of file Paths.
    :return:``(processable, backup_only)`` tuple of lists.
    """
    processable: list = []
    backup_only: list = []
    for path in file_paths:
        if any(t.value.match(path.name) for t in GARMIN_FILE_TYPES):
            processable.append(path)
        else:
            backup_only.append(path)
    return processable, backup_only


def _print_extraction_failures(failures: list) -> None:
    """
    Render an ExtractionFailure list grouped by data_type.

    Caps the per-type detail at 5 lines to avoid spam on long backfills; the full count
    is still shown.

    :param failures: List of ExtractionFailure dataclass instances.
    """
    if not failures:
        return
    click.echo()
    click.secho(
        f"⚠️  Extraction failures ({len(failures)}):",
        fg="yellow",
        bold=True,
    )
    by_type: dict = {}
    for f in failures:
        by_type.setdefault(f.data_type, []).append(f)
    for dt, items in sorted(by_type.items()):
        click.echo(f"   • {dt}: {len(items)} failure(s)")
        for item in items[:5]:
            label = item.date or item.activity_id or "(no context)"
            click.echo(f"       - {label}: {item.error}")
        if len(items) > 5:
            click.echo(f"       ... and {len(items) - 5} more.")


@cli.command()
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.pass_context
def info(ctx: click.Context, db_path: str):
    """
    Show database statistics and information.
    """
    if not database_exists(db_path):
        click.secho(f"❌ Database not found: {db_path}", fg="red")
        click.echo("   Run 'garmin extract' to create a new database.")
        ctx.exit(1)

    click.echo()
    click.echo(
        click.style("📊 Garmin Health Data - Database Info", fg="cyan", bold=True)
    )
    click.echo()

    # Database file info.
    db_file = Path(db_path).expanduser()
    db_size = get_database_size(db_path)

    click.echo(click.style("Database File:", fg="cyan"))
    click.echo(f"   Location: {db_file.absolute()}")
    click.echo(f"   Size: {format_file_size(db_size)}")
    click.echo()

    # Last update dates.
    click.echo(click.style("Last Update Dates:", fg="cyan"))
    last_dates = get_last_update_dates(db_path)

    for data_type, last_date in sorted(last_dates.items()):
        if last_date:
            click.echo(
                f"   • {data_type.replace('_', ' ').title()}: {format_date(last_date)}"
            )
        else:
            click.echo(
                f"   • {data_type.replace('_', ' ').title()}: "
                + click.style("no data", fg="yellow")
            )

    click.echo()

    # Record counts.
    click.echo(click.style("Record Counts:", fg="cyan"))
    counts = get_record_counts(db_path)

    for table_name, count in sorted(counts.items()):
        display_name = table_name.replace("_", " ").title()
        click.echo(f"   • {display_name}: {format_count(count)}")

    click.echo()


@cli.command()
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.pass_context
def verify(ctx: click.Context, db_path: str):
    """
    Verify database integrity and structure.
    """
    if not database_exists(db_path):
        click.secho(f"❌ Database not found: {db_path}", fg="red")
        click.echo("   Run 'garmin extract' to create a new database.")
        ctx.exit(1)

    click.echo()
    click.echo(click.style("🔍 Verifying database...", fg="cyan", bold=True))
    click.echo()

    with get_session(db_path) as session:
        # Check if tables exist.
        from garmin_health_data.models import Base

        tables = Base.metadata.tables.keys()
        click.echo(f"✅ Found {len(tables)} tables")

        # Run SQLite integrity check.
        result = session.execute(text("PRAGMA integrity_check")).fetchone()
        if result[0] == "ok":
            click.secho("✅ Database integrity check passed", fg="green")
        else:
            click.secho(f"❌ Database integrity check failed: {result[0]}", fg="red")

    click.echo()


def _parse_accounts_option(accounts: tuple) -> Optional[list]:
    """
    Normalize a Click ``--accounts`` value into a list of integer user_ids.

    The flag accepts comma-separated or repeated values (mirroring ``extract``):
    ``--accounts 123,456`` or ``--accounts 123 --accounts 456``. ``None`` is returned
    when no accounts were given so callers can scope to "all users".

    :param accounts: Raw Click tuple from a ``multiple=True`` option.
    :return: List of integer user_ids, or None when the option was not used.
    """
    if not accounts:
        return None
    flat = [a.strip() for raw in accounts for a in raw.split(",") if a.strip()]
    if not flat:
        return None
    parsed: list = []
    for token in flat:
        try:
            parsed.append(int(token))
        except ValueError as exc:
            raise click.BadParameter(
                f"--accounts value {token!r} is not an integer user_id."
            ) from exc
    return parsed


def _print_range_banner(start_date, end_date) -> None:
    """
    Echo the same date-range banner ``extract`` prints, for UX consistency.

    :param start_date: Inclusive start date or None.
    :param end_date: Exclusive end date.
    """
    start_label = format_date(start_date) if start_date else "(beginning)"
    click.echo(
        f"📆 Date range: {start_label} (inclusive) "
        f"to {format_date(end_date)} (exclusive)"
    )


@cli.command(name="prune")
@click.option(
    "--end-date",
    "end_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD), EXCLUSIVE — activities on this date are not "
    "pruned (except when start and end are the same day, in which case that "
    "single day is included).",
)
@click.option(
    "--start-date",
    "start_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD), inclusive. Omit to prune everything before "
    "--end-date.",
)
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.option(
    "--accounts",
    multiple=True,
    help="Garmin user IDs to scope to (comma-separated or repeated). Prunes "
    "all users if not specified.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report row counts without deleting.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip the confirmation prompt.",
)
def prune(
    end_date: datetime,
    start_date: Optional[datetime],
    db_path: str,
    accounts: tuple,
    dry_run: bool,
    yes: bool,
):
    """
    Delete per-second sensor rows from activity_ts_metric for activities in range.

    Activity rows themselves, splits, laps, agg metrics, paths, and downsampled buckets
    are preserved. Range semantics match ``extract``: --end-date is exclusive, --start-
    date is inclusive, with the same-day special case.
    """
    accounts_list = _parse_accounts_option(accounts)
    start_d = start_date.date() if start_date else None
    end_d = end_date.date()

    _print_range_banner(start_d, end_d)
    if accounts_list:
        click.echo(f"👤 Scoping to accounts: {', '.join(map(str, accounts_list))}")

    if dry_run:
        result = prune_ts_metrics(
            db_path,
            start=start_d,
            end=end_d,
            user_ids=accounts_list,
            dry_run=True,
        )
        click.secho(
            f"🔍 Dry run: {format_count(result['rows_affected'])} "
            f"activity_ts_metric rows would be deleted across "
            f"{result['activity_count']} activit"
            f"{'y' if result['activity_count'] == 1 else 'ies'}.",
            fg="cyan",
        )
        return

    # Real run: preview row count first, then prompt unless --yes.
    preview = prune_ts_metrics(
        db_path,
        start=start_d,
        end=end_d,
        user_ids=accounts_list,
        dry_run=True,
    )
    if preview["rows_affected"] == 0:
        click.secho("Nothing to prune in this range.", fg="cyan")
        return
    click.echo(
        f"About to delete {format_count(preview['rows_affected'])} "
        f"activity_ts_metric rows across {preview['activity_count']} activities."
    )
    if not yes and not click.confirm("Proceed?", default=False):
        click.secho("Aborted.", fg="yellow")
        return

    result = prune_ts_metrics(
        db_path,
        start=start_d,
        end=end_d,
        user_ids=accounts_list,
        dry_run=False,
    )
    click.secho(
        f"✅ Deleted {format_count(result['rows_affected'])} "
        f"activity_ts_metric rows across {result['activity_count']} activities.",
        fg="green",
    )


@cli.command(name="downsample")
@click.option(
    "--end-date",
    "end_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD), EXCLUSIVE — activities on this date are not "
    "downsampled (except when start and end are the same day, in which case "
    "that single day is included).",
)
@click.option(
    "--start-date",
    "start_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD), inclusive. Omit to downsample everything "
    "before --end-date.",
)
@click.option(
    "--time-grain",
    "time_grain",
    required=True,
    type=TIME_GRAIN,
    help="Bucket grain. Format: integer + unit, where unit is 's' (seconds) "
    "or 'm' (minutes). Examples: 30s, 60s, 1m, 5m, 15m. Hours are not "
    "supported (use minutes instead).",
)
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.option(
    "--accounts",
    multiple=True,
    help="Garmin user IDs to scope to (comma-separated or repeated). "
    "Downsamples all users if not specified.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report the strategy table and counts without writing.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip the confirmation prompt.",
)
def downsample(
    end_date: datetime,
    start_date: Optional[datetime],
    time_grain: int,
    db_path: str,
    accounts: tuple,
    dry_run: bool,
    yes: bool,
):
    """
    Aggregate activity_ts_metric rows into time-bucketed downsampled records.

    Source rows in activity_ts_metric are NOT modified. Activity-level replace
    semantics: re-running for an activity with a different --time-grain wipes
    the prior buckets and inserts new ones; activities with no source rows are
    skipped entirely (their existing downsampled rows are preserved).
    """
    accounts_list = _parse_accounts_option(accounts)
    start_d = start_date.date() if start_date else None
    end_d = end_date.date()

    _print_range_banner(start_d, end_d)
    click.echo(f"⏱️  Bucket grain: {time_grain}s")
    if accounts_list:
        click.echo(f"👤 Scoping to accounts: {', '.join(map(str, accounts_list))}")

    # Preview first: print the strategy table and counts before any write.
    preview = downsample_activities(
        db_path,
        time_grain_seconds=time_grain,
        start=start_d,
        end=end_d,
        user_ids=accounts_list,
        dry_run=True,
    )

    if preview["activity_count"] == 0:
        click.secho("Nothing to downsample in this range.", fg="cyan")
        return

    metric_names = [name for name, _ in preview["metric_strategies"]]
    click.echo()
    click.echo(format_strategy_table(metric_names))
    click.echo()
    click.echo(
        f"Would replace {format_count(preview['rows_deleted'])} existing "
        f"downsampled rows with newly computed buckets for "
        f"{preview['activity_count']} activit"
        f"{'y' if preview['activity_count'] == 1 else 'ies'}."
    )

    if dry_run:
        click.secho("🔍 Dry run: no changes written.", fg="cyan")
        return

    if not yes and not click.confirm("Proceed?", default=False):
        click.secho("Aborted.", fg="yellow")
        return

    result = downsample_activities(
        db_path,
        time_grain_seconds=time_grain,
        start=start_d,
        end=end_d,
        user_ids=accounts_list,
        dry_run=False,
    )
    click.secho(
        f"✅ Downsampled {result['activity_count']} activit"
        f"{'y' if result['activity_count'] == 1 else 'ies'}: "
        f"removed {format_count(result['rows_deleted'])} prior bucket rows, "
        f"inserted {format_count(result['rows_inserted'])} new bucket rows.",
        fg="green",
    )


@cli.command(name="migrate-cascade")
@click.option(
    "--db-path",
    type=click.Path(),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Plan the migration without modifying the database.",
)
@click.option(
    "--no-backup",
    is_flag=True,
    default=False,
    help="Skip the pre-migration backup file. Default behavior copies the "
    "database to <db>.bak.<timestamp> before any writes.",
)
def migrate_cascade_cmd(db_path: str, dry_run: bool, no_backup: bool):
    """
    Retrofit ON DELETE CASCADE onto child FKs in an existing database.

    Pre-upgrade SQLite databases have no FK action on activity-child and
    sleep-child tables, so cascade clauses defined in the new schema are
    silently inert against them. This one-shot migration recreates each
    affected child table via the standard 12-step recreate dance.

    Idempotent: tables that already have cascade are skipped. Pre-flight
    PRAGMA foreign_key_check refuses to migrate a database with existing
    FK violations. Marked for removal in a future major version once enough
    users have run it.
    """
    click.secho(
        "ℹ️  migrate-cascade is intended for one-time migration of pre-2.8 "
        "databases and will be removed in a future major version.",
        fg="cyan",
    )
    try:
        result = migrate_cascade(
            db_path,
            dry_run=dry_run,
            backup=not no_backup,
        )
    except FileNotFoundError as e:
        click.secho(f"❌ {e}", fg="red")
        raise click.Abort() from e
    except RuntimeError as e:
        click.secho(f"❌ {e}", fg="red")
        raise click.Abort() from e

    if result["dry_run"]:
        click.secho(
            f"🔍 Dry run: would migrate "
            f"{format_count(len(result['migrated']))} table(s), "
            f"skip {format_count(len(result['skipped']))} (already cascade).",
            fg="cyan",
        )
    else:
        click.secho(
            f"✅ Migrated {format_count(len(result['migrated']))} table(s); "
            f"skipped {format_count(len(result['skipped']))} (already cascade).",
            fg="green",
        )
        if result["backup_path"]:
            click.echo(f"💾 Backup: {result['backup_path']}")

    if result["migrated"]:
        click.echo("Migrated:")
        for name in result["migrated"]:
            click.echo(f"  • {name}")


if __name__ == "__main__":
    cli()

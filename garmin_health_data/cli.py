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
from garmin_health_data.utils import format_count, format_date, format_file_size
from garmin_health_data.version_check import check_for_newer_version

# Filename timestamp pattern shared by all extracted JSON / FIT / TCX / GPX
# / KML files. Used to group files into per-(user_id, timestamp) FileSets.
_TIMESTAMP_REGEX = (
    r"\d{4}-\d{2}-\d{2}T\d{2}[:\-]\d{2}[:\-]\d{2}"
    r"(?:\.\d{1,6})?(?:[+-]\d{2}[:\-]\d{2}|Z)?"
)


@click.group()
@click.version_option(version=__version__)
def cli():
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
    help="End date (YYYY-MM-DD). Defaults to today.",
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
def extract(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    data_types: tuple,
    db_path: str,
    accounts: tuple,
    extract_only: bool,
    process_only: bool,
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
            f"📆 Date range: {format_date(start_date.date())} to "
            f"{format_date(end_date.date())}"
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

    # Acquire the lifecycle lock so a second concurrent run aborts cleanly
    # rather than racing on file moves.
    try:
        lock_ctx = acquire_lock(files_root)
        lock_ctx.__enter__()
    except LockHeldError as e:
        click.secho(f"❌ {e}", fg="red")
        raise click.Abort()

    try:
        # Recover any files left in process/ from a previously crashed run.
        recovered = recover_stale_process(files_root)
        if recovered:
            click.secho(
                f"♻️  Recovered {recovered} file(s) from a previous run "
                f"(process/ → ingest/).",
                fg="cyan",
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
            click.secho(f"✅ Extracted {format_count(total_files)} files", fg="green")
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

        # Discover files now in process/.
        file_paths = [p for p in process_dir.iterdir() if p.is_file()]

        total_processed = 0
        total_quarantined = 0
        if file_paths:
            files_by_key = _group_files_by_user_and_timestamp(file_paths)

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

                # Skip groups with no recognised types (e.g. TCX/GPX-only).
                if not files_by_type:
                    continue

                matched_paths = [p for paths in files_by_type.values() for p in paths]
                file_set = FileSet(file_paths=matched_paths, files=files_by_type)

                with get_session(db_path) as session:
                    try:
                        processor = GarminProcessor(file_set, session)
                        processor.process_file_set(file_set, session)
                        session.commit()
                        move_files_to_storage(matched_paths, files_root)
                        total_processed += len(matched_paths)
                    except Exception as e:
                        session.rollback()
                        click.secho(
                            f"❌ FileSet {uid}/{timestamp_str} failed: "
                            f"{type(e).__name__}: {e}. Moving to quarantine.",
                            fg="red",
                        )
                        move_files_to_quarantine(matched_paths, files_root)
                        total_quarantined += len(matched_paths)

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

    finally:
        # Always release the lifecycle lock.
        lock_ctx.__exit__(None, None, None)


def _group_files_by_user_and_timestamp(
    file_paths: list,
) -> "OrderedDict[tuple, list[Path]]":
    """
    Group files into per-(user_id, timestamp) FileSets.

    Each Garmin extracted file is named ``<user_id>_<DATA_TYPE>_<timestamp>.<ext>``.
    Files sharing the same ``(user_id, timestamp)`` represent one day's worth
    of data for one account and are processed together as a FileSet.

    :param file_paths: Iterable of file Paths in process/.
    :return: OrderedDict mapping ``(user_id, timestamp_str)`` to a list of
        Paths, sorted by key for deterministic processing order.
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
    Map files to their GARMIN_FILE_TYPES enum value via filename pattern.

    Files that don't match any known pattern are skipped (with a warning) so
    unsupported formats (e.g. TCX, GPX) don't crash the FileSet build.

    :param file_paths: Iterable of file Paths within a single FileSet.
    :return: Dict mapping ``GarminFileTypes`` enum to a list of matching Paths.
    """
    files_by_type: dict = {}
    for file_path in file_paths:
        matched = False
        for file_type_enum in GARMIN_FILE_TYPES:
            if file_type_enum.value.match(file_path.name):
                files_by_type.setdefault(file_type_enum, []).append(file_path)
                matched = True
                break  # Each file matches at most one pattern.
        if not matched:
            click.secho(
                f"⚠️  No matching pattern for file: {file_path.name}",
                fg="yellow",
            )
    return files_by_type


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
    type=click.Path(exists=True),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
def info(db_path: str):
    """
    Show database statistics and information.
    """
    if not database_exists(db_path):
        click.secho(f"❌ Database not found: {db_path}", fg="red")
        click.echo("   Run 'garmin extract' to create a new database")
        return

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
    type=click.Path(exists=True),
    default="garmin_data.db",
    help="Path to SQLite database file.",
)
def verify(db_path: str):
    """
    Verify database integrity and structure.
    """
    if not database_exists(db_path):
        click.secho(f"❌ Database not found: {db_path}", fg="red")
        return

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


if __name__ == "__main__":
    cli()

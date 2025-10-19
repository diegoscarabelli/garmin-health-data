"""
Command-line interface for garmin-health-data.
"""

import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import click

from garmin_health_data.__version__ import __version__
from garmin_health_data.auth import (
    ensure_authenticated,
    get_credentials,
    refresh_tokens,
)
from garmin_health_data.db import (
    database_exists,
    get_database_size,
    get_last_update_dates,
    get_latest_date,
    get_record_counts,
    get_session,
    initialize_database,
)
from garmin_health_data.extractor import extract as extract_data
from garmin_health_data.processor import GarminProcessor
from garmin_health_data.processor_helpers import FileSet
from garmin_health_data.utils import format_count, format_date, format_file_size


@click.group()
@click.version_option(version=__version__)
def cli():
    """
    Garmin Connect health data extraction tool.

    Extract your complete Garmin Connect health data to a local SQLite database.
    """


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
def extract(
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    data_types: tuple,
    db_path: str,
):
    """
    Extract Garmin Connect data and save to SQLite database.
    """
    # Ensure authenticated.
    ensure_authenticated()

    # Initialize database if it doesn't exist.
    if not database_exists(db_path):
        click.echo()
        click.echo(click.style("🗄️  Initializing new database...", fg="cyan"))
        initialize_database(db_path)
        click.echo()

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
                    f"📅 Auto-detected start date: {format_date(start_date.date())} "
                    f"(day after last update)",
                    fg="cyan",
                )
            )
        else:
            # Default to 30 days ago for new database.
            start_date = datetime.now() - timedelta(days=30)
            click.echo(
                click.style(
                    f"📅 Using default start date: {format_date(start_date.date())} "
                    f"(30 days ago)",
                    fg="cyan",
                )
            )

    # Default end date to today.
    if end_date is None:
        end_date = datetime.now()

    # Convert data_types tuple to list (or None for all).
    data_types_list = list(data_types) if data_types else None

    if data_types_list:
        click.echo(f"📊 Extracting data types: {', '.join(data_types_list)}")
    else:
        click.echo("📊 Extracting all available data types")

    click.echo(
        f"📆 Date range: {format_date(start_date.date())} to "
        f"{format_date(end_date.date())}"
    )
    click.echo()

    # Create temporary directory for extraction.
    temp_dir = Path("/tmp/garmin_extraction")
    temp_dir.mkdir(exist_ok=True, parents=True)

    try:
        # Step 1: Extract data from Garmin Connect.
        click.echo(
            click.style(
                "🔄 Step 1/3: Extracting data from Garmin Connect...",
                fg="cyan",
                bold=True,
            )
        )
        click.echo()

        result = extract_data(
            ingest_dir=temp_dir,
            data_interval_start=format_date(start_date.date()),
            data_interval_end=format_date(end_date.date()),
            data_types=data_types_list,
        )

        garmin_files = result.get("garmin_files", 0)
        activity_files = result.get("activity_files", 0)
        total_files = garmin_files + activity_files

        if total_files == 0:
            click.echo()
            click.secho(
                "ℹ️  No new data found for the specified date range", fg="yellow"
            )
            click.echo(
                "   Try extending the date range or check your Garmin Connect account"
            )
            return

        click.echo()
        click.secho(f"✅ Extracted {format_count(total_files)} files", fg="green")
        click.echo(f"   • Garmin data files: {format_count(garmin_files)}")
        click.echo(f"   • Activity files: {format_count(activity_files)}")
        click.echo()

        # Step 2: Process files and load into database.
        click.echo(
            click.style(
                "🔄 Step 2/3: Processing data and loading into database...",
                fg="cyan",
                bold=True,
            )
        )
        click.echo()

        # Get all files from temp directory.
        all_files = list(temp_dir.glob("**/*"))
        file_paths = [f for f in all_files if f.is_file()]

        if file_paths:
            # Group files by timestamp (like openetl does).
            # Each timestamp gets its own FileSet for sequential processing.
            import re
            from collections import OrderedDict

            from garmin_health_data.constants import GARMIN_FILE_TYPES

            timestamp_regex = (
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
                r"(?:\.\d{1,6})?(?:[+-]\d{2}:\d{2}|Z)?"
            )
            files_by_timestamp = OrderedDict()

            for file_path in file_paths:
                # Extract timestamp from filename
                match = re.search(timestamp_regex, file_path.name)
                if match:
                    timestamp_str = match.group(0)
                    if timestamp_str not in files_by_timestamp:
                        files_by_timestamp[timestamp_str] = []
                    files_by_timestamp[timestamp_str].append(file_path)
                else:
                    click.secho(
                        f"⚠️  No timestamp found in filename: {file_path.name}",
                        fg="yellow",
                    )

            # Sort by timestamp to process chronologically
            files_by_timestamp = OrderedDict(sorted(files_by_timestamp.items()))

            # Log how many FileSets will be processed
            num_filesets = len(files_by_timestamp)
            click.echo()
            plural = "s" if num_filesets != 1 else ""
            click.secho(
                f"📦 Processing {format_count(num_filesets)} file set{plural} "
                f"(grouped by timestamp)",
                fg="cyan",
                bold=True,
            )
            click.echo()

            # Create one FileSet per timestamp and process sequentially
            total_processed = 0
            with get_session(db_path) as session:
                for timestamp_str, timestamp_files in files_by_timestamp.items():
                    # Organize files by data type for this timestamp
                    files_by_type = {}
                    for file_path in timestamp_files:
                        matched = False
                        for file_type_enum in GARMIN_FILE_TYPES:
                            if file_type_enum.value.match(file_path.name):
                                if file_type_enum not in files_by_type:
                                    files_by_type[file_type_enum] = []
                                files_by_type[file_type_enum].append(file_path)
                                matched = True
                                break  # Each file matches only one pattern

                        if not matched:
                            click.secho(
                                f"⚠️  No matching pattern for file: {file_path.name}",
                                fg="yellow",
                            )

                    # Create FileSet for this timestamp
                    file_set = FileSet(file_paths=timestamp_files, files=files_by_type)

                    # Process this FileSet
                    processor = GarminProcessor(file_set, session)
                    processor.process_file_set(file_set, session)

                    total_processed += len(timestamp_files)

            click.echo()
            click.secho(
                f"✅ Processed {format_count(total_processed)} files", fg="green"
            )
        else:
            click.secho("⚠️  No files to process", fg="yellow")

        click.echo()

        # Step 3: Display summary.
        click.echo(click.style("📊 Step 3/3: Summary", fg="cyan", bold=True))
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
        click.echo()
        click.echo("💡 Next steps:")
        click.echo("   • Run 'garmin info' to see detailed statistics")
        click.echo("   • Query the database with your favorite SQLite tool")
        click.echo("   • Run 'garmin extract' again later to update with new data")

    finally:
        # Clean up temporary directory.
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


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
        result = session.execute("PRAGMA integrity_check").fetchone()
        if result[0] == "ok":
            click.secho("✅ Database integrity check passed", fg="green")
        else:
            click.secho(f"❌ Database integrity check failed: {result[0]}", fg="red")

    click.echo()


if __name__ == "__main__":
    cli()

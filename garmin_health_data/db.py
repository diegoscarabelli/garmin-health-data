"""
Database initialization and management for garmin-health-data.

Handles SQLite database creation, session management, and query utilities.
"""

import sqlite3
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import click
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

# Handle importlib.resources for different Python versions
if sys.version_info >= (3, 9):
    from importlib.resources import files
else:
    from importlib_resources import files

from garmin_health_data.models import (
    Activity,
    BodyBattery,
    Floors,
    HeartRate,
    IntensityMinutes,
    Respiration,
    Sleep,
    Steps,
    Stress,
    TrainingReadiness,
    User,
)


def get_engine(db_path: str = "garmin_data.db"):
    """
    Create SQLAlchemy engine for SQLite database.

    :param db_path: Path to SQLite database file.
    :return: SQLAlchemy engine.
    """
    db_file = Path(db_path).expanduser()

    # Create database URL.
    db_url = f"sqlite:///{db_file}"

    # Create engine with sensible defaults for SQLite.
    engine = create_engine(
        db_url,
        echo=False,  # Set to True for SQL debugging.
        connect_args={"check_same_thread": False},  # Allow multi-threading.
    )

    return engine


def create_tables(db_path: str = "garmin_data.db") -> None:
    """
    Create all tables in the database by executing the DDL file.

    The schema is defined in tables.ddl which includes inline comments preserved in the
    database.

    :param db_path: Path to SQLite database file.
    """
    # Execute DDL file to create all tables with inline comments.
    # Use importlib.resources to read the packaged resource file.
    try:
        ddl_sql = files("garmin_health_data").joinpath("tables.ddl").read_text()
    except (FileNotFoundError, TypeError):
        # Fallback for development/editable installs
        ddl_file = Path(__file__).parent / "tables.ddl"
        if not ddl_file.exists():
            raise FileNotFoundError(f"Schema DDL file not found: {ddl_file}")
        ddl_sql = ddl_file.read_text()

    # Use raw SQLite connection to execute the script (supports multiple statements).
    # This preserves inline comments in the database schema.
    db_file = Path(db_path).expanduser()
    conn = sqlite3.connect(str(db_file))
    try:
        conn.executescript(ddl_sql)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_session(db_path: str = "garmin_data.db"):
    """
    Context manager for database sessions.

    :param db_path: Path to SQLite database file.
    :yield: SQLAlchemy Session.
    """
    engine = get_engine(db_path)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def initialize_database(db_path: str = "garmin_data.db") -> None:
    """
    Initialize a new database with all tables and indexes.

    :param db_path: Path to SQLite database file.
    """
    db_file = Path(db_path).expanduser()

    if db_file.exists():
        click.echo(f"Database already exists at: {db_file}")
    else:
        click.echo(f"Creating new database at: {db_file}")

    create_tables(db_path)
    click.secho("✅ Database initialized successfully", fg="green")
    click.echo(
        "\nSchema includes inline documentation. To view table definitions:\n"
        f"  sqlite3 {db_file} \"SELECT sql FROM sqlite_master WHERE type='table';\""
    )


def get_last_update_dates(db_path: str = "garmin_data.db") -> Dict[str, Optional[date]]:
    """
    Get the last update date for each data type.

    :param db_path: Path to SQLite database file.
    :return: Dictionary mapping data type name to last update date.
    """
    with get_session(db_path) as session:
        dates = {}

        # Sleep data.
        last_sleep = session.query(func.max(Sleep.start_ts)).scalar()
        dates["sleep"] = last_sleep.date() if last_sleep else None

        # Heart rate.
        last_hr = session.query(func.max(HeartRate.timestamp)).scalar()
        dates["heart_rate"] = last_hr.date() if last_hr else None

        # Activities.
        last_activity = session.query(func.max(Activity.start_ts)).scalar()
        dates["activity"] = last_activity.date() if last_activity else None

        # Stress.
        last_stress = session.query(func.max(Stress.timestamp)).scalar()
        dates["stress"] = last_stress.date() if last_stress else None

        # Body battery.
        last_bb = session.query(func.max(BodyBattery.timestamp)).scalar()
        dates["body_battery"] = last_bb.date() if last_bb else None

        # Steps.
        last_steps = session.query(func.max(Steps.timestamp)).scalar()
        dates["steps"] = last_steps.date() if last_steps else None

        # Respiration.
        last_resp = session.query(func.max(Respiration.timestamp)).scalar()
        dates["respiration"] = last_resp.date() if last_resp else None

        # Floors.
        last_floors = session.query(func.max(Floors.timestamp)).scalar()
        dates["floors"] = last_floors.date() if last_floors else None

        # Intensity minutes.
        last_im = session.query(func.max(IntensityMinutes.timestamp)).scalar()
        dates["intensity_minutes"] = last_im.date() if last_im else None

        # Training readiness.
        last_tr = session.query(func.max(TrainingReadiness.timestamp)).scalar()
        dates["training_readiness"] = last_tr.date() if last_tr else None

        return dates


def get_latest_date(db_path: str = "garmin_data.db") -> Optional[date]:
    """
    Get the most recent date across all data types.

    :param db_path: Path to SQLite database file.
    :return: Most recent date or None if database is empty.
    """
    dates = get_last_update_dates(db_path)
    valid_dates = [d for d in dates.values() if d is not None]

    if not valid_dates:
        return None

    return max(valid_dates)


def get_record_counts(db_path: str = "garmin_data.db") -> Dict[str, int]:
    """
    Get record counts for all major tables.

    :param db_path: Path to SQLite database file.
    :return: Dictionary mapping table name to record count.
    """
    with get_session(db_path) as session:
        counts = {}

        counts["users"] = session.query(func.count(User.user_id)).scalar()
        counts["activities"] = session.query(func.count(Activity.activity_id)).scalar()
        counts["sleep_sessions"] = session.query(func.count(Sleep.sleep_id)).scalar()
        counts["heart_rate_readings"] = session.query(
            func.count(HeartRate.timestamp)
        ).scalar()
        counts["stress_readings"] = session.query(func.count(Stress.timestamp)).scalar()
        counts["body_battery_readings"] = session.query(
            func.count(BodyBattery.timestamp)
        ).scalar()
        counts["step_readings"] = session.query(func.count(Steps.timestamp)).scalar()
        counts["respiration_readings"] = session.query(
            func.count(Respiration.timestamp)
        ).scalar()

        return counts


def get_database_size(db_path: str = "garmin_data.db") -> int:
    """
    Get size of database file in bytes.

    :param db_path: Path to SQLite database file.
    :return: Size in bytes, or 0 if file doesn't exist.
    """
    db_file = Path(db_path).expanduser()

    if not db_file.exists():
        return 0

    return db_file.stat().st_size


def database_exists(db_path: str = "garmin_data.db") -> bool:
    """
    Check if database file exists.

    :param db_path: Path to SQLite database file.
    :return: True if database exists, False otherwise.
    """
    db_file = Path(db_path).expanduser()
    return db_file.exists()

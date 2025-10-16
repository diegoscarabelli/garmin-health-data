"""
Database initialization and management for garmin-health-data.

Handles SQLite database creation, session management, and query utilities.
"""

from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import click
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

from garmin_health_data.models import (
    Activity,
    Base,
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
    Create all tables in the database.

    :param db_path: Path to SQLite database file.
    """
    engine = get_engine(db_path)

    # Create all tables.
    Base.metadata.create_all(engine)

    # Create indexes for time-series queries.
    with engine.begin() as conn:
        # Sleep data indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_sleep_user_start_ts
            ON sleep(user_id, start_ts DESC)
        """
            )
        )

        # Heart rate indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_heart_rate_user_timestamp
            ON heart_rate(user_id, timestamp DESC)
        """
            )
        )

        # Activity indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_activity_user_start_ts
            ON activity(user_id, start_ts DESC)
        """
            )
        )

        # Stress indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_stress_user_timestamp
            ON stress(user_id, timestamp DESC)
        """
            )
        )

        # Body battery indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_body_battery_user_timestamp
            ON body_battery(user_id, timestamp DESC)
        """
            )
        )

        # Steps indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_steps_user_timestamp
            ON steps(user_id, timestamp DESC)
        """
            )
        )

        # Respiration indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_respiration_user_timestamp
            ON respiration(user_id, timestamp DESC)
        """
            )
        )

        # Floors indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_floors_user_timestamp
            ON floors(user_id, timestamp DESC)
        """
            )
        )

        # Intensity minutes indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_intensity_minutes_user_timestamp
            ON intensity_minutes(user_id, timestamp DESC)
        """
            )
        )

        # Training readiness indexes.
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_training_readiness_user_timestamp
            ON training_readiness(user_id, timestamp DESC)
        """
            )
        )

        # Partial UNIQUE indexes for 'latest' flags (matching openetl schema).
        # These enforce that only one record can have latest=1 for each combination.

        # user_profile: Only one latest profile per user.
        conn.execute(
            text(
                """
            CREATE UNIQUE INDEX IF NOT EXISTS user_profile_user_id_latest_unique_idx
            ON user_profile(user_id) WHERE latest = 1
        """
            )
        )

        # personal_record: Only one latest PR per user and type.
        conn.execute(
            text(
                """
            CREATE UNIQUE INDEX IF NOT EXISTS personal_record_user_id_type_id_latest_idx
            ON personal_record(user_id, type_id) WHERE latest = 1
        """
            )
        )

        # race_predictions: Only one latest prediction set per user.
        conn.execute(
            text(
                """
            CREATE UNIQUE INDEX IF NOT EXISTS race_predictions_user_id_latest_unique_idx
            ON race_predictions(user_id) WHERE latest = 1
        """
            )
        )


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

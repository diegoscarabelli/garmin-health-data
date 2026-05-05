"""
Tests for processor helper functions.

This module tests the helper functions used by the Garmin data processor, including the
critical upsert_model_instances function that handles all database writes across the
processor.
"""

import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from garmin_health_data.db import get_engine
from garmin_health_data.models import (
    Base,
    HeartRate,
    Sleep,
    User,
)
from garmin_health_data.processor_helpers import upsert_model_instances


@pytest.fixture
def temp_db():
    """
    Create a temporary database for testing.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_file.close()
    db_path = temp_file.name

    engine = get_engine(db_path)
    Base.metadata.create_all(engine)

    yield db_path

    # Cleanup - dispose engine first to release file locks on Windows.
    engine.dispose()
    try:
        Path(db_path).unlink(missing_ok=True)
    except PermissionError:
        # On Windows, sometimes files are still locked even after dispose
        import time
        import gc

        gc.collect()  # Force garbage collection
        time.sleep(0.1)  # Small delay to let OS release locks
        Path(db_path).unlink(missing_ok=True)


class TestUpsertModelInstances:
    """
    Test the upsert_model_instances function.
    """

    def test_bulk_insert_single_record(self, temp_db):
        """
        Test inserting a single record.
        """
        engine = get_engine(temp_db)

        with Session(engine) as session:
            users = [User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))]
            result = upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            assert len(result) == 1
            count = session.scalar(select(func.count()).select_from(User))
            assert count == 1

    def test_bulk_insert_multiple_records(self, temp_db):
        """
        Test inserting multiple records in a single bulk operation.
        """
        engine = get_engine(temp_db)

        with Session(engine) as session:
            users = [
                User(user_id=i, full_name=f"User {i}", birth_date=date(1990, 1, 1))
                for i in range(1, 101)
            ]
            result = upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            assert len(result) == 100
            count = session.scalar(select(func.count()).select_from(User))
            assert count == 100

    def test_bulk_update_on_conflict(self, temp_db):
        """
        Test updating existing records on conflict.
        """
        engine = get_engine(temp_db)

        # Insert initial records.
        with Session(engine) as session:
            users = [
                User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1)),
                User(user_id=2, full_name="User 2", birth_date=date(1991, 2, 2)),
            ]
            upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Update the same records.
        with Session(engine) as session:
            updated_users = [
                User(
                    user_id=1, full_name="Updated User 1", birth_date=date(1990, 1, 1)
                ),
                User(
                    user_id=2, full_name="Updated User 2", birth_date=date(1991, 2, 2)
                ),
            ]
            upsert_model_instances(
                session=session,
                model_instances=updated_users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Verify updates.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            user2 = (
                session.execute(select(User).where(User.user_id == 2)).scalars().first()
            )
            assert user1.full_name == "Updated User 1"
            assert user2.full_name == "Updated User 2"
            assert session.scalar(select(func.count()).select_from(User)) == 2

    def test_insert_ignore_on_conflict(self, temp_db):
        """
        Test ignoring conflicts (insert-only mode).
        """
        engine = get_engine(temp_db)

        # Insert initial records.
        with Session(engine) as session:
            users = [
                User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1)),
                User(user_id=2, full_name="User 2", birth_date=date(1991, 2, 2)),
            ]
            upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Try to insert duplicates with different names (should be ignored).
        with Session(engine) as session:
            duplicate_users = [
                User(
                    user_id=1,
                    full_name="Should Not Update",
                    birth_date=date(1990, 1, 1),
                ),
                User(user_id=3, full_name="User 3", birth_date=date(1992, 3, 3)),
            ]
            upsert_model_instances(
                session=session,
                model_instances=duplicate_users,
                conflict_columns=["user_id"],
                on_conflict_update=False,  # Ignore conflicts.
            )
            session.commit()

            # Verify user 1 was NOT updated, user 3 was inserted.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            user3 = (
                session.execute(select(User).where(User.user_id == 3)).scalars().first()
            )
            assert user1.full_name == "User 1"  # Not updated.
            assert user3.full_name == "User 3"  # Inserted.
            assert session.scalar(select(func.count()).select_from(User)) == 3

    def test_partial_column_update(self, temp_db):
        """
        Test updating only specific columns on conflict.
        """
        engine = get_engine(temp_db)

        # Insert initial record.
        with Session(engine) as session:
            users = [User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))]
            upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Update only the full_name (not birth_date).
        with Session(engine) as session:
            updated_users = [
                User(user_id=1, full_name="Updated User 1", birth_date=date(2000, 1, 1))
            ]
            upsert_model_instances(
                session=session,
                model_instances=updated_users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
                update_columns=["full_name"],  # Only update full_name.
            )
            session.commit()

            # Verify only full_name was updated.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            assert user1.full_name == "Updated User 1"
            assert user1.birth_date == date(1990, 1, 1)  # Not updated.

    def test_composite_primary_key(self, temp_db):
        """
        Test upsert with composite primary key (user_id + timestamp).
        """
        engine = get_engine(temp_db)

        # Create user first (foreign key requirement).
        with Session(engine) as session:
            user = User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))
            upsert_model_instances(
                session=session,
                model_instances=[user],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Insert heart rate records with composite key.
        with Session(engine) as session:
            timestamp1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            timestamp2 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)

            heart_rates = [
                HeartRate(user_id=1, timestamp=timestamp1, value=70),
                HeartRate(user_id=1, timestamp=timestamp2, value=72),
            ]
            upsert_model_instances(
                session=session,
                model_instances=heart_rates,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,  # Insert-only.
            )
            session.commit()

            count = session.scalar(select(func.count()).select_from(HeartRate))
            assert count == 2

        # Try to insert duplicate (should be ignored).
        with Session(engine) as session:
            duplicate = [HeartRate(user_id=1, timestamp=timestamp1, value=999)]
            upsert_model_instances(
                session=session,
                model_instances=duplicate,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,  # Ignore conflicts.
            )
            session.commit()

            # Verify value was NOT updated.
            hr = (
                session.execute(
                    select(HeartRate).where(HeartRate.timestamp == timestamp1)
                )
                .scalars()
                .first()
            )
            assert hr.value == 70  # Original value.
            assert session.scalar(select(func.count()).select_from(HeartRate)) == 2

    def test_empty_list(self, temp_db):
        """
        Test handling empty list of instances.
        """
        engine = get_engine(temp_db)

        with Session(engine) as session:
            result = upsert_model_instances(
                session=session,
                model_instances=[],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            assert result == []

    def test_mixed_insert_and_update(self, temp_db):
        """
        Test bulk operation with both new and existing records.
        """
        engine = get_engine(temp_db)

        # Insert initial records.
        with Session(engine) as session:
            users = [
                User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1)),
                User(user_id=2, full_name="User 2", birth_date=date(1991, 2, 2)),
            ]
            upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Mix of updates (1, 2) and inserts (3, 4).
        with Session(engine) as session:
            mixed_users = [
                User(
                    user_id=1, full_name="Updated User 1", birth_date=date(1990, 1, 1)
                ),
                User(
                    user_id=2, full_name="Updated User 2", birth_date=date(1991, 2, 2)
                ),
                User(user_id=3, full_name="User 3", birth_date=date(1992, 3, 3)),
                User(user_id=4, full_name="User 4", birth_date=date(1993, 4, 4)),
            ]
            upsert_model_instances(
                session=session,
                model_instances=mixed_users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Verify all operations.
            assert session.scalar(select(func.count()).select_from(User)) == 4
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            user3 = (
                session.execute(select(User).where(User.user_id == 3)).scalars().first()
            )
            assert user1.full_name == "Updated User 1"
            assert user3.full_name == "User 3"

    def test_timestamp_columns_excluded_from_update(self, temp_db):
        """
        Test that create_ts and update_ts are not updated on conflict.
        """
        engine = get_engine(temp_db)

        # Insert initial record.
        with Session(engine) as session:
            users = [User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))]
            upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Get original create_ts.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            original_create_ts = user1.create_ts

        # Update the record (create_ts should not change).
        with Session(engine) as session:
            updated_users = [
                User(user_id=1, full_name="Updated User 1", birth_date=date(1990, 1, 1))
            ]
            upsert_model_instances(
                session=session,
                model_instances=updated_users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
                # create_ts should be excluded automatically.
            )
            session.commit()

            # Verify create_ts did not change.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            assert user1.create_ts == original_create_ts
            assert user1.full_name == "Updated User 1"

    def test_large_batch_performance(self, temp_db):
        """
        Test bulk operation with large number of records (1000+).
        """
        engine = get_engine(temp_db)

        with Session(engine) as session:
            # Insert 1000 records in single bulk operation.
            users = [
                User(user_id=i, full_name=f"User {i}", birth_date=date(1990, 1, 1))
                for i in range(1, 1001)
            ]
            result = upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            assert len(result) == 1000
            count = session.scalar(select(func.count()).select_from(User))
            assert count == 1000

    def test_null_values_in_optional_columns(self, temp_db):
        """
        Test handling NULL values in optional columns.
        """
        engine = get_engine(temp_db)

        with Session(engine) as session:
            # Create user first.
            user = User(user_id=1, full_name=None, birth_date=None)
            upsert_model_instances(
                session=session,
                model_instances=[user],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Verify NULL values were inserted.
            user1 = (
                session.execute(select(User).where(User.user_id == 1)).scalars().first()
            )
            assert user1.full_name is None
            assert user1.birth_date is None

    def test_chunking_exceeds_parameter_limit(self, temp_db):
        """
        Test that large batches are split into chunks to stay within SQLite's
        SQLITE_MAX_VARIABLE_NUMBER limit.

        HeartRate has 4 columns (user_id, timestamp, value, create_ts). With
        _SQLITE_MAX_PARAMS=999 the chunk size is 249 rows, so 500 records forces at
        least two chunks.
        """
        engine = get_engine(temp_db)

        # Create user first (foreign key requirement).
        with Session(engine) as session:
            user = User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))
            upsert_model_instances(
                session=session,
                model_instances=[user],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Insert 500 heart rate records (forces multiple chunks).
        with Session(engine) as session:
            records = [
                HeartRate(
                    user_id=1,
                    timestamp=datetime(
                        2024, 1, 1, i // 60, i % 60, 0, tzinfo=timezone.utc
                    ),
                    value=60 + (i % 40),
                )
                for i in range(500)
            ]
            result = upsert_model_instances(
                session=session,
                model_instances=records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            session.commit()

            assert len(result) == 500
            count = session.scalar(select(func.count()).select_from(HeartRate))
            assert count == 500

    def test_chunking_with_conflict_update(self, temp_db):
        """
        Test that chunked upserts correctly update rows across chunk boundaries.
        """
        engine = get_engine(temp_db)

        # Create user first (foreign key requirement).
        with Session(engine) as session:
            user = User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))
            upsert_model_instances(
                session=session,
                model_instances=[user],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Insert 500 heart rate records.
        with Session(engine) as session:
            records = [
                HeartRate(
                    user_id=1,
                    timestamp=datetime(
                        2024, 1, 1, i // 60, i % 60, 0, tzinfo=timezone.utc
                    ),
                    value=60,
                )
                for i in range(500)
            ]
            upsert_model_instances(
                session=session,
                model_instances=records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=True,
            )
            session.commit()

        # Re-upsert with updated values (should update, not duplicate).
        with Session(engine) as session:
            updated_records = [
                HeartRate(
                    user_id=1,
                    timestamp=datetime(
                        2024, 1, 1, i // 60, i % 60, 0, tzinfo=timezone.utc
                    ),
                    value=99,
                )
                for i in range(500)
            ]
            upsert_model_instances(
                session=session,
                model_instances=updated_records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=True,
            )
            session.commit()

            count = session.scalar(select(func.count()).select_from(HeartRate))
            assert count == 500

            # Verify a record from the second chunk was updated.
            hr = (
                session.execute(
                    select(HeartRate).where(
                        HeartRate.timestamp
                        == datetime(2024, 1, 1, 4, 30, 0, tzinfo=timezone.utc)
                    )
                )
                .scalars()
                .first()
            )
            assert hr.value == 99

    def test_update_ts_refreshes_on_conflict_update(self, temp_db):
        """
        Test that update_ts is refreshed when records are updated.

        This is critical because SQLite's DEFAULT CURRENT_TIMESTAMP only applies on
        INSERT, not UPDATE. The upsert logic must explicitly set update_ts on conflict
        updates.
        """
        import time
        from garmin_health_data.models import Activity

        engine = get_engine(temp_db)

        # Create user first (foreign key requirement).
        with Session(engine) as session:
            user = User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))
            upsert_model_instances(
                session=session,
                model_instances=[user],
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()

        # Insert initial activity record.
        with Session(engine) as session:
            activity = Activity(
                activity_id=1,
                user_id=1,
                start_ts=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                end_ts=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                timezone_offset_hours=0.0,
                activity_type_id=1,
                activity_type_key="running",
                event_type_id=1,
                event_type_key="uncategorized",
                parent=False,
                purposeful=True,
                favorite=False,
                pr=False,
                auto_calc_calories=True,
                has_polyline=False,
                has_images=False,
                has_video=False,
                has_heat_map=False,
                manual_activity=False,
            )
            upsert_model_instances(
                session=session,
                model_instances=[activity],
                conflict_columns=["activity_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Get original update_ts.
            activity1 = (
                session.execute(select(Activity).where(Activity.activity_id == 1))
                .scalars()
                .first()
            )
            original_update_ts = activity1.update_ts

        # Wait to ensure time difference (SQLite CURRENT_TIMESTAMP has second precision).
        time.sleep(1)

        # Update the same activity (change a field).
        with Session(engine) as session:
            updated_activity = Activity(
                activity_id=1,
                user_id=1,
                start_ts=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                end_ts=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                timezone_offset_hours=0.0,
                activity_type_id=1,
                activity_type_key="running",
                event_type_id=1,
                event_type_key="uncategorized",
                parent=False,
                purposeful=True,
                favorite=True,  # Changed from False to True.
                pr=False,
                auto_calc_calories=True,
                has_polyline=False,
                has_images=False,
                has_video=False,
                has_heat_map=False,
                manual_activity=False,
            )
            upsert_model_instances(
                session=session,
                model_instances=[updated_activity],
                conflict_columns=["activity_id"],
                on_conflict_update=True,
            )
            session.commit()

            # Verify update_ts was refreshed.
            activity1 = (
                session.execute(select(Activity).where(Activity.activity_id == 1))
                .scalars()
                .first()
            )
            assert activity1.update_ts > original_update_ts
            assert activity1.favorite is True  # Verify the update worked.


class TestUpsertModelInstancesReturning:
    """
    Tests for the ``returning_columns`` parameter of ``upsert_model_instances``.
    """

    @staticmethod
    def _seed_user(session: Session, user_id: int = 1) -> None:
        """
        Insert a User row required by the FK on sleep.user_id.
        """
        upsert_model_instances(
            session=session,
            model_instances=[User(user_id=user_id, full_name="Test User")],
            conflict_columns=["user_id"],
            on_conflict_update=True,
        )
        session.commit()

    def test_returning_columns_empty_list_raises(self, temp_db):
        """
        Passing ``returning_columns=[]`` is a programming error: it skips the RETURNING
        path but breaks the contract that "if you ask for results, you get one row per
        input." Surface it instead of silently returning an empty list.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)
            with pytest.raises(ValueError, match="non-empty"):
                upsert_model_instances(
                    session=session,
                    model_instances=[
                        Sleep(
                            user_id=1,
                            start_ts=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                            end_ts=datetime(2025, 1, 2, 8, 0, 0, tzinfo=timezone.utc),
                            timezone_offset_hours=0.0,
                        )
                    ],
                    conflict_columns=["user_id", "start_ts"],
                    on_conflict_update=True,
                    returning_columns=[],
                )

    def test_returning_columns_none_returns_input_list(self, temp_db):
        """
        ``returning_columns=None`` preserves backward-compatible behavior: the input
        list is returned unchanged so existing callers that don't need DB-assigned
        columns are unaffected.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            users = [User(user_id=1, full_name="User 1", birth_date=date(1990, 1, 1))]
            result = upsert_model_instances(
                session=session,
                model_instances=users,
                conflict_columns=["user_id"],
                on_conflict_update=True,
            )
            session.commit()
            assert result is users

    def test_returning_assigns_autogenerated_pk_on_insert(self, temp_db):
        """
        ON CONFLICT DO UPDATE with RETURNING populates the auto-generated primary key on
        a fresh insert.

        This is the path that the sleep orchestrator depends on.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)
            sleep = Sleep(
                user_id=1,
                start_ts=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                end_ts=datetime(2025, 1, 2, 8, 0, 0, tzinfo=timezone.utc),
                timezone_offset_hours=0.0,
            )
            persisted = upsert_model_instances(
                session=session,
                model_instances=[sleep],
                conflict_columns=["user_id", "start_ts"],
                on_conflict_update=True,
                returning_columns=["sleep_id"],
            )
            session.commit()

            assert len(persisted) == 1
            assert isinstance(persisted[0], Sleep)
            assert persisted[0].sleep_id is not None
            assert isinstance(persisted[0].sleep_id, int)

    def test_returning_idempotent_returns_same_pk_on_update(self, temp_db):
        """
        ON CONFLICT DO UPDATE with RETURNING returns the existing row's primary key on a
        second pass over the same conflict key, so re-extraction is idempotent.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)
            sleep_kwargs = dict(
                user_id=1,
                start_ts=datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                end_ts=datetime(2025, 1, 2, 8, 0, 0, tzinfo=timezone.utc),
                timezone_offset_hours=0.0,
            )

            first = upsert_model_instances(
                session=session,
                model_instances=[Sleep(**sleep_kwargs)],
                conflict_columns=["user_id", "start_ts"],
                on_conflict_update=True,
                returning_columns=["sleep_id"],
            )
            session.commit()

            second = upsert_model_instances(
                session=session,
                model_instances=[Sleep(**sleep_kwargs)],
                conflict_columns=["user_id", "start_ts"],
                on_conflict_update=True,
                returning_columns=["sleep_id"],
            )
            session.commit()

            assert first[0].sleep_id == second[0].sleep_id
            assert session.scalar(select(func.count()).select_from(Sleep)) == 1

    def test_returning_do_nothing_preserves_input_order_and_existing_values(
        self, temp_db
    ):
        """
        ``on_conflict_update=False`` with ``returning_columns`` returns one row per
        input row, in input order, with existing values preserved for conflicted rows.

        Implemented via the no-op ``DO UPDATE`` trick.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)

            ts1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            ts2 = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
            ts3 = datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc)
            upsert_model_instances(
                session=session,
                model_instances=[
                    HeartRate(user_id=1, timestamp=ts1, value=70),
                    HeartRate(user_id=1, timestamp=ts3, value=72),
                ],
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
            )
            session.commit()

            # Mix of conflict (ts1), new (ts2), conflict (ts3) in non-trivial
            # order to stress position-alignment.
            persisted = upsert_model_instances(
                session=session,
                model_instances=[
                    HeartRate(user_id=1, timestamp=ts1, value=999),
                    HeartRate(user_id=1, timestamp=ts2, value=80),
                    HeartRate(user_id=1, timestamp=ts3, value=999),
                ],
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
                returning_columns=["user_id", "timestamp", "value"],
            )
            session.commit()

            # Position-aligned: result[i] corresponds to input[i].
            assert len(persisted) == 3
            naive = [t.replace(tzinfo=None) for t in (ts1, ts2, ts3)]
            assert [p.timestamp for p in persisted] == naive
            # Existing values preserved for conflicts; new value for the insert.
            assert [p.value for p in persisted] == [70, 80, 72]

    def test_returning_do_nothing_does_not_bump_update_ts(self, temp_db):
        """
        ``update_ts`` must be preserved across an ``on_conflict_update=False`` +
        ``returning_columns`` re-upsert.

        Locks in the contract that the no-op ``DO UPDATE`` trick (only assigning a
        conflict column to itself) does not accidentally refresh audit timestamps.
        """
        import time

        from garmin_health_data.models import Activity

        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)

            kwargs = dict(
                activity_id=1,
                user_id=1,
                start_ts=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                end_ts=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                timezone_offset_hours=0.0,
                activity_type_id=1,
                activity_type_key="r",
                event_type_id=1,
                event_type_key="u",
                parent=False,
                purposeful=True,
                favorite=False,
                pr=False,
                auto_calc_calories=True,
                has_polyline=False,
                has_images=False,
                has_video=False,
                has_heat_map=False,
                manual_activity=False,
            )
            upsert_model_instances(
                session=session,
                model_instances=[Activity(**kwargs)],
                conflict_columns=["activity_id"],
                on_conflict_update=False,
            )
            session.commit()

            original_ts = (
                session.execute(select(Activity).where(Activity.activity_id == 1))
                .scalar_one()
                .update_ts
            )

        # SQLite's CURRENT_TIMESTAMP has second precision; ensure any spurious
        # refresh would produce a different value.
        time.sleep(1.1)

        with Session(engine) as session:
            upsert_model_instances(
                session=session,
                model_instances=[Activity(**kwargs)],
                conflict_columns=["activity_id"],
                on_conflict_update=False,
                returning_columns=["activity_id"],
            )
            session.commit()

            after_ts = (
                session.execute(select(Activity).where(Activity.activity_id == 1))
                .scalar_one()
                .update_ts
            )
            assert after_ts == original_ts

    def test_returning_do_nothing_same_batch_duplicate_keys(self, temp_db):
        """
        Same-batch duplicate conflict keys with ``on_conflict_update=False`` +
        ``returning_columns``: SQLite resolves with first-wins semantics. Exactly one
        row is stored, the result list contains one entry per input row (in input
        order), and every entry reflects the first-input value.

        Pins down a SQLite-specific behavior — PostgreSQL would error here.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)

            ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            persisted = upsert_model_instances(
                session=session,
                model_instances=[
                    HeartRate(user_id=1, timestamp=ts, value=70),
                    HeartRate(user_id=1, timestamp=ts, value=80),
                ],
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=False,
                returning_columns=["user_id", "timestamp", "value"],
            )
            session.commit()

            assert session.scalar(select(func.count()).select_from(HeartRate)) == 1
            assert len(persisted) == 2
            assert [p.value for p in persisted] == [70, 70]

    def test_returning_across_chunk_boundaries(self, temp_db):
        """
        RETURNING results are accumulated across chunked statements.

        With 500 HeartRate rows and the SQLite parameter floor, this exercises at least
        two chunks.
        """
        engine = get_engine(temp_db)
        with Session(engine) as session:
            self._seed_user(session)

            records = [
                HeartRate(
                    user_id=1,
                    timestamp=datetime(
                        2024, 1, 1, i // 60, i % 60, 0, tzinfo=timezone.utc
                    ),
                    value=60 + (i % 40),
                )
                for i in range(500)
            ]
            persisted = upsert_model_instances(
                session=session,
                model_instances=records,
                conflict_columns=["user_id", "timestamp"],
                on_conflict_update=True,
                returning_columns=["user_id", "timestamp", "value"],
            )
            session.commit()

            assert len(persisted) == 500
            assert session.scalar(select(func.count()).select_from(HeartRate)) == 500

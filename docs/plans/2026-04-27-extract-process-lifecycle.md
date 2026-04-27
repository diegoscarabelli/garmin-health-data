# Extract/Process Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the temp-dir extraction model with an openetl-style four-folder lifecycle (ingest/process/storage/quarantine) and add per-date / per-data-type / per-FileSet failure isolation, so partial failures never lose work and every file's fate is inspectable on disk.

**Architecture:** Files now move through `<db_dir>/garmin_files/{ingest,process,storage,quarantine}/`. Extract writes to `ingest/`. A bulk move stages `ingest/` → `process/`. Each `(user_id, timestamp)` FileSet is processed in its own SQLAlchemy session inside a try/except. Successful FileSets' files move to `storage/`; failed ones move to `quarantine/`. The extractor itself gains per-date and per-data-type try/except so a single transient API failure no longer aborts an entire account run. End-of-run summary surfaces every gap.

**Tech Stack:** Python 3.9+, Click, SQLAlchemy 2.x, pytest, garminconnect SDK.

**Issue:** Closes #35 (expanded scope from "save activity files" to full lifecycle).

---

## Pre-flight Notes

- This plan **builds on** the existing `feature/keep-files` branch (PR #37). The `--keep-files` flag added in commit `7e174ba` is removed in Task 12; the lifecycle becomes default.
- Concurrent-run protection is included via `fcntl.flock` on a `.lock` file in the lifecycle parent directory (Task 2 + Task 14).
- `extract_fit_activities` is refactored to read the `ACTIVITIES_LIST` JSON from `ingest/` instead of re-fetching the same endpoint (Task 6). Falls back to the live API call if the file is missing.
- Renaming `_process_day_by_day` → `_extract_day_by_day` diverges from openetl naming. A follow-up openetl PR will sync the rename and the per-date / per-data-type isolation. No other names are changed here.

## File Structure

**New files:**
- `garmin_health_data/lifecycle.py` — folder setup, stale recovery, ingest→process bulk move, per-FileSet move helpers. ~100 lines.
- `tests/test_lifecycle.py` — unit tests for lifecycle module. ~150 lines.

**Modified files:**
- `garmin_health_data/extractor.py` — rename `_process_day_by_day`, add three layers of try/except, return `ExtractionSummary`. ~150 lines changed.
- `garmin_health_data/cli.py` — replace temp dir with lifecycle, per-FileSet session+try/except in process loop, route to storage/quarantine, add `--extract-only`/`--process-only`, remove `--keep-files`, print summary. ~200 lines changed.
- `tests/test_cli.py` — replace `--keep-files` tests with lifecycle tests, add new flag tests. ~150 lines changed.
- `tests/test_extractor.py` — add tests for per-date / per-data-type isolation and summary collection. ~100 lines added.
- `README.md` — document new lifecycle layout, new flags, failure semantics. ~30 lines changed.

---

## Task 1: Lifecycle module — folder setup and stale recovery

**Files:**
- Create: `garmin_health_data/lifecycle.py`
- Create: `tests/test_lifecycle.py`

**Rationale:** All filesystem-state mutations live in one module so the CLI never has to think about path layout. Stale `process/` recovery is bundled in here so that a crashed run's files automatically re-enter the pipeline on the next invocation.

- [ ] **Step 1.1: Write the failing tests**

Create `tests/test_lifecycle.py`:

```python
"""
Tests for filesystem lifecycle helpers (ingest/process/storage/quarantine).
"""

from pathlib import Path

import pytest

from garmin_health_data.lifecycle import (
    LIFECYCLE_DIRS,
    move_files_to_quarantine,
    move_files_to_storage,
    move_ingest_to_process,
    recover_stale_process,
    setup_lifecycle_dirs,
)


def test_setup_creates_all_four_dirs(tmp_path):
    """All four lifecycle directories are created under the base dir."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    for name in LIFECYCLE_DIRS:
        assert (base / name).is_dir()


def test_setup_is_idempotent(tmp_path):
    """Calling setup twice is a no-op the second time."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "marker.txt").write_text("keep me")
    setup_lifecycle_dirs(base)
    assert (base / "ingest" / "marker.txt").read_text() == "keep me"


def test_recover_stale_process_moves_files_back_to_ingest(tmp_path):
    """Files left in process/ from a crashed run are moved back to ingest/."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    stale = base / "process" / "stale.json"
    stale.write_text("data")

    moved = recover_stale_process(base)

    assert moved == 1
    assert not stale.exists()
    assert (base / "ingest" / "stale.json").read_text() == "data"


def test_recover_stale_process_noop_when_empty(tmp_path):
    """Empty process/ recovery is a no-op."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    assert recover_stale_process(base) == 0


def test_move_ingest_to_process_moves_all_files(tmp_path):
    """Bulk move from ingest/ to process/ relocates every file."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "a.json").write_text("a")
    (base / "ingest" / "b.fit").write_bytes(b"\x00")

    moved = move_ingest_to_process(base)

    assert moved == 2
    assert sorted(p.name for p in (base / "process").iterdir()) == ["a.json", "b.fit"]
    assert list((base / "ingest").iterdir()) == []


def test_move_files_to_storage_relocates_each_file(tmp_path):
    """A FileSet's files move from process/ to storage/."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    p1 = base / "process" / "x.json"
    p2 = base / "process" / "y.json"
    p1.write_text("x")
    p2.write_text("y")

    moved = move_files_to_storage([p1, p2], base)

    assert sorted(p.name for p in moved) == ["x.json", "y.json"]
    assert (base / "storage" / "x.json").read_text() == "x"
    assert (base / "storage" / "y.json").read_text() == "y"
    assert not p1.exists()
    assert not p2.exists()


def test_move_files_to_quarantine_relocates_each_file(tmp_path):
    """A failed FileSet's files move from process/ to quarantine/."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    p1 = base / "process" / "bad.json"
    p1.write_text("bad")

    moved = move_files_to_quarantine([p1], base)

    assert [p.name for p in moved] == ["bad.json"]
    assert (base / "quarantine" / "bad.json").read_text() == "bad"
    assert not p1.exists()


def test_move_overwrites_existing_destination(tmp_path):
    """Moving a file to a destination that already exists overwrites cleanly."""
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    src = base / "process" / "dup.json"
    src.write_text("new")
    (base / "storage" / "dup.json").write_text("old")

    move_files_to_storage([src], base)

    assert (base / "storage" / "dup.json").read_text() == "new"
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `pytest tests/test_lifecycle.py -v`
Expected: FAIL with `ImportError: cannot import name 'LIFECYCLE_DIRS' from 'garmin_health_data.lifecycle'` (module does not exist).

- [ ] **Step 1.3: Implement `lifecycle.py`**

Create `garmin_health_data/lifecycle.py`:

```python
"""
Filesystem lifecycle helpers for the four-folder extract/process pipeline.

The pipeline maintains a `garmin_files/` directory next to the SQLite database
with four subdirectories that represent file state:

- ingest/    Newly extracted files awaiting processing.
- process/   Files currently being processed (in-flight).
- storage/   Files successfully loaded into the database (kept as backup).
- quarantine/ Files that failed processing (kept for inspection).

This mirrors the openetl pipeline pattern. State transitions are filesystem
moves; a crashed run leaves files in process/, which the next run recovers
back to ingest/ before continuing.
"""

import shutil
from pathlib import Path
from typing import Iterable, List

LIFECYCLE_DIRS = ("ingest", "process", "storage", "quarantine")


def setup_lifecycle_dirs(base_dir: Path) -> None:
    """
    Create the four lifecycle subdirectories under base_dir.

    Idempotent: existing directories and contents are preserved.

    :param base_dir: Parent directory (e.g. <db_dir>/garmin_files).
    """
    base_dir.mkdir(parents=True, exist_ok=True)
    for name in LIFECYCLE_DIRS:
        (base_dir / name).mkdir(exist_ok=True)


def recover_stale_process(base_dir: Path) -> int:
    """
    Move every file in process/ back to ingest/.

    Called at the start of each run to recover from a previously-crashed run.
    Returns the number of files moved.

    :param base_dir: Lifecycle parent directory.
    :return: Count of files recovered.
    """
    process = base_dir / "process"
    ingest = base_dir / "ingest"
    moved = 0
    for src in process.iterdir():
        if src.is_file():
            dest = ingest / src.name
            shutil.move(str(src), str(dest))
            moved += 1
    return moved


def move_ingest_to_process(base_dir: Path) -> int:
    """
    Move every file from ingest/ to process/ in bulk.

    Called after extraction completes and before processing begins. Returns
    the count of files moved.

    :param base_dir: Lifecycle parent directory.
    :return: Count of files moved.
    """
    ingest = base_dir / "ingest"
    process = base_dir / "process"
    moved = 0
    for src in ingest.iterdir():
        if src.is_file():
            dest = process / src.name
            shutil.move(str(src), str(dest))
            moved += 1
    return moved


def move_files_to_storage(
    file_paths: Iterable[Path], base_dir: Path
) -> List[Path]:
    """
    Move the given files (typically a successfully-processed FileSet) to
    storage/. Returns the new paths.

    Existing files at the destination are overwritten.

    :param file_paths: Source file paths (typically inside process/).
    :param base_dir: Lifecycle parent directory.
    :return: List of new paths under storage/.
    """
    return _move_into(file_paths, base_dir / "storage")


def move_files_to_quarantine(
    file_paths: Iterable[Path], base_dir: Path
) -> List[Path]:
    """
    Move the given files (typically a failed FileSet) to quarantine/.

    :param file_paths: Source file paths (typically inside process/).
    :param base_dir: Lifecycle parent directory.
    :return: List of new paths under quarantine/.
    """
    return _move_into(file_paths, base_dir / "quarantine")


def _move_into(file_paths: Iterable[Path], dest_dir: Path) -> List[Path]:
    """Move every file into dest_dir, returning the new paths."""
    moved: List[Path] = []
    for src in file_paths:
        dest = dest_dir / src.name
        if dest.exists():
            dest.unlink()
        shutil.move(str(src), str(dest))
        moved.append(dest)
    return moved
```

- [ ] **Step 1.4: Run tests to verify they pass**

Run: `pytest tests/test_lifecycle.py -v`
Expected: 8 passed.

- [ ] **Step 1.5: Format and commit**

```bash
cd /Users/diegoscarabelli/repos/garmin-health-data-worktrees/keep-files
python -m black garmin_health_data/lifecycle.py tests/test_lifecycle.py
python -m autoflake --in-place --remove-all-unused-imports garmin_health_data/lifecycle.py tests/test_lifecycle.py
python -m docformatter --in-place garmin_health_data/lifecycle.py tests/test_lifecycle.py
git add garmin_health_data/lifecycle.py tests/test_lifecycle.py
git commit -m "feat(lifecycle): add ingest/process/storage/quarantine helpers"
```

---

## Task 2: Lifecycle module — `fcntl.flock` based concurrency lock

**Files:**
- Modify: `garmin_health_data/lifecycle.py` (add `acquire_lock` context manager + `LockHeldError` exception)
- Modify: `tests/test_lifecycle.py` (lock tests)

**Rationale:** Two concurrent `garmin extract` invocations (cron + manual) would race on file moves between lifecycle directories and could corrupt the pipeline state. `fcntl.flock` on a `.lock` file in the lifecycle parent provides advisory locking with automatic release on process death (no stale locks survive a crash).

- [ ] **Step 2.1: Write the failing tests**

Append to `tests/test_lifecycle.py`:

```python
def test_acquire_lock_succeeds_when_unheld(tmp_path):
    """First lock acquisition succeeds and creates the .lock file."""
    from garmin_health_data.lifecycle import acquire_lock, setup_lifecycle_dirs

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        assert (base / ".lock").exists()


def test_acquire_lock_raises_when_held_by_another_process(tmp_path):
    """A second concurrent acquisition raises LockHeldError."""
    from garmin_health_data.lifecycle import (
        LockHeldError,
        acquire_lock,
        setup_lifecycle_dirs,
    )

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        with pytest.raises(LockHeldError):
            with acquire_lock(base):
                pass


def test_acquire_lock_releases_after_context_exit(tmp_path):
    """Lock is released when context exits, allowing re-acquisition."""
    from garmin_health_data.lifecycle import acquire_lock, setup_lifecycle_dirs

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        pass
    # Should succeed.
    with acquire_lock(base):
        pass


def test_acquire_lock_releases_on_exception(tmp_path):
    """Lock is released even if the with-block raises."""
    from garmin_health_data.lifecycle import acquire_lock, setup_lifecycle_dirs

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with pytest.raises(RuntimeError):
        with acquire_lock(base):
            raise RuntimeError("boom")
    # Should be re-acquirable now.
    with acquire_lock(base):
        pass
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `pytest tests/test_lifecycle.py -v -k "acquire_lock"`
Expected: 4 FAIL with `ImportError: cannot import name 'acquire_lock' from 'garmin_health_data.lifecycle'`.

- [ ] **Step 2.3: Add `acquire_lock` and `LockHeldError` to `lifecycle.py`**

Add at the top of `garmin_health_data/lifecycle.py`:

```python
import fcntl
from contextlib import contextmanager
```

Add (after the existing `LIFECYCLE_DIRS` constant):

```python
class LockHeldError(RuntimeError):
    """Raised when the lifecycle lock is held by another process."""


@contextmanager
def acquire_lock(base_dir: Path):
    """
    Acquire an exclusive advisory lock on `<base_dir>/.lock`.

    Uses `fcntl.flock` with `LOCK_EX | LOCK_NB` so a held lock fails fast
    with `LockHeldError` rather than blocking. The lock is released
    automatically when the context exits or the process dies.

    :param base_dir: Lifecycle parent directory (must already exist).
    :raises LockHeldError: If another process holds the lock.
    """
    lock_path = base_dir / ".lock"
    lock_path.touch(exist_ok=True)
    f = open(lock_path, "r+")
    try:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as e:
            raise LockHeldError(
                f"Another garmin extract run is in progress (lock held on "
                f"{lock_path}). Wait for it to finish or remove the lock "
                f"file if no process is running."
            ) from e
        try:
            yield
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    finally:
        f.close()
```

- [ ] **Step 2.4: Run tests**

Run: `pytest tests/test_lifecycle.py -v`
Expected: all green (12 tests total: 8 from Task 1 + 4 from Task 2).

- [ ] **Step 2.5: Commit**

```bash
python -m black garmin_health_data/lifecycle.py tests/test_lifecycle.py
git add garmin_health_data/lifecycle.py tests/test_lifecycle.py
git commit -m "feat(lifecycle): add fcntl.flock concurrency lock"
```

---

## Task 3: Rename `_process_day_by_day` → `_extract_day_by_day` and add per-date isolation

**Files:**
- Modify: `garmin_health_data/extractor.py:263-311` (function definition and body)
- Modify: `garmin_health_data/extractor.py:343` (single caller)
- Modify: `tests/test_extractor.py` (add per-date isolation test)

**Rationale:** The function does extraction (API calls + write JSON), not processing. The name was inherited from openetl and is misleading. Adding try/except around the API call lets one bad day fail without aborting the rest of the date range.

- [ ] **Step 2.1: Write the failing per-date isolation test**

Append to `tests/test_extractor.py`:

```python
def test_extract_day_by_day_isolates_per_date_failures(tmp_path):
    """
    A transient API failure on one date does not abort extraction of
    subsequent dates. The failure is recorded in the per-day failures list.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.constants import GARMIN_DATA_REGISTRY
    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 3),
        ingest_dir=tmp_path,
        data_types=("SLEEP",),
    )
    extractor.user_id = "test-user"

    sleep_type = GARMIN_DATA_REGISTRY.get_by_name("SLEEP")
    mock_api = MagicMock(
        side_effect=[
            {"value": "ok-day-1"},
            RuntimeError("transient API hiccup"),
            {"value": "ok-day-3"},
        ]
    )
    extractor.garmin_client = MagicMock()
    setattr(extractor.garmin_client, sleep_type.api_method, mock_api)

    saved = extractor._extract_day_by_day(
        sleep_type, date(2025, 1, 1), date(2025, 1, 3)
    )

    assert len(saved) == 2  # two successes, one failure skipped
    assert mock_api.call_count == 3
    assert any("2025-01-02" in f.error for f in extractor.failures)
```

- [ ] **Step 2.2: Run test to verify it fails**

Run: `pytest tests/test_extractor.py::test_extract_day_by_day_isolates_per_date_failures -v`
Expected: FAIL with `AttributeError: 'GarminExtractor' object has no attribute '_extract_day_by_day'`.

- [ ] **Step 2.3: Rename and add per-date try/except**

In `garmin_health_data/extractor.py`:

Add `failures: List["ExtractionFailure"]` initialization in `__init__` (will define `ExtractionFailure` in Task 5; for now use a simple namespace placeholder):

At the top of the file, add:
```python
from dataclasses import dataclass, field
```

Add a placeholder dataclass near the top (will be expanded in Task 5):
```python
@dataclass
class ExtractionFailure:
    """A single extraction failure (per-date, per-data-type, or per-activity)."""
    data_type: str
    date: str  # ISO date or "" for non-date contexts
    activity_id: str  # "" if not applicable
    error: str
```

In `__init__`, add: `self.failures: List[ExtractionFailure] = []`

Rename the function and rewrite the body:

```python
def _extract_day_by_day(
    self, data_type: GarminDataType, start_date: date, end_date: date
) -> List[Path]:
    """
    Extract Garmin data type one day at a time with per-date error isolation.

    Handles both DAILY and RANGE API time parameter patterns by processing
    each day individually. A failure on one date is logged and recorded in
    self.failures; extraction continues with the next date.

    :param data_type: GarminDataType defining the extraction parameters.
    :param start_date: Start date for data extraction (inclusive).
    :param end_date: End date for data extraction (inclusive).
    :return: List of saved file paths.
    """
    saved_files = []
    current_date = start_date

    while current_date <= end_date:
        click.echo(
            f"Fetching {data_type.emoji} {data_type.name} data for "
            f"{current_date}."
        )
        date_str = current_date.strftime("%Y-%m-%d")
        try:
            api_method = getattr(self.garmin_client, data_type.api_method)
            if data_type.api_method_time_param == APIMethodTimeParam.DAILY:
                data = api_method(date_str)
            else:
                data = api_method(date_str, date_str)

            if data:
                saved_files.extend(
                    self._save_garmin_data(data, data_type, current_date)
                )
            else:
                click.secho(
                    f"{data_type.emoji} {data_type.name}: No data for "
                    f"{current_date}.",
                    fg="yellow",
                )
        except Exception as e:
            click.secho(
                f"⚠️  {data_type.name} {date_str} failed: "
                f"{type(e).__name__}: {e}. Continuing.",
                fg="red",
            )
            self.failures.append(
                ExtractionFailure(
                    data_type=data_type.name,
                    date=date_str,
                    activity_id="",
                    error=f"{type(e).__name__}: {e}",
                )
            )

        current_date += timedelta(days=1)
        time.sleep(0.1)  # Rate limiting.

    return saved_files
```

Update the single caller at line 343 from `self._process_day_by_day(...)` to `self._extract_day_by_day(...)`.

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `pytest tests/test_extractor.py -v`
Expected: all green, including the new test.

Run: `pytest -q`
Expected: all green; no other test references the old name.

- [ ] **Step 2.5: Format and commit**

```bash
python -m black garmin_health_data/extractor.py tests/test_extractor.py
git add garmin_health_data/extractor.py tests/test_extractor.py
git commit -m "refactor(extractor): rename _process_day_by_day, add per-date try/except"
```

---

## Task 4: Add per-data-type isolation in `extract_garmin_data`

**Files:**
- Modify: `garmin_health_data/extractor.py:255-259` (the registry loop)
- Modify: `tests/test_extractor.py` (add per-data-type isolation test)

**Rationale:** Catches NO_DATE-type failures (which have no inner per-date loop), setup errors that escape the per-date layer, and any unexpected exception so one bad data type doesn't kill the others for that account.

- [ ] **Step 3.1: Write the failing test**

Append to `tests/test_extractor.py`:

```python
def test_extract_garmin_data_isolates_per_data_type_failures(tmp_path):
    """
    A failure inside _extract_data_by_type for one type does not abort
    extraction of remaining types. The failure is recorded in self.failures.
    """
    from datetime import date
    from unittest.mock import MagicMock, patch

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("SLEEP", "HEART_RATE"),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    def fake_extract(data_type, *_):
        if data_type.name == "SLEEP":
            raise RuntimeError("SLEEP endpoint went away")
        return [tmp_path / "fake.json"]

    with patch.object(extractor, "_extract_data_by_type", side_effect=fake_extract):
        saved = extractor.extract_garmin_data()

    assert len(saved) == 1  # HEART_RATE succeeded
    assert any(f.data_type == "SLEEP" for f in extractor.failures)
```

- [ ] **Step 3.2: Run test to verify it fails**

Run: `pytest tests/test_extractor.py::test_extract_garmin_data_isolates_per_data_type_failures -v`
Expected: FAIL with `RuntimeError: SLEEP endpoint went away` propagating up.

- [ ] **Step 3.3: Wrap the per-data-type loop**

Replace the loop at `garmin_health_data/extractor.py:252-259`:

```python
# Extract Garmin data by iterating over selected data types.
saved_files = []

for data_type in data_types_to_extract:
    try:
        files = self._extract_data_by_type(
            data_type, self.start_date, self.end_date
        )
        saved_files.extend(files)
    except Exception as e:
        click.secho(
            f"⚠️  {data_type.name} extraction failed entirely: "
            f"{type(e).__name__}: {e}. Continuing with next data type.",
            fg="red",
        )
        self.failures.append(
            ExtractionFailure(
                data_type=data_type.name,
                date="",
                activity_id="",
                error=f"{type(e).__name__}: {e}",
            )
        )

return saved_files
```

- [ ] **Step 3.4: Run tests**

Run: `pytest tests/test_extractor.py -v`
Expected: all green.

- [ ] **Step 3.5: Commit**

```bash
git add garmin_health_data/extractor.py tests/test_extractor.py
git commit -m "feat(extractor): add per-data-type try/except in extract_garmin_data"
```

---

## Task 5: Broaden per-activity isolation and wrap activity-list call

**Files:**
- Modify: `garmin_health_data/extractor.py:482-590` (`extract_fit_activities`)
- Modify: `tests/test_extractor.py` (add tests)

**Rationale:** Today only `GarminConnectionError` is caught in the per-activity loop; a parse error or any other exception aborts the run mid-loop. Also, `get_activities_by_date` (the activity-list fetch) has no try/except — if it fails, zero activities are saved and the per-activity protection is moot. We add a dedicated try/except there with a clean "skip ACTIVITY for this account" failure mode.

- [ ] **Step 4.1: Write the failing tests**

Append to `tests/test_extractor.py`:

```python
def test_extract_fit_activities_handles_list_call_failure(tmp_path):
    """
    If the activity-list API call fails, extract_fit_activities returns an
    empty list and records a single failure. No activities are downloaded.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.side_effect = RuntimeError(
        "list endpoint 500"
    )

    result = extractor.extract_fit_activities()

    assert result == []
    assert any(f.data_type == "ACTIVITIES_LIST" for f in extractor.failures)


def test_extract_fit_activities_isolates_per_activity_failures(tmp_path):
    """
    A non-connection exception during one activity download does not abort
    the loop. Subsequent activities still download.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.return_value = [
        {"activityId": 1, "startTimeLocal": "2025-01-01 10:00:00",
         "activityType": {"typeKey": "running"}},
        {"activityId": 2, "startTimeLocal": "2025-01-01 12:00:00",
         "activityType": {"typeKey": "running"}},
    ]
    # First download raises a non-GarminConnectionError; second returns a
    # tiny FIT-like blob that survives _extract_activity_content.
    extractor.garmin_client.download_activity.side_effect = [
        ValueError("boom"),
        b"PK\x03\x04dummyzip",  # will fail _extract_activity_content -> None
    ]

    extractor.extract_fit_activities()

    # Both attempts processed; first recorded as failure.
    assert any(f.activity_id == "1" for f in extractor.failures)
```

- [ ] **Step 4.2: Run tests to verify they fail**

Run: `pytest tests/test_extractor.py::test_extract_fit_activities_handles_list_call_failure tests/test_extractor.py::test_extract_fit_activities_isolates_per_activity_failures -v`
Expected: both FAIL.

- [ ] **Step 4.3: Wrap the list call and broaden the per-activity catch**

In `garmin_health_data/extractor.py`, replace the body of `extract_fit_activities` around lines 500-546.

Replace:
```python
# Get list of activities, API is inclusive of both dates.
start_str = self.start_date.strftime("%Y-%m-%d")
end_str = self.end_date.strftime("%Y-%m-%d")
activities = self.garmin_client.get_activities_by_date(start_str, end_str)
```

With:
```python
start_str = self.start_date.strftime("%Y-%m-%d")
end_str = self.end_date.strftime("%Y-%m-%d")
try:
    activities = self.garmin_client.get_activities_by_date(start_str, end_str)
except Exception as e:
    click.secho(
        f"⚠️  Activity list fetch failed: {type(e).__name__}: {e}. "
        f"No activity files will be downloaded for this account.",
        fg="red",
    )
    self.failures.append(
        ExtractionFailure(
            data_type="ACTIVITIES_LIST",
            date=f"{start_str}..{end_str}",
            activity_id="",
            error=f"{type(e).__name__}: {e}",
        )
    )
    return []
```

Replace the per-activity try/except (lines 536-546):
```python
try:
    raw_data = self.garmin_client.download_activity(
        activity_id,
        dl_fmt=ActivityDownloadFormat.ORIGINAL,
    )
except GarminConnectionError as e:
    click.secho(
        f"⚠️  Skipping activity {activity_id}: {e}.",
        fg="yellow",
    )
    continue
```

With:
```python
try:
    raw_data = self.garmin_client.download_activity(
        activity_id,
        dl_fmt=ActivityDownloadFormat.ORIGINAL,
    )
except Exception as e:
    click.secho(
        f"⚠️  Skipping activity {activity_id}: "
        f"{type(e).__name__}: {e}.",
        fg="yellow",
    )
    self.failures.append(
        ExtractionFailure(
            data_type="ACTIVITY",
            date="",
            activity_id=str(activity_id),
            error=f"{type(e).__name__}: {e}",
        )
    )
    continue
```

- [ ] **Step 4.4: Run tests**

Run: `pytest tests/test_extractor.py -v`
Expected: all green.

- [ ] **Step 4.5: Commit**

```bash
git add garmin_health_data/extractor.py tests/test_extractor.py
git commit -m "feat(extractor): broaden per-activity isolation, wrap activity-list fetch"
```

---

## Task 6: Refactor `extract_fit_activities` to read `ACTIVITIES_LIST` from disk

**Files:**
- Modify: `garmin_health_data/extractor.py:482-505` (top of `extract_fit_activities`)
- Modify: `tests/test_extractor.py` (add tests)

**Rationale:** Today both `_extract_data_by_type` (for the `ACTIVITIES_LIST` data type) and `extract_fit_activities` independently call `get_activities_by_date(start, end)` for the same date range. Two API calls hitting the same endpoint is wasteful and creates the failure mode where one call succeeds and the other fails. Read the saved `ACTIVITIES_LIST` JSON file from `ingest/` (where the registry loop just wrote it) and fall back to the API call if it's missing.

- [ ] **Step 6.1: Write the failing tests**

Append to `tests/test_extractor.py`:

```python
def test_extract_fit_activities_reads_activities_list_from_disk(tmp_path):
    """
    When an ACTIVITIES_LIST JSON file exists in ingest_dir, the activity
    list is read from disk and the API is NOT called.
    """
    import json
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()

    # Pre-write the saved ACTIVITIES_LIST JSON.
    list_file = tmp_path / (
        "test-user_ACTIVITIES_LIST_2025-01-01T12-00-00+00-00.json"
    )
    list_file.write_text(json.dumps([]))

    extractor.extract_fit_activities()

    extractor.garmin_client.get_activities_by_date.assert_not_called()


def test_extract_fit_activities_falls_back_to_api_when_file_missing(tmp_path):
    """
    When no ACTIVITIES_LIST file is in ingest_dir, the API call is used.
    """
    from datetime import date
    from unittest.mock import MagicMock

    from garmin_health_data.extractor import GarminExtractor

    extractor = GarminExtractor(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 1),
        ingest_dir=tmp_path,
        data_types=("ACTIVITY",),
    )
    extractor.user_id = "test-user"
    extractor.garmin_client = MagicMock()
    extractor.garmin_client.get_activities_by_date.return_value = []

    extractor.extract_fit_activities()

    extractor.garmin_client.get_activities_by_date.assert_called_once()
```

- [ ] **Step 6.2: Run tests to verify they fail**

Run: `pytest tests/test_extractor.py::test_extract_fit_activities_reads_activities_list_from_disk -v`
Expected: FAIL — API still called even when file exists.

- [ ] **Step 6.3: Add disk-read fallback in `extract_fit_activities`**

In `garmin_health_data/extractor.py`, replace the activity-list fetch block (the try/except added in Task 5):

```python
start_str = self.start_date.strftime("%Y-%m-%d")
end_str = self.end_date.strftime("%Y-%m-%d")

# Try reading the saved ACTIVITIES_LIST file from ingest_dir first to
# avoid a duplicate API call.
activities = self._load_activities_list_from_disk()
if activities is None:
    try:
        activities = self.garmin_client.get_activities_by_date(
            start_str, end_str
        )
    except Exception as e:
        click.secho(
            f"⚠️  Activity list fetch failed: {type(e).__name__}: {e}. "
            f"No activity files will be downloaded for this account.",
            fg="red",
        )
        self.failures.append(
            ExtractionFailure(
                data_type="ACTIVITIES_LIST",
                date=f"{start_str}..{end_str}",
                activity_id="",
                error=f"{type(e).__name__}: {e}",
            )
        )
        return []
```

Add a new helper method on `GarminExtractor`:

```python
def _load_activities_list_from_disk(self) -> Optional[list]:
    """
    Read the ACTIVITIES_LIST JSON file from ingest_dir if present.

    The registry-driven extract loop writes one such file per run. Returns
    the parsed activities list, or None if no file exists.

    :return: Activities list or None.
    """
    import json

    pattern = f"{self.user_id}_ACTIVITIES_LIST_*.json"
    matches = sorted(self.ingest_dir.glob(pattern))
    if not matches:
        return None
    # Use the newest file in case multiple runs left files behind.
    try:
        with open(matches[-1], "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        click.secho(
            f"⚠️  Could not read {matches[-1].name}: {e}. "
            f"Falling back to API call.",
            fg="yellow",
        )
        return None
```

Add `from typing import Optional` to imports if not already present (it is).

- [ ] **Step 6.4: Run tests**

Run: `pytest tests/test_extractor.py -v`
Expected: all green.

- [ ] **Step 6.5: Commit**

```bash
python -m black garmin_health_data/extractor.py tests/test_extractor.py
git add garmin_health_data/extractor.py tests/test_extractor.py
git commit -m "refactor(extractor): read ACTIVITIES_LIST from disk to avoid duplicate API call"
```

---

## Task 7: Return ExtractionSummary from `extract()`

**Files:**
- Modify: `garmin_health_data/extractor.py:634-805` (the module-level `extract` function)
- Modify: `tests/test_extractor.py` (add summary test)

**Rationale:** The CLI needs a structured summary to render at end-of-run. Each per-account extractor's `failures` list rolls up into a single summary object. Today the function returns `{"garmin_files": int, "activity_files": int}`; we extend it to include `failures` and per-account success counts without breaking the existing two keys.

- [ ] **Step 5.1: Write the failing test**

Append to `tests/test_extractor.py`:

```python
def test_extract_returns_summary_with_failures(tmp_path):
    """
    The module-level extract() returns a dict containing failures from all
    per-account extractors plus the existing file counts.
    """
    from unittest.mock import MagicMock, patch

    from garmin_health_data.extractor import (
        ExtractionFailure,
        extract,
    )

    fake_extractor = MagicMock()
    fake_extractor.extract_garmin_data.return_value = []
    fake_extractor.extract_fit_activities.return_value = []
    fake_extractor.failures = [
        ExtractionFailure(
            data_type="SLEEP", date="2025-01-02",
            activity_id="", error="RuntimeError: hiccup",
        )
    ]

    with patch(
        "garmin_health_data.extractor.discover_accounts",
        return_value=[("user-1", tmp_path)],
    ), patch(
        "garmin_health_data.extractor.GarminExtractor",
        return_value=fake_extractor,
    ):
        result = extract(
            ingest_dir=tmp_path,
            data_interval_start="2025-01-01",
            data_interval_end="2025-01-03",
        )

    assert "failures" in result
    assert len(result["failures"]) == 1
    assert result["failures"][0].data_type == "SLEEP"
```

- [ ] **Step 5.2: Run test to verify it fails**

Run: `pytest tests/test_extractor.py::test_extract_returns_summary_with_failures -v`
Expected: FAIL — `result` dict lacks `"failures"` key.

- [ ] **Step 5.3: Aggregate failures and add to return dict**

In `garmin_health_data/extractor.py` `extract()` function, after the per-account loop and before the existing return statement, add:

```python
all_failures = []
for user_id, _ in discovered:
    # Note: the fake_extractor in tests is the same instance for both
    # accounts, so we collect from the actual instances created above.
    pass
```

Easier approach — collect failures inside the loop. Change the existing per-account loop body to also extend `all_failures`:

```python
all_garmin_files = []
all_activity_files = []
all_failures: List[ExtractionFailure] = []
failed_accounts = []

for user_id, token_dir in discovered:
    try:
        ...
        extractor = GarminExtractor(start_date, end_date, ingest_dir, data_types)
        extractor.authenticate(token_store_dir=str(token_dir))
        garmin_files = extractor.extract_garmin_data()
        activity_files = []
        if data_types is None or (
            data_types and {"ACTIVITY", "EXERCISE_SETS"} & set(data_types)
        ):
            activity_files = extractor.extract_fit_activities()
        all_garmin_files.extend(garmin_files)
        all_activity_files.extend(activity_files)
        all_failures.extend(extractor.failures)
    except Exception:
        ...
        failed_accounts.append(user_id)
```

Update the final `return` to include `failures`:

```python
return {
    "garmin_files": len(all_garmin_files),
    "activity_files": len(all_activity_files),
    "failures": all_failures,
    "failed_accounts": failed_accounts,
}
```

Make sure `ExtractionFailure` is imported / defined at module level (it already is from Task 2).

- [ ] **Step 5.4: Run tests**

Run: `pytest tests/test_extractor.py -v`
Expected: all green.

- [ ] **Step 5.5: Commit**

```bash
git add garmin_health_data/extractor.py tests/test_extractor.py
git commit -m "feat(extractor): return ExtractionSummary with per-account failures"
```

---

## Task 8: CLI — replace temp dir with lifecycle, add stale recovery and bulk move

**Files:**
- Modify: `garmin_health_data/cli.py:193-247` (extraction directory setup and Step 1 block)
- Modify: `tests/test_cli.py` (replace `--keep-files` tests with lifecycle tests)

**Rationale:** Lifecycle directories are now the default. Stale `process/` files from a crashed run get moved back to `ingest/` before extract starts. After extract, `ingest/` contents move in bulk to `process/` so the processing loop knows exactly what to work on.

- [ ] **Step 6.1: Write the failing tests**

Edit `tests/test_cli.py`, remove the three `--keep-files` tests added earlier, and replace with:

```python
def _stub_extract_no_files(*args, **kwargs):
    """Stub returning no extraction results so the CLI exits cleanly."""
    return {
        "garmin_files": 0,
        "activity_files": 0,
        "failures": [],
        "failed_accounts": [],
    }


def test_extract_creates_lifecycle_dirs_next_to_db(tmp_path):
    """
    The extract command creates garmin_files/{ingest,process,storage,quarantine}
    next to the database file before extraction runs.
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
            ["--db-path", str(db_path),
             "--start-date", "2025-01-01", "--end-date", "2025-01-02"],
        )

    assert result.exit_code == 0, result.output
    base = tmp_path / "garmin_files"
    for name in ("ingest", "process", "storage", "quarantine"):
        assert (base / name).is_dir()


def test_extract_recovers_stale_process_files(tmp_path):
    """
    Files left in process/ from a crashed run are moved back to ingest/
    before the new run starts.
    """
    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    (base / "process").mkdir(parents=True)
    (base / "ingest").mkdir(parents=True)
    (base / "storage").mkdir(parents=True)
    (base / "quarantine").mkdir(parents=True)
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
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path),
             "--start-date", "2025-01-01", "--end-date", "2025-01-02"],
        )

    assert result.exit_code == 0, result.output
    assert not stale.exists()
    assert (base / "ingest" / "stale.json").read_text() == '{"old": true}'
```

- [ ] **Step 6.2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: lifecycle tests FAIL (no garmin_files/ dirs created).

- [ ] **Step 6.3: Replace temp-dir setup with lifecycle**

In `garmin_health_data/cli.py`, replace the directory setup block (currently lines 193-201):

```python
# Determine extraction directory. When --keep-files is set, save files
# alongside the database for permanent retention; otherwise use a temp
# directory that gets cleaned up after processing.
if keep_files:
    ingest_dir = Path(db_path).expanduser().resolve().parent / "garmin_files"
    click.echo(f"💾 Keeping extracted files at: {ingest_dir}")
else:
    ingest_dir = Path(tempfile.gettempdir()) / "garmin_extraction"
ingest_dir.mkdir(exist_ok=True, parents=True)
```

With:

```python
# Set up the four-folder lifecycle: ingest, process, storage, quarantine.
files_root = Path(db_path).expanduser().resolve().parent / "garmin_files"
setup_lifecycle_dirs(files_root)
ingest_dir = files_root / "ingest"

# Recover any files left in process/ from a previously crashed run.
recovered = recover_stale_process(files_root)
if recovered:
    click.secho(
        f"♻️  Recovered {recovered} file(s) from a previous run "
        f"(process/ → ingest/).",
        fg="cyan",
    )

click.echo(f"💾 Files directory: {files_root}")
```

Add the import at the top of cli.py:
```python
from garmin_health_data.lifecycle import (
    move_files_to_quarantine,
    move_files_to_storage,
    move_ingest_to_process,
    recover_stale_process,
    setup_lifecycle_dirs,
)
```

Remove `import tempfile` and `import shutil` (no longer used).

In Step 2 of the extract command, **after** the extraction call returns and **before** the file glob, add:

```python
# Move all extracted files from ingest/ to process/ before parsing.
moved_to_process = move_ingest_to_process(files_root)
process_dir = files_root / "process"
```

Change the glob from `ingest_dir.glob("**/*")` to `process_dir.glob("**/*")`.

Remove the `finally` block that does `rmtree(ingest_dir)` — files are kept by default now.

- [ ] **Step 6.4: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green.

- [ ] **Step 6.5: Commit**

```bash
git add garmin_health_data/cli.py tests/test_cli.py
git commit -m "feat(cli): replace temp-dir with four-folder lifecycle, add stale recovery"
```

---

## Task 9: CLI — per-FileSet session, try/except, and storage/quarantine routing

**Files:**
- Modify: `garmin_health_data/cli.py:298-345` (the process loop)
- Modify: `tests/test_cli.py` (add per-FileSet isolation test)

**Rationale:** Mirror openetl's `_try_process_file_set` pattern: each FileSet gets its own SQLAlchemy session and is wrapped in try/except. Successful FileSets' files move to `storage/`; failed FileSets' files move to `quarantine/`. This lets one bad day fail without poisoning the others or losing the work that succeeded.

- [ ] **Step 7.1: Write the failing test**

Append to `tests/test_cli.py`:

```python
def test_process_loop_isolates_per_fileset_failures(tmp_path):
    """
    A FileSet that raises during processing is moved to quarantine/, while
    successful FileSets are moved to storage/. The CLI exits cleanly.
    """
    from unittest.mock import patch

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    for name in ("ingest", "process", "storage", "quarantine"):
        (base / name).mkdir(parents=True)

    # Fake two FileSets in ingest/ before the run, one good one bad.
    good = base / "ingest" / "user1_SLEEP_2025-01-01T12-00-00+00-00.json"
    bad = base / "ingest" / "user1_SLEEP_2025-01-02T12-00-00+00-00.json"
    good.write_text('{"ok": true}')
    bad.write_text('{"corrupt": true}')

    def stub_extract(*args, **kwargs):
        return {"garmin_files": 2, "activity_files": 0,
                "failures": [], "failed_accounts": []}

    call_count = {"n": 0}

    def stub_process(self, file_set, session):
        call_count["n"] += 1
        # Second FileSet fails.
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
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path),
             "--start-date", "2025-01-01", "--end-date", "2025-01-02"],
        )

    assert result.exit_code == 0, result.output
    assert call_count["n"] == 2  # both FileSets attempted
    assert (base / "storage" / good.name).exists()
    assert (base / "quarantine" / bad.name).exists()
    assert not good.exists()
    assert not bad.exists()
```

- [ ] **Step 7.2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_process_loop_isolates_per_fileset_failures -v`
Expected: FAIL — exception propagates and aborts the loop.

- [ ] **Step 7.3: Refactor the process loop**

In `garmin_health_data/cli.py`, find the current process loop (around lines 299-338):

```python
total_processed = 0
with get_session(db_path) as session:
    for (uid, timestamp_str), timestamp_files in files_by_key.items():
        ...
        processor = GarminProcessor(file_set, session)
        processor.process_file_set(file_set, session)
        total_processed += len(timestamp_files)
```

Replace with:

```python
total_processed = 0
total_quarantined = 0
for (uid, timestamp_str), timestamp_files in files_by_key.items():
    # Organize files by data type for this timestamp
    files_by_type = {}
    for file_path in timestamp_files:
        matched = False
        for file_type_enum in GARMIN_FILE_TYPES:
            if file_type_enum.value.match(file_path.name):
                files_by_type.setdefault(file_type_enum, []).append(file_path)
                matched = True
                break
        if not matched:
            click.secho(
                f"⚠️  No matching pattern for file: {file_path.name}",
                fg="yellow",
            )

    if not files_by_type:
        # No matched files in this group; leave them in process/ so the next
        # run sees them. (This is rare and indicates an unsupported file type.)
        continue

    matched_paths = [p for paths in files_by_type.values() for p in paths]
    file_set = FileSet(file_paths=matched_paths, files=files_by_type)

    # One session per FileSet for transaction isolation.
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
    f"✅ Processed {format_count(total_processed)} files; "
    f"❌ quarantined {format_count(total_quarantined)} files.",
    fg="green",
)
```

Note: `processor.process_file_set` may already commit internally — check by reading `garmin_health_data/processor.py`. If it does, drop the explicit `session.commit()` here. If it doesn't, keep it.

- [ ] **Step 7.4: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green.

Run: `pytest -q`
Expected: all green.

- [ ] **Step 7.5: Commit**

```bash
git add garmin_health_data/cli.py tests/test_cli.py
git commit -m "feat(cli): per-FileSet session+isolation, route to storage/quarantine"
```

---

## Task 10: CLI — print extraction summary at end of run

**Files:**
- Modify: `garmin_health_data/cli.py` (Step 3 / Summary section, around lines 349-376)
- Modify: `tests/test_cli.py` (add summary test)

**Rationale:** Surface every gap so the user knows exactly what to retry. Group failures by account → data type → date/activity for readability.

- [ ] **Step 8.1: Write the failing test**

Append to `tests/test_cli.py`:

```python
def test_extract_prints_failure_summary(tmp_path):
    """
    End-of-run summary lists per-data-type failures from the extractor.
    """
    from unittest.mock import patch

    from garmin_health_data.extractor import ExtractionFailure

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    def stub_extract(*args, **kwargs):
        return {
            "garmin_files": 0,
            "activity_files": 0,
            "failures": [
                ExtractionFailure(
                    data_type="SLEEP", date="2025-01-02",
                    activity_id="", error="RuntimeError: hiccup"),
                ExtractionFailure(
                    data_type="SLEEP", date="2025-01-03",
                    activity_id="", error="RuntimeError: hiccup again"),
            ],
            "failed_accounts": [],
        }

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path),
             "--start-date", "2025-01-01", "--end-date", "2025-01-03"],
        )

    assert result.exit_code == 0, result.output
    assert "Extraction failures" in result.output
    assert "SLEEP" in result.output
    assert "2025-01-02" in result.output
    assert "2025-01-03" in result.output
```

- [ ] **Step 8.2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_extract_prints_failure_summary -v`
Expected: FAIL — no summary printed.

- [ ] **Step 8.3: Add summary rendering**

In `garmin_health_data/cli.py`, inside the Step 3 summary block (around line 350), add the failure rendering before the "🎉 Extraction complete!" line:

```python
failures = result.get("failures", [])
if failures:
    click.echo()
    click.secho(
        f"⚠️  Extraction failures ({len(failures)}):",
        fg="yellow", bold=True,
    )
    # Group by data_type for readability.
    by_type: dict = {}
    for f in failures:
        by_type.setdefault(f.data_type, []).append(f)
    for dt, items in sorted(by_type.items()):
        click.echo(f"   • {dt}: {len(items)} failure(s)")
        for item in items[:5]:  # cap at 5 per type to avoid spam
            label = item.date or item.activity_id or "(no context)"
            click.echo(f"       - {label}: {item.error}")
        if len(items) > 5:
            click.echo(f"       ... and {len(items) - 5} more.")
```

- [ ] **Step 8.4: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green.

- [ ] **Step 8.5: Commit**

```bash
git add garmin_health_data/cli.py tests/test_cli.py
git commit -m "feat(cli): print per-data-type failure summary at end of run"
```

---

## Task 11: CLI — add `--extract-only` and `--process-only` flags

**Files:**
- Modify: `garmin_health_data/cli.py` (extract command options + control flow)
- Modify: `tests/test_cli.py`

**Rationale:** With the lifecycle layout, `ingest/` is a stable staging area. Users can extract without processing (just back up files), or skip extraction and re-process whatever is in `ingest/` (useful after fixing a parsing bug or after processing crashed mid-run).

- [ ] **Step 9.1: Write the failing tests**

Append to `tests/test_cli.py`:

```python
def test_extract_only_skips_processing(tmp_path):
    """
    --extract-only writes files to ingest/ and stops without moving them
    to process/ or touching the DB.
    """
    from unittest.mock import patch

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    def stub_extract(*args, **kwargs):
        ingest = kwargs["ingest_dir"]
        (ingest / "user1_SLEEP_2025-01-01T12-00-00+00-00.json").write_text("{}")
        return {"garmin_files": 1, "activity_files": 0,
                "failures": [], "failed_accounts": []}

    runner = CliRunner()
    with (
        patch("garmin_health_data.cli.ensure_authenticated"),
        patch("garmin_health_data.cli.extract_data", side_effect=stub_extract),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path), "--extract-only",
             "--start-date", "2025-01-01", "--end-date", "2025-01-02"],
        )

    assert result.exit_code == 0, result.output
    base = tmp_path / "garmin_files"
    assert list((base / "ingest").iterdir())  # file still in ingest
    assert not list((base / "process").iterdir())
    assert not list((base / "storage").iterdir())


def test_process_only_skips_extraction(tmp_path):
    """
    --process-only does not call the extract API; it only processes whatever
    is currently in ingest/.
    """
    from unittest.mock import MagicMock, patch

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    for name in ("ingest", "process", "storage", "quarantine"):
        (base / name).mkdir(parents=True)
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
```

- [ ] **Step 9.2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py::test_extract_only_skips_processing tests/test_cli.py::test_process_only_skips_extraction -v`
Expected: FAIL — flags not yet defined.

- [ ] **Step 9.3: Add the flags and conditional logic**

In `garmin_health_data/cli.py`, add two more `@click.option` decorators on `extract`:

```python
@click.option(
    "--extract-only",
    is_flag=True,
    default=False,
    help="Extract files into ingest/ and stop. Do not move to process/ "
    "or load into the database.",
)
@click.option(
    "--process-only",
    is_flag=True,
    default=False,
    help="Skip extraction. Process whatever files are currently in ingest/.",
)
```

Add `extract_only: bool, process_only: bool` to the `extract()` signature. Validate near the top of the function:

```python
if extract_only and process_only:
    click.secho(
        "❌ --extract-only and --process-only are mutually exclusive.",
        fg="red",
    )
    raise click.Abort()
```

Wrap the Step 1 (extraction) block with `if not process_only:`. After Step 1, exit early when `extract_only`:

```python
if extract_only:
    click.echo()
    click.secho(
        "✅ Extraction-only mode: files left in ingest/. "
        "Run 'garmin extract --process-only' to load into the database.",
        fg="green",
    )
    return
```

- [ ] **Step 9.4: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green.

- [ ] **Step 9.5: Commit**

```bash
git add garmin_health_data/cli.py tests/test_cli.py
git commit -m "feat(cli): add --extract-only and --process-only flags"
```

---

## Task 12: Remove `--keep-files` flag (lifecycle is now default)

**Files:**
- Modify: `garmin_health_data/cli.py` (remove the `--keep-files` option)
- Modify: `tests/test_cli.py` (already replaced in Task 6, just confirm)

**Rationale:** The lifecycle is the default. The earlier flag is redundant.

- [ ] **Step 10.1: Remove the option**

In `garmin_health_data/cli.py`:

- Remove the `@click.option("--keep-files", ...)` decorator block.
- Remove `keep_files: bool` from the `extract()` signature.
- Remove any remaining `if keep_files:` references (Task 6 should have removed the main one already).

- [ ] **Step 10.2: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green; no test references `--keep-files`.

- [ ] **Step 10.3: Commit**

```bash
git add garmin_health_data/cli.py
git commit -m "refactor(cli): remove --keep-files flag, lifecycle is default"
```

---

## Task 13: CLI — acquire lifecycle lock around the run

**Files:**
- Modify: `garmin_health_data/cli.py` (wrap the body of `extract` in `acquire_lock`)
- Modify: `tests/test_cli.py` (lock test)

**Rationale:** Two concurrent `garmin extract` runs would race on lifecycle file moves. Acquire the `fcntl.flock` from Task 2 right after lifecycle dirs are set up; release on exit. A second run aborts cleanly with a clear message.

- [ ] **Step 13.1: Write the failing test**

Append to `tests/test_cli.py`:

```python
def test_extract_aborts_when_lock_held(tmp_path):
    """
    A second concurrent extract aborts with a clear message when the lock
    is already held.
    """
    from unittest.mock import patch

    from garmin_health_data.lifecycle import acquire_lock, setup_lifecycle_dirs

    db_path = tmp_path / "test.db"
    create_tables(str(db_path))

    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)

    runner = CliRunner()
    with (
        acquire_lock(base),
        patch("garmin_health_data.cli.ensure_authenticated"),
    ):
        result = runner.invoke(
            extract,
            ["--db-path", str(db_path),
             "--start-date", "2025-01-01", "--end-date", "2025-01-02"],
        )

    assert result.exit_code != 0
    assert "Another garmin extract run is in progress" in result.output
```

- [ ] **Step 13.2: Run test to verify it fails**

Run: `pytest tests/test_cli.py::test_extract_aborts_when_lock_held -v`
Expected: FAIL — second run proceeds despite lock.

- [ ] **Step 13.3: Acquire the lock in the CLI**

In `garmin_health_data/cli.py`, add to imports:

```python
from garmin_health_data.lifecycle import (
    LockHeldError,
    acquire_lock,
    move_files_to_quarantine,
    move_files_to_storage,
    move_ingest_to_process,
    recover_stale_process,
    setup_lifecycle_dirs,
)
```

Right after `setup_lifecycle_dirs(files_root)` (added in Task 8), wrap the rest of the `extract()` function body in:

```python
try:
    with acquire_lock(files_root):
        # ... existing body from recover_stale_process onward ...
except LockHeldError as e:
    click.secho(f"❌ {e}", fg="red")
    raise click.Abort()
```

Practically, since `extract()` is a Click command function, it's cleanest to refactor the post-setup body into a helper (e.g. `_run_extract(...)`) and wrap the call to that helper in the lock context. Either approach is fine; keep it readable.

- [ ] **Step 13.4: Run tests**

Run: `pytest tests/test_cli.py -v`
Expected: all green.

- [ ] **Step 13.5: Commit**

```bash
git add garmin_health_data/cli.py tests/test_cli.py
git commit -m "feat(cli): acquire fcntl.flock lock around extract run"
```

---

## Task 14: Documentation restructure (README + new `docs/` pages)

**Files:**
- Modify: `README.md` (trim to overview, install, quick start, features, comparisons; link out to deeper docs)
- Create: `docs/usage.md` (CLI reference: every command, every flag, every option)
- Create: `docs/file-lifecycle.md` (the four-folder pipeline, recovery semantics, failure isolation, concurrency)
- Create: `docs/data-types.md` (full data type registry, schema notes — moved from README)
- Modify: `CHANGELOG.md` (add unreleased entry)

**Rationale:** The current README is monolithic (intro, features, install, quick start, full CLI reference, date-range semantics, data type table, schema, comparisons all in one file). The lifecycle redesign adds enough new surface (lifecycle directories, three failure-isolation layers, two new flags, lock file, end-of-run summary) that stuffing it into the existing README would push it past usability. Splitting along natural axes keeps each doc focused and makes the README a true entry point.

- [ ] **Step 14.1: Trim README.md to entry-point shape**

In `README.md`, keep:
- Intro paragraph (lines 1-3)
- Features list (lines 5-13) — update "Auto-Resume" bullet to mention crash recovery, add a "File Lifecycle" bullet
- Requirements (lines 15-19)
- Quick Start (lines 21-48)
- A new short "Documentation" section linking to:
  - `docs/usage.md` for full CLI reference
  - `docs/file-lifecycle.md` for the lifecycle / recovery / failure isolation behavior
  - `docs/data-types.md` for the data type catalog and schema notes
- Comparisons (existing section)
- Development (existing section)

Remove from README and move into the new docs:
- The detailed Usage section (lines 50-220ish) → `docs/usage.md`
- The "Smart Auto-Detection" section → `docs/usage.md`
- The full Data Categories table → `docs/data-types.md`
- The Database Schema section → `docs/data-types.md`

Final README target length: ~80-120 lines (down from current ~370).

- [ ] **Step 14.2: Create `docs/usage.md`**

Cover every CLI command and flag with concrete examples. Outline:

1. **Commands:** `auth`, `extract`, `info`, `verify`.
2. **`extract` flags** (full reference table):
   - `--start-date`, `--end-date` (with auto-detect rules)
   - `--data-types`, `--accounts`
   - `--db-path`
   - `--extract-only`, `--process-only` (new — explain when to use each)
3. **Date range behavior** (current README content moved verbatim).
4. **Auto-resume / smart auto-detection** (current README content moved + updated to note that `--process-only` doesn't trigger auto-detect).
5. **Multi-account workflows** (current README content moved).
6. **Common workflows** (extract daily / extract specific dates / re-process after fix / extract-only backup).

- [ ] **Step 14.3: Create `docs/file-lifecycle.md`**

Cover the lifecycle architecture in depth. Outline:

1. **Why a lifecycle?** Brief motivation: backup, crash recovery, failure inspection, openetl parity.
2. **Directory layout** (`<db_dir>/garmin_files/{ingest,process,storage,quarantine}/` + `.lock`).
3. **State transitions** (sequence diagram in ASCII or numbered list: extract → ingest → process → storage/quarantine).
4. **Crash recovery** (stale `process/` files moved back to `ingest/` on next run).
5. **Concurrency** (`fcntl.flock`, error message users will see if they hit a held lock, how to clear a stale lock).
6. **Failure isolation** (the three extract layers + per-FileSet processing).
7. **End-of-run summary** (what's reported and how to interpret it).
8. **Inspecting and clearing quarantine** (manual workflow: read the file, fix the issue, move back to ingest, re-run with `--process-only`).
9. **Disk usage** (`storage/` grows monotonically; how to prune if needed).

- [ ] **Step 14.4: Create `docs/data-types.md`**

Move the existing Data Categories table from README and the Database Schema section. Add a short note about how data types map to lifecycle files (every data type produces JSON in `ingest/`, except ACTIVITY which produces FIT/TCX/GPX/KML, and EXERCISE_SETS which is JSON keyed by activity_id).

- [ ] **Step 14.5: Update `CHANGELOG.md`**

Add an unreleased section:

```markdown
## [Unreleased]

### Added
- File lifecycle: every extracted file is preserved in `garmin_files/{ingest,process,storage,quarantine}/` next to the database (closes #35).
- `--extract-only` and `--process-only` flags split the extract/load stages.
- `fcntl.flock` advisory lock prevents concurrent runs from racing on file moves.
- End-of-run summary lists every per-data-type / per-date / per-activity extraction failure.
- Crash recovery: files left in `process/` from a crashed run are auto-moved back to `ingest/` on the next run.

### Changed
- Per-date, per-data-type, and per-activity try/except in the extractor: a single transient failure no longer aborts an entire account run.
- Per-FileSet session and try/except in the processor: a bad file set lands in `quarantine/` instead of poisoning the whole load.
- `extract_fit_activities` reads the saved `ACTIVITIES_LIST` JSON from `ingest/` instead of re-calling the same API endpoint.
- Renamed `_process_day_by_day` → `_extract_day_by_day` for clarity (it is extraction, not processing).

### Removed
- `--keep-files` flag (lifecycle is now default behavior).
```

- [ ] **Step 14.6: Verify all internal links resolve**

```bash
# Check every relative link in README.md and the new docs.
grep -rEo '\[[^]]+\]\([^)]+\)' README.md docs/*.md | \
  grep -vE 'http(s)?://' | \
  awk -F'[()]' '{print $2}' | \
  while read -r p; do
    [ -e "$p" ] || echo "BROKEN: $p"
  done
```

Expected: no `BROKEN:` lines.

- [ ] **Step 14.7: Commit**

```bash
git add README.md docs/usage.md docs/file-lifecycle.md docs/data-types.md CHANGELOG.md
git commit -m "docs: restructure into README + docs/ pages, document lifecycle"
```

---

## Task 15: Audit and update all existing tests for new contracts

**Files:**
- Modify (audit): every file in `tests/`
- Likely affected based on contract changes:
  - `tests/test_extractor.py` (new `failures` attribute on `GarminExtractor`, new return shape from `extract()`)
  - `tests/test_cli.py` (new lifecycle behavior, new flags, no more `--keep-files`)
  - `tests/test_processor.py` (per-FileSet session model — verify `process_file_set` still works when called repeatedly with fresh sessions)
  - `tests/test_processor_helpers.py` (no changes expected; verify)
  - `tests/test_db.py`, `tests/test_db_extended.py` (no changes expected; verify)
  - `tests/test_auth.py`, `tests/test_auth_extended.py` (no changes expected; verify)
  - `tests/test_utils.py` (no changes expected; verify)

**Rationale:** Tasks 1-13 added new tests for new behavior, but existing tests may quietly pass with stale assumptions (e.g. testing the old `extract()` return shape, or relying on the temp dir being cleaned up). This task audits every test file, updates anything that's now wrong, and confirms full coverage.

- [ ] **Step 15.1: Run the full suite to surface failures**

```bash
cd /Users/diegoscarabelli/repos/garmin-health-data-worktrees/keep-files
pytest -v 2>&1 | tee /tmp/test-audit.log
```

Triage failures into:
- (a) Tests asserting old return shape from `extract()` → update to expect `failures` and `failed_accounts` keys.
- (b) Tests asserting the temp dir gets cleaned up → update to expect lifecycle dirs instead.
- (c) Tests relying on old function name `_process_day_by_day` → rename to `_extract_day_by_day`.
- (d) Tests passing `--keep-files` → remove or rewrite.
- (e) Real bugs introduced by the refactor → fix the code.

- [ ] **Step 15.2: For each failing test, decide and fix**

Walk through `/tmp/test-audit.log` from top. For each failure:

1. Read the test file and the failing assertion.
2. Decide: is the test wrong (update test) or is the code wrong (fix code)?
3. Apply the fix.
4. Re-run just that test to confirm.

Common fixes (these are mechanical):

- `result["garmin_files"]` continues to work; new keys `result["failures"]` and `result["failed_accounts"]` are additions, not replacements. Update only tests that assert the dict has exact keys.
- Tests that previously did `mock_extract.assert_called_with(ingest_dir=temp_dir, ...)` should now expect `ingest_dir=tmp_path / "garmin_files" / "ingest"`.
- Tests that called `extractor._process_day_by_day(...)` need the rename.

- [ ] **Step 15.3: Add coverage for any newly-uncovered behavior**

Run with coverage and look for regressions:

```bash
pytest --cov=garmin_health_data --cov-report=term-missing 2>&1 | tee /tmp/test-cov.log
```

Compare the missing-lines list against pre-refactor coverage. If new code paths are uncovered (especially the lock-acquisition error branch, the disk-read fallback in `_load_activities_list_from_disk`, the stale-process recovery path), add minimal tests for them.

- [ ] **Step 15.4: Confirm all green**

```bash
pytest -v
```

Expected: all green. No skips beyond the pre-existing 1.

- [ ] **Step 15.5: Commit**

```bash
git add tests/
git commit -m "test: align existing tests with lifecycle and isolation refactor"
```

---

## Task 16: Final format and full automated test suite

**Files:** all.

- [ ] **Step 16.1: Format everything**

```bash
cd /Users/diegoscarabelli/repos/garmin-health-data-worktrees/keep-files
python -m black garmin_health_data/ tests/
python -m autoflake --in-place --remove-all-unused-imports -r garmin_health_data/ tests/
python -m docformatter --in-place -r garmin_health_data/ tests/
python -m sqlfluff fix garmin_health_data/tables.ddl
```

- [ ] **Step 16.2: Run the full test suite**

Run: `pytest -v`
Expected: all green. No skips beyond the pre-existing 1.

- [ ] **Step 16.3: Commit any formatting drift**

```bash
git status
# If anything changed, commit:
git add -u
git commit -m "style: apply formatters after refactor"
```

---

## Task 17: Functional smoke test on the user's Mac (must pass before push)

**Important:** This task is interactive. The agent prepares the test plan, runs each command on the user's Mac, and observes both CLI output and filesystem/database state. The user is in the loop for any unexpected behavior. **Do not push to GitHub until this task is complete and the user confirms.**

**Pre-conditions:**
- The user has a working Garmin Connect account already authenticated (`~/.garminconnect/<user_id>/` exists).
- A scratch directory dedicated to this smoke test exists, separate from any production database.

**Setup (run once):**

```bash
SMOKE_DIR="$HOME/garmin-smoke-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$SMOKE_DIR"
cd "$SMOKE_DIR"

# Install the in-development build into the worktree's editable env (already done).
which garmin
garmin --version
```

**Helper inspection commands** (run after each scenario):

```bash
# Quick state snapshot.
ls -la garmin_files/ 2>/dev/null
echo "ingest:"     ; ls garmin_files/ingest/     2>/dev/null | wc -l
echo "process:"    ; ls garmin_files/process/    2>/dev/null | wc -l
echo "storage:"    ; ls garmin_files/storage/    2>/dev/null | wc -l
echo "quarantine:" ; ls garmin_files/quarantine/ 2>/dev/null | wc -l
ls -la garmin_files/.lock 2>/dev/null

# DB row counts.
garmin info --db-path "$SMOKE_DIR/garmin_data.db" 2>/dev/null || echo "(no DB yet)"
```

### Test scenarios

For each scenario: state the expected outcome before running, run the command, run the inspection block, **explicitly compare observed vs. expected**, and record any deviation in a `SMOKE_NOTES.md` file in the smoke dir.

- [ ] **Scenario A — clean slate, full pipeline, narrow date range**

```bash
rm -rf "$SMOKE_DIR"/* "$SMOKE_DIR"/.lock 2>/dev/null
cd "$SMOKE_DIR"
garmin extract --start-date 2026-04-20 --end-date 2026-04-22
```

Expected:
- `garmin_files/{ingest,process,storage,quarantine,.lock}` created.
- After run: `ingest/` empty, `process/` empty, `storage/` populated, `quarantine/` empty (assuming no parse failures).
- DB created at `$SMOKE_DIR/garmin_data.db` with rows for 2026-04-20 and 2026-04-21 (end-date is exclusive per [docs/usage.md](#)).
- End-of-run summary printed; "Extraction failures (0)" or no failures section.

- [ ] **Scenario B — re-run same range, idempotency check**

```bash
garmin extract --start-date 2026-04-20 --end-date 2026-04-22
```

Expected:
- Lifecycle handles already-existing files gracefully (overwrites in storage/).
- Row counts in DB unchanged (upserts).
- No errors.

- [ ] **Scenario C — auto-detect from existing DB**

```bash
garmin extract  # no dates -> auto-detect
```

Expected:
- Start date auto-detected as `(MAX(date) in DB) + 1 day`, i.e. 2026-04-22 or later.
- End date defaults to today.
- Files added incrementally to `storage/`.
- DB grows by exactly the new days' worth of data.

- [ ] **Scenario D — `--extract-only`, then `--process-only`**

```bash
rm -rf "$SMOKE_DIR"/garmin_files
garmin extract --start-date 2026-04-15 --end-date 2026-04-17 --extract-only
# Inspect: ingest/ should be populated, storage/ empty, DB unchanged.
garmin extract --process-only
# Inspect: ingest/ empty, storage/ populated, DB updated.
```

Expected:
- After `--extract-only`: only `ingest/` has files; `process/`, `storage/`, `quarantine/` are empty; DB row counts unchanged from prior scenario.
- After `--process-only`: `ingest/` empty, `storage/` has the files, DB updated.

- [ ] **Scenario E — overlapping date ranges (regression check)**

```bash
garmin extract --start-date 2026-04-21 --end-date 2026-04-25
```

Expected:
- Days that overlap with prior runs (2026-04-21..2026-04-23) overwrite cleanly in `storage/`; DB upserts no-op for them.
- New days (2026-04-23..2026-04-24) added.
- No errors, no growth in `quarantine/`.

- [ ] **Scenario F — non-overlapping later range**

```bash
garmin extract --start-date 2026-04-26 --end-date 2026-04-27
```

Expected:
- Just adds the new days.
- DB row counts grow by exactly those days' worth.
- No errors.

- [ ] **Scenario G — backfill earlier range (gap fill)**

```bash
garmin extract --start-date 2026-04-10 --end-date 2026-04-15
```

Expected:
- Earlier days (gaps in DB) get filled.
- Auto-detect would have skipped these; explicit dates force the fetch.
- Files land in `storage/`.

- [ ] **Scenario H — single data type filter**

```bash
garmin extract --start-date 2026-04-20 --end-date 2026-04-22 --data-types SLEEP
```

Expected:
- Only SLEEP-related files move through the pipeline.
- DB only sees sleep updates; other tables unchanged.

- [ ] **Scenario I — concurrent run rejected**

In one terminal:
```bash
garmin extract --start-date 2026-03-01 --end-date 2026-03-31
```
While that runs, in a second terminal:
```bash
garmin extract --start-date 2026-04-01 --end-date 2026-04-02
```

Expected:
- Second invocation aborts immediately with "Another garmin extract run is in progress" message.
- Exit code non-zero.
- First run continues to completion unaffected.

- [ ] **Scenario J — crash recovery**

Start a long extract, kill it mid-process step:
```bash
garmin extract --start-date 2026-02-01 --end-date 2026-02-28 &
PID=$!
sleep 30
kill -9 $PID
```

Inspect: `process/` should have files left over (in-flight when killed).

```bash
garmin extract --start-date 2026-02-01 --end-date 2026-02-28
```

Expected:
- Recovery message: "♻️  Recovered N file(s) from a previous run".
- After run: `process/` empty, `storage/` populated.
- DB has the expected data.

- [ ] **Scenario K — `--extract-only` and `--process-only` mutex**

```bash
garmin extract --extract-only --process-only
```

Expected:
- Exits non-zero with "mutually exclusive" error message.

- [ ] **Step 17.1: Compile findings**

In `$SMOKE_DIR/SMOKE_NOTES.md`, summarize: which scenarios passed verbatim, which had unexpected output, and any bugs found. For every failed scenario, record:
- Command run.
- Expected output.
- Actual output (CLI stderr/stdout snippet, filesystem state, DB row counts).
- Hypothesized cause.

- [ ] **Step 17.2: Fix any bugs found**

For each bug: open a new branch off the worktree, write a failing test, fix, re-run the failing scenario manually to confirm. Commit each fix separately with `fix(smoke):` prefix.

- [ ] **Step 17.3: User sign-off**

Show `SMOKE_NOTES.md` to the user and confirm they accept the smoke results before pushing.

---

## Task 18: Push and update PR description

**Pre-condition:** Task 17 complete and user has signed off. Do NOT request Copilot review here — that is deferred per user instruction.

- [ ] **Step 18.1: Update PR description**

```bash
gh pr edit 37 --title "feat: extract/process lifecycle with failure isolation" \
  --body "$(cat <<'EOF'
## Summary

Replaces the temp-dir extraction model with an openetl-style four-folder lifecycle (`ingest/process/storage/quarantine/`) and adds three layers of failure isolation in extract plus per-FileSet isolation in process. Closes #35 (expanded scope from "save activity files" to full lifecycle).

## What changes

### File lifecycle (default behavior)
- `<db_dir>/garmin_files/{ingest,process,storage,quarantine}/` is now the default. Files are always preserved.
- Crash recovery: `process/` files left from a previous run are auto-moved back to `ingest/` on startup.
- Per-FileSet routing: successful FileSets land in `storage/`; failed ones in `quarantine/`.

### Failure isolation
- **Per-date** in `_extract_day_by_day` (renamed from `_process_day_by_day` for clarity): one bad day no longer aborts the rest of the date range.
- **Per-data-type** in `extract_garmin_data`: one bad data type no longer aborts the rest of the account.
- **Per-activity** in `extract_fit_activities`: broadened from `GarminConnectionError` to `Exception`; activity-list fetch wrapped in try/except.
- **Per-FileSet** in the process loop (mirrors openetl's `_try_process_file_set`): own session per FileSet, try/except, transaction rollback on failure.

### CLI changes
- Removed `--keep-files` flag (lifecycle is default).
- Added `--extract-only` and `--process-only` flags for stage splitting.
- End-of-run summary lists every per-account / per-data-type / per-date failure.
- `fcntl.flock` advisory lock on `garmin_files/.lock` prevents concurrent runs from racing on file moves.

### Other improvements
- `extract_fit_activities` now reads the saved `ACTIVITIES_LIST` JSON from `ingest/` instead of re-calling the same API endpoint that the registry loop already hit. Falls back to the live API call if the file is missing.

### Documentation
- README restructured into entry-point shape with links to deeper docs in `docs/`.
- New: `docs/usage.md`, `docs/file-lifecycle.md`, `docs/data-types.md`.
- `CHANGELOG.md` updated.

## Out of scope (follow-up issues)
- Back-port the per-date / per-data-type isolation, `_extract_day_by_day` rename, and disk-read `ACTIVITIES_LIST` optimization to openetl.

## Test plan

- [x] `pytest` passes (full suite).
- [x] Lifecycle dirs created next to DB.
- [x] Stale `process/` files recovered on startup.
- [x] Per-FileSet failures land in quarantine; successes in storage.
- [x] `--extract-only` skips processing; `--process-only` skips extraction.
- [x] `--extract-only` and `--process-only` are mutually exclusive.
- [x] Concurrent runs blocked by lock.
- [x] End-of-run summary lists failures.
- [x] Manual smoke test on real Garmin account (Task 17, see SMOKE_NOTES.md attached).

Closes #35.
EOF
)"
```

- [ ] **Step 18.2: Push**

```bash
git push
```

- [ ] **Step 18.3: Stop here**

Do NOT request Copilot review. The user will run rounds of Copilot review themselves once they have reviewed the pushed branch.

---

## Self-Review Notes

- **Spec coverage:** every confirmed item from the design discussion has a task — lifecycle (Task 1), lock (Tasks 2 + 13), per-date / per-data-type / per-activity isolation (Tasks 3-5), disk-read ACTIVITIES_LIST (Task 6), summary (Tasks 5 + 10), `--extract-only` / `--process-only` (Task 11), `--keep-files` removal (Task 12), README/docs restructure (Task 14), test audit (Task 15), format + automated suite (Task 16), functional smoke test (Task 17), push (Task 18).
- **Type consistency:** `ExtractionFailure` defined in Task 3 (renumbered), used in Tasks 4, 5, 6, 7, 10. Lifecycle helpers (`setup_lifecycle_dirs`, `recover_stale_process`, `move_ingest_to_process`, `move_files_to_storage`, `move_files_to_quarantine`) defined in Task 1, imported in Task 8. `acquire_lock` / `LockHeldError` defined in Task 2, used in Task 13.
- **Test coverage:** every behavior change has a failing test written before implementation, plus an explicit existing-test audit (Task 15) and a manual functional smoke test (Task 17) before push.
- **Order dependencies:** Task 6 (read ACTIVITIES_LIST from disk) modifies code from Task 5 (activity-list error wrapping); Task 5 must come first. Task 13 (CLI lock) depends on Task 2 (lock helper) and Task 8 (lifecycle dirs). Task 17 must run after Task 16 and before Task 18.
- **Open scope question:** Task 14 proposes splitting the README into a `docs/` folder with three new pages. If you'd prefer a single README with everything inline, this is the place to dial back before execution.

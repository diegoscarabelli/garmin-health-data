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

    :param base_dir: Lifecycle parent directory.
    :return: Count of files recovered.
    """
    process = base_dir / "process"
    ingest = base_dir / "ingest"
    moved = 0
    for src in process.iterdir():
        if src.is_file():
            dest = ingest / src.name
            if dest.exists():
                dest.unlink()
            shutil.move(str(src), str(dest))
            moved += 1
    return moved


def move_ingest_to_process(base_dir: Path) -> int:
    """
    Move every file from ingest/ to process/ in bulk.

    Called after extraction completes and before processing begins.

    :param base_dir: Lifecycle parent directory.
    :return: Count of files moved.
    """
    ingest = base_dir / "ingest"
    process = base_dir / "process"
    moved = 0
    for src in ingest.iterdir():
        if src.is_file():
            dest = process / src.name
            if dest.exists():
                dest.unlink()
            shutil.move(str(src), str(dest))
            moved += 1
    return moved


def move_files_to_storage(file_paths: Iterable[Path], base_dir: Path) -> List[Path]:
    """
    Move the given files (typically a successfully-processed FileSet) to storage/.

    Existing files at the destination are overwritten.

    :param file_paths: Source file paths (typically inside process/).
    :param base_dir: Lifecycle parent directory.
    :return: List of new paths under storage/.
    """
    return _move_into(file_paths, base_dir / "storage")


def move_files_to_quarantine(file_paths: Iterable[Path], base_dir: Path) -> List[Path]:
    """
    Move the given files (typically a failed FileSet) to quarantine/.

    :param file_paths: Source file paths (typically inside process/).
    :param base_dir: Lifecycle parent directory.
    :return: List of new paths under quarantine/.
    """
    return _move_into(file_paths, base_dir / "quarantine")


def _move_into(file_paths: Iterable[Path], dest_dir: Path) -> List[Path]:
    """
    Move every file into dest_dir, returning the new paths.

    :param file_paths: Source file paths.
    :param dest_dir: Destination directory (must already exist).
    :return: List of new paths inside dest_dir.
    """
    moved: List[Path] = []
    for src in file_paths:
        dest = dest_dir / src.name
        if dest.exists():
            dest.unlink()
        shutil.move(str(src), str(dest))
        moved.append(dest)
    return moved

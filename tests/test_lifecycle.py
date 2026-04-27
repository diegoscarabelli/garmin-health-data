"""
Tests for filesystem lifecycle helpers (ingest/process/storage/quarantine).
"""

import pytest

from garmin_health_data.lifecycle import (
    LIFECYCLE_DIRS,
    LockHeldError,
    acquire_lock,
    move_files_to_quarantine,
    move_files_to_storage,
    move_ingest_to_process,
    recover_stale_process,
    setup_lifecycle_dirs,
)


def test_setup_creates_all_four_dirs(tmp_path):
    """
    All four lifecycle directories are created under the base dir.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    for name in LIFECYCLE_DIRS:
        assert (base / name).is_dir()


def test_setup_is_idempotent(tmp_path):
    """
    Calling setup twice is a no-op the second time.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "marker.txt").write_text("keep me")
    setup_lifecycle_dirs(base)
    assert (base / "ingest" / "marker.txt").read_text() == "keep me"


def test_recover_stale_process_moves_files_back_to_ingest(tmp_path):
    """
    Files left in process/ from a crashed run are moved back to ingest/.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    stale = base / "process" / "stale.json"
    stale.write_text("data")

    moved = recover_stale_process(base)

    assert moved == 1
    assert not stale.exists()
    assert (base / "ingest" / "stale.json").read_text() == "data"


def test_recover_stale_process_noop_when_empty(tmp_path):
    """
    Empty process/ recovery is a no-op.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    assert recover_stale_process(base) == 0


def test_move_ingest_to_process_moves_all_files(tmp_path):
    """
    Bulk move from ingest/ to process/ relocates every file.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "a.json").write_text("a")
    (base / "ingest" / "b.fit").write_bytes(b"\x00")

    moved = move_ingest_to_process(base)

    assert moved == 2
    assert sorted(p.name for p in (base / "process").iterdir()) == ["a.json", "b.fit"]
    assert list((base / "ingest").iterdir()) == []


def test_move_files_to_storage_relocates_each_file(tmp_path):
    """
    A FileSet's files move from process/ to storage/.
    """
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
    """
    A failed FileSet's files move from process/ to quarantine/.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    p1 = base / "process" / "bad.json"
    p1.write_text("bad")

    moved = move_files_to_quarantine([p1], base)

    assert [p.name for p in moved] == ["bad.json"]
    assert (base / "quarantine" / "bad.json").read_text() == "bad"
    assert not p1.exists()


def test_move_overwrites_existing_destination(tmp_path):
    """
    Moving a file to a destination that already exists overwrites cleanly.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    src = base / "process" / "dup.json"
    src.write_text("new")
    (base / "storage" / "dup.json").write_text("old")

    move_files_to_storage([src], base)

    assert (base / "storage" / "dup.json").read_text() == "new"


def test_recover_stale_process_overwrites_ingest_file(tmp_path):
    """
    recover_stale_process overwrites an ingest/ file with the same name.

    This can happen if a previous run crashed mid-process and the next run tries to
    recover, but a fresh extraction in between created a new copy in ingest/. The
    crashed-in-process copy is preferred because it was the most recently extracted
    version, and re-extraction is idempotent anyway.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "process" / "dup.json").write_text("from-crashed-run")
    (base / "ingest" / "dup.json").write_text("from-fresh-extract")

    moved = recover_stale_process(base)

    assert moved == 1
    assert (base / "ingest" / "dup.json").read_text() == "from-crashed-run"
    assert not (base / "process" / "dup.json").exists()


def test_move_ingest_to_process_overwrites_process_file(tmp_path):
    """
    move_ingest_to_process overwrites a process/ file with the same name.

    This can happen after recover_stale_process moved a file back to ingest/ that
    already had a same-named copy in process/ from a different timeline. The bulk move
    favours the ingest/ version (most recent).
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    (base / "ingest" / "dup.json").write_text("from-ingest")
    (base / "process" / "dup.json").write_text("stale")

    moved = move_ingest_to_process(base)

    assert moved == 1
    assert (base / "process" / "dup.json").read_text() == "from-ingest"
    assert not (base / "ingest" / "dup.json").exists()


def test_acquire_lock_succeeds_when_unheld(tmp_path):
    """
    First lock acquisition succeeds and creates the .lock file.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        assert (base / ".lock").exists()


def test_acquire_lock_raises_when_held_by_another_process(tmp_path):
    """
    A second concurrent acquisition raises LockHeldError.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        with pytest.raises(LockHeldError):
            with acquire_lock(base):
                pass


def test_acquire_lock_releases_after_context_exit(tmp_path):
    """
    Lock is released when context exits, allowing re-acquisition.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with acquire_lock(base):
        pass
    with acquire_lock(base):
        pass


def test_acquire_lock_releases_on_exception(tmp_path):
    """
    Lock is released even if the with-block raises.
    """
    base = tmp_path / "garmin_files"
    setup_lifecycle_dirs(base)
    with pytest.raises(RuntimeError):
        with acquire_lock(base):
            raise RuntimeError("boom")
    with acquire_lock(base):
        pass

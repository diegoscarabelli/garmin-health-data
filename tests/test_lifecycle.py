"""
Tests for filesystem lifecycle helpers (ingest/process/storage/quarantine).
"""

from garmin_health_data.lifecycle import (
    LIFECYCLE_DIRS,
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

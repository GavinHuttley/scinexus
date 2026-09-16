import contextlib
import gc
import os
import sqlite3
import sys
import threading
from pathlib import Path
from pickle import dumps, loads

import pytest
from citeable import Software
from scitrack import get_text_hexdigest

from scinexus.composable import (
    LOADER,
    NotCompleted,
    NotCompletedType,
    define_app,
    source_proxy,
)
from scinexus.data_store import (
    APPEND,
    OVERWRITE,
    READONLY,
    DataMemberABC,
    DataStoreDirectory,
)
from scinexus.parallel import set_parallel_backend
from scinexus.sqlite_data_store import (
    _MEMORY,
    LOG_TABLE,
    RESULT_TABLE,
    DataStoreSqlite,
    _owner_token,
    has_valid_schema,
    open_sqlite_db_ro,
    open_sqlite_db_rw,
)


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return Path(tmp_path_factory.mktemp("sqlitedb"))


@pytest.fixture
def sample_citations():
    cite1 = Software(
        author=["Doe, J"],
        title="Tool One",
        year=2024,
        url="https://example.com/one",
        version="1.0",
        license="MIT",
        doi="10.0/one",
        publisher="test",
    )
    cite2 = Software(
        author=["Smith, A"],
        title="Tool Two",
        year=2024,
        url="https://example.com/two",
        version="2.0",
        license="MIT",
        doi="10.0/two",
        publisher="test",
    )
    return (cite1, cite2)


@pytest.fixture
def nc_objects():
    return {
        f"id_{i}": NotCompleted(
            NotCompletedType.ERROR, "location", "message", source=f"id_{i}"
        )
        for i in range(3)
    }


@pytest.fixture
def sql_dstore(DATA_DIR, tmp_dir):
    ro_dir_dstore = DataStoreDirectory(DATA_DIR, suffix="fasta")
    path = tmp_dir / "data.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    for m in ro_dir_dstore:
        dstore.write(data=m.read(), unique_id=m.unique_id)
    return dstore


@pytest.fixture
def full_dstore_sqlite(tmp_dir, nc_objects, DATA_DIR):
    path = tmp_dir / "full.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    for uid, obj in nc_objects.items():
        dstore.write_not_completed(unique_id=uid, data=obj.to_json())
    ro = DataStoreDirectory(DATA_DIR, suffix="fasta")
    for m in ro:
        dstore.write(unique_id=m.unique_id, data=m.read())
    log_text = (DATA_DIR / "scitrack.log").read_text()
    dstore.write_log(unique_id="scitrack.log", data=log_text)
    yield dstore
    dstore.close()


@pytest.fixture
def writable_store(tmp_dir):
    path = tmp_dir / "writable.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    yield dstore
    dstore.close()


@pytest.fixture
def populated_store(tmp_dir):
    """A store with data, closed and ready for read-only access."""
    path = tmp_dir / "populated.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()
    return path


def test_db_creation(tmp_dir):
    path = tmp_dir / "test.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert dstore.source == path
    dstore.close()


def test_db_init_log(tmp_dir):
    path = tmp_dir / "test_log.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="test_record", data="test data")
    assert dstore._log_id is not None
    dstore.close()


def test_open_sqlite_db_rw(tmp_dir):
    path = tmp_dir / "test_rw.sqlitedb"
    db = open_sqlite_db_rw(path)
    assert has_valid_schema(db)
    db.close()


def test_rw_sql_dstore_mem():
    """in memory dstore with multiple writes verified via SQL"""
    dstore = DataStoreSqlite(_MEMORY, mode=OVERWRITE)
    records = {f"r{i}": f"data {i}" for i in range(3)}
    for unique_id, data in records.items():
        dstore.write(data=data, unique_id=unique_id)
    expect = len(records)
    query = f"SELECT count(*) as c FROM {RESULT_TABLE} WHERE is_completed=?"
    got = dstore.db.execute(query, (1,)).fetchone()["c"]
    assert got == expect
    assert len(dstore.completed) == expect


def test_not_completed(tmp_dir):
    """multiple not_completed records are stored and retrievable"""
    nc_objects = {
        f"id_{i}": NotCompleted(
            NotCompletedType.ERROR, "location", "message", source=f"id_{i}"
        )
        for i in range(3)
    }
    path = tmp_dir / "test_nc.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    for unique_id, obj in nc_objects.items():
        dstore.write_not_completed(data=obj.to_json(), unique_id=unique_id)
    expect = len(nc_objects)
    query = f"SELECT count(*) as c FROM {RESULT_TABLE} WHERE is_completed=?"
    got = dstore.db.execute(query, (0,)).fetchone()["c"]
    assert got == expect
    assert len(dstore.not_completed) == expect
    dstore.close()


def test_logdata(tmp_dir, DATA_DIR):
    path = tmp_dir / "test_logdata.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    log_text = (DATA_DIR / "scitrack.log").read_text()
    dstore.write_log(unique_id="test.log", data=log_text)
    assert len(dstore.logs) == 1
    got = dstore.logs[0].read()
    assert got == log_text
    dstore.close()


def test_drop_not_completed(nc_objects):
    dstore = DataStoreSqlite(_MEMORY, mode=OVERWRITE)
    for unique_id, obj in nc_objects.items():
        dstore.write_not_completed(data=obj.to_json(), unique_id=unique_id)
    assert len(dstore.not_completed) == len(nc_objects)
    dstore.drop_not_completed()
    assert len(dstore.not_completed) == 0


def test_contains(tmp_dir):
    path = tmp_dir / "test_contains.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="record1", data="data1")
    assert "record1" in dstore
    assert "record2" not in dstore
    dstore.close()


def test_iter(tmp_dir):
    path = tmp_dir / "test_iter.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.write(unique_id="r2", data="d2")
    ids = {m.unique_id for m in dstore}
    assert ids == {"r1", "r2"}
    dstore.close()


def test_members(tmp_dir):
    path = tmp_dir / "test_members.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "msg", source="src")
    dstore.write_not_completed(unique_id="nc1", data=nc.to_json())
    assert len(dstore.members) == 2
    assert all(isinstance(m, DataMemberABC) for m in dstore)
    dstore.close()


def test_len(tmp_dir):
    path = tmp_dir / "test_len.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert len(dstore) == 0
    dstore.write(unique_id="r1", data="d1")
    assert len(dstore) == 1
    dstore.close()


def test_md5_sum(tmp_dir):
    path = tmp_dir / "test_md5.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    data = "test data for md5"
    dstore.write(unique_id="r1", data=data)
    md5 = dstore.md5("r1")
    assert md5 == get_text_hexdigest(data)
    dstore.close()


def test_iterall(tmp_dir):
    path = tmp_dir / "test_iterall.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.write(unique_id="r2", data="d2")
    all_members = list(dstore)
    assert len(all_members) == 2
    dstore.close()


def test_read(tmp_dir):
    path = tmp_dir / "test_read.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    data = "test data content"
    dstore.write(unique_id="r1", data=data)
    got = dstore.read("r1")
    assert got == data
    dstore.close()


def test_read_all_record_types(full_dstore_sqlite):
    """reading from completed, not_completed, and log records all return str"""
    records = [
        full_dstore_sqlite.completed[0],
        full_dstore_sqlite.not_completed[0],
        full_dstore_sqlite.logs[0],
    ]
    assert all(isinstance(r.read(), str) for r in records)


def test_write_success_replaces_not_completed(tmp_dir):
    path = tmp_dir / "test_replace_nc.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "msg", source="src")
    dstore.write_not_completed(unique_id="r1", data=nc.to_json())
    assert len(dstore.not_completed) == 1
    dstore.write(unique_id="r1", data="completed data")
    assert len(dstore.not_completed) == 0
    assert len(dstore.completed) == 1
    dstore.close()


def test_read_log(tmp_dir, DATA_DIR):
    path = tmp_dir / "test_readlog.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    log_text = (DATA_DIR / "scitrack.log").read_text()
    dstore.write_log(unique_id="test.log", data=log_text)
    got = dstore.read(str(Path("logs") / "test.log"))
    assert got == log_text
    dstore.close()


@pytest.mark.parametrize("binary", [False, True])
def test_write_text_binary(binary):
    """correctly write content whether text or binary data"""
    dstore = DataStoreSqlite(_MEMORY, mode=OVERWRITE)
    expect = "some text data"
    if binary:
        expect = dumps(expect)
    m = dstore.write(unique_id="record", data=expect)
    got = m.read()
    assert got == expect


def test_write_if_member_exists(tmp_dir):
    path = tmp_dir / "test_exists.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="data1")
    dstore.write(unique_id="r1", data="data2")
    got = dstore.read("r1")
    assert got == "data2"
    assert len(dstore.completed) == 1
    dstore.close()


def test_new_write_read(tmp_dir):
    path = tmp_dir / "test_new_wr.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="content1")
    dstore.write(unique_id="r2", data="content2")
    assert dstore.read("r1") == "content1"
    assert dstore.read("r2") == "content2"
    dstore.close()


def test_read_unknown_table(tmp_dir):
    path = tmp_dir / "test_unknown.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    with pytest.raises(ValueError):
        dstore.read("unknown_table/r1")
    dstore.close()


def test_limit_on_writable(tmp_dir):
    path = tmp_dir / "test_limit.sqlitedb"
    with pytest.raises(ValueError):
        DataStoreSqlite(path, mode=OVERWRITE, limit=10)


@pytest.mark.parametrize("table_name", ["", RESULT_TABLE])
def test_new_write_id_includes_table(table_name):
    """correctly handles table name if included in unique id"""
    dstore = DataStoreSqlite(_MEMORY, mode=OVERWRITE)
    identifier = "test1.fasta"
    if table_name:
        identifier = str(Path(table_name) / identifier)
    data = "test data"
    m = dstore.write(unique_id=identifier, data=data)
    got = dstore.read(m.unique_id)
    assert got == data


@pytest.mark.parametrize(
    "operation",
    [
        lambda d: d.read("r1"),
        lambda d: d.completed,
        lambda d: d.not_completed,
        lambda d: d.logs,
        lambda d: len(d),
        lambda d: d.write(unique_id="r2", data="d2"),
    ],
)
def test_use_after_close_raises(tmp_dir, operation):
    """a closed store refuses to act rather than half working"""
    # the path is kept clear of the word the match looks for, since other
    # errors from this store quote the path and a file called
    # closed.sqlitedb would satisfy an assertion meant for "is closed"
    path = tmp_dir / "shut.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()

    with pytest.raises(OSError, match="is closed"):
        operation(dstore)


def test_closing_twice(tmp_dir):
    """a second close is not an error"""
    path = tmp_dir / "twice.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")

    dstore.close()
    dstore.close()

    assert dstore._db is None


def test_close_before_the_database_is_opened(tmp_dir):
    """closing a store that never opened its database still ends it"""
    # the connection is made on first use, so this is the ordinary shape of
    # a store that is built and then abandoned
    path = tmp_dir / "untouched.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)

    dstore.close()

    with pytest.raises(OSError, match="is closed"):
        dstore.write(unique_id="r1", data="d1")


@pytest.mark.parametrize("operation", [lambda d: d.lock(), lambda d: d.unlock()])
def test_lock_operations_on_a_closed_store(tmp_dir, operation):
    """taking or releasing the lock of a closed store is refused"""
    path = tmp_dir / "shutlock.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()

    with pytest.raises(OSError, match="is closed"):
        operation(dstore)


def test_collection_without_close_keeps_the_lock(tmp_dir):
    """a store dropped without being closed leaves its lock behind"""
    # the lock marks a session that did not end through close(), so the
    # finaliser must not release it. it also issues no SQL, which at
    # interpreter exit could block on another connection or find the
    # machinery it needs gone
    path = tmp_dir / "dropped.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    with pytest.warns(UserWarning, match="was not closed"):
        del dstore
        gc.collect()

    db = sqlite3.connect(path)
    try:
        held = db.execute("SELECT lock_pid FROM state").fetchone()[0]
    finally:
        db.close()
    assert held == f"{os.getpid()}:{threading.get_native_id()}"


def test_collection_of_a_store_refused_the_lock_stays_quiet(tmp_dir, recwarn):
    """a store that never got the lock does not claim to be holding it"""
    # its open was refused, so it has a connection but no lock, and the
    # advice to close it would not release one taken by anybody else
    path = tmp_dir / "taken.sqlitedb"
    first = DataStoreSqlite(path, mode=OVERWRITE)
    first.write(unique_id="r1", data="d1")
    first._db.execute("UPDATE state SET lock_pid=?", (os.getpid() + 1,))
    first._db.close()
    first._db = None
    first._closed = True

    second = DataStoreSqlite(path, mode=OVERWRITE)
    with pytest.raises(OSError, match="locked"):
        second.read("r1")
    assert second._db is not None

    del second
    gc.collect()

    assert [w for w in recwarn if issubclass(w.category, UserWarning)] == []


@pytest.mark.parametrize(
    ("source", "mode"),
    [
        (_MEMORY, OVERWRITE),
        ("readonly", READONLY),
        ("never_used", OVERWRITE),
    ],
)
def test_collection_without_close_stays_quiet(tmp_dir, source, mode, recwarn):
    """only a store that can strand a lock on disk is worth warning about"""
    # an in-memory store leaves nothing behind, a read only one never takes
    # the lock, and one that never opened its database holds nothing
    if source is not _MEMORY:
        populate = tmp_dir / f"{source}.sqlitedb"
        seed = DataStoreSqlite(populate, mode=OVERWRITE)
        seed.write(unique_id="r1", data="d1")
        seed.close()
        dstore = DataStoreSqlite(populate, mode=mode)
        if source == "readonly":
            dstore.read("r1")
    else:
        dstore = DataStoreSqlite(source, mode=mode)
        dstore.write(unique_id="r1", data="d1")

    del dstore
    gc.collect()

    assert [w for w in recwarn if issubclass(w.category, UserWarning)] == []


def test_close_hands_the_database_on(tmp_dir):
    """a closed store leaves the database open to a new one"""
    # the lock names the process, so without releasing it here the store
    # that follows cannot take the database in a writable mode
    path = tmp_dir / "handover.sqlitedb"
    first = DataStoreSqlite(path, mode=OVERWRITE)
    first.write(unique_id="r1", data="d1")
    first.close()

    second = DataStoreSqlite(path, mode=OVERWRITE)

    assert second.read("r1") == "d1"
    assert second._lock_id == f"{os.getpid()}:{threading.get_native_id()}"
    second.close()


@pytest.mark.parametrize("mode", [OVERWRITE, APPEND])
def test_a_held_lock_refuses_a_writable_store(tmp_dir, mode):
    """a lock left by another session refuses any mode that would write"""
    # the lock marks a store whose session did not end through close(), so
    # its records were never confirmed. neither mode may write over that
    # without the deliberate unlock
    path = tmp_dir / "held.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=?", (os.getpid() + 1,))
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    dstore = DataStoreSqlite(path, mode=mode)

    with pytest.raises(OSError, match="locked"):
        dstore.read("r1")


@pytest.mark.parametrize("mode", [OVERWRITE, APPEND])
def test_a_held_lock_refuses_every_time(tmp_dir, mode):
    """the refusal is not spent by being raised once"""
    # the connection is opened before the lock is sought, so a refusal
    # leaves one behind. taken as proof of a lock, it lets the next access
    # straight through
    path = tmp_dir / "sticky.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=?", (os.getpid() + 1,))
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    dstore = DataStoreSqlite(path, mode=mode)
    with pytest.raises(OSError, match="locked"):
        dstore.read("r1")

    with pytest.raises(OSError, match="locked"):
        dstore.read("r1")
    with pytest.raises(OSError, match="locked"):
        dstore.write(unique_id="r2", data="d2")


@pytest.mark.parametrize("mode", [OVERWRITE, APPEND])
def test_unlock_reaches_a_lock_held_by_another_session(tmp_dir, mode):
    """the deliberate override works on the store it exists for"""
    # unlock has to open the database without seeking the lock, or the
    # refusal it is meant to clear is what stops it running
    path = tmp_dir / "override.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=?", (os.getpid() + 1,))
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    dstore = DataStoreSqlite(path, mode=mode)
    dstore.unlock(force=True)

    # read from the connection unlock opened, before anything else takes
    # the lock the store is now free to take
    assert dstore._db.execute("SELECT lock_pid FROM state").fetchone()[0] is None
    assert dstore.read("r1") == "d1"
    assert dstore._lock_id == f"{os.getpid()}:{threading.get_native_id()}"
    dstore.close()


def test_a_lock_recorded_as_zero_still_locks(tmp_dir):
    """an owner recorded as 0 is an owner"""
    path = tmp_dir / "zero.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=0")
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    dstore = DataStoreSqlite(path, mode=APPEND)

    assert dstore.locked
    with pytest.raises(OSError, match="locked"):
        dstore.write(unique_id="r2", data="d2")


def test_a_held_lock_allows_a_readonly_store(tmp_dir):
    """a locked store can still be read, only not written"""
    path = tmp_dir / "readable.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=?", (os.getpid() + 1,))
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    dstore = DataStoreSqlite(path, mode=READONLY)

    assert dstore.read("r1") == "d1"
    dstore.close()


def _abandon_with_a_lock_in_a_later_row(path, lock_pid):
    """leaves the state table as the racy implementation could have

    Row 1 free, because the session that held it called unlock(), and row 2
    still holding the lock of a session that never closed.
    """
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned.record_type = str
    # the racy lock() inserted rather than updated, so a second session got
    # its own row. seed it the same way to get row 2 for the right reason
    abandoned._db.execute("INSERT INTO state(lock_pid) VALUES (?)", (lock_pid,))
    # that version's unlock() cleared state_id 1 alone, which is how a
    # database ends up free in its first row and locked below it
    abandoned._db.execute("UPDATE state SET lock_pid=NULL WHERE state_id=1")
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True


@pytest.mark.parametrize("mode", [OVERWRITE, APPEND])
def test_a_lock_in_a_later_state_row_refuses_a_writable_store(tmp_dir, mode):
    """a lock is a lock whichever state row records it"""
    # the racy lock() left rows past the first, and unlock() cleared only
    # row 1. read from row 1 alone, a lock below it is invisible, so the
    # store it marks as never confirmed opens for writing
    path = tmp_dir / "laterrow.sqlitedb"
    _abandon_with_a_lock_in_a_later_row(path, f"{os.getpid() + 1}:1")

    dstore = DataStoreSqlite(path, mode=mode)

    assert dstore.locked
    with pytest.raises(OSError, match="locked"):
        dstore.read("r1")


def test_a_lock_in_a_later_state_row_shows_on_a_readonly_store(tmp_dir):
    """a read-only store reports the lock it cannot take"""
    # READONLY returns from lock() before any claim, so nothing on that path
    # can put a later row right: the reader itself has to see it
    path = tmp_dir / "laterrow_ro.sqlitedb"
    _abandon_with_a_lock_in_a_later_row(path, f"{os.getpid() + 1}:1")

    dstore = DataStoreSqlite(path, mode=READONLY)

    assert dstore.locked
    assert dstore.read("r1") == "d1"
    dstore.close()


@pytest.mark.parametrize("mode", [OVERWRITE, APPEND])
def test_unlock_reaches_a_lock_in_a_later_state_row(tmp_dir, mode):
    """the deliberate override clears every lock recorded, not merely row 1"""
    # clearing one row per call leaves the store refusing after the user has
    # forced it open, and unlock() reads the lock before clearing it, so a
    # row it cannot see is a row it never clears
    from scinexus.misc import get_object_provenance

    path = tmp_dir / "laterrow_unlock.sqlitedb"
    _abandon_with_a_lock_in_a_later_row(path, f"{os.getpid() + 1}:1")

    dstore = DataStoreSqlite(path, mode=mode)
    dstore.unlock(force=True)

    assert dstore.read("r1") == "d1"
    assert dstore._lock_id == _owner_token()
    # claiming the store collapses the rows the racy version left behind,
    # keeping row 1 and the record_type it carries
    rows = dstore._db.execute("SELECT state_id FROM state").fetchall()
    assert [r["state_id"] for r in rows] == [1]
    assert dstore.record_type == get_object_provenance(str)
    dstore.close()


def test_the_owner_token_tells_threads_apart():
    """two threads of one process do not produce the same owner"""
    # the main thread's native id equals the pid on Linux, so a token built
    # from the pid twice is indistinguishable from a real one when it is
    # only ever read from the main thread
    tokens = []

    def record():
        tokens.append(_owner_token())

    record()
    other = threading.Thread(target=record)
    other.start()
    other.join()

    assert tokens[0] != tokens[1]
    assert all(t.startswith(f"{os.getpid()}:") for t in tokens)


def test_the_lock_names_the_thread_as_well_as_the_process(tmp_dir):
    """ownership identifies one store, not merely one process"""
    path = tmp_dir / "owner.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")

    assert dstore._lock_id == f"{os.getpid()}:{threading.get_native_id()}"
    dstore.close()


def test_unlock_declines_a_lock_taken_by_another_thread(tmp_dir):
    """a lock this thread did not take is not this thread's to release"""
    # threads share a pid, so an owner recorded as one gave every thread of
    # a process the run of every lock any of them held
    path = tmp_dir / "otherthread.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    foreign = f"{os.getpid()}:{threading.get_native_id() + 1}"
    dstore._db.execute("UPDATE state SET lock_pid=?", (foreign,))

    dstore.unlock()

    assert dstore._lock_id == foreign
    dstore.unlock(force=True)
    assert dstore._lock_id is None
    dstore.close()


def test_unlock_declines_a_lock_in_the_older_format(tmp_dir):
    """a bare pid names a session this one cannot identify itself with"""
    path = tmp_dir / "legacy.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore._db.execute("UPDATE state SET lock_pid=?", (os.getpid(),))

    dstore.unlock()

    assert dstore._lock_id == os.getpid()
    dstore.unlock(force=True)
    assert dstore._lock_id is None
    dstore.close()


_LOCK_RACERS = 8
_LOCK_ROUNDS = 5
_LOCK_TIMEOUT = 30


def _race_for_the_lock(path):
    """let several sessions reach for the lock of a fresh store at once

    Returns the rows left in the state table.
    """
    open_sqlite_db_rw(path).close()
    barrier = threading.Barrier(_LOCK_RACERS, timeout=_LOCK_TIMEOUT)

    def claim():
        store = DataStoreSqlite(path, mode=OVERWRITE)
        barrier.wait(timeout=_LOCK_TIMEOUT)
        with contextlib.suppress(OSError, sqlite3.OperationalError):
            _ = store.db
        # closed in the thread that opened the connection, which is the
        # only one sqlite3 will let touch it
        with contextlib.suppress(sqlite3.Error):
            store.close()

    threads = [threading.Thread(target=claim) for _ in range(_LOCK_RACERS)]
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=_LOCK_TIMEOUT)
    finally:
        sys.setswitchinterval(old_interval)

    raw = sqlite3.connect(path)
    try:
        return raw.execute("SELECT state_id FROM state").fetchall()
    finally:
        raw.close()


@pytest.mark.parametrize("attempt", list(range(_LOCK_ROUNDS)))
def test_only_one_session_takes_the_lock(tmp_dir, attempt):
    """sessions racing for a free store leave one state row between them"""
    # reading the state table and claiming it are one transaction, so a
    # racer either finds the store free and takes it or finds it taken.
    # as two statements each racer found it free and inserted its own row,
    # and since unlock() clears state_id 1 the rest were held for good.
    # one round in two showed it, so a red case here reruns green: it is
    # the parametrised set that carries the test, not any single attempt
    rows = _race_for_the_lock(tmp_dir / f"race{attempt}.sqlitedb")

    assert rows == [(1,)]


def test_a_failed_claim_leaves_the_connection_usable(tmp_dir, monkeypatch):
    """a claim that raises ends its transaction rather than stranding it"""
    path = tmp_dir / "failclaim.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.unlock()

    def explode():
        msg = "no token"
        raise RuntimeError(msg)

    monkeypatch.setattr("scinexus.sqlite_data_store._owner_token", explode)
    with pytest.raises(RuntimeError):
        dstore.lock()
    assert not dstore._db.in_transaction

    monkeypatch.undo()
    dstore.lock()

    assert dstore._lock_id == _owner_token()
    dstore.close()


def test_a_claim_refused_at_the_commit_leaves_the_store_usable(tmp_dir):
    """a commit that cannot complete does not strand the connection"""
    path = tmp_dir / "busycommit.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.unlock()
    dstore._db.execute("PRAGMA busy_timeout=100")

    # a reader inside a read transaction blocks the exclusive lock a commit
    # needs without blocking the reserved one the claim opens with, so the
    # claim fails at the last statement of the three
    reader = sqlite3.connect(path, isolation_level=None, timeout=1)
    reader.execute("BEGIN")
    reader.execute("SELECT * FROM state").fetchall()
    try:
        with pytest.raises(sqlite3.OperationalError):
            dstore.lock()
        assert not dstore._db.in_transaction
    finally:
        reader.close()

    dstore.lock()

    assert dstore._lock_id == _owner_token()
    dstore.close()


def test_a_locked_store_is_refused_without_waiting_for_the_write_lock(tmp_dir):
    """the refusal reads the owner, which does not need the write lock"""
    path = tmp_dir / "refuse.sqlitedb"
    abandoned = DataStoreSqlite(path, mode=OVERWRITE)
    abandoned.write(unique_id="r1", data="d1")
    abandoned._db.execute("UPDATE state SET lock_pid=?", ("999999:999999",))
    abandoned._db.close()
    abandoned._db = None
    abandoned._closed = True

    # another session holding the write lock would make a refusal that
    # contends for it wait out the busy timeout and fail as OperationalError
    writer = sqlite3.connect(path, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    try:
        dstore = DataStoreSqlite(path, mode=OVERWRITE)
        dstore._connection().execute("PRAGMA busy_timeout=30000")

        with pytest.raises(OSError, match="locked by"):
            dstore.lock()
    finally:
        writer.close()
    dstore.close()


def test_is_locked(tmp_dir):
    path = tmp_dir / "test_locked.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    assert dstore.locked
    dstore.close()


def test_lock_unlock(tmp_dir):
    path = tmp_dir / "test_lock_unlock.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    assert dstore.locked
    dstore.unlock()
    assert not dstore.locked
    dstore.lock()
    assert dstore.locked
    dstore.unlock()
    assert not dstore.locked
    dstore.close()


def test_lock_firsttime(tmp_dir):
    path = tmp_dir / "test_lockfirst.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    # accessing db triggers lock
    _ = dstore.db
    assert dstore.locked
    # delete state row and re-lock from empty state
    dstore.db.execute("DELETE FROM state WHERE state_id=1")
    dstore.lock()
    assert dstore.locked
    dstore.unlock()
    assert not dstore.locked
    dstore.close()


def test_db_without_logs(tmp_dir):
    path = tmp_dir / "test_nologs.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    assert len(dstore.logs) == 0
    dstore.close()


def test_md5_none(tmp_dir):
    path = tmp_dir / "test_md5none.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert dstore.md5("nonexistent") is None
    dstore.close()


def test_md5_missing(tmp_dir):
    path = tmp_dir / "test_md5missing.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert dstore.md5("missing_record") is None
    dstore.close()


def test_open_data_store_sqlitedb_err():
    from scinexus.io import open_data_store

    with pytest.raises(NotImplementedError):
        open_data_store(":memory:", mode="r")


def test_pickleable_roundtrip(tmp_dir):
    path = tmp_dir / "test_pickle.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()
    dstore2 = DataStoreSqlite(path, mode=READONLY)
    re_dstore = loads(dumps(dstore2))
    assert re_dstore.read("r1") == "d1"
    re_dstore.close()
    dstore2.close()


def test_pickleable_member_roundtrip(tmp_dir):
    path = tmp_dir / "test_pickle_member.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()
    dstore2 = DataStoreSqlite(path, mode=READONLY)
    member = dstore2[0]
    re_member = loads(dumps(member))
    assert re_member.read() == "d1"
    re_member.data_store.close()
    dstore2.close()


def test_getitem(tmp_dir):
    path = tmp_dir / "test_getitem.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.write(unique_id="r2", data="d2")
    first = dstore[0]
    assert first.unique_id == "r1"
    with pytest.raises(IndexError):
        _ = dstore[len(dstore)]
    dstore.close()


def test_empty_data_store(tmp_dir):
    path = tmp_dir / "test_empty.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert len(dstore) == 0
    dstore.close()


def test_no_logs(tmp_dir):
    path = tmp_dir / "test_nologs2.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert len(dstore.logs) == 0
    dstore.close()


def test_no_not_completed(tmp_dir):
    path = tmp_dir / "test_nonc.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    assert len(dstore.not_completed) == 0
    dstore.close()


def test_write_read_only_datastore(tmp_dir):
    path = tmp_dir / "test_ro.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write(unique_id="r1", data="d1")
    dstore.close()
    ro = DataStoreSqlite(path, mode=READONLY)
    with pytest.raises(IOError):
        ro.write(unique_id="r2", data="d2")
    ro.close()


def test_write_citations_sqlite(tmp_dir, sample_citations):
    path = tmp_dir / "test_cite.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    loaded = dstore._load_citations()
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"
    assert loaded[1].title == "Tool Two"
    dstore.close()


def test_write_citations_empty_sqlite(tmp_dir):
    path = tmp_dir / "test_cite_empty.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=())
    loaded = dstore._load_citations()
    assert len(loaded) == 0
    dstore.close()


def test_write_bib_sqlite(tmp_dir, sample_citations):
    path = tmp_dir / "test_bib.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    bib_path = tmp_dir / "refs.bib"
    dstore.write_bib(bib_path)
    assert bib_path.exists()
    content = bib_path.read_text()
    assert "Tool One" in content
    assert "Tool Two" in content
    dstore.close()


def test_summary_citations_sqlite(tmp_dir, sample_citations):
    path = tmp_dir / "test_sumcite.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    result = dstore.summary_citations
    assert isinstance(result, list)
    assert len(result) == 2
    dstore.close()


def test_describe_sqlite_with_display(tmp_dir):
    from scinexus.data_store import set_summary_display

    path = tmp_dir / "test_display.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    captured = {}

    def display(data, *, name=""):
        captured["data"] = data
        captured["name"] = name
        return "DISPLAY"

    set_summary_display(display)
    try:
        result = dstore.describe
        assert result == "DISPLAY"
        assert captured["name"] == "describe"
        assert "title" in captured["data"]
        assert "completed" in captured["data"]
    finally:
        set_summary_display(None)
        dstore.close()


def test_open_sqlite_db_ro_invalid_schema(tmp_dir):
    path = tmp_dir / "bad_schema.sqlitedb"
    db = sqlite3.connect(str(path))
    db.execute("CREATE TABLE IF NOT EXISTS bogus(id INTEGER PRIMARY KEY)")
    db.close()
    with pytest.raises(ValueError, match="valid schema"):
        open_sqlite_db_ro(path)


def test_lock_raises_when_db_none(tmp_dir):
    path = tmp_dir / "lock_none.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    # _db is None before first access to .db property
    with pytest.raises(RuntimeError, match="unexpectedly None"):
        dstore.lock()


def test_lock_overwrite_on_locked_db(tmp_dir):
    path = tmp_dir / "lock_ow.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    _ = dstore.db  # opens and locks
    # fake a different pid in the lock
    dstore._db.execute(
        "UPDATE state SET lock_pid=? WHERE state_id=1",
        (os.getpid() + 1,),
    )
    dstore2 = DataStoreSqlite(path, mode=OVERWRITE)
    dstore2._db = dstore._db
    with pytest.raises(OSError, match="locked"):
        dstore2.lock()
    # the shared connection is a fiction of this test, so hand it back
    # before dstore2 is collected and reports a store it never opened
    dstore2._db = None
    dstore.close()


def test_lock_update_existing_state(writable_store):
    writable_store.unlock()
    assert not writable_store.locked
    # re-lock: state row exists but lock_pid is NULL → UPDATE path
    writable_store.lock()
    assert writable_store.locked


def test_unlock_readonly(populated_store):
    ro = DataStoreSqlite(populated_store, mode=READONLY)
    ro.unlock()  # should be a no-op, no error
    ro.close()


def test_unlock_already_unlocked(writable_store):
    writable_store.unlock()
    assert not writable_store.locked
    writable_store.unlock()  # should be a no-op


def test_write_duplicate_not_added_to_completed(writable_store):
    writable_store.write(unique_id="r1", data="d1_updated")
    assert len(writable_store.completed) == 1


@pytest.mark.parametrize("unique_id", ["", "   ", ".hidden"])
def test_an_identifier_that_names_no_record_is_refused(writable_store, unique_id):
    """the identifier rule is the same one a directory store applies"""
    with pytest.raises(ValueError):
        writable_store.write(unique_id=unique_id, data="data")


@pytest.mark.parametrize("unique_id", ["", ".hidden"])
def test_a_not_completed_identifier_is_refused_too(writable_store, unique_id):
    """both kinds of record, as in a directory store"""
    with pytest.raises(ValueError):
        writable_store.write_not_completed(unique_id=unique_id, data="{}")


def test_write_log_with_table_prefix(tmp_dir, DATA_DIR):
    path = tmp_dir / "log_prefix.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    log_text = (DATA_DIR / "scitrack.log").read_text()
    dstore.write_log(unique_id=f"{LOG_TABLE}/test.log", data=log_text)
    assert len(dstore.logs) == 1
    dstore.close()


def test_write_not_completed_with_table_prefix(tmp_dir):
    path = tmp_dir / "nc_prefix.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "msg", source="src")
    dstore.write_not_completed(unique_id=f"{RESULT_TABLE}/nc1", data=nc.to_json())
    assert len(dstore.not_completed) == 1
    dstore.close()


def test_write_citations_update_existing(tmp_dir, sample_citations):
    path = tmp_dir / "cite_update.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    # write again to trigger UPDATE path
    dstore.write_citations(data=(sample_citations[0],))
    loaded = dstore._load_citations()
    assert len(loaded) == 1
    dstore.close()


def test_load_citations_no_table(tmp_dir):
    path = tmp_dir / "no_cite_table.sqlitedb"
    # create db without citations table
    db = sqlite3.connect(str(path))
    db.execute(
        "CREATE TABLE IF NOT EXISTS state"
        "(state_id INTEGER PRIMARY KEY, record_type TEXT, lock_pid INTEGER)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {LOG_TABLE}"
        "(log_id INTEGER PRIMARY KEY, log_name TEXT, date timestamp, data BLOB)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {RESULT_TABLE}"
        "(record_id TEXT PRIMARY KEY, log_id INTEGER, md5 BLOB, is_completed INTEGER, data BLOB)",
    )
    db.close()
    dstore = DataStoreSqlite(path, mode=READONLY)
    result = dstore._load_citations()
    assert result == []
    dstore.close()


def test_describe_locked_by_another_session(writable_store):
    """the description names the holder and this session separately"""
    foreign = f"{os.getpid()}:{threading.get_native_id() + 1}"
    writable_store._db.execute(
        "UPDATE state SET lock_pid=? WHERE state_id=1",
        (foreign,),
    )
    result = writable_store._describe()
    assert "Locked db store" in result["title"]
    assert foreign in result["title"]
    assert _owner_token() in result["title"]


def test_describe_locked_by_this_session(writable_store):
    """a store this session holds says so rather than naming tokens"""
    assert writable_store._lock_id == _owner_token()

    result = writable_store._describe()

    assert result["title"] == "Locked to the current process and thread."


def test_describe_unlocked(writable_store):
    writable_store.unlock()
    result = writable_store._describe()
    assert result["title"] == "Unlocked db store."


def test_record_type_getter_and_setter(writable_store):
    from scinexus.misc import get_object_provenance

    writable_store.record_type = str  # set using a type object
    assert writable_store.record_type == get_object_provenance(str)


def test_record_type_overwrite_error(writable_store):
    writable_store.record_type = str
    with pytest.raises(OSError, match="cannot overwrite"):
        writable_store.record_type = int


def test_summary_not_completed(tmp_dir):
    path = tmp_dir / "summary_nc.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "test msg", source="src")
    dstore.write_not_completed(unique_id="nc1", data=nc.to_json())
    result = dstore._summary_not_completed()
    assert isinstance(result, list)
    assert len(result) == 1
    dstore.close()


def test_db_property_none_after_open(tmp_dir):
    from unittest.mock import patch

    path = tmp_dir / "db_none.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    with (
        patch("scinexus.sqlite_data_store.open_sqlite_db_rw", return_value=None),
        patch.object(dstore, "lock"),
    ):
        with pytest.raises(ValueError, match="unexpectedly None"):
            _ = dstore.db


def test_write_member_none(tmp_dir):
    from unittest.mock import patch

    path = tmp_dir / "write_none.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    with patch.object(dstore, "_write", return_value=None):
        with pytest.raises(RuntimeError, match="failed to produce a member"):
            dstore.write(unique_id="r1", data="d1")
    dstore.close()


def test_write_not_completed_member_none(tmp_dir):
    from unittest.mock import patch

    path = tmp_dir / "nc_none.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "msg", source="src")
    with patch.object(dstore, "_write", return_value=None):
        with pytest.raises(RuntimeError, match="failed to produce a member"):
            dstore.write_not_completed(unique_id="nc1", data=nc.to_json())
    dstore.close()


def test_write_citations_no_table(tmp_dir, sample_citations):
    path = tmp_dir / "no_cite_write.sqlitedb"
    db = sqlite3.connect(str(path))
    db.execute(
        "CREATE TABLE IF NOT EXISTS state"
        "(state_id INTEGER PRIMARY KEY, record_type TEXT, lock_pid INTEGER)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {LOG_TABLE}"
        "(log_id INTEGER PRIMARY KEY, log_name TEXT, date timestamp, data BLOB)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {RESULT_TABLE}"
        "(record_id TEXT PRIMARY KEY, log_id INTEGER, md5 BLOB, "
        "is_completed INTEGER, data BLOB)",
    )
    db.close()
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    # Replace _db with a connection to the DB without citations table
    # (the lazy db property would call open_sqlite_db_rw which creates it)
    dstore._db = sqlite3.connect(
        str(path),
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    dstore._db.row_factory = sqlite3.Row
    assert not dstore._has_citations_table()
    dstore.write_citations(data=sample_citations)
    assert dstore._has_citations_table()
    loaded = dstore._load_citations()
    assert len(loaded) == 2
    dstore.close()


_RECORDS = 400
_READERS = 12
_OPEN_RACERS = 4
_OPEN_RACE_ROUNDS = 20


@define_app(app_type=LOADER)
class read_member:
    """loader naming each record alongside what it holds"""

    def main(self, member: DataMemberABC) -> str:
        return f"{member.unique_id}={member.read()}"


def _expected_records():
    """identifier to contents, each naming the other"""
    return {f"k{i:05d}": f"v{i:05d}" for i in range(_RECORDS)}


@pytest.fixture
def many_record_store(tmp_dir):
    """path to a closed store holding _RECORDS records"""
    path = tmp_dir / "many.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    for unique_id, data in _expected_records().items():
        dstore.write(unique_id=unique_id, data=data)
    dstore.close()
    return path


def _run_on_threads(target, num_threads):
    """call target() on num_threads threads that start together

    Raises if any of them is still running afterwards.
    """
    barrier = threading.Barrier(num_threads, timeout=_LOCK_TIMEOUT)

    def run():
        barrier.wait(timeout=_LOCK_TIMEOUT)
        target()

    threads = [threading.Thread(target=run) for _ in range(num_threads)]
    # on a build with the GIL this makes the interleaving a check-then-set
    # races on happen within a few hundred instructions. on a free-threaded
    # one it does nothing, because the threads are already running at once
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=_LOCK_TIMEOUT)
    finally:
        sys.setswitchinterval(old_interval)

    # a thread wedged on a lock leaves whatever it was counting empty,
    # which every assertion about "no wrong values" is satisfied by
    alive = [thread.name for thread in threads if thread.is_alive()]
    assert alive == [], f"still running after {_LOCK_TIMEOUT}s: {alive}"


def _load_through_threads(dstore):
    """what a loader over dstore produces on the thread backend"""
    set_parallel_backend("threads")
    app = read_member()
    results = app.as_completed(dstore, parallel=True, show_progress=False)
    return {r.obj if isinstance(r, source_proxy) else r for r in results}


@pytest.mark.free_threaded
def test_concurrent_reads_answer_with_their_own_record(many_record_store):
    """a record read from any thread is the record that was asked for"""
    # a connection carries one statement cache, keyed by the text of the
    # statement and shared between threads without protection, so two
    # threads running this SELECT with different identifiers otherwise bind
    # to one statement and read each other's rows. wrong values are counted
    # rather than only exceptions: an exception is the loudest form of the
    # damage, not the usual one
    dstore = DataStoreSqlite(many_record_store, mode=READONLY)
    expected = _expected_records()
    right = []
    wrong = []
    errors = []

    def read_every_record():
        try:
            for unique_id, data in expected.items():
                got = dstore.read(unique_id)
                (right if got == data else wrong).append((unique_id, got))
        except Exception as err:  # noqa: BLE001
            errors.append(repr(err))

    _run_on_threads(read_every_record, _READERS)

    assert errors == []
    assert wrong == []
    # counted, so that threads which never got to read cannot pass this by
    # leaving the other two empty
    assert len(right) == _READERS * _RECORDS
    dstore.close()


@pytest.mark.free_threaded
@pytest.mark.usefixtures("reset_parallel_backend")
def test_a_loader_reads_a_store_through_the_thread_backend(many_record_store):
    """a pipeline over a .sqlitedb gets every record back, whole"""
    dstore = DataStoreSqlite(many_record_store, mode=READONLY)

    got = _load_through_threads(dstore)

    assert got == {f"{k}={v}" for k, v in _expected_records().items()}
    dstore.close()


@pytest.mark.free_threaded
@pytest.mark.usefixtures("reset_parallel_backend")
def test_a_loader_reads_an_in_memory_store_through_the_thread_backend():
    """an in-memory store is read from the pool like any other"""
    # its database lives inside its one connection, so there is nothing for
    # a second connection to open and serialising that one is the only
    # arrangement available to it
    dstore = DataStoreSqlite(_MEMORY, mode=OVERWRITE)
    for unique_id, data in _expected_records().items():
        dstore.write(unique_id=unique_id, data=data)

    got = _load_through_threads(dstore)

    assert got == {f"{k}={v}" for k, v in _expected_records().items()}
    dstore.close()


def _connections_from_racing_threads(dstore, num_threads):
    """the connection each of several threads got from one store at once"""
    opened = []

    def open_it():
        opened.append(dstore._connection())

    _run_on_threads(open_it, num_threads)
    return opened


@pytest.mark.free_threaded
def test_racing_the_first_access_opens_one_connection(many_record_store):
    """the database is opened once, however many threads reach it together"""
    # the open is a check then a set, and every loser's connection is
    # orphaned: a descriptor and a page cache nothing will ever close
    for _ in range(_OPEN_RACE_ROUNDS):
        dstore = DataStoreSqlite(many_record_store, mode=READONLY)

        opened = _connections_from_racing_threads(dstore, _OPEN_RACERS)

        assert len(opened) == _OPEN_RACERS
        assert len({id(conn) for conn in opened}) == 1
        dstore.close()


@pytest.mark.free_threaded
def test_concurrent_completed_scans_the_store_once(many_record_store, monkeypatch):
    """the members are read from the database once, not once per reader"""
    dstore = DataStoreSqlite(many_record_store, mode=READONLY)
    scans = []
    select_members = dstore._select_members

    def counted(**kwargs):
        scans.append(kwargs["table_name"])
        return select_members(**kwargs)

    monkeypatch.setattr(dstore, "_select_members", counted)
    counts = []

    def take_completed():
        counts.append(len(dstore.completed))

    _run_on_threads(take_completed, _READERS)

    assert counts == [_RECORDS] * _READERS
    assert scans == [RESULT_TABLE]
    dstore.close()

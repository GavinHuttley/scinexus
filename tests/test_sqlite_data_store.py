import os
import sqlite3
from pathlib import Path
from pickle import dumps, loads

import pytest
from citeable import Software
from scitrack import get_text_hexdigest

from scinexus.composable import NotCompleted, NotCompletedType
from scinexus.data_store import OVERWRITE, READONLY, DataMemberABC, DataStoreDirectory
from scinexus.sqlite_data_store import (
    _LOG_TABLE,
    _MEMORY,
    _RESULT_TABLE,
    DataStoreSqlite,
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
    assert dstore._log_id is not None  # noqa: SLF001
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
    query = f"SELECT count(*) as c FROM {_RESULT_TABLE} WHERE is_completed=?"
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
    query = f"SELECT count(*) as c FROM {_RESULT_TABLE} WHERE is_completed=?"
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


@pytest.mark.parametrize("table_name", ["", _RESULT_TABLE])
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
    loaded = dstore._load_citations()  # noqa: SLF001
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"
    assert loaded[1].title == "Tool Two"
    dstore.close()


def test_write_citations_empty_sqlite(tmp_dir):
    path = tmp_dir / "test_cite_empty.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=())
    loaded = dstore._load_citations()  # noqa: SLF001
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
    dstore._db.execute(  # noqa: SLF001
        "UPDATE state SET lock_pid=? WHERE state_id=1",
        (os.getpid() + 1,),
    )
    dstore2 = DataStoreSqlite(path, mode=OVERWRITE)
    dstore2._db = dstore._db  # noqa: SLF001
    with pytest.raises(OSError, match="locked"):
        dstore2.lock()
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


def test_write_log_with_table_prefix(tmp_dir, DATA_DIR):
    path = tmp_dir / "log_prefix.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    log_text = (DATA_DIR / "scitrack.log").read_text()
    dstore.write_log(unique_id=f"{_LOG_TABLE}/test.log", data=log_text)
    assert len(dstore.logs) == 1
    dstore.close()


def test_write_not_completed_with_table_prefix(tmp_dir):
    path = tmp_dir / "nc_prefix.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    nc = NotCompleted(NotCompletedType.FAIL, "dummy", "msg", source="src")
    dstore.write_not_completed(unique_id=f"{_RESULT_TABLE}/nc1", data=nc.to_json())
    assert len(dstore.not_completed) == 1
    dstore.close()


def test_write_citations_update_existing(tmp_dir, sample_citations):
    path = tmp_dir / "cite_update.sqlitedb"
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    # write again to trigger UPDATE path
    dstore.write_citations(data=(sample_citations[0],))
    loaded = dstore._load_citations()  # noqa: SLF001
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
        f"CREATE TABLE IF NOT EXISTS {_LOG_TABLE}"
        "(log_id INTEGER PRIMARY KEY, log_name TEXT, date timestamp, data BLOB)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {_RESULT_TABLE}"
        "(record_id TEXT PRIMARY KEY, log_id INTEGER, md5 BLOB, is_completed INTEGER, data BLOB)",
    )
    db.close()
    dstore = DataStoreSqlite(path, mode=READONLY)
    result = dstore._load_citations()  # noqa: SLF001
    assert result == []
    dstore.close()


def test_describe_locked_different_pid(writable_store):
    writable_store._db.execute(  # noqa: SLF001
        "UPDATE state SET lock_pid=? WHERE state_id=1",
        (os.getpid() + 1,),
    )
    result = writable_store._describe()  # noqa: SLF001
    assert "Locked db store" in result["title"]
    assert str(os.getpid() + 1) in result["title"]


def test_describe_unlocked(writable_store):
    writable_store.unlock()
    result = writable_store._describe()  # noqa: SLF001
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
    result = dstore._summary_not_completed()  # noqa: SLF001
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
        f"CREATE TABLE IF NOT EXISTS {_LOG_TABLE}"
        "(log_id INTEGER PRIMARY KEY, log_name TEXT, date timestamp, data BLOB)",
    )
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {_RESULT_TABLE}"
        "(record_id TEXT PRIMARY KEY, log_id INTEGER, md5 BLOB, "
        "is_completed INTEGER, data BLOB)",
    )
    db.close()
    dstore = DataStoreSqlite(path, mode=OVERWRITE)
    # Replace _db with a connection to the DB without citations table
    # (the lazy db property would call open_sqlite_db_rw which creates it)
    dstore._db = sqlite3.connect(  # noqa: SLF001
        str(path),
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    dstore._db.row_factory = sqlite3.Row  # noqa: SLF001
    dstore._open = True  # noqa: SLF001
    assert not dstore._has_citations_table()  # noqa: SLF001
    dstore.write_citations(data=sample_citations)
    assert dstore._has_citations_table()  # noqa: SLF001
    loaded = dstore._load_citations()  # noqa: SLF001
    assert len(loaded) == 2
    dstore.close()

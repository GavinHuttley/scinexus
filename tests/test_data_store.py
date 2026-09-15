import copy
import functools
import json
import pathlib
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from itertools import product
from pathlib import Path
from pickle import dumps, loads

import pytest
from citeable import Software
from scitrack import get_text_hexdigest

try:
    import cogent3 as c3
    from cogent3.util.union_dict import UnionDict
except ImportError:
    c3 = None
    UnionDict = None

from scinexus import open_data_store
from scinexus.composable import NotCompleted, NotCompletedType
from scinexus.data_store import (
    APPEND,
    CITATIONS_FILE,
    COMPLETED_CHECKSUM,
    MD5_TABLE,
    NOT_COMPLETED_CHECKSUM,
    NOT_COMPLETED_TABLE,
    OVERWRITE,
    READONLY,
    DataStoreDirectory,
    ReadOnlyDataStoreZipped,
    get_data_source,
    get_id_from_source,
    get_summary_display,
    get_unique_id,
    load_record_from_json,
    make_record_for_json,
    set_id_from_source,
    set_summary_display,
    summary_not_completeds,
)

# over-ride cogent3 setting
set_summary_display(None)


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return Path(tmp_path_factory.mktemp("datastore"))


@pytest.fixture
def fasta_dir(DATA_DIR, tmp_dir):
    tmp_dir = Path(tmp_dir)
    filenames = DATA_DIR.glob("*.fasta")
    fasta_dir = tmp_dir / "fasta"
    fasta_dir.mkdir(parents=True, exist_ok=True)
    for fn in filenames:
        dest = fasta_dir / fn.name
        dest.write_text(fn.read_text())
    return fasta_dir


@pytest.fixture
def write_dir(tmp_dir):
    tmp_dir = Path(tmp_dir)
    write_dir = tmp_dir / "write"
    write_dir.mkdir(parents=True, exist_ok=True)
    yield write_dir
    shutil.rmtree(write_dir, ignore_errors=True)


@pytest.fixture
def w_dstore(write_dir):
    return DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)


@pytest.fixture
def ro_dstore(fasta_dir):
    return DataStoreDirectory(fasta_dir, suffix="fasta", mode=READONLY)


@pytest.fixture
def completed_objects(ro_dstore):
    return {f"{Path(m.unique_id).stem}": m.read() for m in ro_dstore}


@pytest.fixture
def nc_objects():
    return {
        f"id_{i}": NotCompleted(
            NotCompletedType.ERROR, "location", "message", source=f"id_{i}"
        )
        for i in range(3)
    }


@pytest.fixture(scope="session")
def log_data(DATA_DIR):
    path = DATA_DIR / "scitrack.log"
    return path.read_text()


@pytest.fixture
def full_dstore(write_dir, nc_objects, completed_objects, log_data):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    for id_, data in nc_objects.items():
        dstore.write_not_completed(unique_id=id_, data=data.to_json())

    for id_, data in completed_objects.items():
        dstore.write(unique_id=id_, data=data)

    dstore.write_log(unique_id="scitrack.log", data=log_data)
    return dstore


@pytest.fixture
def nc_dir(tmp_dir):
    nc_dir = tmp_dir / "nc_test"
    nc_dir.mkdir(parents=True, exist_ok=True)
    yield nc_dir
    shutil.rmtree(nc_dir, ignore_errors=True)


@pytest.fixture
def nc_dstore(DATA_DIR, nc_dir):
    dstore = DataStoreDirectory(nc_dir, suffix="fasta", mode=OVERWRITE)
    log_filename = "scitrack.log"
    dstore.write_log(unique_id=log_filename, data=(DATA_DIR / log_filename).read_text())
    nc = [
        NotCompleted(
            NotCompletedType.FAIL,
            f"dummy{i}",
            f"dummy_message{i}",
            source=f"dummy_source{i}",
        )
        for i in range(3)
    ]
    for i, item in enumerate(nc):
        dstore.write_not_completed(unique_id=f"nc{i + 1}", data=item.to_json())
    assert len(dstore.not_completed) == 3
    filenames = DATA_DIR.glob("*.fasta")
    for fn in filenames:
        identifier = fn.name
        dstore.write(unique_id=identifier, data=fn.read_text())
    return dstore


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


def _get_member_data(members):
    return {m.unique_id: m.read() for m in members}


@pytest.fixture
def zipped_basic(fasta_dir):
    path = shutil.make_archive(
        base_name=str(fasta_dir.parent / fasta_dir.name),
        format="zip",
        base_dir=fasta_dir.name,
        root_dir=fasta_dir.parent,
    )
    return pathlib.Path(path)


@pytest.fixture
def zipped_full(full_dstore):
    source = pathlib.Path(full_dstore.source)
    path = shutil.make_archive(
        base_name=str(source.parent / source.name),
        format="zip",
        base_dir=source.name,
        root_dir=source.parent,
    )
    return ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")


@pytest.fixture
def zipped_hidden(fasta_dir):
    # create a hidden file
    hidden = fasta_dir / ".hidden.fasta"
    hidden.write_text(">s1\nACGT\n")
    path = shutil.make_archive(
        base_name=str(fasta_dir.parent / (fasta_dir.name + "_hidden")),
        format="zip",
        base_dir=fasta_dir.name,
        root_dir=fasta_dir.parent,
    )
    hidden.unlink()
    return pathlib.Path(path)


def test_data_member_eq(ro_dstore, fasta_dir):
    ro_dstore2 = DataStoreDirectory(fasta_dir, mode="r", suffix="fasta")
    name = "brca1.fasta"
    mem1 = next(m for m in ro_dstore.completed if m.unique_id == name)
    mem2 = next(m for m in ro_dstore2.completed if m.unique_id == name)
    assert mem1 != mem2


def test_fail_try_append(full_dstore, completed_objects):
    full_dstore._mode = APPEND
    id_, data = next(iter(completed_objects.items()))
    with pytest.raises(IOError):
        full_dstore.write(unique_id=id_, data=data)


def test_contains(ro_dstore):
    """correctly identify when a data store contains a member"""
    assert "brca1.fasta" in ro_dstore
    assert "brca1" in ro_dstore


def test_len(ro_dstore):
    """DataStore returns correct len"""
    expect = len(list(ro_dstore.source.glob("*.fasta")))
    assert expect == len(ro_dstore) == len(ro_dstore.members)


def test_getitem(ro_dstore):
    with pytest.raises(IndexError):
        _ = ro_dstore[len(ro_dstore)]

    last = ro_dstore[-1]
    first = ro_dstore[0]
    assert last.unique_id != first.unique_id


def test_iterall(ro_dstore):
    expect = {fn.name for fn in ro_dstore.source.glob("*.fasta")}
    got = {m.unique_id for m in ro_dstore}
    assert expect == got


def test_read(ro_dstore):
    """correctly read content"""
    expect = (ro_dstore.source / "brca1.fasta").read_text()
    got = ro_dstore.read("brca1.fasta")
    assert got == expect


def test_pickleable_roundtrip(ro_dstore):
    """pickling of data stores should be reversible"""
    re_dstore = loads(dumps(ro_dstore))
    assert str(ro_dstore) == str(re_dstore)
    assert ro_dstore[0].read() == re_dstore[0].read()


def test_deepcopy_roundtrip(ro_dstore):
    """apps deepcopy their arguments on every call, so a store must survive it

    The member cache is guarded by a lock, and a lock cannot be pickled.
    deepcopy goes through the same reduce protocol as pickle, so a store
    passed to an app as an argument would otherwise raise on every call
    rather than only when someone pickled it.
    """
    copied = copy.deepcopy(ro_dstore)
    assert str(copied) == str(ro_dstore)
    assert copied[0].read() == ro_dstore[0].read()
    assert len(copied.completed) == len(ro_dstore.completed)


def test_pickleable_member_roundtrip(ro_dstore):
    """pickling of data store members should be reversible"""
    re_member = loads(dumps(ro_dstore[0]))
    data = re_member.read()
    assert len(data) > 0


def test_empty_directory(fasta_dir):
    dstore = DataStoreDirectory(fasta_dir, suffix=".txt")
    assert len(dstore) == 0


def test_no_logs(ro_dstore):
    assert len(ro_dstore.logs) == 0


def test_no_not_completed(ro_dstore):
    assert len(ro_dstore.not_completed) == 0


def test_logs(nc_dstore):
    assert len(nc_dstore.logs) == 1
    log = nc_dstore.logs[0].read()
    assert isinstance(log, str)


def test_not_completed(nc_dstore):
    assert len(nc_dstore.not_completed) == 3
    nc = nc_dstore.not_completed[0].read()
    assert isinstance(nc, str)


def test_drop_not_completed(nc_dstore):
    num_completed = len(nc_dstore.completed)
    num_not_completed = len(nc_dstore.not_completed)
    num_md5 = len(list((nc_dstore.source / MD5_TABLE).glob("*")))
    assert num_not_completed == 3
    assert num_completed == 6
    assert len(nc_dstore) == 9
    assert num_md5 == num_completed + num_not_completed
    nc_dstore.drop_not_completed()
    assert len(nc_dstore.not_completed) == 0
    num_md5 = len(list((nc_dstore.source / MD5_TABLE).glob("*")))
    assert num_md5 == num_completed


def test_write_not_completed_twice_caches_one_member(w_dstore):
    """re-writing a not-completed record leaves one cached member, not two"""
    data = NotCompleted(
        NotCompletedType.ERROR,
        "location",
        "message",
        source="nc1",
    ).to_json()
    expect = [str(Path(NOT_COMPLETED_TABLE) / "nc1.json")]

    w_dstore.write_not_completed(unique_id="nc1", data=data)

    # the attribute, not the property: the property rebuilds itself from
    # disk when empty, so it reports a member even if nothing recorded one
    assert [m.unique_id for m in w_dstore._not_completed] == expect

    # the second write rescans nothing, so the cached list is the same
    # object as before it and identity alone cannot tell the two apart
    w_dstore.write_not_completed(unique_id="nc1", data=data)

    nc_dir = w_dstore.source / NOT_COMPLETED_TABLE
    assert len(list(nc_dir.glob("*.json"))) == 1
    assert [m.unique_id for m in w_dstore._not_completed] == expect

    w_dstore.drop_not_completed()

    assert list(nc_dir.glob("*.json")) == []


def test_append_mode_refuses_to_rewrite_a_not_completed_record(w_dstore):
    """APPEND refuses a second not-completed record, as it does a completed one"""
    data = NotCompleted(
        NotCompletedType.ERROR,
        "location",
        "message",
        source="nc1",
    ).to_json()
    w_dstore.write_not_completed(unique_id="nc1", data=data)
    path = w_dstore.source / NOT_COMPLETED_TABLE / "nc1.json"

    w_dstore._mode = APPEND

    with pytest.raises(OSError):
        w_dstore.write_not_completed(unique_id="nc1", data='{"replaced": true}')

    assert path.read_text() == data


def test_append_mode_refuses_to_rewrite_a_compressed_not_completed(w_dstore):
    """the APPEND refusal follows a compressed identifier to the record it names"""
    # the identifier resolves to nc1.json, so the refusal has to recognise
    # the second write as the record already stored, not as a new one
    data = NotCompleted(
        NotCompletedType.ERROR,
        "location",
        "message",
        source="nc1",
    ).to_json()
    w_dstore.write_not_completed(unique_id="nc1.json.gz", data=data)
    assert f"{NOT_COMPLETED_TABLE}/nc1.json" in w_dstore

    w_dstore._mode = APPEND

    with pytest.raises(OSError):
        w_dstore.write_not_completed(unique_id="nc1.json.gz", data="{}")


def test_write_not_completed_beside_a_colliding_completed_record(write_dir):
    """a completed record of the same name does not suppress the write"""
    # a store whose own suffix is json is where the completed id nc1.json
    # and the not-completed one collide once the bare nc1 is completed
    dstore = DataStoreDirectory(write_dir, suffix="json", mode=OVERWRITE)
    dstore.write(unique_id="nc1.json", data='{"completed": true}')
    dstore._mode = APPEND

    member = dstore.write_not_completed(unique_id="nc1", data='{"failed": true}')

    assert member is not None
    assert (write_dir / NOT_COMPLETED_TABLE / "nc1.json").exists()


def test_append_mode_refuses_a_rename_onto_an_existing_record(w_dstore):
    """APPEND refuses a write whose identifier is renamed onto one in the store"""
    # c1.txt carries the wrong suffix for this store and becomes c1.fasta
    # before anything is written, so only the renamed id can be checked
    w_dstore.write(unique_id="c1.fasta", data=">first\nAAAA\n")
    w_dstore._mode = APPEND

    with pytest.raises(OSError):
        w_dstore.write(unique_id="c1.txt", data=">second\nTTTT\n")


def test_write_twice_caches_one_member_for_log_suffix(write_dir):
    """a store whose own suffix is log records a re-written member once"""
    # log is the one suffix _write exempts from its duplicate guard, so
    # this is the store where a repeat write reaches the cache at all
    dstore = DataStoreDirectory(write_dir, suffix="log", mode=OVERWRITE)
    dstore.write(unique_id="c1.log", data="first")
    dstore.write(unique_id="c1.log", data="second")

    assert [m.unique_id for m in dstore._completed] == ["c1.log"]
    assert len(dstore) == 1


@pytest.fixture
def mixed_md5_dstore(tmp_dir):
    """a store where only one of two not-completed records has a checksum"""
    # a checksum is optional -- md5() returns None without one and
    # _validate counts it under md5_missing -- so a store assembled by hand
    # or by an earlier writer can hold both kinds, and a drop meets both
    source = tmp_dir / "mixed_md5"
    (source / NOT_COMPLETED_TABLE).mkdir(parents=True)
    (source / MD5_TABLE).mkdir(parents=True)
    for i in range(2):
        nc = NotCompleted(
            NotCompletedType.ERROR,
            "location",
            "message",
            source=f"id_{i}",
        )
        data = nc.to_json()
        (source / NOT_COMPLETED_TABLE / f"id_{i}.json").write_text(data)
        if i == 0:
            checksum = f"id_{i}.{NOT_COMPLETED_CHECKSUM}"
            (source / MD5_TABLE / checksum).write_text(get_text_hexdigest(data))
    return DataStoreDirectory(source, suffix="fasta", mode=OVERWRITE)


def test_drop_not_completed_without_md5_file(mixed_md5_dstore):
    """a record with no checksum file is dropped like any other"""
    source = mixed_md5_dstore.source
    assert len(mixed_md5_dstore.not_completed) == 2

    # id_1 is the one without a checksum. dropping by identifier leaves the
    # cache in place, unlike a full drop, so the assertion below observes
    # the list itself rather than a rebuild of it
    mixed_md5_dstore.drop_not_completed(unique_id="id_1")

    expect = [str(Path(NOT_COMPLETED_TABLE) / "id_0.json")]
    assert [m.unique_id for m in mixed_md5_dstore.not_completed] == expect
    assert not (source / NOT_COMPLETED_TABLE / "id_1.json").exists()

    mixed_md5_dstore.drop_not_completed()

    assert list((source / NOT_COMPLETED_TABLE).glob("*.json")) == []
    assert list((source / MD5_TABLE).glob("*")) == []


def test_drop_not_completed_keeps_what_limit_hides(nc_dstore):
    """a full drop empties the limited view and keeps the rest"""
    nc_dstore._limit = 1
    nc_dstore._not_completed = []
    nc_dir = nc_dstore.source / NOT_COMPLETED_TABLE
    assert len(nc_dstore.not_completed) == 1
    assert len(list(nc_dir.glob("*.json"))) == 3

    nc_dstore.drop_not_completed()

    # records remain, so the directory is still in use
    assert nc_dir.exists()
    assert len(list(nc_dir.glob("*.json"))) == 2


def test_drop_not_completed_by_id_keeps_what_limit_hides(nc_dstore):
    """a record the limited view omits is not dropped by identifier"""
    nc_dstore._limit = 1
    nc_dstore._not_completed = []
    nc_dir = nc_dstore.source / NOT_COMPLETED_TABLE
    shown = {Path(m.unique_id).name for m in nc_dstore.not_completed}
    hidden = next(p for p in nc_dir.glob("*.json") if p.name not in shown)

    nc_dstore.drop_not_completed(unique_id=hidden.stem)

    assert hidden.exists()


@pytest.mark.parametrize(
    "unique_id",
    [
        "nc1",
        "nc1.fasta",
        "nc1.json",
        "nc1.txt",
        "nc1.fasta.gz",
        "nc1.json.gz",
        "a.b.fasta",
    ],
)
def test_write_drops_the_twin_however_the_id_is_spelled(w_dstore, unique_id):
    """the record a write supersedes is found whatever extension the id carries"""
    # nc1.json.gz belongs here now: the store names the file, so it is
    # stored as nc1.json like every other spelling. it used to be stored
    # under its own name, which the *.json scan could not match
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="nc1")
    w_dstore.write_not_completed(unique_id=unique_id, data=record.to_json())
    nc_dir = w_dstore.source / NOT_COMPLETED_TABLE
    assert len(list(nc_dir.glob("*.json"))) == 1

    w_dstore.write(unique_id=unique_id, data=">s\nACGT\n")

    assert list(nc_dir.glob("*.json")) == []


def test_write_leaves_unrelated_not_completed_records(w_dstore):
    """superseding one record does not touch another whose name ends the same"""
    # write("c1.fasta") looks for not_completed/c1.json, and nc1.json and
    # abc1.json both end with that name
    for uid in ("nc1", "abc1", "c1"):
        record = NotCompleted(NotCompletedType.ERROR, "location", "message", source=uid)
        w_dstore.write_not_completed(unique_id=uid, data=record.to_json())

    w_dstore.write(unique_id="c1.fasta", data=">s\nACGT\n")

    nc_dir = w_dstore.source / NOT_COMPLETED_TABLE
    assert sorted(p.name for p in nc_dir.glob("*.json")) == ["abc1.json", "nc1.json"]


def test_write_keeps_the_checksum_of_the_record_it_wrote(w_dstore):
    """superseding a not-completed record leaves the new checksum in place"""
    # both kinds were kept under one name, so the drop that supersedes the
    # twin deleted the checksum _write had written moments earlier
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="id_0")
    w_dstore.write_not_completed(unique_id="id_0", data=record.to_json())
    data = ">s\nACGT\n"

    w_dstore.write(unique_id="id_0.fasta", data=data)

    assert w_dstore.md5("id_0.fasta") == get_text_hexdigest(data)


def test_the_two_kinds_of_record_keep_separate_checksums(w_dstore):
    """a completed and a not-completed record of one name each keep their own"""
    data = ">s\nACGT\n"
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="id_0")
    nc_data = record.to_json()
    w_dstore.write(unique_id="id_0.fasta", data=data)

    w_dstore.write_not_completed(unique_id="id_0", data=nc_data)

    assert w_dstore.md5("id_0.fasta") == get_text_hexdigest(data)
    nc_id = str(Path(NOT_COMPLETED_TABLE) / "id_0.json")
    assert w_dstore.md5(nc_id) == get_text_hexdigest(nc_data)


@pytest.mark.parametrize("unique_id", ["id_0.fasta", "id_0.fasta.gz"])
def test_the_checksum_of_a_record_is_found_where_it_was_put(w_dstore, unique_id):
    """writing, reading and dropping agree on where a checksum lives"""
    # they were three separate computations of the name, and a compressed
    # identifier made all three disagree. the store now names the file, so
    # both spellings here are the one record, id_0.fasta
    data = ">s\nACGT\n"
    w_dstore.write(unique_id=unique_id, data=data)

    assert w_dstore.md5(unique_id) == get_text_hexdigest(data)


@pytest.mark.parametrize(
    "unique_id",
    ["id_0", "id_0.fasta", "id_0.fasta.gz", "id_0.gz", "id_0.genbank"],
)
def test_the_store_suffix_decides_the_stored_name(w_dstore, unique_id):
    """however the identifier is spelled, the store names the file"""
    # a suffix the identifier carried used to survive into the name, so a
    # store of .fasta could hold an id_0.fasta.gz its own scan cannot see.
    # globbing every file, not just *.fasta, so a record left under some
    # other name is a failure rather than something the pattern hides
    w_dstore.write(unique_id=unique_id, data=">s\nACGT\n")

    stored = [p.name for p in w_dstore.source.glob("*") if p.is_file()]
    assert stored == ["id_0.fasta"]


@pytest.mark.parametrize("unique_id", ["id_0", "id_0.fasta", "id_0.fasta.gz"])
def test_a_compound_suffix_is_appended_once(tmp_dir, unique_id):
    """a store of .fasta.gz stores id_0.fasta.gz, not id_0.fasta.fasta.gz"""
    dstore = DataStoreDirectory(tmp_dir / "gz", suffix="fasta.gz", mode=OVERWRITE)

    dstore.write(unique_id=unique_id, data=">s\nACGT\n")

    stored = [p.name for p in dstore.source.glob("*") if p.is_file()]
    assert stored == ["id_0.fasta.gz"]


def test_a_compound_suffix_store_writes_compressed(tmp_dir):
    """the suffix the store names picks the engine the record is written with"""
    # _write chooses the mode from the name, and open_ the handler, so the
    # compression follows from the suffix rather than from the identifier
    import gzip

    dstore = DataStoreDirectory(tmp_dir / "gz", suffix="fasta.gz", mode=OVERWRITE)
    data = ">s\nACGT\n"

    dstore.write(unique_id="id_0", data=data)

    assert (
        gzip.decompress((dstore.source / "id_0.fasta.gz").read_bytes()) == data.encode()
    )


@pytest.mark.parametrize(
    "unique_id",
    ["nc1", "nc1.json", "nc1.json.gz", "nc1.fasta.gz"],
)
def test_a_not_completed_record_is_stored_as_plain_json(w_dstore, unique_id):
    """not-completed records are json whatever the identifier carried"""
    # nc1.fasta.gz used to become nc1.json.json, because the store suffix
    # was replaced inside the name rather than the name being rebuilt
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="nc1")

    w_dstore.write_not_completed(unique_id=unique_id, data=record.to_json())

    nc_dir = w_dstore.source / NOT_COMPLETED_TABLE
    assert [p.name for p in nc_dir.glob("*")] == ["nc1.json"]


def test_an_identifier_containing_the_suffix_keeps_its_stem(w_dstore):
    """the suffix is replaced at the end of the name, not wherever it occurs"""
    # the store suffix used to be replaced by str.replace over the whole
    # name, so fasta_seqs was stored as json_seqs.json
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="x")

    w_dstore.write_not_completed(unique_id="fasta_seqs", data=record.to_json())

    nc_dir = w_dstore.source / NOT_COMPLETED_TABLE
    assert [p.name for p in nc_dir.glob("*")] == ["fasta_seqs.json"]


def test_a_case_variant_extension_is_a_different_record(w_dstore):
    """FASTA is not the suffix of a .fasta store, so it stays in the stem"""
    # the suffix is matched as written. an extension that is not it, for
    # whatever reason, is part of the name the store appends its own to
    w_dstore.write(unique_id="id_0.fasta", data=">s\nACGT\n")

    w_dstore.write(unique_id="id_0.FASTA", data=">s\nTTTT\n")

    stored = sorted(p.name for p in w_dstore.source.glob("*") if p.is_file())
    assert stored == ["id_0.FASTA.fasta", "id_0.fasta"]


def test_an_identifier_carrying_a_directory_is_stored_by_its_name(w_dstore):
    """a path-like identifier names a record, it does not name a location"""
    # it used to be kept whole and handed to open_, which raised
    # FileNotFoundError for a subdirectory the store had not created
    w_dstore.write(unique_id="sub/id_0.fasta", data=">s\nACGT\n")

    assert [p.name for p in w_dstore.source.glob("*.fasta")] == ["id_0.fasta"]


def test_md5_falls_back_to_the_older_checksum_name(tmp_dir):
    """a store written before the rename still reports its checksums"""
    source = tmp_dir / "legacy"
    (source / MD5_TABLE).mkdir(parents=True)
    data = ">s\nACGT\n"
    (source / "id_0.fasta").write_text(data)
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(data))

    dstore = DataStoreDirectory(source, suffix="fasta", mode=READONLY)

    assert dstore.md5("id_0.fasta") == get_text_hexdigest(data)


def test_dropping_removes_a_shared_checksum_only_it_can_claim(tmp_dir):
    """a drop takes the shared file when no completed record wants it"""
    source = tmp_dir / "sharedgone"
    (source / MD5_TABLE).mkdir(parents=True)
    (source / NOT_COMPLETED_TABLE).mkdir(parents=True)
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="id_0")
    failed = record.to_json()
    (source / NOT_COMPLETED_TABLE / "id_0.json").write_text(failed)
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(failed))
    dstore = DataStoreDirectory(source, suffix="fasta", mode=OVERWRITE)

    dstore.drop_not_completed()

    assert list((source / MD5_TABLE).glob("*")) == []


def test_dropping_keeps_a_shared_checksum_a_completed_record_may_own(tmp_dir):
    """a drop leaves the shared file when a completed record carries the stem"""
    # it holds whichever of the two wrote last, which was never recorded,
    # so taking it would be guessing and leaving it makes it answer for the
    # completed record. neither is right, so it is left and reported
    source = tmp_dir / "sharedkept"
    (source / MD5_TABLE).mkdir(parents=True)
    (source / NOT_COMPLETED_TABLE).mkdir(parents=True)
    done = ">s\nACGT\n"
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="id_0")
    (source / "id_0.fasta").write_text(done)
    (source / NOT_COMPLETED_TABLE / "id_0.json").write_text(record.to_json())
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(done))
    dstore = DataStoreDirectory(source, suffix="fasta", mode=OVERWRITE)

    dstore.drop_not_completed()

    assert (source / MD5_TABLE / "id_0.txt").exists()
    assert dstore.md5("id_0.fasta") == get_text_hexdigest(done)


def test_zipped_md5_falls_back_to_the_older_checksum_name(tmp_dir):
    """the same fallback works inside an archive, which cannot be renamed"""
    source = tmp_dir / "ziplegacy"
    (source / MD5_TABLE).mkdir(parents=True)
    data = ">s\nACGT\n"
    (source / "id_0.fasta").write_text(data)
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(data))
    path = shutil.make_archive(
        base_name=str(source.parent / source.name),
        format="zip",
        base_dir=source.name,
        root_dir=source.parent,
    )

    dstore = ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")

    assert dstore.md5("id_0.fasta") == get_text_hexdigest(data)


@pytest.fixture
def legacy_md5_dstore(tmp_dir):
    """a store whose checksums are all under the name both kinds shared"""
    source = tmp_dir / "legacy_store"
    (source / MD5_TABLE).mkdir(parents=True)
    (source / NOT_COMPLETED_TABLE).mkdir(parents=True)
    md5_dir = source / MD5_TABLE

    # only a completed record carries this stem
    done = ">s\nACGT\n"
    (source / "solo_done.fasta").write_text(done)
    (md5_dir / "solo_done.txt").write_text(get_text_hexdigest(done))

    # only a not-completed record carries this one
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="x")
    failed = record.to_json()
    (source / NOT_COMPLETED_TABLE / "solo_failed.json").write_text(failed)
    (md5_dir / "solo_failed.txt").write_text(get_text_hexdigest(failed))

    # both kinds carry this one, so the file cannot be attributed
    (source / "both.fasta").write_text(done)
    (source / NOT_COMPLETED_TABLE / "both.json").write_text(failed)
    (md5_dir / "both.txt").write_text(get_text_hexdigest(done))

    # no record carries this one at all
    (md5_dir / "gone.txt").write_text("orphaned")

    return source


def test_migrate_checksums(legacy_md5_dstore):
    """the ones that can be attributed are renamed and the rest reported"""
    dstore = DataStoreDirectory(legacy_md5_dstore, suffix="fasta", mode=OVERWRITE)

    got = dstore.migrate_checksums()

    assert got == {
        "migrated": 2,
        "ambiguous": ["both"],
        "orphaned": ["gone"],
        "superseded": [],
    }
    md5_dir = legacy_md5_dstore / MD5_TABLE
    assert (md5_dir / f"solo_done.{COMPLETED_CHECKSUM}").exists()
    assert (md5_dir / f"solo_failed.{NOT_COMPLETED_CHECKSUM}").exists()
    assert sorted(p.name for p in md5_dir.glob("*.txt")) == ["both.txt", "gone.txt"]


def test_migrate_checksums_is_idempotent(legacy_md5_dstore):
    """running it again finds only what it could not attribute"""
    dstore = DataStoreDirectory(legacy_md5_dstore, suffix="fasta", mode=OVERWRITE)
    first = dstore.migrate_checksums()

    got = dstore.migrate_checksums()

    assert got["migrated"] == 0
    # it has not forgotten what it could not do
    assert got["ambiguous"] == first["ambiguous"]
    assert got["orphaned"] == first["orphaned"]


def test_migrate_checksums_reads_past_the_limit(legacy_md5_dstore):
    """a limited view does not make a record look absent"""
    # limit truncates the member lists, and a stem seen in neither would
    # then be attributed to whichever kind was in view, or called orphaned
    dstore = DataStoreDirectory(
        legacy_md5_dstore,
        suffix="fasta",
        mode=OVERWRITE,
        limit=1,
    )

    got = dstore.migrate_checksums()

    assert got["ambiguous"] == ["both"]
    assert got["orphaned"] == ["gone"]
    assert got["migrated"] == 2


def test_migrate_checksums_keeps_a_checksum_already_under_the_new_name(tmp_dir):
    """a record whose checksum this version wrote keeps that one"""
    # the shared file may belong to a record since dropped, while the one
    # under the current name was written for this record by this version
    source = tmp_dir / "superseded"
    (source / MD5_TABLE).mkdir(parents=True)
    dstore = DataStoreDirectory(source, suffix="fasta", mode=OVERWRITE)
    data = ">s\nACGT\n"
    dstore.write(unique_id="rec.fasta", data=data)
    (source / MD5_TABLE / "rec.txt").write_text("stale")

    got = dstore.migrate_checksums()

    assert got["superseded"] == ["rec"]
    assert got["migrated"] == 0
    assert dstore.md5("rec.fasta") == get_text_hexdigest(data)


def test_migrate_checksums_keeps_the_checksums_readable(legacy_md5_dstore):
    """a migrated record reports the same checksum it did before"""
    dstore = DataStoreDirectory(legacy_md5_dstore, suffix="fasta", mode=OVERWRITE)
    before = dstore.md5("solo_done.fasta")

    dstore.migrate_checksums()

    assert dstore.md5("solo_done.fasta") == before


@pytest.mark.parametrize("mode", [READONLY, APPEND])
def test_migrate_checksums_needs_write_mode(legacy_md5_dstore, mode):
    """migrating rewrites records already there, so it takes mode w"""
    # read only cannot rewrite anything, and append undertakes not to touch
    # what is already in the store, which is exactly what this does
    dstore = DataStoreDirectory(legacy_md5_dstore, suffix="fasta", mode=mode)

    with pytest.raises(OSError, match='mode="w"'):
        dstore.migrate_checksums()

    assert (legacy_md5_dstore / MD5_TABLE / "solo_done.txt").exists()


def test_validate_counts_checksums_in_the_older_layout(tmp_dir):
    """a store says how many of its checksums are still unattributed"""
    # so a caller learns there is migrating to do without having to run it
    source = tmp_dir / "counted"
    (source / MD5_TABLE).mkdir(parents=True)
    data = ">s\nACGT\n"
    (source / "id_0.fasta").write_text(data)
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(data))
    dstore = DataStoreDirectory(source, suffix="fasta", mode=READONLY)

    assert dstore._validate()["md5_legacy"] == 1


def test_validate_counts_no_legacy_checksums_in_a_new_store(w_dstore):
    """a store written since the rename has none of them"""
    w_dstore.write(unique_id="id_0.fasta", data=">s\nACGT\n")

    assert w_dstore._validate()["md5_legacy"] == 0


def test_validate_counts_legacy_checksums_in_an_archive(tmp_dir):
    """an archive reports them too, though it can never migrate them"""
    source = tmp_dir / "zipcounted"
    (source / MD5_TABLE).mkdir(parents=True)
    data = ">s\nACGT\n"
    (source / "id_0.fasta").write_text(data)
    (source / MD5_TABLE / "id_0.txt").write_text(get_text_hexdigest(data))
    path = shutil.make_archive(
        base_name=str(source.parent / source.name),
        format="zip",
        base_dir=source.name,
        root_dir=source.parent,
    )

    dstore = ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")

    assert dstore._validate()["md5_legacy"] == 1


def test_close_a_directory_store(w_dstore):
    """a directory store can be closed, and holds nothing back afterwards"""
    # it exists so a caller can close whatever open_data_store returned
    # without asking which backend it got. a directory store holds no
    # connection and no lock, so there is nothing for closing to end
    w_dstore.write(unique_id="c1.fasta", data=">s\nACGT\n")

    w_dstore.close()
    w_dstore.close()

    assert w_dstore.read("c1.fasta") == ">s\nACGT\n"
    assert [m.unique_id for m in w_dstore.completed] == ["c1.fasta"]


def test_close_a_zipped_store(zipped_basic):
    """a read only zip store can be closed too"""
    dstore = ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta")

    dstore.close()

    assert len(dstore.completed) > 0


def test_write_read_only_datastore(ro_dstore):
    with pytest.raises(IOError):
        ro_dstore.write(unique_id="brca1.fasta", data="test data")


def test_write(fasta_dir, w_dstore):
    """correctly write content"""
    expect = Path(fasta_dir / "brca1.fasta").read_text()
    identifier = "brca1.fasta"
    w_dstore.write(unique_id=identifier, data=expect)
    got = w_dstore.read(identifier)
    assert got == expect


def test_multi_write(fasta_dir, w_dstore):
    """correctly write multiple files to data store"""
    expect_a = Path(fasta_dir / "brca1.fasta").read_text()
    expect_b = Path(fasta_dir / "primates_brca1.fasta").read_text()
    identifier_a = "brca2.fasta"
    identifier_b = "primates_brca2.fasta"
    w_dstore.write(unique_id=identifier_a, data=expect_a)
    w_dstore.write(unique_id=identifier_b, data=expect_b)
    got_a = w_dstore.read(identifier_a)
    got_b = w_dstore.read(identifier_b)
    assert got_a == expect_a
    assert got_b == expect_b


def test_append(w_dstore):
    """correctly write content"""
    identifier = "test1.fasta"
    data = "test data"
    w_dstore.write(unique_id=identifier, data=data)
    got = w_dstore.read(identifier)
    assert got == data


class _CountingMembersStore(DataStoreDirectory):
    """counts how often the member list gets built"""

    members_built = 0

    @property
    def members(self):
        self.members_built += 1
        return super().members


@pytest.mark.parametrize("summary", [lambda d: d.validate(), str])
def test_summaries_build_the_member_list_once(write_dir, summary):
    """a summary works from one member list rather than rebuilding it"""
    # each build is a fresh concatenation of the two cached halves, so two
    # of them taken either side of a write disagree about what is held
    dstore = _CountingMembersStore(write_dir, suffix="fasta", mode=OVERWRITE)
    for i in range(3):
        dstore.write(unique_id=f"c{i}.fasta", data=f">s{i}\nACGT\n")
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="nc1")
    dstore.write_not_completed(unique_id="nc1", data=record.to_json())

    dstore.members_built = 0
    summary(dstore)

    assert dstore.members_built == 1


def test_no_not_completed_subdir(nc_dstore):
    expect = f"{len(nc_dstore.completed) + len(nc_dstore.not_completed)}x member"
    assert str(nc_dstore).startswith(expect)
    nc_dstore.drop_not_completed()
    not_dir = nc_dstore.source / NOT_COMPLETED_TABLE
    assert list(not_dir.glob("*.json")) == []
    expect = f"{len(nc_dstore.completed)}x member"
    assert str(nc_dstore).startswith(expect)
    expect = f"{len(nc_dstore)}x member"
    assert str(nc_dstore).startswith(expect)
    assert len(nc_dstore) == len(nc_dstore.completed)


def test_limit_datastore(nc_dstore):
    assert len(nc_dstore) == len(nc_dstore.completed) + len(nc_dstore.not_completed)
    nc_dstore._limit = len(nc_dstore.completed) // 2
    nc_dstore._completed = []
    nc_dstore._not_completed = []
    assert len(nc_dstore.completed) == len(nc_dstore.not_completed) == nc_dstore.limit
    assert len(nc_dstore) == len(nc_dstore.completed) + len(nc_dstore.not_completed)
    nc_dstore.drop_not_completed()
    assert len(nc_dstore) == len(nc_dstore.completed)
    assert len(nc_dstore.not_completed) == 0
    nc_dstore._limit = len(nc_dstore.completed) // 2
    nc_dstore._completed = []
    nc_dstore._not_completed = []
    assert len(nc_dstore) == len(nc_dstore.completed) == nc_dstore.limit
    assert len(nc_dstore.not_completed) == 0


def test_md5_sum(nc_dstore):
    for m in nc_dstore.members:
        data = m.read()
        md5 = nc_dstore.md5(m.unique_id)
        assert md5 == get_text_hexdigest(data)


def test_md5_none(fasta_dir):
    dstore = DataStoreDirectory(fasta_dir, suffix="fasta")
    for m in dstore.members:
        assert m.md5 is None


def test_md5_missing(nc_dstore):
    assert nc_dstore.md5("unknown") is None


def test_write_if_member_exists(full_dstore, write_dir):
    """correctly write content"""
    expect = Path(write_dir / "brca1.fasta").read_text()
    identifier = "brca1.fasta"
    len_dstore = len(full_dstore)
    full_dstore.write(unique_id=identifier, data=expect)
    assert len_dstore == len(full_dstore)
    got = full_dstore.read(identifier)
    assert got == expect
    full_dstore._mode = OVERWRITE
    full_dstore.write(unique_id=identifier, data=expect)
    assert len_dstore == len(full_dstore)
    got = full_dstore.read(identifier)
    assert got == expect


def test_write_success_replaces_not_completed(full_dstore):
    """correctly write content"""
    nc = full_dstore.not_completed[0].unique_id
    data = full_dstore.completed[0].read()
    new_id = Path(nc.replace(".json", f".{full_dstore.suffix}")).name
    num = len(full_dstore)
    full_dstore.write(unique_id=new_id, data=data)
    assert len(full_dstore) == num


@pytest.mark.parametrize("klass", [str, Path])
def test_get_data_source_attr(klass):
    """handles case where input has source attribute string object or pathlib object"""

    class dummy:
        source = None

    obj = dummy()
    value = klass("some/path.txt")
    obj.source = value
    got = get_data_source(obj)
    assert got == "path.txt"


@pytest.mark.parametrize(
    "name",
    ["path/name.txt", "path/name.gz", "path/name.fasta.gz", "name.fasta.gz"],
)
def test_get_unique_id(name):
    got = get_unique_id(name)
    assert got == "name"


def test_get_unique_id_none():
    got = get_unique_id(None)
    assert got is None


def test_set_id_from_source_returns_default_initially(
    reset_id_from_source: None,
) -> None:
    """Default extractor is `get_unique_id` when nothing is registered."""
    assert get_id_from_source() is get_unique_id


def test_set_id_from_source_registers_and_clears(
    reset_id_from_source: None,
) -> None:
    """A registered function replaces the default; None restores it."""

    def my_extractor(obj: object) -> str | None:
        return f"custom-{obj}"

    set_id_from_source(my_extractor)
    assert get_id_from_source() is my_extractor
    assert get_id_from_source()("foo") == "custom-foo"

    set_id_from_source(None)
    assert get_id_from_source() is get_unique_id


@pytest.mark.parametrize("data", [{}, set(), {"info": {}}])
def test_get_data_source_none(data):
    assert get_data_source(data) is None


def test_load_record_from_json():
    """handle different types of input"""
    orig = {"data": "blah", "identifier": "some.json", "completed": True}
    data = orig.copy()
    data2 = data.copy()
    data2["data"] = json.dumps(data)
    for d in (data, json.dumps(data), data2):
        expected = "blah" if d != data2 else json.loads(data2["data"])
        id_, data_, compl = load_record_from_json(d)
        assert id_ == "some.json"
        assert data_ == expected
        assert compl is True


def test_zipped_ro_fail(zipped_basic):
    with pytest.raises(ValueError):
        ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta", mode="w")


def test_zipped_ro_ioerror():
    with pytest.raises(IOError):
        ReadOnlyDataStoreZipped("blah-1234.zip", suffix="fasta")


def test_zipped_ro_basic(zipped_basic, ro_dstore):
    dstore = ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta")
    assert len(dstore.completed) == len(ro_dstore.completed)
    assert len(dstore) == len(ro_dstore)
    expect = _get_member_data(ro_dstore.completed)
    got = _get_member_data(dstore.completed)
    assert expect == got
    expect = _get_member_data(ro_dstore.not_completed)
    got = _get_member_data(dstore.not_completed)
    assert expect == got


def test_zipped_ro_basic_hidden(zipped_hidden, zipped_basic):
    orig = ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta")
    dstore = ReadOnlyDataStoreZipped(zipped_hidden, suffix="fasta")
    assert len(dstore) == len(orig)
    assert all(not m.unique_id.startswith(".") for m in dstore)


def test_zipped_ro_full(zipped_full, full_dstore):
    got_ids = {m.unique_id for m in zipped_full.completed}
    expect_ids = {m.unique_id for m in full_dstore.completed}
    assert got_ids == expect_ids

    got_ids = {m.unique_id for m in zipped_full.not_completed}
    expect_ids = {m.unique_id for m in full_dstore.not_completed}
    assert got_ids == expect_ids
    assert len(zipped_full) == len(full_dstore)

    expect = _get_member_data(full_dstore.completed)
    got = _get_member_data(zipped_full.completed)
    assert expect == got
    expect = _get_member_data(full_dstore.not_completed)
    got = _get_member_data(zipped_full.not_completed)
    assert expect == got


def test_zipped_logs(zipped_full, full_dstore):
    assert len(zipped_full.logs) == len(full_dstore.logs)
    expect = _get_member_data(full_dstore.logs)
    got = _get_member_data(zipped_full.logs)
    assert expect == got


def test_zipped_md5(zipped_full, full_dstore):
    expect = {m.unique_id: full_dstore.md5(m.unique_id) for m in full_dstore.completed}
    got = {m.unique_id: zipped_full.md5(m.unique_id) for m in zipped_full.completed}
    assert got == expect


def test_write_citations_directory(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    path = write_dir / CITATIONS_FILE
    assert path.exists()
    loaded = dstore._load_citations()
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"
    assert loaded[1].title == "Tool Two"


def test_write_citations_empty_directory(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=())
    path = write_dir / CITATIONS_FILE
    assert not path.exists()


def test_write_bib_directory(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    bib_path = write_dir / "refs.bib"
    dstore.write_bib(bib_path)
    assert bib_path.exists()
    content = bib_path.read_text()
    assert "Tool One" in content
    assert "Tool Two" in content


def test_write_bib_no_citations(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    bib_path = write_dir / "refs.bib"
    with pytest.warns(UserWarning, match="No citations stored"):
        dstore.write_bib(bib_path)
    assert not bib_path.exists()


def test_load_citations_no_file(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    assert dstore._load_citations() == []


def test_load_citations_zipped(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    source = pathlib.Path(dstore.source)
    path = shutil.make_archive(
        base_name=str(source.parent / source.name),
        format="zip",
        base_dir=source.name,
        root_dir=source.parent,
    )
    zipped = ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")
    loaded = zipped._load_citations()
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"


def test_citations_file_not_in_completed(write_dir, sample_citations):
    """The bibliography.citations file must not appear in the completed members list."""
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write(unique_id="sample.fasta", data=">s1\nACGT\n")
    dstore.write_citations(data=sample_citations)
    assert (write_dir / CITATIONS_FILE).exists()
    dstore._completed = []
    member_ids = {m.unique_id for m in dstore.completed}
    assert CITATIONS_FILE not in member_ids
    assert "sample.fasta" in member_ids


@pytest.fixture
def _restore_display():
    """Ensure the global display function is reset after each test."""
    yield
    set_summary_display(None)


def test_summary_display_default_is_none(_restore_display):
    assert get_summary_display() is None


def test_summary_display_set_and_get(_restore_display):
    def my_display(data, *, name=""):
        return data

    set_summary_display(my_display)
    assert get_summary_display() is my_display


def test_summary_display_set_none_clears(_restore_display):
    set_summary_display(lambda data, **kw: data)
    set_summary_display(None)
    assert get_summary_display() is None


def test_describe_without_display(ro_dstore, _restore_display):
    result = ro_dstore.describe
    assert isinstance(result, dict)
    assert "completed" in result


def test_describe_with_display(ro_dstore, _restore_display):
    captured = {}

    def display(data, *, name=""):
        captured["data"] = data
        captured["name"] = name
        return f"DISPLAY:{name}"

    set_summary_display(display)
    result = ro_dstore.describe
    assert result == "DISPLAY:describe"
    assert isinstance(captured["data"], dict)
    assert "completed" in captured["data"]
    assert captured["name"] == "describe"


def test_summary_logs_with_display(full_dstore, _restore_display):
    captured = {}

    def display(data, *, name=""):
        captured["data"] = data
        captured["name"] = name
        return "transformed"

    set_summary_display(display)
    result = full_dstore.summary_logs
    assert result == "transformed"
    assert captured["name"] == "summary_logs"
    assert isinstance(captured["data"], list)


def test_validate_with_display(ro_dstore, _restore_display):
    captured = {}

    def display(data, *, name=""):
        captured["name"] = name
        return "validated"

    set_summary_display(display)
    result = ro_dstore.validate()
    assert result == "validated"
    assert captured["name"] == "validate"


def test_protected_methods_bypass_display(ro_dstore, _restore_display):
    set_summary_display(lambda data, **kw: "SHOULD_NOT_SEE")
    assert isinstance(ro_dstore._describe(), dict)
    assert isinstance(ro_dstore._summary_logs(), list)
    assert isinstance(ro_dstore._summary_not_completed(), list)
    assert isinstance(ro_dstore._validate(), dict)


def test_summary_citations_with_display(write_dir, sample_citations, _restore_display):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    captured = {}

    def display(data, *, name=""):
        captured["name"] = name
        captured["data"] = data
        return "citations_display"

    set_summary_display(display)
    result = dstore.summary_citations
    assert result == "citations_display"
    assert captured["name"] == "summary_citations"
    assert isinstance(captured["data"], list)


def test_validate_incorrect_md5(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    dstore.write(unique_id="item.txt", data="original")
    # corrupt the md5
    md5_path = write_dir / MD5_TABLE / f"item.{COMPLETED_CHECKSUM}"
    md5_path.write_text("wrong_md5_value")
    result = dstore._validate()
    assert result["md5_incorrect"] == 1


def test_readonly_nonexistent_dir(tmp_path):
    with pytest.raises(OSError, match="does not exist"):
        DataStoreDirectory(tmp_path / "nonexistent", suffix="txt", mode=READONLY)


def test_not_completed_with_limit(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    for i in range(5):
        nc = NotCompleted(NotCompletedType.ERROR, "test", f"msg {i}", source=f"src_{i}")
        dstore.write_not_completed(unique_id=f"nc_{i}.json", data=nc.to_json())
    limited = DataStoreDirectory(write_dir, suffix="txt", mode=READONLY, limit=2)
    assert len(limited.not_completed) == 2


def test_summary_not_completeds(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    for i in range(3):
        nc = NotCompleted(
            NotCompletedType.ERROR, "myapp", f"error msg {i}", source=f"s{i}"
        )
        dstore.write_not_completed(unique_id=f"nc_{i}.json", data=nc.to_json())
    rows = summary_not_completeds(dstore.not_completed)
    assert len(rows) >= 1
    assert rows[0]["origin"] == "myapp"
    assert rows[0]["num"] == 3


def test_make_record_for_json():
    result = make_record_for_json("id1", {"key": "value"}, True)
    assert result["identifier"] == "id1"
    assert result["completed"] is True
    assert isinstance(result["data"], str)
    parsed = json.loads(result["data"])
    assert parsed == {"key": "value"}


def test_make_record_for_json_with_rich_dict():
    class FakeObj:
        def to_rich_dict(self):
            return {"type": "fake", "data": 42}

    result = make_record_for_json("id2", FakeObj(), True)
    parsed = json.loads(result["data"])
    assert parsed == {"type": "fake", "data": 42}


def test_zipped_readonly_write_methods(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("data")
    zpath = shutil.make_archive(str(src), "zip", root_dir=src.parent, base_dir=src.name)
    zstore = ReadOnlyDataStoreZipped(zpath, suffix="txt")

    with pytest.raises(TypeError):
        zstore.write(unique_id="x", data="d")
    with pytest.raises(TypeError):
        zstore.write_not_completed(unique_id="x", data="d")
    with pytest.raises(TypeError):
        zstore.write_log(unique_id="x", data="d")
    with pytest.raises(TypeError):
        zstore.write_citations(data=())
    with pytest.raises(TypeError):
        zstore.drop_not_completed()


def test_zipped_md5_returns_none(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("data")
    zpath = shutil.make_archive(str(src), "zip", root_dir=src.parent, base_dir=src.name)
    zstore = ReadOnlyDataStoreZipped(zpath, suffix="txt")
    assert zstore.md5("a.txt") is None


def test_zipped_load_citations_missing(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("data")
    zpath = shutil.make_archive(str(src), "zip", root_dir=src.parent, base_dir=src.name)
    zstore = ReadOnlyDataStoreZipped(zpath, suffix="txt")
    assert zstore._load_citations() == []


def test_zipped_completed_with_limit(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    for i in range(5):
        (src / f"f_{i}.txt").write_text(f"data {i}")
    zpath = shutil.make_archive(str(src), "zip", root_dir=src.parent, base_dir=src.name)
    zstore = ReadOnlyDataStoreZipped(zpath, suffix="txt", limit=2)
    assert len(zstore.completed) == 2


def test_summary_logs_continuation_line(write_dir):
    from scitrack import CachingLogger

    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    logger = CachingLogger(create_dir=True)
    log_path = write_dir / "test.log"
    logger.log_file_path = str(log_path)
    logger.log_message("a long message\nthat continues", label="multi")
    logger.shutdown()
    dstore.write_log(unique_id="test.log", data=log_path.read_text())
    rows = dstore._summary_logs()
    assert len(rows) == 1


def test_data_member_str(ro_dstore):
    member = ro_dstore[0]
    assert str(member) == member.unique_id


def test_data_member_repr(ro_dstore):
    member = ro_dstore[0]
    r = repr(member)
    assert "DataMember" in r
    assert member.unique_id in r


def test_datastore_repr(ro_dstore):
    r = repr(ro_dstore)
    assert "DataStoreDirectory" in r
    assert "source=" in r


def test_contains_non_string(ro_dstore):
    assert 42 not in ro_dstore


def test_write_not_completed_readonly(ro_dstore):
    with pytest.raises(OSError, match="readonly"):
        ro_dstore.write_not_completed(unique_id="x", data="d")


def test_write_log_readonly(ro_dstore):
    with pytest.raises(OSError, match="readonly"):
        ro_dstore.write_log(unique_id="x", data="d")


def test_summary_logs_malformed_continuation():
    from unittest.mock import MagicMock

    log_text = "2024-01-01\t00:00:00\n\tcontinuation without key"
    member = MagicMock()
    member.read.return_value = log_text
    member.unique_id = "bad.log"

    class FakeDS(DataStoreDirectory):
        @property
        def logs(self):
            return [member]

    fake = FakeDS.__new__(FakeDS)
    fake._completed = []
    fake._not_completed = []
    fake._init_vals = {}
    with pytest.raises(ValueError, match="malformed log data"):
        fake._summary_logs()


def test_tidy_and_check_suffix_empty():
    from scinexus.data_store import _tidy_and_check_suffix

    with pytest.raises(ValueError, match="suffix is required"):
        _tidy_and_check_suffix(None)

    with pytest.raises(ValueError, match="suffix is required"):
        _tidy_and_check_suffix("*")

    with pytest.raises(ValueError, match="suffix is required"):
        _tidy_and_check_suffix(".**")


def test_summary_not_completeds_with_bytes():
    from unittest.mock import MagicMock

    members = []
    for i in range(3):
        m = MagicMock()
        m.read.return_value = b"binary data"
        m.unique_id = f"item_{i}"
        members.append(m)
    result = summary_not_completeds(members)
    assert result == []


def test_summary_not_completeds_with_deserialise(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    from scinexus.composable import NotCompleted, NotCompletedType

    for i in range(3):
        nc = NotCompleted(
            NotCompletedType.ERROR, "myapp", f"error msg {i}", source=f"s{i}"
        )
        dstore.write_not_completed(unique_id=f"deser_{i}.json", data=nc.to_json())
    rows = summary_not_completeds(dstore.not_completed, deserialise=lambda x: x)
    assert len(rows) >= 1


def test_summary_not_completeds_long_sources(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="txt", mode=OVERWRITE)
    from scinexus.composable import NotCompleted, NotCompletedType

    for i in range(10):
        long_source = f"very_long_source_name_for_item_{i}_padding"
        nc = NotCompleted(
            NotCompletedType.ERROR,
            "myapp",
            f"error msg {i}",
            source=long_source,
        )
        dstore.write_not_completed(unique_id=f"long_{i}.json", data=nc.to_json())
    rows = summary_not_completeds(dstore.not_completed)
    assert len(rows) >= 1
    for row in rows:
        if len(row["source"]) > 45:
            assert row["source"].endswith("...")


def test_get_data_source_data_member(ro_dstore):
    member = ro_dstore[0]
    result = get_data_source(member)
    assert result == member.unique_id


def test_zipped_not_completed_with_limit(tmp_path):
    from scinexus.composable import NotCompleted, NotCompletedType

    src = tmp_path / "src"
    src.mkdir()
    nc_dir = src / "not_completed"
    nc_dir.mkdir()
    (src / "a.txt").write_text("data")
    for i in range(5):
        nc = NotCompleted(NotCompletedType.ERROR, "test", f"msg {i}", source=f"s{i}")
        (nc_dir / f"nc_{i}.json").write_text(nc.to_json())
    zpath = shutil.make_archive(str(src), "zip", root_dir=src.parent, base_dir=src.name)
    zstore = ReadOnlyDataStoreZipped(zpath, suffix="txt", limit=2)
    assert len(zstore.not_completed) == 2


def test_zipped_mode_property(zipped_basic):
    zstore = ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta")
    assert zstore.mode is READONLY


def _make_minimal_ds(mode=OVERWRITE):
    from scinexus.data_store import DataStoreABC

    class MinimalDS(DataStoreABC):
        @property
        def source(self):
            return "test"

        @property
        def mode(self):
            return mode

        @property
        def limit(self):
            return None

        def read(self, unique_id):
            return ""

        def write(self, *, unique_id, data):
            super().write(unique_id=unique_id, data=data)

        def write_not_completed(self, *, unique_id, data):
            super().write_not_completed(unique_id=unique_id, data=data)

        def write_log(self, *, unique_id, data):
            super().write_log(unique_id=unique_id, data=data)

        @property
        def logs(self):
            return []

        @property
        def completed(self):
            return []

        @property
        def not_completed(self):
            return []

        def drop_not_completed(self, *, unique_id=None):
            pass

        def md5(self, unique_id):
            return None

    return MinimalDS()


def test_base_write_citations_warns():
    ds = _make_minimal_ds()
    with pytest.warns(UserWarning, match="does not support saving citations"):
        ds.write_citations(data=(object(),))


def test_base_summary_citations_warns():
    ds = _make_minimal_ds()
    with pytest.warns(UserWarning, match="does not support saving citations"):
        result = ds._summary_citations()
    assert result == []


def test_base_load_citations_returns_empty():
    ds = _make_minimal_ds()
    assert ds._load_citations() == []


def test_base_write_not_completed_readonly():
    ds = _make_minimal_ds(mode=READONLY)
    with pytest.raises(OSError, match="readonly"):
        ds.write_not_completed(unique_id="x", data="d")


def test_base_write_log_readonly():
    ds = _make_minimal_ds(mode=READONLY)
    with pytest.raises(OSError, match="readonly"):
        ds.write_log(unique_id="x", data="d")


def test_base_write_citations_empty_data():
    ds = _make_minimal_ds()
    ds.write_citations(data=())


@pytest.mark.mpi
def test_source_check_create_not_master(tmp_path):
    from unittest.mock import patch

    from scinexus import data_store as ds_mod

    target = tmp_path / "should_not_exist"
    with patch.object(ds_mod, "is_master_process", return_value=False):
        dstore = DataStoreDirectory(target, suffix="txt", mode=OVERWRITE)
    assert not target.exists()
    assert dstore.source == target


def test_write_read_not_completed(nc_dstore):
    nc_dstore.drop_not_completed()
    assert len(nc_dstore.not_completed) == 0
    nc = NotCompleted("ERROR", "test", "for tracing", source="blah")
    writer = c3.get_app("write_seqs", data_store=nc_dstore)
    writer.main(nc, identifier="blah")
    assert len(nc_dstore.not_completed) == 1
    got = nc_dstore.not_completed[0].read()
    assert nc.to_json() == got


def test_summary_logs_missing_field(nc_dstore):
    log_path = Path(nc_dstore.source) / nc_dstore.logs[0].unique_id
    data = [
        l for l in log_path.read_text().splitlines() if "composable function" not in l
    ]
    log_path.write_text("\n".join(data))
    # doesn't fail because of a missing field in the log data
    assert isinstance(nc_dstore.summary_logs, list)


@pytest.fixture
def app_dstore_in(tmp_path):
    pytest.importorskip("cogent3")
    in_path = tmp_path / "in_data"
    in_path.mkdir(parents=True)
    fasta_content = ">seq\nACGT"
    with open(in_path / "one.fa", "w") as file:
        file.write(fasta_content)

    dstore_in = open_data_store(in_path, suffix=".fa", mode="r")
    dstore_out = open_data_store(tmp_path / "data_out", suffix="fa", mode="w")
    loader = c3.get_app("load_unaligned")
    writer = c3.get_app("write_seqs", dstore_out)

    pipe = loader + writer
    return pipe, dstore_in


def test_write_multiple_times_apply_to(app_dstore_in):
    app, dstore_in = app_dstore_in
    app.apply_to(dstore_in)
    orig_length = len(app.data_store)
    app.apply_to(dstore_in)
    assert len(app.data_store) == orig_length


def test_directory_data_store_write_compressed(tmp_path):
    out = open_data_store(base_path=tmp_path / "demo", suffix="fa.gz", mode="w")
    writer = c3.get_app("write_seqs", data_store=out)
    seqs = c3.make_aligned_seqs(
        {"s1": "CG--T", "s2": "CGTTT"},
        moltype="dna",
        info={"source": "test"},
    )
    got = writer(seqs)  # pylint: disable=not-callable
    assert got, got


def test_apply_to_not_completed(nc_dstore, tmp_path):
    loader = c3.get_app("load_unaligned")
    num_seqs = c3.get_app("take_n_seqs", number=3, fixed_choice=False)
    out_dstore = open_data_store(tmp_path / "output", suffix="fa", mode="w")
    writer = c3.get_app("write_seqs", data_store=out_dstore, format_name="fasta")
    app = loader + num_seqs + writer
    fini = app.apply_to(nc_dstore)
    assert 0 < len(fini.completed) <= len(nc_dstore.completed)


def test_summary_citations_directory(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    cited = dstore.summary_citations
    assert isinstance(cited, list)
    assert len(cited) == 2
    assert "app" in cited[0]
    assert "citation" in cited[0]


def test_write_bib_tilde_path(write_dir, sample_citations, HOME_TMP_DIR):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    bib_path = f"~/{HOME_TMP_DIR.name}/refs.bib"
    dstore.write_bib(bib_path)
    expected = pathlib.Path(bib_path).expanduser()
    assert expected.exists()
    content = expected.read_text()
    assert "Tool One" in content
    assert "Tool Two" in content


def test_summary_citations_zipped(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    source = pathlib.Path(dstore.source)
    path = shutil.make_archive(
        base_name=str(source),
        format="zip",
        base_dir=source.name,
        root_dir=source.parent,
    )
    zipped = ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")
    cited = zipped.summary_citations
    assert isinstance(cited, list)
    assert len(cited) == 2


def test_write_citations_zipped_raises(zipped_basic):
    zipped = ReadOnlyDataStoreZipped(zipped_basic, suffix="fasta")
    with pytest.raises(TypeError, match="read only"):
        zipped.write_citations(data=(None,))


def test_old_directory_store_without_citations(fasta_dir):
    """Opening a directory store created before citations were added works."""
    # fasta_dir has .fasta files but no .citations file
    dstore = DataStoreDirectory(fasta_dir, suffix="fasta", mode=READONLY)
    assert dstore._load_citations() == []
    cited = dstore.summary_citations
    assert isinstance(cited, list)
    assert len(cited) == 0


_dict_types = [dict]
if UnionDict is not None:
    _dict_types.append(UnionDict)
_types = tuple(product(_dict_types, (str, Path)))


@pytest.mark.parametrize(("container_type", "source_stype"), _types)
def test_get_data_source_dict(container_type, source_stype):
    """handles case where input is dict (sub)class instance with top level source key"""
    value = source_stype("some/path.txt")
    data = container_type(source=value)
    got = get_data_source(data)
    assert got == "path.txt"


@pytest.mark.parametrize("klass", [str, Path])
def test_get_data_source_seqcoll(klass):
    """handles case where input is sequence collection object"""
    from cogent3 import make_unaligned_seqs

    value = klass("some/path.txt")
    obj = make_unaligned_seqs(
        {"seq1": "ACGG"},
        moltype="dna",
        info={"random_key": 1234},
        source=value,
    )
    got = get_data_source(obj)
    assert got == "path.txt"


_CONCURRENT_MEMBERS = 300
_CONCURRENT_READERS = 8
_CONCURRENT_ROUNDS = 20
_WRITE_ROUNDS = 40
_MOVED_RECORDS = 40
# generous: it only has to exceed a scan, and a hang here should fail not stall
_SYNC_TIMEOUT = 30


@pytest.fixture
def many_member_dir(tmp_dir):
    """a store directory holding enough members to widen the scan window"""
    source = Path(tmp_dir) / "many_members"
    (source / NOT_COMPLETED_TABLE).mkdir(parents=True, exist_ok=True)
    for i in range(_CONCURRENT_MEMBERS):
        (source / f"id_{i}.fasta").write_text(f">seq_{i}\nACGT\n")
        nc = NotCompleted(
            NotCompletedType.ERROR, "location", "message", source=f"id_{i}"
        )
        (source / NOT_COMPLETED_TABLE / f"id_{i}.json").write_text(nc.to_json())
    yield source
    shutil.rmtree(source, ignore_errors=True)


@pytest.fixture(params=["directory", "zipped"])
def many_member_store(request, many_member_dir):
    """the same members as a directory store and as a zipped store"""
    if request.param == "directory":
        return DataStoreDirectory(many_member_dir, suffix="fasta", mode=READONLY)

    path = shutil.make_archive(
        base_name=str(many_member_dir.parent / many_member_dir.name),
        format="zip",
        base_dir=many_member_dir.name,
        root_dir=many_member_dir.parent,
    )
    return ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")


def _count_members(dstore, attr, barrier, _):
    barrier.wait(timeout=_SYNC_TIMEOUT)
    return len(getattr(dstore, attr))


@pytest.mark.parametrize("attr", ["completed", "not_completed"])
def test_member_list_is_whole_for_concurrent_readers(many_member_store, attr):
    """concurrent readers each see every member, never a partial scan

    The scan must not publish its backing list until the list is complete.
    Filling the list in place instead makes it truthy from the first append
    onwards, so a second thread arriving mid-scan fails the emptiness guard,
    takes the early return, and is handed the list while it is still being
    filled. It then sees a short count and nothing raises.
    """
    private = f"_{attr}"
    observed = set()
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(_CONCURRENT_ROUNDS):
            setattr(many_member_store, private, [])
            barrier = threading.Barrier(_CONCURRENT_READERS, timeout=_SYNC_TIMEOUT)
            read = functools.partial(_count_members, many_member_store, attr, barrier)
            with ThreadPoolExecutor(max_workers=_CONCURRENT_READERS) as executor:
                observed.update(executor.map(read, range(_CONCURRENT_READERS)))
    finally:
        sys.setswitchinterval(old_interval)

    assert observed == {_CONCURRENT_MEMBERS}


class _PausingWriteStore(DataStoreDirectory):
    """holds a writer between the file landing on disk and the cache append

    ``_write`` puts the file on disk and ``write`` records the member in the
    cached list afterwards. Pausing in that gap makes the window a test can
    drive deterministically rather than one it has to race for.
    """

    def _write(self, **kwargs):
        member = super()._write(**kwargs)
        self.file_on_disk.set()
        self.reader_published.wait(timeout=_SYNC_TIMEOUT)
        return member


def test_concurrent_write_is_recorded_once(write_dir):
    """a member written while a reader scans is recorded exactly once

    A fresh store's first write is the exposed case: the scan legitimately
    finds nothing, so the emptiness guard stays open. A reader entering
    after the file lands on disk but before the cache append therefore
    scans, finds the new file, and publishes it -- and the append then
    records the same member a second time.
    """
    dstore = _PausingWriteStore(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.file_on_disk = threading.Event()
    dstore.reader_published = threading.Event()

    def read_once():
        assert dstore.file_on_disk.wait(timeout=_SYNC_TIMEOUT)
        len(dstore.completed)
        dstore.reader_published.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        reader = executor.submit(read_once)
        dstore.write(unique_id="brand_new.fasta", data=">new\nACGT\n")
        reader.result(timeout=_SYNC_TIMEOUT)

    assert [m.unique_id for m in dstore.completed] == ["brand_new.fasta"]


class _PausingNotCompletedStore(DataStoreDirectory):
    """holds a not-completed write between the mkdir and the file landing

    ``write_not_completed`` makes the directory and ``_write`` then opens the
    file in it. Pausing between the two makes the window a test can drive
    rather than one it has to race for.
    """

    def _write(self, **kwargs):
        self.writer_paused.set()
        self.release_writer.wait(timeout=_SYNC_TIMEOUT)
        return super()._write(**kwargs)


def test_drop_during_a_not_completed_write_leaves_it_somewhere_to_write(write_dir):
    """a drop running mid-write leaves the directory the write is opening in"""
    dstore = _PausingNotCompletedStore(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.writer_paused = threading.Event()
    dstore.release_writer = threading.Event()
    record = NotCompleted(NotCompletedType.ERROR, "location", "message", source="nc1")

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = executor.submit(
            dstore.write_not_completed,
            unique_id="nc1",
            data=record.to_json(),
        )
        try:
            assert dstore.writer_paused.wait(timeout=_SYNC_TIMEOUT)
            # the whole drop runs in the window between the writer making
            # the directory and opening its file in it
            dstore.drop_not_completed()
        finally:
            dstore.release_writer.set()

        member = writer.result(timeout=_SYNC_TIMEOUT)

    assert member.unique_id == str(Path(NOT_COMPLETED_TABLE) / "nc1.json")
    assert (write_dir / NOT_COMPLETED_TABLE / "nc1.json").exists()


def test_drop_not_completed_twice(nc_dstore):
    """dropping an already emptied store is not an error"""
    nc_dstore.drop_not_completed()

    nc_dstore.drop_not_completed()

    assert nc_dstore.not_completed == []


def test_a_write_never_hides_the_record_it_moves(write_dir):
    """a record moving from not_completed to completed stays countable"""
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    for i in range(_MOVED_RECORDS):
        record = NotCompleted(
            NotCompletedType.ERROR,
            "location",
            "message",
            source=f"r{i}",
        )
        dstore.write_not_completed(unique_id=f"r{i}", data=record.to_json())

    observed = set()
    stop = threading.Event()

    def count_members():
        while not stop.is_set():
            observed.add(len(dstore))

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        with ThreadPoolExecutor(max_workers=_CONCURRENT_READERS + 1) as executor:
            readers = [
                executor.submit(count_members) for _ in range(_CONCURRENT_READERS)
            ]
            for i in range(_MOVED_RECORDS):
                dstore.write(unique_id=f"r{i}.fasta", data=f">s{i}\nACGT\n")
            stop.set()
            for reader in readers:
                reader.result(timeout=_SYNC_TIMEOUT)
    finally:
        sys.setswitchinterval(old_interval)

    # each write moves one record between the halves, so the count is
    # invariant. only the low side is asserted: a count above it comes from
    # the completed file landing before the not-completed record is
    # removed, which is a state the directory itself passes through
    assert min(observed) == _MOVED_RECORDS


def _scan_completed(dstore, barrier, _):
    barrier.wait(timeout=_SYNC_TIMEOUT)
    return len(dstore.completed)


def test_concurrent_write_is_not_lost_from_the_cache(many_member_dir):
    """a member written while readers scan stays in the cached list

    A reader that begins its scan before the write lands builds a list from
    the old directory contents. Publishing that list after the writer has
    appended its member drops the member from the cache while leaving it on
    disk, so ``unique_id in self`` then reports False for a record that
    exists -- which is what stops ``_check_writable`` raising in APPEND mode.
    """
    dstore = DataStoreDirectory(many_member_dir, suffix="fasta", mode=OVERWRITE)
    missing = []
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for i in range(_WRITE_ROUNDS):
            unique_id = f"written_{i}.fasta"
            dstore._completed = []
            barrier = threading.Barrier(_CONCURRENT_READERS + 1, timeout=_SYNC_TIMEOUT)
            scan = functools.partial(_scan_completed, dstore, barrier)
            with ThreadPoolExecutor(max_workers=_CONCURRENT_READERS + 1) as executor:
                readers = [executor.submit(scan, n) for n in range(_CONCURRENT_READERS)]
                barrier.wait(timeout=_SYNC_TIMEOUT)
                dstore.write(unique_id=unique_id, data=">new\nACGT\n")
                for reader in readers:
                    reader.result(timeout=_SYNC_TIMEOUT)

            if unique_id not in {m.unique_id for m in dstore.completed}:
                missing.append(unique_id)
    finally:
        sys.setswitchinterval(old_interval)

    assert missing == []

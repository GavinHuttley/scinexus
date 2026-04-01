import json
import pathlib
import shutil
from pathlib import Path
from pickle import dumps, loads

import pytest
from citeable import Software
from scitrack import get_text_hexdigest

from scinexus.composable import NotCompleted, NotCompletedType
from scinexus.data_store import (
    _CITATIONS_FILE,
    _MD5_TABLE,
    _NOT_COMPLETED_TABLE,
    APPEND,
    OVERWRITE,
    READONLY,
    DataStoreDirectory,
    ReadOnlyDataStoreZipped,
    convert_directory_datastore,
    get_data_source,
    get_unique_id,
    load_record_from_json,
)


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


# ---- Tests ----


def test_data_member_eq(ro_dstore, fasta_dir):
    ro_dstore2 = DataStoreDirectory(fasta_dir, mode="r", suffix="fasta")
    name = "brca1.fasta"
    mem1 = next(m for m in ro_dstore.completed if m.unique_id == name)
    mem2 = next(m for m in ro_dstore2.completed if m.unique_id == name)
    assert mem1 != mem2


def test_convert_directory_datastore(fasta_dir, tmp_dir):
    outpath = tmp_dir / "converted"
    new_dstore = convert_directory_datastore(fasta_dir, outpath, ".fasta")
    assert len(new_dstore) > 0


def test_fail_try_append(full_dstore, completed_objects):
    full_dstore._mode = APPEND  # noqa: SLF001
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
    re_dstore = loads(dumps(ro_dstore))  # noqa: S301
    assert str(ro_dstore) == str(re_dstore)
    assert ro_dstore[0].read() == re_dstore[0].read()


def test_pickleable_member_roundtrip(ro_dstore):
    """pickling of data store members should be reversible"""
    re_member = loads(dumps(ro_dstore[0]))  # noqa: S301
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
    num_md5 = len(list((nc_dstore.source / _MD5_TABLE).glob("*.txt")))
    assert num_not_completed == 3
    assert num_completed == 6
    assert len(nc_dstore) == 9
    assert num_md5 == num_completed + num_not_completed
    nc_dstore.drop_not_completed()
    assert len(nc_dstore.not_completed) == 0
    num_md5 = len(list((nc_dstore.source / _MD5_TABLE).glob("*.txt")))
    assert num_md5 == num_completed


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


def test_no_not_completed_subdir(nc_dstore):
    expect = f"{len(nc_dstore.completed) + len(nc_dstore.not_completed)}x member"
    assert str(nc_dstore).startswith(expect)
    nc_dstore.drop_not_completed()
    assert not Path(nc_dstore.source / _NOT_COMPLETED_TABLE).exists()
    expect = f"{len(nc_dstore.completed)}x member"
    assert str(nc_dstore).startswith(expect)
    expect = f"{len(nc_dstore)}x member"
    assert str(nc_dstore).startswith(expect)
    assert len(nc_dstore) == len(nc_dstore.completed)
    not_dir = nc_dstore.source / _NOT_COMPLETED_TABLE
    not_dir.mkdir(exist_ok=True)


def test_limit_datastore(nc_dstore):
    assert len(nc_dstore) == len(nc_dstore.completed) + len(nc_dstore.not_completed)
    nc_dstore._limit = len(nc_dstore.completed) // 2  # noqa: SLF001
    nc_dstore._completed = []  # noqa: SLF001
    nc_dstore._not_completed = []  # noqa: SLF001
    assert len(nc_dstore.completed) == len(nc_dstore.not_completed) == nc_dstore.limit
    assert len(nc_dstore) == len(nc_dstore.completed) + len(nc_dstore.not_completed)
    nc_dstore.drop_not_completed()
    assert len(nc_dstore) == len(nc_dstore.completed)
    assert len(nc_dstore.not_completed) == 0
    nc_dstore._limit = len(nc_dstore.completed) // 2  # noqa: SLF001
    nc_dstore._completed = []  # noqa: SLF001
    nc_dstore._not_completed = []  # noqa: SLF001
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
    full_dstore._mode = OVERWRITE  # noqa: SLF001
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


# ---- Zipped tests ----


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


# ---- Citation tests ----


def test_write_citations_directory(write_dir, sample_citations):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=sample_citations)
    path = write_dir / _CITATIONS_FILE
    assert path.exists()
    loaded = dstore._load_citations()  # noqa: SLF001
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"
    assert loaded[1].title == "Tool Two"


def test_write_citations_empty_directory(write_dir):
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write_citations(data=())
    path = write_dir / _CITATIONS_FILE
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
    assert dstore._load_citations() == []  # noqa: SLF001


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
    loaded = zipped._load_citations()  # noqa: SLF001
    assert len(loaded) == 2
    assert loaded[0].title == "Tool One"


def test_citations_file_not_in_completed(write_dir, sample_citations):
    """The bibliography.citations file must not appear in the completed members list."""
    dstore = DataStoreDirectory(write_dir, suffix="fasta", mode=OVERWRITE)
    dstore.write(unique_id="sample.fasta", data=">s1\nACGT\n")
    dstore.write_citations(data=sample_citations)
    assert (write_dir / _CITATIONS_FILE).exists()
    dstore._completed = []  # noqa: SLF001
    member_ids = {m.unique_id for m in dstore.completed}
    assert _CITATIONS_FILE not in member_ids
    assert "sample.fasta" in member_ids

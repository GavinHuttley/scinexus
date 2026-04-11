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
    re_dstore = loads(dumps(ro_dstore))
    assert str(ro_dstore) == str(re_dstore)
    assert ro_dstore[0].read() == re_dstore[0].read()


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
    assert isinstance(ro_dstore._describe(), dict)  # noqa: SLF001
    assert isinstance(ro_dstore._summary_logs(), list)  # noqa: SLF001
    assert isinstance(ro_dstore._summary_not_completed(), list)  # noqa: SLF001
    assert isinstance(ro_dstore._validate(), dict)  # noqa: SLF001


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
    md5_path = write_dir / _MD5_TABLE / "item.txt"
    md5_path.write_text("wrong_md5_value")
    result = dstore._validate()  # noqa: SLF001
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
    assert zstore._load_citations() == []  # noqa: SLF001


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
    rows = dstore._summary_logs()  # noqa: SLF001
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
    fake._completed = []  # noqa: SLF001
    fake._not_completed = []  # noqa: SLF001
    fake._init_vals = {}  # noqa: SLF001
    with pytest.raises(ValueError, match="malformed log data"):
        fake._summary_logs()  # noqa: SLF001


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
        result = ds._summary_citations()  # noqa: SLF001
    assert result == []


def test_base_load_citations_returns_empty():
    ds = _make_minimal_ds()
    assert ds._load_citations() == []  # noqa: SLF001


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

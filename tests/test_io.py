import bz2
import gzip
import json
import pathlib
import pickle
import shutil

import pytest

import scinexus
from scinexus.composable import NotCompleted
from scinexus.data_store import DataStoreDirectory, ReadOnlyDataStoreZipped
from scinexus.deserialise import deserialise_object
from scinexus.io import (
    DEFAULT_DESERIALISER,
    DEFAULT_SERIALISER,
    compress,
    decompress,
    from_json,
    from_primitive,
    open_data_store,
    pickle_it,
    to_json,
    to_primitive,
)
from scinexus.sqlite_data_store import DataStoreSqlite


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("io")


@pytest.fixture(autouse=True)
def workingdir(tmp_dir, monkeypatch):
    monkeypatch.chdir(tmp_dir)


@pytest.fixture
def fasta_dir(DATA_DIR, tmp_dir):
    tmp_dir = pathlib.Path(tmp_dir)
    filenames = DATA_DIR.glob("*.fasta")
    fasta_dir = tmp_dir / "fasta"
    fasta_dir.mkdir(parents=True, exist_ok=True)
    for fn in filenames:
        dest = fasta_dir / fn.name
        dest.write_text(fn.read_text())
    return fasta_dir


@pytest.fixture
def zipped_full(fasta_dir):
    source = fasta_dir
    path = shutil.make_archive(
        base_name=str(source),
        format="zip",
        base_dir=source,
        root_dir=source.parent,
    )
    return ReadOnlyDataStoreZipped(pathlib.Path(path), suffix="fasta")


def test_define_data_store(fasta_dir):
    """returns an iterable data store"""
    found = open_data_store(fasta_dir, suffix=".fasta")
    assert len(found) > 1
    found = open_data_store(fasta_dir, suffix=".fasta", limit=2)
    assert len(found) == 2

    found = list(open_data_store(fasta_dir, suffix=".fasta*"))
    assert len(found) > 2

    with pytest.raises(ValueError):
        open_data_store(fasta_dir, suffix="*")

    with pytest.raises(ValueError):
        open_data_store(fasta_dir)

    with pytest.raises(TypeError):
        open_data_store(fasta_dir, 1)


@pytest.mark.parametrize(
    ("serialiser", "deserialiser"),
    [
        (json.dumps, json.loads),
        (pickle.dumps, pickle.loads),
        (lambda x: x, deserialise_object),
    ],
)
def test_deserialiser(serialiser, deserialiser):
    data = {"1": 1, "abc": [1, 2]}
    deserialised = from_primitive(deserialiser=deserialiser)
    assert deserialised(serialiser(data)) == data


def test_pickle_unpickle_apps():
    data = {"a": [1, 2, 3]}
    pkld = to_primitive() + to_json()
    upkld = from_json() + from_primitive()
    assert upkld(pkld(data)) == data


def test_pickle_it_unpickleable():
    def foo(): ...

    app = pickle_it()
    got = app(foo)
    assert isinstance(got, NotCompleted)


@pytest.mark.parametrize(
    ("comp", "decomp"),
    [(bz2.compress, bz2.decompress), (gzip.compress, gzip.decompress)],
)
def test_compress_decompress(comp, decomp):
    data = pickle.dumps({"1": 1, "abc": [1, 2]})
    decompressor = decompress(decompressor=decomp)
    compressor = compress(compressor=comp)
    assert decompressor(compressor(data)) == data


@pytest.mark.parametrize("data", [{"a": [0, 1]}])
def test_default_serialiser_deserialiser(data):
    s = DEFAULT_SERIALISER(data)
    ds = DEFAULT_DESERIALISER(s)
    assert ds == data


def test_to_json():
    to_j = to_json()
    data = {"a": [0, 1]}
    assert to_j(data) == json.dumps(data)


def test_from_json():
    from_j = from_json()
    assert from_j('{"a": [0, 1]}') == {"a": [0, 1]}


def test_to_from_json():
    to_j = to_json()
    from_j = from_json()
    app = to_j + from_j
    data = {"a": [0, 1]}
    assert app(data) == data
    assert app(data) is not data


def test_open_suffix_dirname(tmp_dir):
    outpath = tmp_dir / "melsubgroup_aln_flydivas_v1.2"
    outpath.mkdir(exist_ok=True)
    dstore = open_data_store(outpath, suffix="txt")
    assert isinstance(dstore, DataStoreDirectory)


def test_open_zipped(zipped_full):
    got = open_data_store(zipped_full.source, mode="r", suffix="fasta")
    assert len(got) == len(zipped_full)
    assert isinstance(got, type(zipped_full))


def test_open_data_store_sqlitedb(tmp_dir):
    path = tmp_dir / "test.sqlitedb"
    dstore = open_data_store(path, mode="w")
    assert isinstance(dstore, DataStoreSqlite)


# Tests for top-level scinexus.open_data_store


def test_toplevel_open_data_store(fasta_dir):
    """top-level open_data_store delegates to scinexus.io"""
    found = scinexus.open_data_store(fasta_dir, suffix="fasta")
    assert isinstance(found, DataStoreDirectory)
    assert len(found) > 1


def test_toplevel_open_data_store_sqlitedb(tmp_dir):
    path = tmp_dir / "test_toplevel.sqlitedb"
    dstore = scinexus.open_data_store(path, mode="w")
    assert isinstance(dstore, DataStoreSqlite)


# Tests for set_summary_display / get_summary_display


def test_get_summary_display_default():
    """default summary display is None"""
    orig = scinexus.get_summary_display()
    try:
        scinexus.set_summary_display(None)
        assert scinexus.get_summary_display() is None
    finally:
        scinexus.set_summary_display(orig)


def test_set_get_summary_display():
    """set then get returns the same function"""
    orig = scinexus.get_summary_display()
    try:
        sentinel = lambda data, *, name: data  # noqa: E731
        scinexus.set_summary_display(sentinel)
        assert scinexus.get_summary_display() is sentinel
    finally:
        scinexus.set_summary_display(orig)


def test_set_summary_display_none():
    """setting None clears the display function"""
    orig = scinexus.get_summary_display()
    try:
        scinexus.set_summary_display(lambda data, *, name: data)
        scinexus.set_summary_display(None)
        assert scinexus.get_summary_display() is None
    finally:
        scinexus.set_summary_display(orig)


def test_summary_display_applied(fasta_dir):
    """a registered display function is called by summary properties"""
    orig = scinexus.get_summary_display()
    calls = []

    def track(data, *, name):
        calls.append(name)
        return data

    try:
        scinexus.set_summary_display(track)
        dstore = scinexus.open_data_store(fasta_dir, suffix="fasta")
        _ = dstore.describe
        assert "describe" in calls
    finally:
        scinexus.set_summary_display(orig)


def test_register_datastore_reader_duplicate_none():
    from scinexus.io import _datastore_reader_map, register_datastore_reader

    assert None in _datastore_reader_map
    with pytest.raises(ValueError, match="already in"):
        register_datastore_reader(None)


def test_register_datastore_reader_non_string():
    from scinexus.io import register_datastore_reader

    with pytest.raises(TypeError, match="is not a string"):
        register_datastore_reader(123)


def test_register_datastore_reader_empty_string():
    from scinexus.io import register_datastore_reader

    with pytest.raises(ValueError, match="white-space"):
        register_datastore_reader("")


def test_register_datastore_reader_duplicate_suffix():
    from scinexus.io import register_datastore_reader

    with pytest.raises(ValueError, match="already in"):
        register_datastore_reader("zip")


def test_open_data_store_unknown_suffix(tmp_dir):
    path = tmp_dir / "test.xyz"
    path.write_text("data")
    with pytest.raises(KeyError):
        open_data_store(path, suffix="fasta")


def test_open_data_store_no_suffix_write_mode(tmp_dir):
    outpath = tmp_dir / "newdir"
    with pytest.raises(ValueError, match="suffix is required"):
        open_data_store(outpath, mode="w")


def test_open_data_store_memory_readonly():
    with pytest.raises(NotImplementedError, match="readonly"):
        open_data_store(":memory:", mode="r")

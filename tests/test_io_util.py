import bz2
import gzip
import pathlib
import zipfile
from urllib.parse import urlparse

import pytest

from scinexus.composable import NotCompleted
from scinexus.io_util import (
    _path_relative_to_zip_parent,
    atomic_write,
    get_format_suffixes,
    is_url,
    iter_line_blocks,
    iter_splitlines,
    open_,
    open_url,
    path_exists,
)


@pytest.fixture
def tmp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_io")


@pytest.fixture
def home_file(DATA_DIR, HOME_TMP_DIR):
    """makes a temporary directory with file"""
    fn = "sample.tsv"
    contents = (DATA_DIR / fn).read_text()
    (HOME_TMP_DIR / fn).write_text(contents)
    return str(HOME_TMP_DIR / fn)


@pytest.mark.parametrize("transform", [str, pathlib.Path])
def test_open_home(DATA_DIR, home_file, transform):
    """expands tilde for opening / writing to home"""
    data_path = DATA_DIR / "sample.tsv"
    expect = data_path.read_text()
    with open_(transform(home_file)) as infile:
        got = infile.read()
        assert got == expect


def test_does_not_write_if_exception(tmp_dir):
    """file does not exist if an exception raised before closing"""
    test_filepath = tmp_dir / "Atomic_write_test"
    with pytest.raises(AssertionError), atomic_write(test_filepath, mode="w") as f:
        f.write("abc")
        raise AssertionError
    assert not test_filepath.exists()


@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz"])
def test_writes_compressed_formats(DATA_DIR, tmp_dir, suffix):
    """correctly writes / reads different compression formats"""
    fpath = DATA_DIR / "sample.tsv"
    expect = pathlib.Path(fpath).read_text()
    outpath = tmp_dir / f"{fpath.name}.{suffix}"
    with atomic_write(outpath, mode="wt") as f:
        f.write(expect)

    with open_(outpath) as infile:
        got = infile.read()

    assert got == expect, f"write failed for {suffix}"


def test_atomic_invalid_parent_dir():
    with pytest.raises(OSError), atomic_write("invalid_dir/test.txt") as out:
        out.write("will not work")


def test_rename(tmp_dir):
    """Renames file as expected"""
    test_filepath = tmp_dir / "Atomic_write_test"
    open(test_filepath, "w").close()
    assert test_filepath.exists()
    with atomic_write(test_filepath, mode="w") as f:
        f.write("abc")


def test_atomic_write_noncontext(tmp_dir):
    """atomic write works as more regular file object"""
    path = tmp_dir / "foo.txt"
    zip_path = path.parent / f"{path.name}.zip"
    aw = atomic_write(path, in_zip=zip_path, mode="w")
    aw.write("some data")
    aw.close()
    with open_(zip_path) as ifile:
        got = ifile.read()
    assert got == "some data"


def test_open_handles_bom(tmp_dir):
    """handle files with a byte order mark"""
    text = "some text"

    textfile = tmp_dir / "sample.txt"
    textfile.write_text(text, encoding="utf-8-sig")

    gzip_file = tmp_dir / "sample.txt.gz"
    with gzip.open(gzip_file, "wt", encoding="utf-8-sig") as outfile:
        outfile.write(text)

    bzip_file = tmp_dir / "sample.txt.bz2"
    with bz2.open(bzip_file, "wt", encoding="utf-8-sig") as outfile:
        outfile.write(text)

    zip_file = tmp_dir / "sample.zip"
    with zipfile.ZipFile(zip_file, "w") as outfile:
        outfile.write(textfile, "sample.txt")

    for path in (bzip_file, gzip_file, textfile, zip_file):
        with open_(path) as infile:
            got = infile.read()
            assert got == text, f"failed reading {path}"


@pytest.mark.parametrize("non", [None, ""])
def test_open_empty_raises(non):
    with pytest.raises(ValueError):
        open_(non)


def test_aw_zip_from_path(tmp_dir):
    """supports inferring zip archive name from path"""
    path = tmp_dir / "foo.txt"
    zip_path = path.parent / f"{path.name}.zip"
    aw = atomic_write(zip_path, in_zip=True, mode="w")
    aw.write("some data")
    aw.close()
    with open_(zip_path) as ifile:
        got = ifile.read()
        assert got == "some data"

    path = tmp_dir / "foo2.txt"
    zip_path = path.parent / f"{path.name}.zip"
    aw = atomic_write(path, in_zip=zip_path, mode="w")
    aw.write("some data")
    aw.close()
    with open_(zip_path) as ifile:
        got = ifile.read()
        assert got == "some data"


def test_expanduser(tmp_dir):
    """expands user correctly"""
    home = pathlib.Path("~").expanduser()
    test_filepath = tmp_dir / "Atomic_write_test"
    test_filepath = str(test_filepath).replace(str(home), "~")
    with atomic_write(test_filepath, mode="w") as f:
        f.write("abc")


def test_path_relative_to_zip_parent():
    """correctly generates member paths for a zip archive"""
    zip_path = pathlib.Path("some/path/to/a/data.zip")
    for member in ("data/member.txt", "member.txt", "a/b/c/member.txt"):
        got = _path_relative_to_zip_parent(zip_path, pathlib.Path(member))
        assert got.parts[0] == "data"


@pytest.mark.parametrize(
    ("name", "expect"),
    [
        ("suffixes.GZ", (None, "gz")),
        ("suffixes.ABCD", ("abcd", None)),
        ("suffixes.ABCD.BZ2", ("abcd", "bz2")),
        ("suffixes.abcd.BZ2", ("abcd", "bz2")),
        ("suffixes.ABCD.bz2", ("abcd", "bz2")),
    ],
)
def test_get_format_suffixes_returns_lower_case(name, expect):
    """should always return lower case"""
    assert get_format_suffixes(name) == expect


@pytest.mark.parametrize(
    ("name", "expect"),
    [
        ("no_suffixes", (None, None)),
        ("suffixes.gz", (None, "gz")),
        ("suffixes.abcd", ("abcd", None)),
        ("suffixes.abcd.bz2", ("abcd", "bz2")),
        ("suffixes.zip", (None, "zip")),
    ],
)
def test_get_format_suffixes(name, expect):
    """correctly return suffixes for compressed etc.. formats"""
    assert get_format_suffixes(name) == expect


@pytest.mark.parametrize(
    ("name", "expect"),
    [
        ("no_suffixes", (None, None)),
        ("suffixes.gz", (None, "gz")),
        ("suffixes.abcd", ("abcd", None)),
        ("suffixes.abcd.bz2", ("abcd", "bz2")),
        ("suffixes.zip", (None, "zip")),
    ],
)
def test_get_format_suffixes_pathlib(name, expect):
    """correctly return suffixes for compressed etc.. formats from pathlib"""
    assert get_format_suffixes(pathlib.Path(name)) == expect


@pytest.mark.parametrize(
    ("val", "expect"),
    [
        ({}, False),
        ("not an existing path", False),
        ("(a,b,(c,d))", False),
        ("(a:0.1,b:0.1,(c:0.1,d:0.1):0.1)", False),
        (__file__, True),
        (pathlib.Path(__file__), True),
        (NotCompleted("FAIL", "test", message="none", source="unknown"), False),
    ],
)
def test_path_exists(val, expect):
    """robustly identifies whether an object is a valid path and exists"""
    assert path_exists(val) == expect


def test_open_reads_zip(tmp_dir):
    """correctly reads a zip compressed file"""
    text_path = tmp_dir / "foo.txt"
    with open(text_path, "w") as f:
        f.write("any str")

    zip_path = tmp_dir / "foo.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(text_path)

    with open_(zip_path) as got:
        assert got.readline() == "any str"


def test_open_writes_zip(tmp_dir):
    """correctly writes a zip compressed file"""
    zip_path = tmp_dir / "foo.txt.zip"

    with open_(zip_path, "w") as f:
        f.write("any str")

    with zipfile.ZipFile(zip_path, "r") as zf:
        name = zf.namelist()[0]
        got = zf.open(name).read()
        assert got == b"any str"


def test_open_zip_multi(tmp_dir):
    """zip with multiple records cannot be opened using open_"""
    text_path1 = tmp_dir / "foo.txt"
    with open(text_path1, "w") as f:
        f.write("any str")

    text_path2 = tmp_dir / "bar.txt"
    with open(text_path2, "w") as f:
        f.write("any str")

    zip_path = tmp_dir / "foo.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(text_path1)
        zf.write(text_path2)

    with pytest.raises(ValueError):
        open_(zip_path)


def test_open_url_write_exceptions():
    """Test 'w' mode (should raise Exception)"""
    with pytest.raises(Exception):
        open_url("http://example.com/test.txt", mode="w")


def test_open_url_exceptions():
    """non-http(s) address for url (should raise Exception)"""
    with pytest.raises(Exception):
        open_url("ftp://example.com/test.txt")


def test_iter_splitlines_one(tmp_path):
    path = tmp_path / "one-line.txt"
    value = "We have text on one line."
    path.write_text(value)
    got = list(iter_splitlines(path))
    assert got == [value]


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_iter_splitlines_line_diff_newline(tmp_path, newline):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    with open_(path, mode="w", newline=newline) as out:
        out.write("\n".join(value))
    got = list(iter_splitlines(path, chunk_size=5))
    assert got == value


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_iter_splitlines_file_endswith_newline(tmp_path, newline):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    with open_(path, mode="w", newline=newline) as out:
        out.write("\n".join(value) + "\n")
    got = list(iter_splitlines(path, chunk_size=5))
    assert got == value


def test_iter_splitlines_chunk_size_exceeds_file_size(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_splitlines(path, chunk_size=5_000_000))
    assert got == value


@pytest.mark.parametrize(
    "value",
    [
        "With text\nending on a\nended in newline.",
        "With text\nending\non a\nended in newline.",
    ],
)
def test_iter_splitlines_chunk_endswith_newline(tmp_path, value):
    path = tmp_path / "multi-line.txt"
    value = value.splitlines()
    path.write_text("\n".join(value))
    got = list(iter_splitlines(path, chunk_size=11))
    assert got == value


def test_iter_splitlines_chunk_empty_file(tmp_path):
    path = tmp_path / "zero.txt"
    path.write_text("")
    got = list(iter_splitlines(path))
    assert not got


@pytest.mark.parametrize("transform", [str, pathlib.Path])
def test_iter_splitlines_tilde(home_file, transform):
    expect = pathlib.Path(home_file).expanduser().read_text().splitlines()
    got = list(iter_splitlines(transform(home_file)))
    assert len(got) == len(expect)


def test_iter_line_blocks_correct_size(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_line_blocks(path, num_lines=2, chunk_size=5))
    expect = [value[:2], value[-1:]]
    assert got == expect


def test_iter_line_blocks_empty(tmp_path):
    path = tmp_path / "zero.txt"
    path.write_text("")
    got = list(iter_line_blocks(path, num_lines=2))
    assert not got


def test_iter_line_blocks_one(tmp_path):
    path = tmp_path / "one-line.txt"
    value = "We have text on one line."
    path.write_text(value)
    got = list(iter_line_blocks(path, num_lines=2))
    assert got == [[value]]


def test_iter_line_blocks_none_num_lines(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_line_blocks(path, num_lines=None))
    expect = [value]
    assert got == expect


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        b"file://example.txt",
        pathlib.Path("example.txt").absolute().as_uri(),
    ],
)
def test_is_url(url):
    assert is_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "example.txt",
        pathlib.Path("example.txt"),
        b"example.txt",
        r"D:\foo\example.txt",
    ],
)
def test_not_is_url(url):
    assert not is_url(url)


def test_open_url_local(DATA_DIR, tmp_path):
    """using file:///"""
    file_name = "sample.tsv"
    local_path = DATA_DIR / file_name
    with open_(local_path) as infile:
        local_data = infile.read()

    with open_url(local_path.absolute().as_uri()) as infile:
        remote_data = infile.read()

    assert remote_data.splitlines() == local_data.splitlines()


@pytest.fixture
def gzip_uri(DATA_DIR, tmp_path):
    inpath = DATA_DIR / "sample.tsv"
    data = inpath.read_text()
    outpath = tmp_path / "sample.tsv.gz"
    with open_(outpath, "wb") as outfile:
        outfile.write(data.encode("utf8"))
    return outpath.as_uri()


@pytest.mark.parametrize("mode", ["r", "rb", "rt"])
def test_open_url_gzip_mode(gzip_uri, mode):
    with open_url(gzip_uri, mode=mode) as infile:
        got = infile.read()
    expect_type = bytes if "b" in mode else str
    assert isinstance(got, expect_type)


@pytest.mark.slow
@pytest.mark.parametrize(
    "mode",
    ["r", "rb", "rt", None],
)
@pytest.mark.internet
def test_open_url(DATA_DIR, mode):
    """different open mode's all work"""
    file_name = "formattest.fasta"
    remote_root = "https://github.com/user-attachments/files/20321056/{}.gz"

    with open_(DATA_DIR / file_name, mode=mode) as infile:
        local_data = infile.read()

    with open_url(remote_root.format(file_name), mode=mode) as infile:
        remote_data = infile.read()

    assert remote_data.splitlines() == local_data.splitlines()

    # Test using a ParseResult for url
    with open_url(urlparse(remote_root.format(file_name)), mode=mode) as infile:
        remote_data = infile.read()
    assert remote_data.splitlines() == local_data.splitlines()


@pytest.mark.slow
@pytest.mark.internet
def test_open_url_compressed(DATA_DIR):
    """comparing compressed file handling"""
    file_name = "formattest.fasta.gz"
    remote_root = "https://github.com/user-attachments/files/20321056/{}"

    with open_(DATA_DIR / file_name) as infile:
        local_data = infile.read()

    with open_url(remote_root.format(file_name), mode="rt") as infile:
        remote_data = infile.read()

    assert remote_data.splitlines() == local_data.splitlines()


def test_get_compression_open_no_args():
    from scinexus.io_util import _get_compression_open

    with pytest.raises(ValueError, match="either path or compression"):
        _get_compression_open()


def test_open_via_url(DATA_DIR):
    uri = (DATA_DIR / "sample.tsv").absolute().as_uri()
    with open_(uri) as infile:
        got = infile.read()
    assert len(got) > 0


def test_atomic_write_tmpdir_not_exist(tmp_path):
    from scinexus.io_util import atomic_write

    bad_tmpdir = tmp_path / "nonexistent_tmpdir"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        atomic_write(tmp_path / "test.txt", tmpdir=bad_tmpdir, mode="w")


def test_close_rename_zip_in_zip_none(tmp_path):
    path = tmp_path / "test.txt"
    zip_path = tmp_path / "test.zip"
    aw = atomic_write(path, in_zip=zip_path, mode="w")
    aw._in_zip = None
    with pytest.raises(RuntimeError, match="in_zip path is unexpectedly None"):
        aw._close_rename_zip(aw._tmppath)


def test_atomic_write_exit_without_enter(tmp_path):
    aw = atomic_write(tmp_path / "test.txt", mode="w")
    with pytest.raises(ValueError, match="file object is unexpectedly None"):
        aw.__exit__(None, None, None)


def test_iter_splitlines_url(DATA_DIR):
    uri = (DATA_DIR / "sample.tsv").absolute().as_uri()
    got = list(iter_splitlines(uri))
    assert len(got) > 0

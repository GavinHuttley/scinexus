import bz2
import copy
import email.message
import gc
import gzip
import io
import pathlib
import typing
import urllib.response
import zipfile
from urllib.parse import urlparse

import pytest
import typeguard

import scinexus.io_util
from scinexus.composable import NotCompleted
from scinexus.io_util import (
    _path_relative_to_zip_parent,
    atomic_write,
    get_format_suffixes,
    is_url,
    iter_line_blocks,
    iter_record_chunks,
    iter_splitlines,
    open_,
    open_url,
    open_zip,
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


def test_open_writes_zip_binary(tmp_dir):
    """a wb mode writes bytes to a zip unchanged

    The payload is not valid utf-8 and carries a carriage return, so a
    member that had been through a text encode or a newline translation
    would not match. Reading the member back with zipfile rather than
    with open_ keeps this independent of the read path.
    """
    zip_path = tmp_dir / "foo.txt.zip"
    payload = b"\xff\xfe\x00\x80 raw \r\n bytes"

    with open_(zip_path, "wb") as f:
        f.write(payload)

    with zipfile.ZipFile(zip_path, "r") as zf:
        name = zf.namelist()[0]
        assert zf.open(name).read() == payload


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


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_write_uses_given_newline(tmp_path, suffix, newline):
    """a newline argument controls the line endings that are written

    Without the argument a bare newline is translated to os.linesep, so
    whichever of the two below matches the platform cannot tell a
    forwarded argument from a dropped one. The other one can, so the
    pair covers any platform.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wt", newline=newline) as outfile:
        outfile.write("first\nsecond\n")

    with open_(outpath, mode="rb") as infile:
        assert infile.read() == f"first{newline}second{newline}".encode()


@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_read_uses_given_newline(tmp_path, suffix):
    """a newline argument turns off the translation of line endings"""
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wb") as outfile:
        outfile.write(b"first\r\nsecond\r\n")

    with open_(outpath, mode="rt", encoding="utf-8", newline="") as infile:
        assert infile.read() == "first\r\nsecond\r\n"


@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_read_uses_given_errors(tmp_path, suffix):
    """an errors argument reaches the decoder"""
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wb") as outfile:
        outfile.write(b"caf\xe9")

    with open_(outpath, mode="rt", encoding="utf-8", errors="replace") as infile:
        assert infile.read() == "caf\N{REPLACEMENT CHARACTER}"


@pytest.mark.parametrize(
    "kwargs", [{"encoding": "utf-8"}, {"errors": "x"}, {"newline": ""}]
)
@pytest.mark.parametrize("mode", ["rb", "wb"])
@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_binary_rejects_decoding_arguments(tmp_path, suffix, mode, kwargs):
    """a decoding argument with a binary mode raises for every suffix

    zip was the odd one out: it dropped the encoding on the floor and
    let ZipFile.open report errors and newline as an unexpected keyword,
    so a caller catching ValueError caught five suffixes and missed the
    sixth.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wb") as outfile:
        outfile.write(b"data\n")

    with pytest.raises(ValueError, match=r"not supported|doesn't take|does not take"):
        open_(outpath, mode=mode, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [{"encoding": None}, {"errors": None}, {"newline": None}],
)
@pytest.mark.parametrize("mode", ["rb", "wb"])
@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_binary_allows_none_decoding_arguments(tmp_path, suffix, mode, kwargs):
    """a decoding argument of None is not an argument

    builtin open, gzip, bz2 and lzma all test the value rather than ask
    whether the name was passed, so that a caller relaying an unset
    argument of its own is not rejected for it. A guard that checked for
    the name would make zip and a url the only two that broke such a
    caller.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wb") as outfile:
        outfile.write(b"data\n")

    with open_(outpath, mode=mode, **kwargs) as handle:
        assert handle is not None


def test_open_zip_write_does_not_take_atomic_write_parameters(tmp_path):
    """a caller argument cannot bind to a parameter of atomic_write

    tmpdir is the dangerous one. Bound as a parameter it names the
    directory that _close_rename_zip removes with rmtree once the write
    succeeds, so forwarding it would delete a directory of the caller's.
    """
    keep = tmp_path / "keep"
    keep.mkdir()
    (keep / "precious.txt").write_text("do not delete me")

    handle = open_(tmp_path / "out.tsv.zip", mode="wt", tmpdir=keep)

    # the file is not opened until the atomic_write is entered, so that
    # is where an argument the open cannot take is found
    with pytest.raises(TypeError, match="tmpdir"):
        handle.__enter__()

    assert (keep / "precious.txt").exists()
    # the rejection happens after __init__ has made somewhere to write,
    # so the failure has to take that directory away again
    assert sorted(p.name for p in tmp_path.iterdir()) == ["keep"]


@pytest.mark.parametrize(
    ("kwargs", "expect"),
    [
        ({"encoding": "not-a-real-codec"}, LookupError),
        ({"encoding": "utf-8", "newline": "X"}, ValueError),
    ],
)
def test_open_zip_read_closes_member_on_a_bad_argument(tmp_path, kwargs, expect):
    """a rejected decoding argument does not leave the member open

    The codec is looked up and the newline validated by the wrapper, so
    the two arrive as different exception types and a handler narrowed
    to one of them would leak on the other.
    """
    outpath = tmp_path / "sample.tsv.zip"
    with open_(outpath, mode="wt") as outfile:
        outfile.write("data\n")

    opened = []
    real_open = zipfile.ZipFile.open

    def spy(self, name, mode="r", pwd=None, **kwargs):
        member = real_open(self, name, mode, pwd, **kwargs)
        opened.append(member)
        return member

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(zipfile.ZipFile, "open", spy)
        with pytest.raises(expect):
            open_(outpath, mode="rt", **kwargs)

    assert [member.closed for member in opened] == [True]


class _NotSeekable(io.BufferedIOBase):
    """a readable stream with no seek, as an http response is

    http.client.HTTPResponse is a BufferedIOBase whose seekable comes
    straight from io.IOBase and answers False.
    """

    def __init__(self, data):
        self._buffer = io.BytesIO(data)

    def read(self, size=-1):
        return self._buffer.read(size)

    def readable(self):
        return True

    def seekable(self):
        return False


@pytest.mark.parametrize("mode", ["rt", "rb"])
def test_open_zip_reads_a_stream_that_cannot_seek(tmp_path, mode):
    """a zip read buffers a stream ZipFile could not otherwise use

    ZipFile finds the central directory by seeking to the end of the
    archive, so a stream without seek is reported as not being a zip at
    all rather than as something that cannot be read this way.
    """
    outpath = tmp_path / "sample.tsv.zip"
    # newline is named so the member holds the bytes asserted below on
    # every platform. Without it a bare "\n" is written as os.linesep,
    # so the archive holds "\r\n" on Windows and a binary read of it
    # does not match
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\n")

    with open_zip(_NotSeekable(outpath.read_bytes()), mode=mode) as infile:
        got = infile.read()

    assert got == (b"id\tname\n" if "b" in mode else "id\tname\n")


def test_open_zip_closes_a_stream_it_had_to_buffer(tmp_path):
    """the drained stream is closed, since nothing else holds it

    open_url hands the response over and returns the member, so if the
    buffering did not close it the connection behind it would stay open
    with no handle anywhere to close it with.
    """
    outpath = tmp_path / "sample.tsv.zip"
    with open_(outpath, mode="wt") as outfile:
        outfile.write("id\tname\n")

    stream = _NotSeekable(outpath.read_bytes())
    with open_zip(stream) as infile:
        infile.read()

    assert stream.closed


def test_open_zip_closes_a_stream_whose_read_fails():
    """a stream that fails part way through is still closed

    Buffering it is what takes ownership, so the failure has to release
    it. A network stream that drops mid-transfer is the case: the read
    raises, and without this the response stays open with nothing
    holding it while the exception goes past.
    """

    class _FailsPartWay(_NotSeekable):
        def read(self, _size=-1):
            msg = "connection dropped"
            raise OSError(msg)

    stream = _FailsPartWay(b"")

    with pytest.raises(OSError, match="connection dropped"):
        open_zip(stream)

    assert stream.closed


def test_open_zip_leaves_a_seekable_stream_alone(tmp_path):
    """a stream that can seek is used directly, not copied

    ZipFile reads only the parts of the archive it needs from a
    seekable source, so buffering one would be a whole extra copy of it
    in memory for nothing.
    """
    outpath = tmp_path / "sample.tsv.zip"
    with open_(outpath, mode="wt") as outfile:
        outfile.write("id\tname\n")

    stream = io.BytesIO(outpath.read_bytes())
    with open_zip(stream) as infile:
        assert infile.read() == "id\tname\n"

    assert not stream.closed


@pytest.mark.parametrize("mode", ["wt", "wb"])
def test_open_zip_write_rejects_a_stream(mode):
    """a write names an archive to create, so it needs a path"""
    with pytest.raises(TypeError, match="not an open stream"):
        open_zip(io.BytesIO(), mode=mode)


@pytest.mark.parametrize("mode", ["rt", "rb"])
def test_open_url_reads_a_zip_over_a_stream_that_cannot_seek(
    tmp_path, monkeypatch, mode
):
    """a zip url works where the response cannot seek

    http.client.HTTPResponse has no seek, so this is what a real zip
    over http does. A file:// response wraps a real file and can seek,
    which is why the existing url tests did not catch it.
    """
    outpath = tmp_path / "sample.tsv.zip"
    # newline is named so the member holds the bytes asserted below on
    # every platform. Without it a bare "\n" is written as os.linesep,
    # so the archive holds "\r\n" on Windows and a binary read of it
    # does not match
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\n")

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        return urllib.response.addinfourl(
            _NotSeekable(outpath.read_bytes()),
            email.message.Message(),
            url,
        )

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with open_url(outpath.as_uri(), mode=mode) as infile:
        got = infile.read()

    assert got == (b"id\tname\n" if "b" in mode else "id\tname\n")


def test_open_zip_write_twice_replaces_the_member(tmp_path):
    """a second write to the same path replaces the first

    Appending instead left two entries under one name, which zipfile
    warns about and open_ then refuses to read at all, so the second
    write destroyed the archive rather than updating it.
    """
    outpath = tmp_path / "sample.tsv.zip"
    for text in ("first\n", "second\n"):
        with open_(outpath, mode="wt") as outfile:
            outfile.write(text)

    with zipfile.ZipFile(outpath) as zf:
        assert zf.namelist() == ["sample.tsv"]

    with open_(outpath) as infile:
        assert infile.read() == "second\n"


def test_open_zip_write_keeps_the_other_members(tmp_path):
    """replacing one member leaves the rest of the archive alone

    atomic_write can be aimed at a named member of a multi-member
    archive, so replacing one must not rewrite the archive down to just
    that one.
    """
    outpath = tmp_path / "sample.zip"
    with zipfile.ZipFile(outpath, "w") as zf:
        zf.writestr("sample/first.tsv", "one\n")
        zf.writestr("sample/second.tsv", "two\n")

    # as above, the newline keeps the bytes the same on every platform
    aw = atomic_write(
        pathlib.Path("sample/second.tsv"),
        in_zip=outpath,
        mode="wt",
        open_kwargs={"newline": "\n"},
    )
    aw.write("replaced\n")
    aw.close()

    with zipfile.ZipFile(outpath) as zf:
        assert zf.namelist() == ["sample/first.tsv", "sample/second.tsv"]
        assert zf.read("sample/first.tsv") == b"one\n"
        assert zf.read("sample/second.tsv") == b"replaced\n"


@pytest.mark.parametrize(
    "member",
    ["sample/sub/../target.tsv", "/sample/target.tsv", "sample/target.tsv"],
)
def test_open_zip_write_twice_under_a_normalised_name(tmp_path, member):
    """the name compared is the name the archive will store

    ZipInfo.from_file collapses "..", drops a leading separator and a
    drive, so a member path is not in general the name that comes back
    from namelist(). Comparing the path instead appends a duplicate
    under the stored name and the archive stops being readable.
    """
    outpath = tmp_path / "sample.zip"
    for text in ("first\n", "second\n"):
        aw = atomic_write(pathlib.Path(member), in_zip=outpath, mode="wt")
        aw.write(text)
        aw.close()

    with zipfile.ZipFile(outpath) as zf:
        assert len(zf.namelist()) == 1

    with open_(outpath) as infile:
        assert infile.read() == "second\n"


def test_open_zip_write_into_a_zero_byte_file(tmp_path):
    """a 0-byte archive file is written to, not refused

    That file is what an interrupted earlier write leaves behind. A
    read-only open of one raises BadZipFile, so testing for the member
    that way would turn a recoverable state into a permanent one.
    """
    outpath = tmp_path / "sample.tsv.zip"
    outpath.write_bytes(b"")

    with open_(outpath, mode="wt") as outfile:
        outfile.write("data\n")

    with open_(outpath) as infile:
        assert infile.read() == "data\n"


def test_open_zip_write_keeps_member_compression_and_comments(tmp_path):
    """a member copied across keeps how it was stored

    Copying by name rather than through the ZipInfo would re-store
    every other member with the default method and lose the comments.
    """
    outpath = tmp_path / "sample.zip"
    with zipfile.ZipFile(outpath, "w") as zf:
        info = zipfile.ZipInfo("sample/first.tsv")
        info.compress_type = zipfile.ZIP_DEFLATED
        info.comment = b"a member comment"
        zf.writestr(info, "one\n" * 100)
        zf.writestr("sample/second.tsv", "two\n")
        zf.comment = b"an archive comment"

    aw = atomic_write(pathlib.Path("sample/second.tsv"), in_zip=outpath, mode="wt")
    aw.write("replaced\n")
    aw.close()

    with zipfile.ZipFile(outpath) as zf:
        assert zf.comment == b"an archive comment"
        kept = zf.getinfo("sample/first.tsv")
        assert kept.compress_type == zipfile.ZIP_DEFLATED
        assert kept.comment == b"a member comment"


def test_open_zip_write_keeps_the_mode_of_the_archive(tmp_path):
    """the rewritten archive is not widened to the umask default

    What is asserted is that the mode survives the rewrite, not that it
    is any particular value. Windows models only the read-only bit, so
    a chmod to 0o600 there leaves the file at 0o666 and the check is
    weak rather than wrong: it holds, but a new file would have had
    that mode anyway. On a platform with real permission bits the
    rewritten file would come back 0o644 from the umask without the
    mode being carried across.
    """
    outpath = tmp_path / "sample.tsv.zip"
    with open_(outpath, mode="wt") as outfile:
        outfile.write("first\n")

    outpath.chmod(0o600)
    before = outpath.stat().st_mode & 0o777

    with open_(outpath, mode="wt") as outfile:
        outfile.write("second\n")

    assert outpath.stat().st_mode & 0o777 == before


def test_open_zip_write_cleans_up_when_the_rewrite_fails(tmp_path, monkeypatch):
    """a failed rewrite leaves the archive and the directory as they were"""
    outpath = tmp_path / "sample.tsv.zip"
    with open_(outpath, mode="wt") as outfile:
        outfile.write("first\n")

    before = outpath.read_bytes()

    def boom(*_args, **_kwargs):
        msg = "copy failed"
        raise OSError(msg)

    monkeypatch.setattr(scinexus.io_util.shutil, "copyfileobj", boom)

    with (
        pytest.raises(OSError, match="copy failed"),
        open_(outpath, mode="wt") as outfile,
    ):
        outfile.write("second\n")

    assert outpath.read_bytes() == before
    assert [p.name for p in tmp_path.iterdir()] == ["sample.tsv.zip"]


def test_open_zip_write_repairs_an_already_duplicated_member(tmp_path):
    """an archive the old code wrecked is made readable again

    Two entries under one name is what appending produced, so a write
    to such an archive has to leave one member rather than three.
    """
    outpath = tmp_path / "sample.zip"
    with zipfile.ZipFile(outpath, "w") as zf:
        zf.writestr("sample/target.tsv", "first\n")
    with zipfile.ZipFile(outpath, "a") as zf:
        zf.writestr("sample/target.tsv", "second\n")

    aw = atomic_write(pathlib.Path("sample/target.tsv"), in_zip=outpath, mode="wt")
    aw.write("third\n")
    aw.close()

    with zipfile.ZipFile(outpath) as zf:
        assert zf.namelist() == ["sample/target.tsv"]

    with open_(outpath) as infile:
        assert infile.read() == "third\n"


def test_open_url_write_exceptions():
    """Test 'w' mode (should raise Exception)"""
    with pytest.raises(Exception):
        open_url("http://example.com/test.txt", mode="w")


def test_open_url_exceptions():
    """non-http(s) address for url (should raise Exception)"""
    with pytest.raises(Exception):
        open_url("ftp://example.com/test.txt")


def test_open_url_uses_given_encoding(tmp_path):
    """an encoding argument is used where the headers name no charset

    A file:// response carries no charset, so without the argument the
    text is decoded with the locale default. The euro sign is the byte
    to test with: 0x80 is the euro in cp1252, a control character in
    latin-1 and not valid utf-8, so no locale decodes it to the
    expected string by accident.
    """
    path = tmp_path / "sample.txt"
    text = "price 5\N{EURO SIGN}"
    path.write_bytes(text.encode("cp1252"))

    with open_url(path.as_uri(), encoding="cp1252") as infile:
        assert infile.read() == text


def test_open_url_encoding_beats_header_charset(tmp_path, monkeypatch):
    """an encoding argument overrides the charset the headers name

    A file:// response names no charset, so a response with one has to
    be built here for the override to be exercised at all.
    """
    path = tmp_path / "sample.txt"
    text = "price 5\N{EURO SIGN}"
    path.write_bytes(text.encode("cp1252"))

    headers = email.message.Message()
    headers["Content-Type"] = "text/plain; charset=utf-8"

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        return urllib.response.addinfourl(path.open("rb"), headers, url)

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    # utf-8 from the header would raise on the 0x80 byte
    with open_url(path.as_uri(), encoding="cp1252") as infile:
        assert infile.read() == text


@pytest.mark.parametrize(
    ("kwargs", "expect"),
    [
        ({"encoding": "not-a-real-codec"}, LookupError),
        ({"newline": "X"}, ValueError),
        ({"not_an_argument": 1}, TypeError),
    ],
)
def test_open_url_closes_response_on_a_bad_argument(
    tmp_path,
    monkeypatch,
    kwargs,
    expect,
):
    """a rejected argument does not leave the connection open

    The wrapper is built after the request has been made, so an unknown
    codec or an illegal newline has to release the response rather than
    hand the caller an exception and no handle to close with.
    """
    path = tmp_path / "sample.txt"
    path.write_bytes(b"data")

    responses = []

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        response = urllib.response.addinfourl(
            path.open("rb"),
            email.message.Message(),
            url,
        )
        responses.append(response)
        return response

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with pytest.raises(expect):
        open_url(path.as_uri(), **kwargs)

    assert [r.closed for r in responses] == [True]


def test_open_url_uses_given_errors(tmp_path):
    """an errors argument reaches the decoder"""
    path = tmp_path / "sample.txt"
    path.write_bytes(b"caf\xe9")

    with open_url(path.as_uri(), encoding="utf-8", errors="replace") as infile:
        assert infile.read() == "caf\N{REPLACEMENT CHARACTER}"


def test_open_url_uses_given_newline(tmp_path):
    """a newline argument turns off the translation of line endings"""
    path = tmp_path / "sample.txt"
    path.write_bytes(b"first\r\nsecond\r\n")

    with open_url(path.as_uri(), newline="") as infile:
        assert infile.read() == "first\r\nsecond\r\n"


@pytest.mark.parametrize(
    "kwargs",
    [{"encoding": "utf-8"}, {"errors": "replace"}, {"newline": ""}],
)
def test_open_url_binary_rejects_text_arguments(tmp_path, kwargs):
    """text-only arguments in a binary mode are a caller error

    builtin open raises for the same combination, rather than accepting
    an argument it will not use.
    """
    path = tmp_path / "sample.txt"
    path.write_bytes(b"data")

    with pytest.raises(ValueError, match="binary mode does not take"):
        open_url(path.as_uri(), mode="rb", **kwargs)


@pytest.mark.parametrize("mode", ["rt", "rb"])
def test_open_url_closes_response_on_a_corrupt_archive(tmp_path, monkeypatch, mode):
    """a decompression that fails also releases the response

    The archive is opened after the request has been made, so this is
    the same problem as a rejected argument and not a different one.
    The error is BadZipFile rather than anything the wrapper raises,
    which is why the release cannot hang off a list of argument errors.
    """
    path = tmp_path / "sample.zip"
    path.write_bytes(b"not a zip at all")

    responses = []

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        response = urllib.response.addinfourl(
            path.open("rb"),
            email.message.Message(),
            url,
        )
        responses.append(response)
        return response

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with pytest.raises(zipfile.BadZipFile):
        open_url(path.as_uri(), mode=mode)

    assert [r.closed for r in responses] == [True]


def test_open_url_closes_response_on_a_keyboard_interrupt(tmp_path, monkeypatch):
    """an interrupt mid-construction releases the response too

    This is the whole of what registering the cleanup buys over
    catching Exception and re-raising: a KeyboardInterrupt does not
    derive from Exception, so it used to go past and leave the socket
    open with no handle on it anywhere.
    """
    path = tmp_path / "sample.txt"
    path.write_bytes(b"data")

    responses = []

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        response = urllib.response.addinfourl(
            path.open("rb"),
            email.message.Message(),
            url,
        )
        responses.append(response)
        return response

    def interrupt(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)
    monkeypatch.setattr(scinexus.io_util, "TextIOWrapper", interrupt)

    with pytest.raises(KeyboardInterrupt):
        open_url(path.as_uri())

    assert [r.closed for r in responses] == [True]


@pytest.mark.parametrize("suffix", ["gz", "bz2", "xz", "lzma", "zip", "tsv"])
@pytest.mark.parametrize("mode", ["rt", "rb"])
def test_open_url_reader_closes_the_response(tmp_path, monkeypatch, suffix, mode):
    """closing what open_url returned closes the response behind it

    gzip, bz2 and lzma leave a file object they were handed open when
    the reader over them is closed, and so does a zip member, so for a
    compressed url the response was left open with no handle on it
    anywhere once open_url had returned. Reading such a url in a loop
    leaks one connection per call.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\n")

    responses = []

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        response = urllib.response.addinfourl(
            outpath.open("rb"),
            email.message.Message(),
            url,
        )
        responses.append(response)
        return response

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with open_url(outpath.as_uri(), mode=mode) as infile:
        assert infile.read() in (b"id\tname\n", "id\tname\n")

    assert [r.closed for r in responses] == [True]


@pytest.mark.parametrize("suffix", ["gz", "bz2", "xz", "lzma", "zip", "tsv"])
def test_open_url_text_returns_an_io_object(tmp_path, monkeypatch, suffix):
    """what comes back is still an I/O object to anything that asks

    typeguard tests IO[str] with isinstance against io.TextIOBase, so
    an object that merely forwards every call to a reader is rejected.
    It is a runtime dependency here, and an app whose signature names a
    handle would have started refusing compressed urls, and only
    compressed urls.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\n")

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        return urllib.response.addinfourl(
            outpath.open("rb"),
            email.message.Message(),
            url,
        )

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with open_url(outpath.as_uri()) as infile:
        typeguard.check_type(infile, typing.IO[str])


@pytest.mark.parametrize("hint", [typing.IO[bytes], typing.IO[typing.Any]])
@pytest.mark.parametrize("suffix", ["gz", "bz2", "xz", "lzma", "zip"])
def test_open_url_binary_returns_an_io_object(tmp_path, monkeypatch, suffix, hint):
    """as above for a binary read of a compressed url

    The uncompressed suffixes are not here. A binary read of one hands
    back the response itself, so whether it is an I/O object is the
    transport's business: http.client.HTTPResponse is a BufferedIOBase
    and the addinfourl wrapping a file:// read is not. That is not
    something this changes either way.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\n")

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        return urllib.response.addinfourl(
            outpath.open("rb"),
            email.message.Message(),
            url,
        )

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with open_url(outpath.as_uri(), mode="rb") as infile:
        typeguard.check_type(infile, hint)


@pytest.mark.parametrize("suffix", ["gz", "bz2", "xz", "lzma", "zip"])
def test_open_url_binary_reader_reads_like_the_stream_under_it(
    tmp_path,
    monkeypatch,
    suffix,
):
    """the owning reader serves reads, seeks and iteration unchanged"""
    outpath = tmp_path / f"sample.tsv.{suffix}"
    with open_(outpath, mode="wt", newline="\n") as outfile:
        outfile.write("id\tname\nrow\tone\n")

    def fake_urlopen(url, timeout=None):  # noqa: ARG001
        return urllib.response.addinfourl(
            outpath.open("rb"),
            email.message.Message(),
            url,
        )

    monkeypatch.setattr(scinexus.io_util, "urlopen", fake_urlopen)

    with open_url(outpath.as_uri(), mode="rb") as infile:
        assert infile.readable()
        assert infile.read(3) == b"id\t"
        assert list(infile) == [b"name\n", b"row\tone\n"]
        if infile.seekable():
            assert infile.seek(0) == 0
            assert infile.tell() == 0
            assert infile.read() == b"id\tname\nrow\tone\n"

    assert infile.closed


def test_closing_reader_read1_serves_the_reader(tmp_path):
    """read1 is served like read, not left to the base class

    io.BufferedIOBase leaves read1 raising, and TextIOWrapper prefers
    it over read when the buffer offers one.
    """
    outpath = tmp_path / "sample.bin"
    outpath.write_bytes(b"id\tname\n")

    stream = outpath.open("rb")
    reader = scinexus.io_util._ClosingReader(io.BytesIO(b"id\tname\n"), stream)
    with reader:
        assert reader.read1(3) == b"id\t"
        assert reader.read1() == b"name\n"

    assert stream.closed


def test_closing_reader_without_a_reader_does_not_recurse():
    """an instance made without __init__ raises rather than recursing

    A proxy that looks its own attribute up through __getattr__ calls
    itself forever when that attribute is missing, which is the state
    copy.copy builds before it probes for __setstate__.
    """
    bare = scinexus.io_util._ClosingReader.__new__(scinexus.io_util._ClosingReader)

    # name is not defined on the class, so it is the delegation that
    # answers for it, where read and close are the class's own
    with pytest.raises(AttributeError):
        _ = bare.name

    assert not hasattr(bare, "__setstate__")
    assert copy.copy(scinexus.io_util._ClosingReader(io.BytesIO(b""), io.BytesIO(b"")))


def test_closing_reader_without_a_reader_closes_quietly(recwarn):
    """such an instance can be closed, and so can be finalised

    io.IOBase calls close from __del__, and an exception there cannot
    be raised, so it is reported as an unraisable and turns into a
    warning under pytest. A copy.copy of one of these is finalised
    exactly this way.
    """
    bare = scinexus.io_util._ClosingReader.__new__(scinexus.io_util._ClosingReader)
    bare.close()

    assert bare.closed

    del bare
    gc.collect()

    unraisable = [w for w in recwarn if "Unraisable" in type(w.message).__name__]
    assert unraisable == []


def test_open_url_binary_rejects_unknown_argument(tmp_path):
    """an unknown argument is a TypeError in either mode

    A text mode gets this from TextIOWrapper. A binary mode has nothing
    to hand the argument to, so it has to raise the error itself rather
    than report the name as a text-only argument or ignore it.
    """
    path = tmp_path / "sample.txt"
    path.write_bytes(b"data")

    with pytest.raises(TypeError, match="not_an_argument"):
        open_url(path.as_uri(), mode="rb", not_an_argument=1)


def test_open_url_binary_allows_encoding_none(tmp_path):
    """an explicit encoding of None is not an argument, as for open

    open_ hands its kwargs to open_url before it pops the encoding, so
    a None reaching a binary mode this way must not be rejected.
    """
    path = tmp_path / "sample.txt"
    path.write_bytes(b"data")

    with open_url(path.as_uri(), mode="rb", encoding=None) as infile:
        assert infile.read() == b"data"


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


def test_iter_splitlines_line_spans_multiple_chunks(tmp_path):
    path = tmp_path / "long-lines.txt"
    value = ["a" * 12, "b" * 12]
    path.write_text("\n".join(value))
    # each line is spread over 3 chunks
    got = list(iter_splitlines(path, chunk_size=5))
    assert got == value


@pytest.mark.parametrize("chunk_size", [5, None])
def test_iter_splitlines_trailing_carriage_return(tmp_path, chunk_size):
    path = tmp_path / "trailing-cr.txt"
    value = "We have some\n\r"
    path.write_text(value, newline="")
    got = list(iter_splitlines(path, chunk_size=chunk_size))
    # the file ends on a line terminator, splitlines() would report a
    # trailing empty line here
    assert got == ["We have some"]


LINE_BOUNDARIES = [
    "\n",
    "\r",
    "\r\n",
    "\v",
    "\f",
    "\x1c",
    "\x1d",
    "\x1e",
    "\x85",
    "\N{LINE SEPARATOR}",
    "\N{PARAGRAPH SEPARATOR}",
]


@pytest.mark.parametrize("chunk_size", [3, 4, 5, 6, 7, None])
@pytest.mark.parametrize("boundary", LINE_BOUNDARIES)
def test_iter_splitlines_line_boundaries(tmp_path, boundary, chunk_size):
    # str.splitlines() breaks on more than "\n", and the result must not
    # depend on where the chunk boundary falls
    path = tmp_path / "boundary.txt"
    content = f"abcd{boundary}efgh"
    path.write_text(content, newline="", encoding="utf8")
    got = list(iter_splitlines(path, chunk_size=chunk_size))
    assert got == ["abcd", "efgh"]


@pytest.mark.parametrize("chunk_size", [3, 4, 5, 6, 7, None])
@pytest.mark.parametrize("boundary", [b"\v", b"\f", b"\x1c", b"\x1d", b"\x1e"])
def test_iter_splitlines_as_bytes_not_line_boundaries(tmp_path, boundary, chunk_size):
    # bytes.splitlines() breaks only on "\n", "\r" and "\r\n"
    path = tmp_path / "boundary.dat"
    content = b"abcd" + boundary + b"efgh"
    path.write_bytes(content)
    got = list(iter_splitlines(path, chunk_size=chunk_size, as_bytes=True))
    assert got == [content]


@pytest.mark.parametrize("chunk_size", [1, 2, 3, None])
@pytest.mark.parametrize(
    ("content", "expect"),
    [
        ("abcd\n", ["abcd"]),
        ("abcd\n\n", ["abcd"]),
        ("abcd\n\n\n", ["abcd", ""]),
        ("abcd\n\x0c", ["abcd"]),
        ("abcd\nefgh\n\n", ["abcd", "efgh"]),
        ("abcd\n\nefgh", ["abcd", "", "efgh"]),
        ("\n", []),
        ("\n\n", [""]),
    ],
)
def test_iter_splitlines_no_trailing_empty_line(tmp_path, content, expect, chunk_size):
    path = tmp_path / "trailing.txt"
    path.write_text(content, newline="", encoding="utf8")
    got = list(iter_splitlines(path, chunk_size=chunk_size))
    assert got == expect


@pytest.mark.parametrize("chunk_size", [1, 2, 3, None])
@pytest.mark.parametrize(
    ("content", "expect"),
    [
        (b"abcd\n", [b"abcd"]),
        (b"abcd\n\n", [b"abcd"]),
        (b"abcd\n\r", [b"abcd"]),
        (b"abcd\r\n\r\n", [b"abcd"]),
        (b"abcd\n\n\n", [b"abcd", b""]),
        (b"\n", []),
    ],
)
def test_iter_splitlines_as_bytes_no_trailing_empty_line(
    tmp_path,
    content,
    expect,
    chunk_size,
):
    path = tmp_path / "trailing.dat"
    path.write_bytes(content)
    got = list(iter_splitlines(path, chunk_size=chunk_size, as_bytes=True))
    assert got == expect


def test_iter_splitlines_chunk_empty_file(tmp_path):
    path = tmp_path / "zero.txt"
    path.write_text("")
    got = list(iter_splitlines(path))
    assert not got


def test_iter_splitlines_as_bytes_one(tmp_path):
    path = tmp_path / "one-line.txt"
    value = "We have text on one line."
    path.write_text(value)
    got = list(iter_splitlines(path, as_bytes=True))
    assert got == [value.encode("utf8")]


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
@pytest.mark.parametrize("chunk_size", [5, None])
def test_iter_splitlines_as_bytes_multi_line(tmp_path, newline, chunk_size):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    with open_(path, mode="w", newline=newline) as out:
        out.write("\n".join(value))
    got = list(iter_splitlines(path, chunk_size=chunk_size, as_bytes=True))
    assert got == [line.encode("utf8") for line in value]


def test_iter_splitlines_as_bytes_file_endswith_newline(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value) + "\n")
    got = list(iter_splitlines(path, chunk_size=5, as_bytes=True))
    assert got == [line.encode("utf8") for line in value]


def test_iter_splitlines_as_bytes_empty_file(tmp_path):
    path = tmp_path / "zero.txt"
    path.write_text("")
    got = list(iter_splitlines(path, as_bytes=True))
    assert not got


def test_iter_splitlines_as_bytes_not_decodable(tmp_path):
    # bytes that are not valid utf8 are returned unchanged
    path = tmp_path / "binary.dat"
    value = [b"\xff\xfe some", b"\x00 bytes"]
    path.write_bytes(b"\n".join(value))
    got = list(iter_splitlines(path, chunk_size=4, as_bytes=True))
    assert got == value


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 4, None])
@pytest.mark.parametrize(
    "content",
    [
        b"ab\rcd",
        b"ab\r\rcd",
        b"ab\r\ncd",
        b"ab\r\n\r\ncd",
        b"ab\r",
        b"ab\r\n",
        b"\rab",
        b"\nab",
    ],
)
def test_iter_splitlines_as_bytes_carriage_return(tmp_path, content, chunk_size):
    # binary mode does no newline translation, so a chunk boundary can
    # fall between the "\r" and the "\n" of a "\r\n"
    path = tmp_path / "cr.dat"
    path.write_bytes(content)
    got = list(iter_splitlines(path, chunk_size=chunk_size, as_bytes=True))
    assert got == content.splitlines()


def test_iter_splitlines_as_bytes_compressed(tmp_path):
    path = tmp_path / "multi-line.txt.gz"
    value = ["We have some", "text on different lines", "which load"]
    with open_(path, mode="wt") as out:
        out.write("\n".join(value))
    got = list(iter_splitlines(path, chunk_size=5, as_bytes=True))
    assert got == [line.encode("utf8") for line in value]


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


def test_iter_line_blocks_as_bytes_correct_size(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_line_blocks(path, num_lines=2, chunk_size=5, as_bytes=True))
    expect = [line.encode("utf8") for line in value]
    assert got == [expect[:2], expect[-1:]]


def test_iter_line_blocks_as_bytes_empty(tmp_path):
    path = tmp_path / "zero.txt"
    path.write_text("")
    got = list(iter_line_blocks(path, num_lines=2, as_bytes=True))
    assert not got


def test_iter_line_blocks_as_bytes_none_num_lines(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_line_blocks(path, num_lines=None, as_bytes=True))
    assert got == [[line.encode("utf8") for line in value]]


def test_iter_line_blocks_none_num_lines(tmp_path):
    path = tmp_path / "multi-line.txt"
    value = ["We have some", "text on different lines", "which load"]
    path.write_text("\n".join(value))
    got = list(iter_line_blocks(path, num_lines=None))
    expect = [value]
    assert got == expect


class _RecordingReader:
    """wraps a reader and records the bytes of the file each read took

    A text read reports what the characters it returned cost in the
    file, which is the budget chunk_size is meant to be setting.
    """

    def __init__(self, handle, taken):
        self._handle = handle
        self._taken = taken

    def read(self, size=-1):
        data = self._handle.read(size)
        # a text handle reports its own encoding, which is what turns
        # the characters it returned back into a count of file bytes
        taken = (
            len(data) if isinstance(data, bytes) else len(data.encode(self.encoding))
        )
        self._taken.append(taken)
        return data

    @property
    def encoding(self):
        return self._handle.encoding

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._handle.close()


@pytest.fixture
def bytes_taken(monkeypatch):
    """records the bytes of the file each read consumed"""
    taken = []
    real_open = scinexus.io_util.open_

    def recording_open(filename, mode="rt", **kwargs):
        return _RecordingReader(real_open(filename, mode, **kwargs), taken)

    monkeypatch.setattr(scinexus.io_util, "open_", recording_open)
    return taken


# ascii mixed with CJK, at 1.8 bytes per character. Plain CJK would be
# 3 bytes per character, but charset_normalizer reads the first 100
# bytes of it as cp874, a single byte encoding, and then a read of n
# characters costs n bytes and there is no overshoot left to measure
MIXED_LINE = "id\t\N{CJK UNIFIED IDEOGRAPH-6F22}\N{CJK UNIFIED IDEOGRAPH-5B57}" * 30


def test_iter_splitlines_chunk_size_is_bytes_in_text_mode(tmp_path, bytes_taken):
    """a text read spends chunk_size on bytes, not on characters

    read(n) on a text handle returns n characters, so a utf-8 file of
    mixed ascii and CJK takes about 1.8 times chunk_size bytes of the
    file per chunk, which is that much more memory than the caller
    budgeted for. Plain CJK is 3 times.
    """
    path = tmp_path / "mixed.txt"
    path.write_text("\n".join([MIXED_LINE] * 40), encoding="utf-8")

    chunk_size = 3000
    lines = list(iter_splitlines(path, chunk_size=chunk_size))

    assert lines == [MIXED_LINE] * 40
    assert max(bytes_taken) <= chunk_size


def test_iter_splitlines_chunk_size_survives_compression(tmp_path, bytes_taken):
    """a compressed file is still read in chunks

    st_size of a compressed file is the compressed size, so comparing
    it against chunk_size switched chunking off for anything that
    compressed below the budget, however large it was uncompressed.
    """
    path = tmp_path / "repetitive.txt.gz"
    with open_(path, mode="wt") as outfile:
        outfile.write("\n".join(["x" * 99] * 20_000))

    chunk_size = 100_000
    assert path.stat().st_size < chunk_size
    lines = list(iter_splitlines(path, chunk_size=chunk_size))

    assert lines == ["x" * 99] * 20_000
    assert max(bytes_taken) <= chunk_size


def test_iter_record_chunks_chunk_size_survives_compression(tmp_path, bytes_taken):
    """the same st_size shortcut is in iter_record_chunks"""
    path = tmp_path / "repetitive.bin.gz"
    with open_(path, mode="wb") as outfile:
        outfile.write(b">record\n" + b"A" * 99 + b">record\n" + b"A" * 400_000)

    chunk_size = 50_000
    assert path.stat().st_size < chunk_size
    got = list(iter_record_chunks(path=path, delimiter=b">", chunk_size=chunk_size))

    assert got == [b"", b"record\n" + b"A" * 99, b"record\n" + b"A" * 400_000]
    assert max(bytes_taken) <= chunk_size


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 4, 5, 7])
def test_iter_splitlines_multi_byte_across_a_chunk_boundary(tmp_path, chunk_size):
    """a character split by a chunk boundary is not lost or doubled

    A chunk ending part way through a three-byte character decodes to
    the empty string, which a reader must not take for the end of the
    file.
    """
    path = tmp_path / "cjk.txt"
    expect = ["\N{CJK UNIFIED IDEOGRAPH-6F22}\N{CJK UNIFIED IDEOGRAPH-5B57}", "ab"]
    path.write_text("\n".join(expect), encoding="utf-8")

    assert list(iter_splitlines(path, chunk_size=chunk_size)) == expect


def test_iter_splitlines_strips_a_byte_order_mark(tmp_path):
    """a BOM is consumed by the codec, not yielded as part of the line

    charset_normalizer answers UTF-8-SIG for a file with one, and it is
    that codec rather than plain utf-8 that takes the mark off.
    """
    path = tmp_path / "bom.txt"
    path.write_text("first\nsecond", encoding="utf-8-sig")

    assert list(iter_splitlines(path, chunk_size=4)) == ["first", "second"]


def _outcome(read):
    """what a read returned, or the name of the decoding error it raised

    Only a decoding failure becomes a value to compare, since that is
    the one outcome the two readers are allowed to differ on. Anything
    else, from either of them, goes on and fails the test rather than
    being turned into a value that the other side might match.
    """
    try:
        return ("read", read())
    except UnicodeDecodeError as e:
        return ("raised", type(e).__name__)


def _agrees_with_open(path, **kwargs):
    """whether iter_splitlines and open_ give the same lines or error"""

    def via_open():
        with open_(path) as infile:
            return infile.read().splitlines()

    return _outcome(lambda: list(iter_splitlines(path, **kwargs))) == _outcome(via_open)


@pytest.mark.parametrize("suffix", ["bin", "gz", "bz2", "xz", "lzma", "zip"])
def test_iter_splitlines_unsniffable_file_behaves_like_open(tmp_path, suffix):
    """bytes no sniff can name are handled the way open_ handles them

    charset_normalizer answers None for these, and a None handed to
    codecs.getincrementaldecoder is a TypeError, where open_ passes it
    on to the opener. Which fallback that means depends on the suffix:
    every opener but one takes None as the locale default, and open_zip
    substitutes latin-1, which decodes any byte. So a zip of these
    bytes reads as text where the others raise, and this has to follow
    it. What is asserted is that the two agree, not which of the two
    outcomes happens, so a locale that decodes these bytes rather than
    raising on them still exercises the same thing.
    """
    path = tmp_path / f"raw.{suffix}"
    with open_(path, mode="wb") as outfile:
        outfile.write(b"\xff\xfe\x00\x80 raw \r\n bytes")

    assert _agrees_with_open(path)


@pytest.mark.parametrize("chunk_size", [8, 64, None])
def test_iter_splitlines_truncated_character_behaves_like_open(tmp_path, chunk_size):
    """a file ending part way through a character is not quietly cut

    The decoder is told the last chunk is the last one, and it reports
    the held-back bytes it can no longer complete. Without that it
    would return what it had and the truncated tail would vanish, which
    is a wrong answer rather than an error.
    """
    path = tmp_path / "truncated.txt"
    body = "\n".join([MIXED_LINE] * 5).encode("utf-8")
    path.write_bytes(body[:-1])

    assert _agrees_with_open(path, chunk_size=chunk_size)


@pytest.mark.parametrize("suffix", ["gz", "bz2", "xz", "lzma", "zip", "tsv"])
def test_iter_splitlines_text_agrees_with_open_for_every_suffix(tmp_path, suffix):
    """the lines are what reading the whole thing and splitting gives"""
    path = tmp_path / f"sample.tsv.{suffix}"
    # the encoding is named because the content is not ascii and a text
    # write that is given none uses the locale's, which on Windows is
    # cp1252 and cannot encode CJK at all
    with open_(path, mode="wt", encoding="utf-8") as outfile:
        outfile.write("\n".join([MIXED_LINE] * 20))

    assert _agrees_with_open(path, chunk_size=64)


@pytest.mark.parametrize("num_lines", [0, -1, 1.5])
def test_iter_line_blocks_unusable_num_lines(tmp_path, num_lines):
    """a block size that cannot be met is an error, not the whole file

    len(lines) == num_lines never holds for any of these, so the whole
    file came back as one block, which is what num_lines=None asks for
    and the opposite of what a small number asks for. A fraction is in
    the list because nothing downstream of num_lines would catch one.
    """
    path = tmp_path / "multi-line.txt"
    path.write_text("a\nb\nc\n")

    with pytest.raises(ValueError, match="num_lines"):
        list(iter_line_blocks(path, num_lines=num_lines))


def test_iter_line_blocks_num_lines_one(tmp_path):
    """the smallest usable block size still works

    The guard above has to stop below one without taking one with it.
    """
    path = tmp_path / "multi-line.txt"
    path.write_text("a\nb\nc\n")

    assert list(iter_line_blocks(path, num_lines=1)) == [["a"], ["b"], ["c"]]


@pytest.mark.parametrize("chunk_size", [0, -1])
@pytest.mark.parametrize("as_bytes", [False, True])
def test_iter_splitlines_non_positive_chunk_size(tmp_path, chunk_size, as_bytes):
    """zero and a negative fail in opposite directions, neither loudly

    read(0) returns an empty string, which the loop takes for the end
    of the file, so nothing at all was yielded for a file that plainly
    has lines. read(-1) reads to the end, so the whole file arrived in
    one chunk, which is what None already means and the opposite of the
    bounded memory the argument asks for.
    """
    path = tmp_path / "multi-line.txt"
    path.write_text("a\nb\nc\n")

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_splitlines(path, chunk_size=chunk_size, as_bytes=as_bytes))


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_iter_record_chunks_non_positive_chunk_size(tmp_path, chunk_size):
    """zero yields nothing and a negative reads the lot, neither loudly"""
    path = tmp_path / "records.bin"
    path.write_bytes(b">a\nAAA>b\nBBB")

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_record_chunks(path=path, delimiter=b">", chunk_size=chunk_size))


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_iter_line_blocks_non_positive_chunk_size(tmp_path, chunk_size):
    """the chunk_size check reaches through iter_line_blocks

    It delegates the reading, so it needs no check of its own for this.
    """
    path = tmp_path / "multi-line.txt"
    path.write_text("a\nb\nc\n")

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_line_blocks(path, chunk_size=chunk_size))


def test_iter_splitlines_chunk_size_checked_before_the_path(tmp_path):
    """a bad chunk_size is reported as one, not as a missing file

    The check sits above the stat, so the argument the caller got wrong
    is what they are told about rather than the path they would have
    got wrong next.
    """
    missing = tmp_path / "does-not-exist.txt"

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_splitlines(missing, chunk_size=0))

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_record_chunks(path=missing, delimiter=b">", chunk_size=0))


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


COMPRESSED_SAMPLE = "id\tname\n1\talpha\n2\tbeta\n3\tgamma\n"


@pytest.fixture(params=["gz", "bz2", "zip", "lzma", "xz"])
def compressed_path(tmp_path, request):
    """path to text written under each compressed suffix

    The content is ascii deliberately. open_ sniffs the encoding of a
    local file while open_url takes it from the response headers, which
    a file:// url does not set, so the two sides only agree on content
    that decodes the same way under any locale.
    """
    outpath = tmp_path / f"sample.tsv.{request.param}"
    with open_(outpath, "wt") as outfile:
        outfile.write(COMPRESSED_SAMPLE)
    return outpath


@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_uses_given_encoding(tmp_path, suffix):
    """an encoding argument is used instead of sniffing the content

    The text is written as cp1252, which is not valid utf-8, so reading
    it back correctly is only possible with the encoding the caller
    names.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    text = "temperature 20\N{DEGREE SIGN}C \N{EURO SIGN}5"
    with open_(outpath, mode="wt", encoding="cp1252") as outfile:
        outfile.write(text)

    # the bytes are checked as well as the round trip, since a round
    # trip alone cannot tell an encoding that was honoured from one that
    # was ignored the same way on both sides
    with open_(outpath, mode="rb") as infile:
        assert infile.read() == text.encode("cp1252")

    with open_(outpath, mode="rt", encoding="cp1252") as infile:
        assert infile.read() == text


@pytest.mark.parametrize("mode", ["w", "wt", "wb"])
@pytest.mark.parametrize("suffix", ["gz", "bz2", "zip", "lzma", "xz", "tsv"])
def test_open_write_modes(tmp_path, suffix, mode):
    """a bare w writes text, as it does for an uncompressed file

    Writing a str to a handle that turned out to be binary raises
    TypeError, so a round trip with a payload chosen to match the mode
    is what pins text against binary.
    """
    outpath = tmp_path / f"sample.tsv.{suffix}"
    binary = "b" in mode
    payload = COMPRESSED_SAMPLE.encode("utf-8") if binary else COMPRESSED_SAMPLE

    with open_(outpath, mode=mode) as outfile:
        outfile.write(payload)

    with open_(outpath, mode="rb" if binary else "rt") as infile:
        assert infile.read() == payload


def test_open_compressed_bare_read_mode(compressed_path):
    """a bare r reads text, as it does for an uncompressed file

    gzip, bz2 and lzma read a bare r as binary where builtin open and
    open_zip read it as text, so open_ has to say which it means.
    """
    with open_(compressed_path, mode="r") as infile:
        got = infile.read()

    assert got == COMPRESSED_SAMPLE


@pytest.mark.parametrize("mode", ["r", "rb", "rt"])
def test_open_url_compressed_local(compressed_path, mode):
    """a compressed file:// url reads the same as the local path

    zip is the regression guarded here, it was the only suffix whose
    handler defaulted to text rather than bytes.
    """
    # open_url gives bytes only when "b" is in the mode, so "r" reads as
    # text. The local read spells that out because open_(path, "r") is
    # separately broken for gz, bz2, xz and lzma.
    with open_(compressed_path, mode="rb" if "b" in mode else "rt") as infile:
        expect = infile.read()

    with open_url(compressed_path.as_uri(), mode=mode) as infile:
        got = infile.read()

    assert got == expect


@pytest.mark.parametrize("compressed_path", ["zip"], indirect=True)
@pytest.mark.parametrize("mode", ["rb", "rt"])
def test_open_compressed_url(compressed_path, mode):
    """open_ hands a compressed url to open_url

    Only zip is worth running here. open_ delegates any url whole, so
    the suffix fan-out belongs to test_open_url_compressed_local and
    repeating it would add cases that cannot fail.
    """
    with open_(compressed_path, mode=mode) as infile:
        expect = infile.read()

    with open_(compressed_path.as_uri(), mode=mode) as infile:
        got = infile.read()

    assert got == expect


@pytest.mark.parametrize("compressed_path", ["zip"], indirect=True)
@pytest.mark.parametrize("as_bytes", [False, True])
def test_iter_splitlines_compressed_url(compressed_path, as_bytes):
    """a consumer of open_ reaches compressed urls too"""
    expect = list(iter_splitlines(compressed_path, as_bytes=as_bytes))
    got = list(iter_splitlines(compressed_path.as_uri(), as_bytes=as_bytes))

    assert got == expect


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


def test_iter_splitlines_url_as_bytes(DATA_DIR):
    path = (DATA_DIR / "sample.tsv").absolute()
    uri = path.as_uri()
    got = list(iter_splitlines(uri, as_bytes=True))
    assert got == [line.encode("utf8") for line in iter_splitlines(path)]


@pytest.mark.parametrize("chunk_size", [1, 16, 64, 1024, 5_000_000])
def test_iter_record_chunks_chunk_size_independence(tmp_path, chunk_size):
    delim = b"\n//"
    data = b"record1\n//record2 is longer\n//record3"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=delim, chunk_size=chunk_size))
    assert got == [b"record1", b"record2 is longer", b"record3"]


def test_iter_record_chunks_delimiter_spans_chunk_boundary(tmp_path):
    delim = b"\n//"
    data = b"AAAAA" + delim + b"BBBBB" + delim + b"CCCCC"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    chunk_size = data.index(delim) + 1
    got = list(iter_record_chunks(path=path, delimiter=delim, chunk_size=chunk_size))
    assert got == [b"AAAAA", b"BBBBB", b"CCCCC"]


def test_iter_record_chunks_record_larger_than_chunk(tmp_path):
    delim = b">"
    record = b"x" * 1000
    data = delim + record + delim + b"short"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=delim, chunk_size=16))
    assert got == [b"", record, b"short"]


def test_iter_record_chunks_chunk_size_one(tmp_path):
    delim = b">"
    data = b">a>b>c"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=delim, chunk_size=1))
    assert got == [b"", b"a", b"b", b"c"]


def test_iter_record_chunks_ends_on_delimiter(tmp_path):
    delim = b"\n//"
    data = b"record1\n//record2\n//"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=delim, chunk_size=8))
    assert got == [b"record1", b"record2"]


def test_iter_record_chunks_no_delimiter(tmp_path):
    data = b"no delimiter present at all"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=b">", chunk_size=8))
    assert got == [data]


def test_iter_record_chunks_empty_file(tmp_path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    got = list(iter_record_chunks(path=path, delimiter=b">"))
    assert got == []


def test_iter_record_chunks_empty_delimiter_raises(tmp_path):
    path = tmp_path / "records.bin"
    path.write_bytes(b"anything")
    with pytest.raises(ValueError, match="delimiter must be non-empty"):
        list(iter_record_chunks(path=path, delimiter=b""))


@pytest.mark.parametrize("compression", ["gz", "bz2"])
def test_iter_record_chunks_compressed(tmp_path, compression):
    data = b">a\nAAA>b\nBBB>c\nCCC"
    path = tmp_path / f"records.bin.{compression}"
    with open_(path, mode="wb") as f:
        f.write(data)
    got = list(iter_record_chunks(path=path, delimiter=b">", chunk_size=4))
    assert got == [b"", b"a\nAAA", b"b\nBBB", b"c\nCCC"]


@pytest.mark.parametrize("chunk_size", [None, 5_000_000])
def test_iter_record_chunks_read_all(tmp_path, chunk_size):
    data = b">a>b>c"
    path = tmp_path / "records.bin"
    path.write_bytes(data)
    got = list(iter_record_chunks(path=path, delimiter=b">", chunk_size=chunk_size))
    assert got == [b"", b"a", b"b", b"c"]


def test_iter_record_chunks_url(tmp_path):
    data = b">a\nAAA>b\nBBB>c\nCCC"
    src = tmp_path / "records.bin"
    src.write_bytes(data)
    uri = src.absolute().as_uri()
    got = list(iter_record_chunks(path=uri, delimiter=b">", chunk_size=4))
    assert got == [b"", b"a\nAAA", b"b\nBBB", b"c\nCCC"]

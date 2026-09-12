from __future__ import annotations

import codecs
import contextlib
import functools
import locale
import re
import shutil
import uuid
from bz2 import open as bzip_open
from gzip import open as gzip_open
from io import BufferedIOBase, BytesIO, TextIOWrapper
from lzma import open as lzma_open
from os import PathLike
from pathlib import Path, PurePath
from tempfile import mkdtemp
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NamedTuple,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import ParseResult, urlparse
from urllib.request import urlopen

from charset_normalizer import detect

_wout_period = re.compile(r"^\.")


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import TracebackType

PathType = str | PathLike[Any] | PurePath | Path

_StrOrBytes = TypeVar("_StrOrBytes", str, bytes)


class _Separators(NamedTuple, Generic[_StrOrBytes]):
    """line separators matching the mode a file object was opened in"""

    boundaries: _StrOrBytes
    newline: _StrOrBytes
    carriage_return: _StrOrBytes
    empty: _StrOrBytes


# str.splitlines() breaks on all of these, bytes.splitlines() breaks
# only on newline and carriage return
_TEXT_SEPARATORS = _Separators(
    "\n\r\v\f\x1c\x1d\x1e\x85\N{LINE SEPARATOR}\N{PARAGRAPH SEPARATOR}",
    "\n",
    "\r",
    "",
)
_BINARY_SEPARATORS = _Separators(b"\n\r", b"\n", b"\r", b"")


@functools.singledispatch
def is_url(path: str | bytes | Path | PathLike | ParseResult) -> bool:  # noqa: ARG001
    """whether a path is a url"""
    return False


@is_url.register
def _(path: str) -> bool:
    return is_url(urlparse(path))


@is_url.register
def _(path: bytes) -> bool:
    return is_url(urlparse(path.decode("utf8")))


@is_url.register
def _(path: ParseResult) -> bool:
    return path.scheme in {"http", "https", "file"}


def _get_compression_open(
    path: PathType | None = None,
    compression: str | None = None,
) -> Callable[..., Any] | None:
    """returns function for opening compression formats

    Parameters
    ----------
    path
        file path or url
    compression
        file compression suffix

    Returns
    -------
    function for opening compressed files or None if unknown compression
    """
    if not (path or compression):
        msg = "either path or compression argument must be provided"
        raise ValueError(msg)
    if compression is None and path is not None:
        _, compression = get_format_suffixes(path)
    return None if compression is None else _compression_handlers.get(compression)


def _detect_encoding(path: PathType) -> str | None:
    """returns the encoding a text read of path will decode with

    Parameters
    ----------
    path
        file path, decompressed first if it names a compression

    Returns
    -------
    the name of the encoding, or None where the content identifies none
    and the opener would take that as the locale default

    Notes
    -----
    The name is sniffed from the first hundred bytes. Where that
    identifies nothing the answer is whatever the opener for the path
    would then do, which is the locale default for every suffix but
    zip. open_zip substitutes latin-1, which decodes any byte, so a
    caller reading the bytes itself has to substitute it too rather
    than decode the same archive differently from open_.
    """
    op = _get_compression_open(path) or open
    with op(path, mode="rb") as infile:
        data = infile.read(100)

    encoding = cast("str | None", detect(data)["encoding"])
    if encoding is None and get_format_suffixes(path)[1] == "zip":
        return "latin-1"

    return encoding


def _check_binary_mode_args(encoding: str | None, kwargs: dict[str, Any]) -> None:
    """raises if a decoding argument is paired with a binary mode

    Parameters
    ----------
    encoding
        the encoding the caller named, None meaning they named none
    kwargs
        the remaining arguments, inspected for errors and newline. Ones
        whose value is None are removed, having named nothing

    Notes
    -----
    builtin open, gzip, bz2 and lzma all raise ValueError here, and all
    of them test the value against None rather than asking whether the
    argument was passed, so that a caller relaying an unset argument of
    its own is not rejected for it. The openers that do their own
    decoding follow them on both counts.
    """
    named = ["encoding"] if encoding is not None else []
    for name in ("errors", "newline"):
        if name not in kwargs:
            continue
        if kwargs[name] is None:
            del kwargs[name]
        else:
            named.append(name)

    if named:
        msg = f"binary mode does not take {', '.join(named)}"
        raise ValueError(msg)


def open_zip(filename: PathType | IO[Any], mode: str = "r", **kwargs: Any) -> IO[Any]:
    """open a single member zip-compressed file

    Parameters
    ----------
    filename
        path to the archive, or, for a read, an open stream of it. One
        that cannot seek is read into memory and closed, since a zip
        cannot be read front to back. A write raises TypeError for a
        stream, having an archive to create rather than one to read
    mode
        a read mode returns the member, a write mode returns an
        atomic_write() instance
    kwargs
        encoding, errors and newline are used for the decoding of a
        text read. A text write hands all three to the file it opens,
        along with anything else it is given. On a read the rest go to
        ZipFile.open, which takes pwd and force_zip64

    Note
    ----
    A read raises ValueError if the archive holds more than one record.

    A read in a text mode decodes with the given encoding, falling back
    to latin-1, which decodes any byte. A read in a binary mode returns
    the member itself and does no decoding.

    A binary mode raises ValueError if given an encoding, errors or
    newline whose value is not None. A None means the caller named
    nothing, as it does for builtin open.
    """
    mode = mode or "r"
    binary_mode = "b" in mode
    mode = mode[:1]

    # a path has no seekable attribute and a stream does, which is how
    # the two are told apart without asking the caller
    is_stream = hasattr(filename, "seekable")

    encoding = kwargs.pop("encoding", None)
    if binary_mode:
        _check_binary_mode_args(encoding, kwargs)

    if mode.startswith("w"):
        if is_stream:
            # atomic_write would reach Path(path) with it and complain
            # about __fspath__, naming its own parameter rather than
            # what the caller did
            msg = "a write needs a path to the archive, not an open stream"
            raise TypeError(msg)

        # mode has been truncated to its first letter, so put the b back
        # for a binary write. what is left goes to the file the writes
        # land in, under its own argument rather than splatted, so that
        # a name like tmpdir cannot bind to a parameter of atomic_write
        write_mode = "wb" if binary_mode else mode
        return atomic_write(  # type: ignore[return-value]
            cast("PathType", filename),
            mode=write_mode,
            in_zip=True,
            encoding=encoding,
            open_kwargs=kwargs,
        )

    from zipfile import ZipFile

    # ZipFile.open takes pwd and force_zip64 and nothing else, so the
    # arguments that belong to the decoding are taken out here. the
    # guard above means a binary mode has none of them left to take
    text_kwargs = {
        name: kwargs.pop(name) for name in ("errors", "newline") if name in kwargs
    }

    # latin-1 decodes any byte, so it is the fallback when the caller
    # names no encoding. the test is against None rather than falsiness,
    # so that an empty encoding reaches the codec lookup and is rejected
    # here as it is for the other suffixes
    encoding = encoding if encoding is not None else "latin-1"
    mode = mode.strip("t")

    # a zip is read back to front, ZipFile seeks to the end to find the
    # central directory, so a stream that cannot seek has to be held in
    # memory first. an http response is such a stream, and reporting it
    # as not being a zip is what ZipFile does with one
    if is_stream:
        stream = cast("IO[bytes]", filename)
        if not stream.seekable():
            # draining it is what takes ownership, and from there it is
            # closed whether the read finishes or not: nothing else
            # holds it, so a read that fails part way, as a network
            # stream that drops does, would otherwise leave it open with
            # the exception going past it
            with contextlib.closing(stream):
                filename = BytesIO(stream.read())

    with ZipFile(filename) as zf:
        if len(zf.namelist()) != 1:
            msg = "Archive is supposed to have only one record."
            raise ValueError(msg)

        opened = zf.open(
            zf.namelist()[0],
            mode=cast("Literal['r', 'w']", mode),
            **kwargs,
        )

        if binary_mode:
            return opened

        # the member is open and is not the caller's to close until it
        # is returned, so a wrapper that rejects the encoding with
        # LookupError or the newline with ValueError has to release it.
        # the cleanup is registered rather than hung off those two
        # names, so that it also runs for whatever else may come out of
        # the constructor
        with contextlib.ExitStack() as cleanup:
            cleanup.callback(opened.close)
            wrapper = TextIOWrapper(opened, encoding=encoding, **text_kwargs)
            cleanup.pop_all()

        return wrapper


_compression_handlers: dict[str, Callable[..., Any]] = {
    "gz": gzip_open,
    "bz2": bzip_open,
    "zip": open_zip,
    "xz": lzma_open,
    "lzma": lzma_open,
}


def open_(filename: PathType, mode: str = "rt", **kwargs: Any) -> IO[Any]:
    """open that handles different compression

    Parameters
    ----------
    filename
        path or url, if a url delegates processing to open_url
    mode
        standard file opening mode
    kwargs
        passed to open functions

    Returns
    -------
    an object compatible with the file protocol

    Notes
    -----
    A bare "r" or "w" means text, for a compressed file as much as for
    an uncompressed one, so it agrees with builtin open rather than with
    gzip, bz2 and lzma, which take a bare mode as binary. Use "rb" or
    "wb" for bytes.

    This applies to "r" and "w" only. A bare "a" is still binary for gz,
    bz2, xz and lzma and text for an uncompressed file, and neither "a"
    nor "x" works on a zip at all, since a zip member can only be opened
    for reading or writing.
    """
    if not filename:
        msg = f"{filename} not a valid file name or url"
        raise ValueError(msg)

    if is_url(filename):
        return open_url(filename, mode=mode, **kwargs)  # type: ignore[arg-type]

    mode = mode or "rt"
    if mode in {"r", "w"}:
        # gzip, bz2 and lzma take a bare "r" or "w" as binary, where
        # builtin open and open_zip take it as text. Say which is meant,
        # so that r and w agree across the suffixes and, for a read, so
        # the encoding worked out below is one the handler will accept.
        # "a" and "x" are left alone, open_zip cannot support them
        mode = f"{mode}t"
    filename = Path(filename).expanduser()
    op = _get_compression_open(filename) or open

    encoding = kwargs.pop("encoding", None)
    # the pop above means kwargs can no longer hold an encoding, so ask
    # the value whether the caller named one
    need_encoding = mode.startswith("r") and "b" not in mode
    if need_encoding and encoding is None:
        encoding = _detect_encoding(filename)

    return op(filename, mode, encoding=encoding, **kwargs)


def _decompressed(response: IO[Any], compression: str | None) -> IO[Any]:
    """returns a reader for the content of a url response

    Parameters
    ----------
    response
        an open url response
    compression
        compression suffix of the url, None leaves the response alone

    Notes
    -----
    The response itself is returned, not closed, so a caller that has to
    abandon the result still has the object whose close releases the
    connection.
    """
    if compression:
        opener = _get_compression_open(compression=compression)
        if opener is not None:
            # the handlers disagree on what their default mode means,
            # open_zip's "r" is text while the others are binary, so ask
            # for bytes and leave the text decision to the caller
            return cast("IO[Any]", opener(response, mode="rb"))

    return response


class _ClosingReader(BufferedIOBase):
    """a binary reader that also closes the stream it was built over

    Notes
    -----
    gzip, bz2 and lzma close only a file they opened themselves, so a
    file object handed to them is left open when the reader over it is
    closed, and a zip member does the same. A url response decompressed
    through one of them therefore stays open once open_url has
    returned, with no handle on it anywhere to close it with.

    It is an io.BufferedIOBase rather than a bare delegating object so
    that it is still an I/O object to anything that asks. typeguard
    tests IO[bytes] and IO[str] with isinstance against the io ABCs, so
    a plain proxy is rejected by every one of them, and a text read is
    a real TextIOWrapper built over this rather than a proxy around
    one.

    one.
    """

    def __init__(self, reader: IO[bytes], stream: IO[Any]) -> None:
        """
        Parameters
        ----------
        reader
            the object reads are served from
        stream
            the one underneath it, closed after it
        """
        self._reader = reader
        self._stream = stream

    def __getattr__(self, name: str) -> Any:
        # only reached when normal lookup fails, and the lookup of
        # _reader must not come back here when __init__ has not run, as
        # it has not for the instance copy.copy makes with __new__
        reader = self.__dict__.get("_reader")
        if reader is None:
            raise AttributeError(name)

        return getattr(reader, name)

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        return self._reader.read(-1 if size is None else size)

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def seekable(self) -> bool:
        return self._reader.seekable()

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._reader.seek(offset, whence)

    def tell(self) -> int:
        return self._reader.tell()

    def close(self) -> None:
        """closes the reader, then the stream under it

        Notes
        -----
        The two are looked for rather than assumed. IOBase calls this
        from __del__, and an instance that never went through __init__
        has neither, which is what copy.copy builds before it looks for
        __setstate__. An exception in __del__ cannot be raised, so it
        would be reported as an unraisable rather than handled.
        """
        reader = self.__dict__.get("_reader")
        stream = self.__dict__.get("_stream")
        try:
            if reader is not None:
                reader.close()
        finally:
            try:
                if stream is not None:
                    stream.close()
            finally:
                super().close()


def open_url(url: str | ParseResult, mode: str = "rt", **kwargs: Any) -> IO[Any]:
    """open a url

    Parameters
    ----------
    url
        A url of file in http or https web address
    mode
        mode of reading file, 'rb', 'rt', 'r'
    kwargs
        encoding, errors and newline for a text mode, used to decode the
        response. An encoding overrides the charset named by the
        response headers

    Raises
    ------
    Raises IOError if mode is write or it's not a url.

    Raises ValueError if encoding, errors or newline is given with a
    binary mode. A value of None counts as not given, as it does for
    builtin open.

    Returns
    -------
    file object which reads binary if "b" in mode, else text.
    """
    _, compression = get_format_suffixes(
        url.path if isinstance(url, ParseResult) else url,
    )
    mode = mode or "r"

    if "r" not in mode:
        msg = "opening a url only allowed in read mode"
        raise OSError(msg)

    if not is_url(url):
        msg = f"URL scheme must be http, https or file, not {str(url)[:20]!r}"
        raise OSError(msg)

    binary_mode = "b" in mode
    encoding = kwargs.pop("encoding", None)
    if binary_mode:
        # an encoding of None is not an argument: open_ hands its kwargs
        # on before it pops the encoding, so a None arrives here for any
        # binary read
        _check_binary_mode_args(encoding, kwargs)

        if kwargs:
            # the guard above leaves nothing that belongs to a text mode,
            # so anything still here is a name the function does not
            # take. a binary mode has nothing to hand it to, where a
            # text mode would at least reach TextIOWrapper and be told
            # off by it, so raise the error that would have come from
            # there rather than ignore the argument
            msg = f"open_url() got an unexpected keyword argument {min(kwargs)!r}"
            raise TypeError(msg)

    url_parsed = url if isinstance(url, ParseResult) else urlparse(url)

    response = urlopen(url_parsed.geturl(), timeout=10)

    # the request has been made, and until the reader is returned there
    # is no handle on the response for anyone else to close, so a
    # failure between here and there leaks a socket. The wrapper gives
    # LookupError for an unknown codec, ValueError for an illegal
    # newline and TypeError for an unknown argument, and opening a zip
    # adds BadZipFile and the ValueError for a multi-member archive.
    # That list is what has been seen rather than what can happen, so
    # the cleanup is registered rather than hung off it
    with contextlib.ExitStack() as cleanup:
        cleanup.callback(response.close)
        # the charset is taken before the decompression, which for a
        # zip that cannot seek has already closed the response by the
        # time it returns
        if not binary_mode and encoding is None:
            encoding = response.headers.get_content_charset()

        source = _decompressed(response, compression)
        if compression:
            # without a compression the source is the response itself,
            # and closing it, or a TextIOWrapper over it, closes it.
            # With one there is a decompressor in between that will not,
            # so the ownership is spelled out. It goes on the binary
            # layer so that a text read is a TextIOWrapper over this
            # rather than something wrapped around a TextIOWrapper
            # typeshed models BufferedIOBase as an IOBase and not as an
            # IO[bytes], where at runtime it answers to both, so the
            # cast says what isinstance already agrees with
            source = cast("IO[Any]", _ClosingReader(source, response))

        reader = source if binary_mode else TextIOWrapper(source, encoding, **kwargs)
        cleanup.pop_all()

    return reader


def _path_relative_to_zip_parent(zip_path: Path, member_path: Path) -> Path:
    """returns member_path relative to zip_path

    Parameters
    ----------
    zip_path: Path
    member_path: Path

    Notes
    -----
    with zip_path = "parentdir/named.zip", then member_path="named/member.tsv"
    or path="member.tsv" will return "named/member.tsv"
    """
    zip_name = zip_path.name.replace(".zip", "")
    if zip_name not in member_path.parts:
        return Path(zip_name) / member_path

    return Path(*member_path.parts[member_path.parts.index(zip_name) :])


class atomic_write:
    """performs atomic write operations, cleans up if fails"""

    def __init__(
        self,
        path: PathType,
        tmpdir: PathType | None = None,
        in_zip: PathType | bool | None = None,
        mode: str = "w",
        encoding: str | None = None,
        *,
        open_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """

        Parameters
        ----------
        path
            path to file, or relative to directory specified by in_zip
        tmpdir
            directory where temporary file will be created
        in_zip
            path to the zip archive containing path,
            e.g. if in_zip="path/to/data.zip", then path="data/seqs.tsv"
            Decompressing the archive will produce the "data/seqs.tsv"
        mode
            file writing mode
        encoding
            text encoding
        open_kwargs
            further arguments for open_, so errors and newline reach the
            file the writes go to. A dict rather than **kwargs, so that
            a name matching one of the parameters above cannot bind to
            it instead
        """
        path = Path(path).expanduser()
        _, cmp = get_format_suffixes(path)

        zip_path: Path | None = None
        if in_zip:
            if isinstance(in_zip, bool):
                zip_path = path if cmp == "zip" else None
            else:
                zip_path = Path(in_zip)

        if zip_path and cmp == "zip":
            path = Path(str(path)[: str(path).rfind(".zip")])

        if zip_path:
            path = _path_relative_to_zip_parent(zip_path, path)

        self._path = path
        self._cmp = cmp
        self._mode = mode
        self._file: IO[Any] | None = None
        self._encoding = encoding
        self._open_kwargs = open_kwargs or {}
        self._in_zip = zip_path
        self._tmppath = self._make_tmppath(tmpdir)

        self.succeeded: bool | None = None
        self._close_func = (
            self._close_rename_zip if zip_path else self._close_rename_standard
        )

    def _make_tmppath(self, tmpdir: PathType | None) -> Path:
        """returns path of temporary file

        Parameters
        ----------
        tmpdir: Path
            to directory

        Returns
        -------
        full path to a temporary file

        Notes
        -----
        Uses a random uuid as the file name, adds suffixes from path
        """
        suffixes = (
            "".join(self._path.suffixes)
            if not self._in_zip
            else "".join(self._path.suffixes[:-1])
        )
        parent = self._in_zip.parent if self._in_zip else self._path.parent
        if not parent.exists():
            raise OSError(f"Parent dir '{parent}' of provided path does not exist")

        name = f"{uuid.uuid4()}{suffixes}"
        tmpdir = Path(mkdtemp(dir=parent)) if tmpdir is None else Path(tmpdir)

        if not tmpdir.exists():
            msg = f"{tmpdir} directory does not exist"
            raise FileNotFoundError(msg)

        return tmpdir / name

    def _get_fileobj(self) -> IO[Any]:
        """returns file to be written to"""
        if self._file is None:
            # __init__ has already made the directory to write into, and
            # a failure here means the caller never gets the object
            # whose exit or close would have removed it again. an
            # argument the open will not take is TypeError, an unknown
            # codec LookupError and a text-only argument under a binary
            # mode ValueError, but the open can also fail for reasons
            # that have nothing to do with the arguments, so the cleanup
            # is registered rather than tied to those three
            with contextlib.ExitStack() as cleanup:
                cleanup.callback(shutil.rmtree, self._tmppath.parent)
                self._file = open_(
                    self._tmppath,
                    self._mode,
                    encoding=self._encoding,
                    **self._open_kwargs,
                )
                cleanup.pop_all()

        return self._file

    def __enter__(self) -> IO[Any]:
        return self._get_fileobj()

    def _close_rename_standard(self, src: Path) -> None:
        dest = Path(self._path)
        try:
            dest.unlink()
        except FileNotFoundError:
            pass
        finally:
            src.rename(dest)

        shutil.rmtree(src.parent)

    def _close_rename_zip(self, src: Path) -> None:
        from zipfile import ZipFile, ZipInfo

        if self._in_zip is None:
            msg = "in_zip path is unexpectedly None"
            raise RuntimeError(msg)

        # ZipInfo.from_file is what turns a member path into the name an
        # archive stores. It drops a drive, collapses "..", strips a
        # leading separator and uses posix separators, so asking it is
        # the only way to compare against namelist() in the same terms.
        # A path of "arch/sub/../target.txt" is stored as
        # "arch/target.txt", and comparing the two appends a duplicate
        arcname = ZipInfo.from_file(src, self._path).filename

        try:
            # append mode is used for the test as well as the write. It
            # tolerates a missing file and the 0-byte residue of an
            # interrupted write, where a read-only open of either raises
            # BadZipFile, and it saves reading the central directory a
            # second time. Closing without writing leaves the archive
            # byte for byte as it was
            with ZipFile(self._in_zip, "a") as out:
                replacing = arcname in out.namelist()
                if not replacing:
                    out.write(str(src), arcname=arcname)

            if replacing:
                self._replace_zip_member(src, arcname, self._in_zip)
        finally:
            shutil.rmtree(src.parent)

    def _replace_zip_member(self, src: Path, arcname: str, in_zip: Path) -> None:
        """rewrites the archive with src in place of the member arcname

        Parameters
        ----------
        src
            path to the file holding the new content
        arcname
            name of the member it replaces
        in_zip
            path to the archive being rewritten

        Notes
        -----
        A zip has no way to take a member out, so appending under a name
        already in the archive leaves two entries under that name. The
        one a reader finds then depends on whether it works from the
        central directory or from the local headers, and open_ refuses
        the archive outright for holding more than one record. The
        archive is rewritten instead, and put in place with a rename so
        that a failure part way through leaves the original untouched.

        The rename gives the archive a new inode, so a hard link to it
        goes stale. The mode is carried across, which the umask default
        on the new file would otherwise widen, and a symlinked archive
        is written through to its target rather than replaced by a
        plain file.
        """
        from zipfile import ZipFile

        rewritten = in_zip.parent / f"{uuid.uuid4()}.zip"

        # every other member is decompressed on the way across, so this
        # can fail on the content of a member that has nothing to do
        # with the one being replaced: BadZipFile for a bad CRC,
        # RuntimeError for an encrypted member, OSError for the disk.
        # That list is what has been seen rather than what can happen,
        # so the cleanup is registered rather than hung off it. It
        # covers the put-back as well, so that nothing is left beside
        # the original until the rename has taken. Once that succeeds
        # the path no longer exists and the callback would be a no-op,
        # but it is dropped rather than relied on to do nothing
        with contextlib.ExitStack() as cleanup:
            cleanup.callback(rewritten.unlink, missing_ok=True)
            with (
                ZipFile(in_zip) as existing,
                ZipFile(rewritten, "w") as out,
            ):
                out.comment = existing.comment
                for info in existing.infolist():
                    if info.filename == arcname:
                        continue
                    # the member is copied through its ZipInfo so that
                    # its name, timestamp and compression survive. the
                    # reader is opened first because opening the writer
                    # zeroes the sizes on the ZipInfo it is handed
                    with existing.open(info) as member, out.open(info, "w") as dest:
                        shutil.copyfileobj(member, dest)

                out.write(str(src), arcname=arcname)

            target = in_zip.resolve() if in_zip.is_symlink() else in_zip
            shutil.copymode(target, rewritten)
            rewritten.replace(target)
            cleanup.pop_all()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._file is None:
            msg = "file object is unexpectedly None"
            raise ValueError(msg)
        self._file.close()
        if exc_type is None:
            self._close_func(self._tmppath)
            self.succeeded = True
        else:
            self.succeeded = False
            shutil.rmtree(self._tmppath.parent)

    def write(self, text: str | bytes) -> None:
        """writes text to file"""
        fileobj = self._get_fileobj()
        fileobj.write(text)

    def close(self) -> None:
        """closes file"""
        self.__exit__(None, None, None)


def get_format_suffixes(filename: PathType) -> tuple[str | None, str | None]:
    """returns file, compression suffixes"""
    filename = Path(filename)
    if not filename.suffix:
        return None, None

    suffixes = [_wout_period.sub("", sfx).lower() for sfx in filename.suffixes[-2:]]
    cmp_suffix = suffixes[-1] if suffixes[-1] in _compression_handlers else None

    if len(suffixes) == 2 and cmp_suffix is not None:
        suffix = suffixes[0]
    elif cmp_suffix is None:
        suffix = suffixes[-1]
    else:
        suffix = None
    return suffix, cmp_suffix


def path_exists(path: PathType) -> bool:
    """whether path is a valid path and it exists"""
    with contextlib.suppress(Exception):
        return Path(path).exists()
    return False


class _DecodedReader:
    """reads a byte stream in byte-sized chunks and decodes them

    Notes
    -----
    read(n) on a text file object returns n characters, so under a
    multi-byte encoding it takes several times n bytes of the file. A
    reader working to a memory budget wants the budget spent on bytes,
    so the bytes are read here and decoded on the way past.

    Line endings are not translated, where a text file object would
    turn "\\r\\n" into "\\n". It makes no difference to the lines that
    come out, since str.splitlines() breaks on "\\r", "\\n" and "\\r\\n"
    alike and drops the terminator either way.
    """

    def __init__(self, infile: IO[bytes], encoding: str | None) -> None:
        """
        Parameters
        ----------
        infile
            open file object in a binary mode
        encoding
            name of the encoding, None meaning the locale default, as
            it does for builtin open
        """
        self._infile = infile
        # the test is against None rather than falsiness, so that an
        # empty encoding reaches the codec lookup and is rejected there,
        # as it is on the other paths through this module
        if encoding is None:
            encoding = locale.getpreferredencoding(do_setlocale=False)

        self._decoder = codecs.getincrementaldecoder(encoding)()

    def read(self, size: int = -1) -> str:
        """returns the text decoded from the next size bytes

        Parameters
        ----------
        size
            number of bytes to read, a negative meaning read it all

        Notes
        -----
        A chunk ending part way through a character decodes to the
        empty string, with the bytes held by the decoder until the rest
        of the character arrives. That is not the end of the file, so
        reading continues until there is either text to return or
        nothing left to read. Returning the empty string instead would
        be read as the end of the file and would silently truncate.

        A stateful encoding can hold far more than one character back.
        A run of iso-2022-jp escape sequences decodes to nothing at
        all, so a read of that run returns only once past it, having
        taken more than size bytes to do so.
        """
        if size < 0:
            return self._decoder.decode(self._infile.read(), final=True)

        while True:
            raw = self._infile.read(size)
            text = self._decoder.decode(raw, final=not raw)
            if text or not raw:
                return text


def _chunk_size_for(path: Path, chunk_size: int | None) -> int | None:
    """returns None where path is small enough to read in one go

    Parameters
    ----------
    path
        file the chunk size is for
    chunk_size
        number of bytes to read in one go, None meaning read it all

    Notes
    -----
    st_size of a compressed file is the compressed size, which says
    nothing about how much comes out of it, so the shortcut is only
    taken where the two are the same number. A file that compresses
    below the budget can decompress to any size at all.
    """
    if chunk_size is None:
        return None

    _, compression = get_format_suffixes(path)
    if compression is None and path.stat().st_size < chunk_size:
        return None

    return chunk_size


def _check_chunk_size(chunk_size: int | None) -> None:
    """raises if chunk_size is not a usable number of bytes

    Parameters
    ----------
    chunk_size
        number of bytes to read in one go, None meaning read it all

    Raises
    ------
    ValueError
        if chunk_size is not a positive whole number, None excepted

    Notes
    -----
    Zero and a negative fail in opposite directions, and neither says
    so. read(0) returns an empty string, which a read loop cannot tell
    from the end of the file, so nothing at all is yielded for a file
    that plainly has contents. read(-1) reads to the end, so the whole
    file arrives in one chunk, which is what None already means and the
    opposite of the bounded memory the argument exists to ask for.

    A fraction is rejected too. read() raises TypeError for one, but
    the same value reaching num_lines has nothing downstream to catch
    it, so both are refused in the same terms.
    """
    if chunk_size is not None and (chunk_size <= 0 or chunk_size % 1):
        msg = f"chunk_size must be a positive whole number of bytes, not {chunk_size!r}"
        raise ValueError(msg)


def _splitlines(
    infile: IO[_StrOrBytes],
    chunk_size: int | None,
    sep: _Separators[_StrOrBytes],
) -> Iterator[_StrOrBytes]:
    """yields lines from an open file object

    Parameters
    ----------
    infile
        open file object, in text or binary mode
    chunk_size
        number of bytes to load in one go, None means read it all
    sep
        line separators matching the mode of infile

    Notes
    -----
    No line ending translation is assumed of the reader. A carriage
    return reaches here in either mode, so a "\\r\\n" split by a chunk
    boundary has to be stitched back together rather than counted as
    two line endings. A reader that does translate, such as a file
    object opened in a text mode, simply never presents the case.
    """
    # fragments of a line that spans a chunk boundary, joined only
    # when the line is complete and about to be yielded
    pending: list[_StrOrBytes] = []
    # an empty line, held back until a later line proves it was not the
    # last one, as a trailing empty line is not yielded
    held: _StrOrBytes | None = None
    # whether the previous chunk ended on a carriage return, which may
    # be the first half of a "\r\n" split by the chunk boundary
    split_return = False
    while True:
        data = infile.read() if chunk_size is None else infile.read(chunk_size)
        if not data:  # end of file
            break

        lines = data.splitlines()
        if split_return and data.startswith(sep.newline):
            # this newline completes the line ending started by the
            # previous chunk, it does not begin a new line
            del lines[0]

        split_return = data.endswith(sep.carriage_return)
        # a chunk ending on a line boundary has no unfinished last line,
        # otherwise that line continues into the next chunk
        tail = None if data[-1:] in sep.boundaries else lines.pop(-1)

        if lines and pending:
            pending.append(lines[0])
            lines[0] = sep.empty.join(pending)
            pending.clear()

        if lines:
            if held is not None:
                # these lines follow it, so it was not the last one
                yield held
                held = None

            if not lines[-1]:
                held = lines.pop(-1)

            yield from lines

        if tail is not None:
            pending.append(tail)

    if pending:
        if held is not None:
            yield held
        yield sep.empty.join(pending)


@overload
def iter_splitlines(
    path: PathType,
    chunk_size: int | None = ...,
    *,
    as_bytes: Literal[False] = ...,
) -> Iterator[str]: ...


@overload
def iter_splitlines(
    path: PathType,
    chunk_size: int | None = ...,
    *,
    as_bytes: Literal[True],
) -> Iterator[bytes]: ...


@overload
def iter_splitlines(
    path: PathType,
    chunk_size: int | None = ...,
    *,
    as_bytes: bool,
) -> Iterator[str | bytes]: ...


def iter_splitlines(
    path: PathType,
    chunk_size: int | None = 1_000_000,
    *,
    as_bytes: bool = False,
) -> Iterator[Any]:
    """yields line from file

    Parameters
    ----------
    path
        data file
    chunk_size
        number of bytes to load in one go from path
    as_bytes
        if True, lines are returned as bytes and path is opened in
        binary mode, otherwise lines are returned as str

    Raises
    ------
    ValueError
        if chunk_size is not a positive whole number, None excepted.
        Raised on the first iteration, as any error inside a generator
        function is, and before path is looked at, so a bad argument is
        reported as one rather than as a missing file

    Notes
    -----
    Loads chunks of data from the file, yields one line at a time.

    An empty last line is not yielded, so a file ending on a line
    terminator gives the same lines as one that does not.

    chunk_size counts bytes read in one go, in both modes and whether
    or not the file is compressed. Text is read as bytes and decoded on
    the way past, rather than read through a text file object, whose
    read(n) returns n characters and so takes several times n bytes of
    a file in a multi-byte encoding. For a compressed file the count is
    of the bytes coming out of the decompression, which is the memory
    the read costs, rather than of the bytes on disk.

    The two modes do not always split a file into the same number of
    lines. str.splitlines() breaks on "\\r", "\\n" and "\\r\\n" and on a
    further eight characters including vertical tab and form feed.
    bytes.splitlines() breaks only on "\\r", "\\n" and "\\r\\n".
    """
    _check_chunk_size(chunk_size)

    url = is_url(path)
    if url:
        chunk_size = None
    else:
        path = Path(path).expanduser()
        chunk_size = _chunk_size_for(path, chunk_size)

    if as_bytes:
        with open_(path, mode="rb") as infile:
            # open_ is typed as returning IO[Any], the cast binds the
            # type variable so a mismatched separator is a type error
            binary = cast("IO[bytes]", infile)
            yield from _splitlines(binary, chunk_size, _BINARY_SEPARATORS)
    elif url:
        # a url is read whole, so there is no budget to keep to, and the
        # charset from the response headers that open_url uses is better
        # evidence than a sniff of the content
        with open_(path) as infile:
            yield from _splitlines(
                cast("IO[str]", infile), chunk_size, _TEXT_SEPARATORS
            )
    else:
        encoding = _detect_encoding(path)
        with open_(path, mode="rb") as infile:
            reader = _DecodedReader(cast("IO[bytes]", infile), encoding)
            yield from _splitlines(
                cast("IO[str]", reader),
                chunk_size,
                _TEXT_SEPARATORS,
            )


@overload
def iter_line_blocks(
    path: PathType,
    num_lines: int | None = ...,
    chunk_size: int | None = ...,
    *,
    as_bytes: Literal[False] = ...,
) -> Iterator[list[str]]: ...


@overload
def iter_line_blocks(
    path: PathType,
    num_lines: int | None = ...,
    chunk_size: int | None = ...,
    *,
    as_bytes: Literal[True],
) -> Iterator[list[bytes]]: ...


@overload
def iter_line_blocks(
    path: PathType,
    num_lines: int | None = ...,
    chunk_size: int | None = ...,
    *,
    as_bytes: bool,
) -> Iterator[list[str] | list[bytes]]: ...


def iter_line_blocks(
    path: PathType,
    num_lines: int | None = 1000,
    chunk_size: int | None = 5_000_000,
    *,
    as_bytes: bool = False,
) -> Iterator[list[Any]]:
    """yields list of num_lines lines from path

    Parameters
    ----------
    path
        data file
    num_lines
        number of lines per block. If None just returns all lines.
    chunk_size
        number of bytes to load in one go from path
    as_bytes
        if True, lines are returned as bytes and path is opened in
        binary mode, otherwise lines are returned as str

    Raises
    ------
    ValueError
        if num_lines or chunk_size is not a positive whole number,
        None excepted. Raised on the first iteration, as any error
        inside a generator function is

    Notes
    -----
    Lines are produced by iter_splitlines, see its notes for how the
    two modes differ. If num_lines is None the whole file accumulates
    in one block, so peak memory is the size of the file.
    """
    if num_lines is not None and (num_lines <= 0 or num_lines % 1):
        # the block is complete when its length equals num_lines, which
        # never holds for a value that is not a positive whole number,
        # so the whole file came back as one block. That is what
        # num_lines=None asks for and the opposite of what a small
        # number asks for
        msg = f"num_lines must be a positive whole number of lines, not {num_lines!r}"
        raise ValueError(msg)

    lines = []
    for line in iter_splitlines(path, chunk_size=chunk_size, as_bytes=as_bytes):
        lines.append(line)
        if len(lines) == num_lines:
            yield lines
            lines = []

    if lines:
        yield lines


def iter_record_chunks(
    *,
    path: PathType,
    delimiter: bytes,
    chunk_size: int | None = 5_000_000,
) -> Iterator[bytes]:
    """yield bytes between successive occurrences of ``delimiter``

    Parameters
    ----------
    path
        data file. Accepts a path, URL, or any ``PathType`` and opens it
        via ``open_(path, mode="rb")`` so compressed formats are handled
        transparently. If ``path`` is a URL the stream is read in full
        (``chunk_size`` is forced to ``None``).
    delimiter
        bytes delimiter on which records are split. Must be non-empty.
    chunk_size
        bytes read per iteration. If ``None``, or if the file is
        uncompressed and smaller than ``chunk_size``, it is read in a
        single call. A compressed file is read in chunks whatever its
        size on disk, since that size says nothing about how much comes
        out of the decompression.

    Yields
    ------
    bytes
        each item is the content between two successive delimiters. The
        first item is whatever precedes the first delimiter (often
        empty for files that start with a delimiter). The final item is
        whatever follows the last delimiter; callers filter as needed
        for their format.

    Raises
    ------
    ValueError
        if ``delimiter`` is empty, or if ``chunk_size`` is zero or less.

    Notes
    -----
    Reads ``path`` in chunks of ``chunk_size`` bytes and splits on
    ``delimiter``, holding any trailing partial record across chunk
    boundaries so that delimiters spanning a boundary are detected
    correctly. Peak memory is bounded by ``chunk_size`` plus the size
    of the largest record, rather than the full file size.

    Operates on raw bytes only; callers that need text decoding should
    do so per yielded record.

    Examples
    --------
    >>> import tempfile, pathlib
    >>> with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
    ...     _ = f.write(b">a\\nAAA>b\\nBBB>c\\nCCC")
    ...     tmp = pathlib.Path(f.name)
    >>> list(iter_record_chunks(path=tmp, delimiter=b">", chunk_size=8))
    [b'', b'a\\nAAA', b'b\\nBBB', b'c\\nCCC']
    >>> tmp.unlink()
    """
    if not delimiter:
        msg = "delimiter must be non-empty"
        raise ValueError(msg)

    _check_chunk_size(chunk_size)

    if is_url(path):
        chunk_size = None
    else:
        path = Path(path).expanduser()
        chunk_size = _chunk_size_for(path, chunk_size)

    # We accommodate a chunked read falling within a delimiter
    # by extracting the overlap_len of the last (potentially partial)
    # record and prepending it to the next chunk.
    # We only need to keep the last len(delimiter) - 1 bytes, as a delimiter
    # cannot span more than that.
    overlap_len = len(delimiter) - 1
    with open_(path, mode="rb") as infile:
        pending: list[bytes] = []
        # carry represents the portion of the last (potentially partial) record
        # that we need to prepend to the next chunk.
        carry = b""
        while True:
            chunk = infile.read() if chunk_size is None else infile.read(chunk_size)
            if not chunk:
                break

            parts = (carry + chunk).split(delimiter)
            last = parts.pop()
            cut = max(len(last) - overlap_len, 0)
            carry = last[cut:]
            for part in parts:
                pending.append(part)
                yield b"".join(pending)
                pending.clear()

            if cut:
                pending.append(last[:cut])

        if pending or carry:
            pending.append(carry)
            yield b"".join(pending)

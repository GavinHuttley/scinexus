from __future__ import annotations

import contextlib
import functools
import re
import shutil
import uuid
from bz2 import open as bzip_open
from gzip import open as gzip_open
from io import TextIOWrapper
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


def open_zip(filename: PathType, mode: str = "r", **kwargs: Any) -> IO[Any]:
    """open a single member zip-compressed file

    Parameters
    ----------
    filename
        path to the archive
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
    # import of standard library io module as some code quality tools
    # confuse this with a circular import
    mode = mode or "r"
    binary_mode = "b" in mode
    mode = mode[:1]

    encoding = kwargs.pop("encoding", None)
    if binary_mode:
        _check_binary_mode_args(encoding, kwargs)

    if mode.startswith("w"):
        # mode has been truncated to its first letter, so put the b back
        # for a binary write. what is left goes to the file the writes
        # land in, under its own argument rather than splatted, so that
        # a name like tmpdir cannot bind to a parameter of atomic_write
        write_mode = "wb" if binary_mode else mode
        return atomic_write(  # type: ignore[return-value]
            filename,
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

        try:
            return TextIOWrapper(opened, encoding=encoding, **text_kwargs)
        except Exception:
            # an unknown codec or an illegal newline is rejected by the
            # wrapper, leaving the member open with no handle on it
            opened.close()
            raise


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
        with op(filename, mode="rb") as infile:
            data = infile.read(100)

        encoding = detect(data)
        encoding = encoding["encoding"]

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
    try:
        if binary_mode:
            return _decompressed(response, compression)

        if encoding is None:
            encoding = response.headers.get_content_charset()

        return TextIOWrapper(
            _decompressed(response, compression),
            encoding=encoding,
            **kwargs,
        )
    except Exception:
        # the wrapper rejects an unknown codec or an illegal newline
        # after the request has been made, and the caller gets an
        # exception rather than the object that would have closed it
        response.close()
        raise


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
            try:
                self._file = open_(
                    self._tmppath,
                    self._mode,
                    encoding=self._encoding,
                    **self._open_kwargs,
                )
            except Exception:
                # an argument the open will not take is only found here,
                # after __init__ has made the directory to write into,
                # and the caller never gets the object whose exit or
                # close would have removed it
                shutil.rmtree(self._tmppath.parent)
                raise

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
        from zipfile import ZipFile

        if self._in_zip is None:
            msg = "in_zip path is unexpectedly None"
            raise RuntimeError(msg)

        with ZipFile(self._in_zip, "a") as out:
            out.write(str(src), arcname=self._path)

        shutil.rmtree(src.parent)

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
    A file opened in text mode has its line endings translated to
    newline by the reader, so a carriage return is only ever seen for a
    file opened in binary mode.
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

    Notes
    -----
    Loads chunks of data from the file, yields one line at a time.

    An empty last line is not yielded, so a file ending on a line
    terminator gives the same lines as one that does not.

    The two modes do not always split a file into the same number of
    lines. Text mode reads with universal newlines, so "\\r", "\\n" and
    "\\r\\n" all become line breaks, and str.splitlines() breaks on a
    further eight characters including vertical tab and form feed.
    Binary mode does no translation and bytes.splitlines() breaks only
    on "\\r", "\\n" and "\\r\\n".
    """
    if is_url(path):
        chunk_size = None
    else:
        path = Path(path).expanduser()
        if chunk_size and path.stat().st_size < chunk_size:
            # file is smaller than provided chunk_size, just
            # load it all
            chunk_size = None

    if as_bytes:
        with open_(path, mode="rb") as infile:
            # open_ is typed as returning IO[Any], the cast binds the
            # type variable so a mismatched separator is a type error
            binary = cast("IO[bytes]", infile)
            yield from _splitlines(binary, chunk_size, _BINARY_SEPARATORS)
    else:
        with open_(path) as infile:
            text = cast("IO[str]", infile)
            yield from _splitlines(text, chunk_size, _TEXT_SEPARATORS)


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

    Notes
    -----
    Lines are produced by iter_splitlines, see its notes for how the
    two modes differ. If num_lines is None the whole file accumulates
    in one block, so peak memory is the size of the file.
    """
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
        bytes read per iteration. If ``None``, or if the on-disk file is
        smaller than ``chunk_size``, the file is read in a single call.

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
        if ``delimiter`` is empty.

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

    if is_url(path):
        chunk_size = None
    else:
        path = Path(path).expanduser()
        if chunk_size and path.stat().st_size < chunk_size:
            chunk_size = None

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

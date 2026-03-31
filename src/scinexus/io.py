import contextlib
import json
import os
import pickle
import typing
import zipfile
from functools import singledispatch
from gzip import compress as gzip_compress
from gzip import decompress as gzip_decompress
from pathlib import Path

from scinexus.deserialise import deserialise_object

from .composable import define_app
from .data_store import (
    READONLY,
    DataStoreABC,
    DataStoreDirectory,
    Mode,
    ReadOnlyDataStoreZipped,
)
from .sqlite_data_store import _MEMORY, DataStoreSqlite

_datastore_reader_map = {}


class register_datastore_reader:
    """
    registration decorator for read only data store classes

    The registration key must be a string that of the file format suffix
    (more than one suffix can be registered at a time).

    Parameters
    ----------
    args: str or sequence of str
        must be unique, a preceding '.' will be added if not already present
    """

    def __init__(self, *args) -> None:
        args = list(args)
        for i, suffix in enumerate(args):
            if suffix is None:
                assert suffix not in _datastore_reader_map, (
                    f"{suffix!r} already in {list(_datastore_reader_map)}"
                )
                continue

            if not isinstance(suffix, str):
                msg = f"{suffix!r} is not a string"
                raise TypeError(msg)

            if suffix.strip() == suffix and not suffix:
                msg = "cannot have white-space suffix"
                raise ValueError(msg)

            suffix = suffix.strip()
            if suffix:
                suffix = suffix if suffix[0] == "." else f".{suffix}"

            assert suffix not in _datastore_reader_map, (
                f"{suffix!r} already in {list(_datastore_reader_map)}"
            )
            args[i] = suffix

        self._type_str = tuple(args)

    def __call__(self, func):
        for type_str in self._type_str:
            _datastore_reader_map[type_str] = func
        return func


# register the main readers
register_datastore_reader("zip")(ReadOnlyDataStoreZipped)
register_datastore_reader(None)(DataStoreDirectory)
register_datastore_reader("sqlitedb")(DataStoreSqlite)


def open_data_store(
    base_path: str | Path,
    suffix: str | None = None,
    limit: int | None = None,
    mode: str | Mode = READONLY,
    **kwargs,
) -> DataStoreABC:
    """returns DataStore instance of a type specified by the path suffix

    Parameters
    ----------
    base_path
        path to directory or db
    suffix
        suffix of filenames
    limit
        the number of matches to return
    mode
        opening mode, either r, w, a as per file opening modes
    """
    mode = Mode(mode)
    if not isinstance(suffix, str | type(None)):
        msg = f"suffix {type(suffix)} not one of string or None"
        raise ValueError(msg)

    kwargs = {"limit": limit, "mode": mode, "suffix": suffix, **kwargs}
    base_path = Path(base_path)
    base_path = (
        base_path if base_path.name == ":memory:" else base_path.expanduser().absolute()
    )
    if base_path.is_dir():
        ds_suffix = None
    elif base_path.suffix == ".sqlitedb" or base_path.name == _MEMORY:
        ds_suffix = ".sqlitedb"
        kwargs.pop("suffix")
    elif zipfile.is_zipfile(base_path):
        ds_suffix = ".zip"
    elif base_path.suffix:
        ds_suffix = base_path.suffix
    else:
        # triggered when mode="w"
        ds_suffix = None

    if base_path.name == _MEMORY and mode is READONLY:
        msg = "in memory readonly sqlitedb"
        raise NotImplementedError(msg)

    if ds_suffix is None and suffix is None:
        msg = "a suffix is required if using a directory data store"
        raise ValueError(msg)

    klass = _datastore_reader_map[ds_suffix]

    return klass(base_path, **kwargs)


@define_app(skip_not_completed=False)
def pickle_it(data: typing.Any) -> bytes:
    """Serialises data using pickle."""
    return pickle.dumps(data)


@define_app(skip_not_completed=False)
def unpickle_it(data: bytes) -> typing.Any:
    "Deserialises pickle data."
    return pickle.loads(data)


@define_app(skip_not_completed=False)
class compress:
    """Compresses bytes data."""

    def __init__(self, compressor: callable = gzip_compress) -> None:
        """
        Parameters
        ----------
        compressor
            function for compressing bytes data, defaults to gzip
        """
        self.compressor = compressor

    def main(self, data: bytes) -> bytes:
        return self.compressor(data)


@define_app(skip_not_completed=False)
class decompress:
    """Decompresses data."""

    def __init__(self, decompressor: callable = gzip_decompress) -> None:
        """
        Parameters
        ----------
        decompressor
            a function for decompression, defaults to the gzip decompress
            function
        """
        self.decompressor = decompressor

    def main(self, data: bytes) -> bytes:
        return self.decompressor(data)


def as_dict(obj: typing.Any) -> dict:
    """returns result of to_rich_dict method if it exists"""
    with contextlib.suppress(AttributeError):
        obj = obj.to_rich_dict()
    return obj


@define_app(skip_not_completed=False)
class to_primitive:
    """convert an object to primitive python types suitable for serialisation"""

    def __init__(self, convertor: callable = as_dict) -> None:
        self.convertor = convertor

    def main(self, data: typing.Any) -> typing.Any:
        """returns dict from an object"""
        return self.convertor(data)


@define_app(skip_not_completed=False)
class from_primitive:
    """deserialises from primitive python types"""

    def __init__(self, deserialiser: callable = deserialise_object) -> None:
        self.deserialiser = deserialiser

    def main(self, data: typing.Any) -> typing.Any:
        """either json or a dict from an object"""
        return self.deserialiser(data)


@define_app
def to_json(data: dict) -> str:
    """Convert primitive python types to json string."""
    return json.dumps(data)


@define_app
def from_json(data: str) -> dict:
    """Convert json string to primitive python types."""
    return json.loads(data)


@singledispatch
def _read_it(path) -> str:
    try:
        data = path.read()
    except AttributeError:
        msg = f"unexpected type {type(path)}"
        raise OSError(msg)
    return data


@_read_it.register
def _(path: os.PathLike) -> str:
    path = path.expanduser().absolute()
    return path.read_text()


@_read_it.register
def _(path: str) -> os.PathLike:
    return _read_it(Path(path))


DEFAULT_DESERIALISER = unpickle_it() + from_primitive()

DEFAULT_SERIALISER = to_primitive() + pickle_it()

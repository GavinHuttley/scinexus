"""scinexus: composable app infrastructure for scientific computing."""

import typing as _typing

from scinexus._version import __version__
from scinexus.composable import (
    AppBase,
    ComposableApp,
    NotCompleted,
    WriterApp,
    define_app,
    is_app,
    is_app_composable,
)

if _typing.TYPE_CHECKING:
    from scinexus.data_store import DataStoreABC

__all__ = [
    "AppBase",
    "ComposableApp",
    "NotCompleted",
    "WriterApp",
    "__version__",
    "define_app",
    "is_app",
    "is_app_composable",
    "open_data_store",
]


def open_data_store(*args, **kwargs) -> "DataStoreABC":
    from scinexus.io import open_data_store as _open_data_store

    return _open_data_store(*args, **kwargs)

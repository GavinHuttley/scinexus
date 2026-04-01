"""scinexus: composable app infrastructure for scientific computing."""

import typing as _typing

from scinexus._version import __version__
from scinexus.composable import (
    AppBase,
    ComposableApp,
    NotCompleted,
    NotCompletedType,
    WriterApp,
    define_app,
    is_app,
    is_app_composable,
)

if _typing.TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from scinexus.data_store import DataStoreABC

__all__ = [
    "AppBase",
    "ComposableApp",
    "NotCompleted",
    "NotCompletedType",
    "WriterApp",
    "__version__",
    "define_app",
    "get_summary_display",
    "is_app",
    "is_app_composable",
    "open_data_store",
    "set_summary_display",
]


def open_data_store(*args, **kwargs) -> "DataStoreABC":
    from scinexus.io import open_data_store as _open_data_store

    return _open_data_store(*args, **kwargs)


def set_summary_display(*args, **kwargs) -> None:
    from scinexus.data_store import set_summary_display as _ssd

    return _ssd(*args, **kwargs)


def get_summary_display() -> "Callable[..., Any] | None":
    from scinexus.data_store import get_summary_display as _gsd

    return _gsd()

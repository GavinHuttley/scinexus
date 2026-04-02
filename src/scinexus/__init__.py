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
from scinexus.progress import Progress

if _typing.TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from typing import Any

    from scinexus.data_store import DataStoreABC
    from scinexus.progress import ProgressType

__all__ = [
    "AppBase",
    "ComposableApp",
    "NotCompleted",
    "NotCompletedType",
    "Progress",
    "WriterApp",
    "__version__",
    "define_app",
    "get_progress",
    "get_summary_display",
    "is_app",
    "is_app_composable",
    "open_data_store",
    "set_default_progress",
    "set_summary_display",
]


def open_data_store(*args: "Any", **kwargs: "Any") -> "DataStoreABC":
    from scinexus.io import open_data_store as _open_data_store

    return _open_data_store(*args, **kwargs)


def set_summary_display(*args: "Any", **kwargs: "Any") -> None:
    from scinexus.data_store import set_summary_display as _ssd

    return _ssd(*args, **kwargs)


def get_summary_display() -> "Callable[..., Any] | None":
    from scinexus.data_store import get_summary_display as _gsd

    return _gsd()


def get_progress(show_progress: "bool | Progress" = False) -> Progress:
    from scinexus.progress import get_progress as _gp

    return _gp(show_progress)


def set_default_progress(
    progress: "ProgressType | Progress | None" = None,
) -> None:
    from scinexus.progress import set_default_progress as _sdp

    _sdp(progress)

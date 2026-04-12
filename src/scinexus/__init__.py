"""scinexus: composable app infrastructure for scientific computing."""

import typing as _typing

from scinexus._version import __version__
from scinexus.composable import (
    AppBase,
    ComposableApp,
    LoaderApp,
    NonComposableApp,
    NotCompleted,
    NotCompletedType,
    WriterApp,
    define_app,
    is_app,
    is_app_composable,
)
from scinexus.progress import (
    Progress,
    ProgressContext,
    get_progress,
    set_default_progress,
)

if _typing.TYPE_CHECKING:  # pragma: no cover
    from typing import Any

__all__ = [
    "AppBase",
    "ComposableApp",
    "LoaderApp",
    "NonComposableApp",
    "NotCompleted",
    "NotCompletedType",
    "Progress",
    "ProgressContext",
    "WriterApp",
    "__version__",
    "define_app",
    "get_id_from_source",
    "get_progress",
    "get_summary_display",
    "is_app",
    "is_app_composable",
    "open_",
    "open_data_store",
    "set_default_progress",
    "set_id_from_source",
    "set_summary_display",
]

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "open_": ("scinexus.io_util", "open_"),
    "open_data_store": ("scinexus.io", "open_data_store"),
    "set_summary_display": ("scinexus.data_store", "set_summary_display"),
    "get_summary_display": ("scinexus.data_store", "get_summary_display"),
    "set_id_from_source": ("scinexus.data_store", "set_id_from_source"),
    "get_id_from_source": ("scinexus.data_store", "get_id_from_source"),
}


def __getattr__(name: str) -> "Any":
    if name in _LAZY_IMPORTS:
        module_path, attr = _LAZY_IMPORTS[name]
        import importlib

        mod = importlib.import_module(module_path)
        return getattr(mod, attr)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

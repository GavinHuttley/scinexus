"""Deprecated module — use ``scinexus.progress`` instead.

All public names in this module emit ``DeprecationWarning`` on use.
Scheduled for removal in version 2027.1.
"""

from __future__ import annotations

import functools
import threading
from typing import TYPE_CHECKING, ParamSpec, TypeVar

from scinexus.warning import deprecated, deprecated_callable

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from typing import Any

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

_REMOVAL_VERSION = "2026.9"


class LogFileOutput:  # pragma: no cover
    """Deprecated — no replacement, use ``NoProgress`` instead."""

    @deprecated_callable(
        version=_REMOVAL_VERSION,
        reason="use scinexus.progress.NoProgress instead",
        is_discontinued=True,
    )
    def __init__(self, **kw: Any) -> None:  # pragma: no cover
        pass


class ProgressContext:  # pragma: no cover
    """Deprecated — use ``scinexus.progress.TqdmProgress`` instead."""

    @deprecated_callable(
        version=_REMOVAL_VERSION,
        reason="use scinexus.progress.TqdmProgress instead",
        new="scinexus.progress.TqdmProgress",
    )
    def __init__(
        self,
        progress_bar_type: Any = None,
        depth: int = -1,
        message: str | None = None,
        mininterval: float = 1.0,
    ) -> None:
        pass


class NullContext:  # pragma: no cover
    """Deprecated — use ``scinexus.progress.NoProgress`` instead."""

    @deprecated_callable(
        version=_REMOVAL_VERSION,
        reason="use scinexus.progress.NoProgress instead",
        new="scinexus.progress.NoProgress",
    )
    def __init__(self) -> None:
        pass


@deprecated_callable(
    version=_REMOVAL_VERSION,
    reason="use scinexus.progress.get_progress instead",
    new="scinexus.progress.get_progress",
)
def display_wrap(slow_function: Callable[P, R]) -> Callable[P, R]:  # pragma: no cover
    """Deprecated — use ``get_progress`` instead of the decorator pattern."""

    @functools.wraps(slow_function)
    def f(*args: P.args, **kw: P.kwargs) -> R:
        kw.pop("show_progress", None)
        return slow_function(*args, **kw)

    return f


CURRENT = threading.local()


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "NULL_CONTEXT":
        deprecated(
            "module",
            "NULL_CONTEXT",
            "scinexus.progress.NoProgress()",
            _REMOVAL_VERSION,
            "use scinexus.progress.NoProgress() instead",
        )
        from scinexus.progress import NoProgress

        return NoProgress()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

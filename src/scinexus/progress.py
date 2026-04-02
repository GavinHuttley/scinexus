"""Generic progress bar framework with pluggable backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, TypeVar

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Iterator
    from typing import Any

T = TypeVar("T")

ProgressType = Literal["tqdm", "rich"]


class Progress(ABC):
    """A callable that wraps an iterable to display progress."""

    @abstractmethod
    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]: ...

    @abstractmethod
    def child(self) -> Progress: ...


class NoProgress(Progress):
    """A no-op progress wrapper that passes through the iterable unchanged."""

    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]:
        yield from iterable

    def child(self) -> NoProgress:
        return self


class TqdmProgress(Progress):
    """Progress wrapper using tqdm.auto (handles TTY, Jupyter, etc.)."""

    def __init__(
        self,
        position: int = 0,
        mininterval: float = 0.1,
        bar_format: str | None = None,
        dynamic_ncols: bool = True,
        **tqdm_kwargs: Any,
    ) -> None:
        self._position = position
        self._mininterval = mininterval
        self._bar_format = bar_format
        self._dynamic_ncols = dynamic_ncols
        self._tqdm_kwargs = tqdm_kwargs

    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]:
        from tqdm.auto import tqdm  # type: ignore[import-untyped]

        bar = tqdm(
            iterable,
            total=total,
            desc=msg,
            position=self._position,
            leave=self._position == 0,
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            dynamic_ncols=self._dynamic_ncols,
            **self._tqdm_kwargs,
        )
        try:
            yield from bar
        finally:
            bar.close()

    def child(self) -> TqdmProgress:
        return TqdmProgress(
            position=self._position + 1,
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            dynamic_ncols=self._dynamic_ncols,
            **self._tqdm_kwargs,
        )


class RichProgress(Progress):
    """Progress wrapper using the rich library (requires ``pip install scinexus[rich]``)."""

    def __init__(
        self,
        progress: Any = None,
        refresh_per_second: float = 10.0,
        disable: bool = False,
    ) -> None:
        self._progress = progress
        self._refresh_per_second = refresh_per_second
        self._disable = disable

    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]:
        from rich.progress import (  # type: ignore[import-not-found]
            Progress as RProgress,
        )

        if self._progress is None:
            self._progress = RProgress(
                refresh_per_second=self._refresh_per_second,
                disable=self._disable,
            )
            self._progress.start()
        task = self._progress.add_task(msg, total=total)
        try:
            for item in iterable:
                yield item
                self._progress.advance(task)
        finally:
            self._progress.remove_task(task)

    def child(self) -> RichProgress:
        return RichProgress(
            progress=self._progress,
            refresh_per_second=self._refresh_per_second,
            disable=self._disable,
        )


_default_progress: Progress | None = None


def set_default_progress(progress: ProgressType | Progress | None = None) -> None:
    """Set the default Progress used when ``show_progress=True``.

    Parameters
    ----------
    progress
        A ``Progress`` instance, a string literal (``"tqdm"`` or ``"rich"``),
        or ``None`` to reset to the default (``TqdmProgress``).
    """
    global _default_progress  # noqa: PLW0603

    if progress is None or isinstance(progress, Progress):
        _default_progress = progress
    elif progress == "tqdm":
        _default_progress = TqdmProgress()
    elif progress == "rich":
        _default_progress = RichProgress()
    else:
        msg = f"unknown progress type {progress!r}, expected 'tqdm', 'rich', or a Progress instance"
        raise ValueError(msg)


def get_progress(show_progress: bool | Progress = False) -> Progress:
    """Resolve a ``show_progress`` argument into a ``Progress`` instance.

    Parameters
    ----------
    show_progress
        If a ``Progress`` instance, returned as-is. If ``True``, returns the
        module default (set via ``set_default_progress``, or ``TqdmProgress``).
        If falsy, returns ``NoProgress``.
    """
    if isinstance(show_progress, Progress):
        return show_progress
    if not show_progress:
        return NoProgress()
    return _default_progress if _default_progress is not None else TqdmProgress()

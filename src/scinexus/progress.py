"""Generic progress bar framework with pluggable backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, Literal, TypeVar

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Iterator
    from typing import Any, Self

T = TypeVar("T")

ProgressType = Literal["tqdm", "rich"]


class ProgressContext(ABC):
    """A handle for push-based progress updates."""

    def __init__(self, start: float = 0.0, end: float = 1.0) -> None:
        """
        Parameters
        ----------
        start
            start of the progress range (0.0-1.0)
        end
            end of the progress range (0.0-1.0)
        """
        self._start = start
        self._end = end

    def _map(self, progress: float) -> float:
        return self._start + progress * (self._end - self._start)

    @abstractmethod
    def update(self, *, progress: float, msg: str = "") -> None:
        """Update the progress bar.

        Parameters
        ----------
        progress
            fraction complete [0.0, 1.0], mapped to [start, end] range
        msg
            description text to display
        """

    def close(self) -> None:
        """Close the progress bar. Override in subclasses with cleanup."""

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


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

    @abstractmethod
    def context(
        self,
        *,
        msg: str = "",
        start: float = 0.0,
        end: float = 1.0,
    ) -> ProgressContext:
        """Create a push-based progress context.

        Parameters
        ----------
        msg
            initial description
        start
            start of the progress range (0.0-1.0)
        end
            end of the progress range (0.0-1.0)
        """


class _NoOpContext(ProgressContext):
    """A no-op progress context that silently ignores updates."""

    def update(self, *, progress: float, msg: str = "") -> None:
        pass


_NO_OP_CONTEXT = _NoOpContext()


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
        """Return self since no-op progress needs no nesting."""
        return self

    def context(
        self,
        *,
        msg: str = "",
        start: float = 0.0,
        end: float = 1.0,
    ) -> ProgressContext:
        """Return a shared no-op context that silently ignores updates."""
        return _NO_OP_CONTEXT


class _TqdmContext(ProgressContext):
    """Push-based progress context backed by a tqdm bar."""

    def __init__(self, bar: Any, start: float, end: float) -> None:
        super().__init__(start, end)
        self._bar = bar

    def update(self, *, progress: float, msg: str = "") -> None:
        self._bar.n = self._map(progress)
        if msg:
            self._bar.set_description(msg)
        self._bar.refresh()

    def close(self) -> None:
        self._bar.close()


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
        """
        Parameters
        ----------
        position
            cursor position for the progress bar
        mininterval
            minimum update interval in seconds
        bar_format
            custom bar format string passed to tqdm
        dynamic_ncols
            whether to dynamically resize the bar to terminal width
        **tqdm_kwargs
            additional keyword arguments forwarded to tqdm
        """
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
        """Return a child TqdmProgress at the next cursor position."""
        return TqdmProgress(
            position=self._position + 1,
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            dynamic_ncols=self._dynamic_ncols,
            **self._tqdm_kwargs,
        )

    def context(
        self,
        *,
        msg: str = "",
        start: float = 0.0,
        end: float = 1.0,
    ) -> ProgressContext:
        """Create a tqdm-backed push-based progress context."""
        from tqdm.auto import tqdm  # type: ignore[import-untyped]

        bar = tqdm(
            total=1.0,
            desc=msg,
            position=self._position,
            leave=self._position == 0,
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            dynamic_ncols=self._dynamic_ncols,
            **self._tqdm_kwargs,
        )
        return _TqdmContext(bar, start, end)


class _RichContext(ProgressContext):
    """Push-based progress context backed by a rich progress bar."""

    def __init__(self, progress: Any, task: Any, start: float, end: float) -> None:
        super().__init__(start, end)
        self._progress = progress
        self._task = task

    def update(self, *, progress: float, msg: str = "") -> None:
        self._progress.update(
            self._task, completed=self._map(progress), description=msg or None
        )

    def close(self) -> None:
        self._progress.remove_task(self._task)


class RichProgress(Progress):
    """Progress wrapper using the rich library (requires ``pip install scinexus[rich]``)."""

    def __init__(
        self,
        progress: Any = None,
        refresh_per_second: float = 10.0,
        disable: bool = False,
        **rich_kwargs: Any,
    ) -> None:
        """
        Parameters
        ----------
        progress
            an existing ``rich.progress.Progress`` instance, or None to
            create one on first call
        refresh_per_second
            how often to refresh the display
        disable
            whether to disable progress output
        **rich_kwargs
            additional keyword arguments forwarded to ``rich.progress.Progress``
        """
        self._progress = progress
        self._refresh_per_second = refresh_per_second
        self._disable = disable
        self._rich_kwargs = rich_kwargs

    def _ensure_progress(self) -> Any:
        from rich.progress import (  # type: ignore[import-not-found]
            Progress as RProgress,
        )

        if self._progress is None:
            self._progress = RProgress(
                refresh_per_second=self._refresh_per_second,
                disable=self._disable,
                **self._rich_kwargs,
            )
            self._progress.start()
        return self._progress

    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]:
        rp = self._ensure_progress()
        task = rp.add_task(msg, total=total)
        try:
            for item in iterable:
                yield item
                rp.advance(task)
        finally:
            rp.remove_task(task)

    def child(self) -> RichProgress:
        """Return a child RichProgress sharing the same display."""
        return RichProgress(
            progress=self._progress,
            refresh_per_second=self._refresh_per_second,
            disable=self._disable,
            **self._rich_kwargs,
        )

    def context(
        self,
        *,
        msg: str = "",
        start: float = 0.0,
        end: float = 1.0,
    ) -> ProgressContext:
        """Create a rich-backed push-based progress context."""
        rp = self._ensure_progress()
        task = rp.add_task(msg, total=1.0)
        return _RichContext(rp, task, start, end)


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

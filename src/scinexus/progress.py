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
    def child(self, *, leave: bool | None = None) -> Progress: ...

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

    def child(self, *, leave: bool | None = None) -> NoProgress:
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
        bar_format: str
        | None = "{desc}: {bar} {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        dynamic_ncols: bool = True,
        leave: bool | None = None,
        colour: str | None = None,
        bar_width: int | None = 60,
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
        leave
            whether the bar persists after completion. ``None`` uses
            position-based logic (persist at position 0, clear otherwise).
        colour
            bar colour, forwarded to tqdm
        bar_width
            total width of the progress bar in characters
        **tqdm_kwargs
            additional keyword arguments forwarded to tqdm
        """
        self._position = position
        self._mininterval = mininterval
        self._bar_format = bar_format
        self._dynamic_ncols = dynamic_ncols
        self._leave = leave
        self._colour = colour
        self._bar_width = bar_width
        self._tqdm_kwargs = tqdm_kwargs
        self._bar: Any = None
        self._children: list[TqdmProgress] = []

    def _resolve_leave(self) -> bool:
        return self._leave if self._leave is not None else self._position == 0

    def _make_bar(self, *, total: float | None, msg: str) -> Any:
        from tqdm.auto import tqdm  # type: ignore[import-untyped]

        ncols_kwargs: dict[str, Any] = (
            {"ncols": self._bar_width, "dynamic_ncols": False}
            if self._bar_width is not None
            else {"dynamic_ncols": self._dynamic_ncols}
        )
        return tqdm(
            total=total,
            desc=msg,
            position=self._position,
            leave=self._resolve_leave(),
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            colour=self._colour,
            **ncols_kwargs,
            **self._tqdm_kwargs,
        )

    def __call__(
        self,
        iterable: Iterable[T],
        *,
        total: int | None = None,
        msg: str = "",
    ) -> Iterator[T]:
        if self._bar is None:
            self._bar = self._make_bar(total=total, msg=msg)
        else:
            self._bar.n = 0
            self._bar.last_print_n = 0
            self._bar.total = total
            if msg:
                self._bar.set_description(msg)
        for item in iterable:
            yield item
            self._bar.update(1)
        self._bar.refresh()

    def __del__(self) -> None:
        try:
            for child in reversed(self._children):
                if child._bar is not None:
                    child._bar.close()
                    child._bar = None
            if self._bar is not None:
                self._bar.close()
        except Exception:  # noqa: BLE001
            pass

    def child(self, *, leave: bool | None = None) -> TqdmProgress:
        """Return a child TqdmProgress at the next cursor position.

        Parameters
        ----------
        leave
            whether the child bar persists after completion. ``None``
            inherits the parent setting.
        """
        child = TqdmProgress(
            position=self._position + 1,
            mininterval=self._mininterval,
            bar_format=self._bar_format,
            dynamic_ncols=self._dynamic_ncols,
            leave=leave if leave is not None else self._leave,
            colour=self._colour,
            bar_width=self._bar_width,
            **self._tqdm_kwargs,
        )
        self._children.append(child)
        return child

    def context(
        self,
        *,
        msg: str = "",
        start: float = 0.0,
        end: float = 1.0,
    ) -> ProgressContext:
        """Create a tqdm-backed push-based progress context."""
        bar = self._make_bar(total=1.0, msg=msg)
        return _TqdmContext(bar, start, end)


class _RichContext(ProgressContext):
    """Push-based progress context backed by a rich progress bar."""

    def __init__(
        self,
        progress: Any,
        task: Any,
        start: float,
        end: float,
        *,
        leave: bool,
    ) -> None:
        super().__init__(start, end)
        self._progress = progress
        self._task = task
        self._leave = leave

    def update(self, *, progress: float, msg: str = "") -> None:
        self._progress.update(
            self._task, completed=self._map(progress), description=msg or None
        )

    def close(self) -> None:
        if self._leave:
            self._progress.update(self._task, completed=1.0)
        else:
            self._progress.remove_task(self._task)


class RichProgress(Progress):
    """Progress wrapper using the rich library (requires ``pip install scinexus[rich]``)."""

    def __init__(
        self,
        progress: Any = None,
        refresh_per_second: float = 10.0,
        disable: bool = False,
        leave: bool = False,
        colour: str | None = None,
        bar_width: int | None = 60,
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
        leave
            whether completed bars persist in the display
        colour
            bar colour, applied when auto-creating the display
        bar_width
            width of the progress bar portion in characters
        **rich_kwargs
            additional keyword arguments forwarded to ``rich.progress.Progress``
        """
        self._progress = progress
        self._owns_progress = progress is None
        self._refresh_per_second = refresh_per_second
        self._disable = disable
        self._leave = leave
        self._colour = colour
        self._bar_width = bar_width
        self._rich_kwargs = rich_kwargs
        self._task: Any = None
        self._last_total: int | None = None
        self._children: list[RichProgress] = []

    def _ensure_progress(self) -> Any:
        if self._progress is None:
            from rich.progress import (  # type: ignore[import-not-found]
                BarColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )
            from rich.progress import (
                Progress as RProgress,
            )

            bar_kwargs: dict[str, Any] = {}
            if self._bar_width is not None:
                bar_kwargs["bar_width"] = self._bar_width
            if self._colour is not None:
                bar_kwargs["complete_style"] = self._colour
                bar_kwargs["finished_style"] = self._colour
            bar_column = BarColumn(**bar_kwargs)
            self._progress = RProgress(
                TextColumn("[progress.description]{task.description}"),
                bar_column,
                TimeElapsedColumn(),
                TimeRemainingColumn(),
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
        if self._task is None:
            self._task = rp.add_task(msg, total=total)
        else:
            rp.reset(self._task, total=total, description=msg)
        self._last_total = total
        for item in iterable:
            yield item
            rp.advance(self._task)
        rp.refresh()

    def _cleanup_task(self) -> None:
        if self._task is not None and self._progress is not None:
            if self._leave and self._last_total is not None:
                self._progress.update(self._task, completed=self._last_total)
            else:
                self._progress.remove_task(self._task)
            self._task = None

    def __del__(self) -> None:
        try:
            for child in reversed(self._children):
                child._cleanup_task()
            self._cleanup_task()
            if self._owns_progress and self._progress is not None and not self._disable:
                self._progress.stop()
                self._progress.console.file.write("\n")
                self._progress.console.file.flush()
        except Exception:  # noqa: BLE001
            pass

    def child(self, *, leave: bool | None = None) -> RichProgress:
        """Return a child RichProgress sharing the same display.

        Parameters
        ----------
        leave
            whether the child bar persists after completion. ``None``
            inherits the parent setting.
        """
        child = RichProgress(
            progress=self._ensure_progress(),
            refresh_per_second=self._refresh_per_second,
            disable=self._disable,
            leave=leave if leave is not None else self._leave,
            colour=self._colour,
            bar_width=self._bar_width,
            **self._rich_kwargs,
        )
        self._children.append(child)
        return child

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
        return _RichContext(rp, task, start, end, leave=self._leave)


_default_progress: Progress | None = None


def set_default_progress(
    progress: ProgressType | Progress | None = None, **kwargs: Any
) -> None:
    """Set the default Progress used when ``show_progress=True``.

    Parameters
    ----------
    progress
        A ``Progress`` instance, a string literal (``"tqdm"`` or ``"rich"``),
        or ``None`` to reset to the default (``TqdmProgress``).
    **kwargs
        additional keyword arguments forwarded to the backend constructor
        when ``progress`` is ``"tqdm"`` or ``"rich"``
    """
    global _default_progress  # noqa: PLW0603

    if progress is None or isinstance(progress, Progress):
        _default_progress = progress
    elif progress == "tqdm":
        _default_progress = TqdmProgress(**kwargs)
    elif progress == "rich":
        _default_progress = RichProgress(**kwargs)
    else:
        msg = f"unknown progress type {progress!r}, expected 'tqdm', 'rich', or a Progress instance"
        raise ValueError(msg)


def get_progress(show_progress: bool | Progress = False, **kwargs: Any) -> Progress:
    """Resolve a ``show_progress`` argument into a ``Progress`` instance.

    Parameters
    ----------
    show_progress
        If a ``Progress`` instance, returned as-is. If ``True``, returns the
        module default (set via ``set_default_progress``, or ``TqdmProgress``).
        If falsy, returns ``NoProgress``.
    **kwargs
        additional keyword arguments forwarded to the backend constructor.
        When a default has been set via ``set_default_progress``, a new
        instance of the same type is created with these kwargs. Ignored
        when ``show_progress`` is a ``Progress`` instance.
    """
    if isinstance(show_progress, Progress):
        return show_progress
    if not show_progress:
        return NoProgress()
    if _default_progress is not None:
        if kwargs:
            return type(_default_progress)(**kwargs)
        return _default_progress
    return TqdmProgress(**kwargs)

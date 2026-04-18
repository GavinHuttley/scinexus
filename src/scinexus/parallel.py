from __future__ import annotations

import concurrent.futures as concurrentfutures
import multiprocessing
import os
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import Sized
from typing import TYPE_CHECKING, Generic, Literal, ParamSpec, TypeVar, cast

from scinexus.misc import extend_docstring_from

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Generator, Iterable
    from typing import Any

MPI: Any
if os.environ.get("DONT_USE_MPI", 0):
    MPI = None
else:
    try:
        from mpi4py import MPI  # type: ignore[import-not-found,no-redef]
        from mpi4py import futures as MPIfutures
    except ImportError:
        MPI = None
    else:
        COMM = MPI.COMM_WORLD
        if COMM.Get_attr(MPI.UNIVERSE_SIZE) == 1:
            MPI = None


USING_MPI = MPI is not None


P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

BackendType = Literal["multiprocess", "loky", "mpi"]


class Parallel(ABC):
    """abstract base class for parallel execution backends

    Subclass this to integrate a custom parallel engine (e.g. ray, dask).
    """

    @abstractmethod
    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        """yield results of ``f`` applied to each element of ``s``, in order"""

    @abstractmethod
    def as_completed(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        """yield results of ``f`` applied to each element of ``s``, in completion order"""

    @abstractmethod
    def is_master_process(self) -> bool:
        """return True if the current process is the master"""

    @abstractmethod
    def get_rank(self) -> int:
        """return the rank of the current process"""

    @abstractmethod
    def get_size(self) -> int:
        """return the number of available workers"""


class MultiprocessBackend(Parallel):
    """parallel backend using the stdlib ``concurrent.futures.ProcessPoolExecutor``"""

    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        max_workers = _resolve_max_workers_local(max_workers)
        chunksize = _resolve_chunksize(s, max_workers, kwargs.get("chunksize"))
        ctx = multiprocessing.get_context("spawn")
        with concurrentfutures.ProcessPoolExecutor(
            max_workers=max_workers, mp_context=ctx
        ) as executor:
            yield from executor.map(f, s, chunksize=chunksize)

    def as_completed(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        max_workers = _clamp_max_workers_local(max_workers)
        ctx = multiprocessing.get_context("spawn")
        with concurrentfutures.ProcessPoolExecutor(
            max_workers=max_workers, mp_context=ctx
        ) as executor:
            to_do = [executor.submit(f, e) for e in s]
            for result in concurrentfutures.as_completed(to_do):
                yield result.result()

    def is_master_process(self) -> bool:
        return multiprocessing.parent_process() is None

    def get_rank(self) -> int:
        return _get_rank_local()

    def get_size(self) -> int:
        return multiprocessing.cpu_count()


class LokyBackend(Parallel):
    """parallel backend using the loky library

    loky provides reusable process pools that are more robust than the
    stdlib ``ProcessPoolExecutor``, particularly in Jupyter notebooks.
    Requires ``pip install scinexus[loky]``.
    """

    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        import loky  # type: ignore[import-untyped]

        max_workers = _resolve_max_workers_local(max_workers)
        chunksize = _resolve_chunksize(s, max_workers, kwargs.get("chunksize"))
        with loky.get_reusable_executor(max_workers=max_workers) as executor:
            yield from executor.map(f, s, chunksize=chunksize)

    def as_completed(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        import loky  # type: ignore[import-untyped]

        max_workers = _clamp_max_workers_local(max_workers)
        with loky.get_reusable_executor(max_workers=max_workers) as executor:
            to_do = [executor.submit(f, e) for e in s]
            for result in concurrentfutures.as_completed(to_do):
                yield result.result()

    def is_master_process(self) -> bool:
        import loky  # type: ignore[import-untyped]

        ctxt = loky.backend.get_context()
        return ctxt.parent_process() is None

    def get_rank(self) -> int:
        return _get_rank_local()

    def get_size(self) -> int:
        return multiprocessing.cpu_count()


class MPIBackend(Parallel):
    """parallel backend using MPI via mpi4py

    Requires ``pip install scinexus[mpi]`` and an MPI implementation
    (e.g. OpenMPI).
    """

    def __init__(self) -> None:
        if MPI is None:
            msg = "Cannot use MPI"
            raise RuntimeError(msg)
        self._mpi = MPI
        self._comm = COMM
        self._futures = MPIfutures
        self._size: int = self._comm.Get_attr(self._mpi.UNIVERSE_SIZE)

    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        if_serial = cast(
            "Literal['raise', 'ignore', 'warn']",
            kwargs.get("if_serial", "raise"),
        )
        self._check_serial(if_serial)
        max_workers = max_workers or 1
        max_workers = self._clamp_workers(max_workers)
        chunksize = _resolve_chunksize(s, max_workers, kwargs.get("chunksize"))
        with self._futures.MPIPoolExecutor(max_workers=max_workers) as executor:
            yield from executor.map(f, s, chunksize=chunksize)

    def as_completed(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        if_serial = cast(
            "Literal['raise', 'ignore', 'warn']",
            kwargs.get("if_serial", "raise"),
        )
        self._check_serial(if_serial)
        max_workers = max_workers or 1
        pickled_f: Callable[[T], R] = PicklableAndCallable(f)
        max_workers = self._clamp_workers(max_workers)
        chunksize = _resolve_chunksize(s, max_workers, kwargs.get("chunksize"))
        with self._futures.MPIPoolExecutor(
            max_workers=max_workers,
            chunksize=chunksize,
        ) as executor:
            to_do = [executor.submit(pickled_f, e) for e in s]
            for result in concurrentfutures.as_completed(to_do):
                yield result.result()

    def is_master_process(self) -> bool:
        process_cmd = sys.argv[0]
        process_file = process_cmd.split(os.sep)[-1]
        if process_file == "server.py":
            return False
        return self._comm.Get_rank() == 0

    def get_rank(self) -> int:
        return self._comm.Get_rank()

    def get_size(self) -> int:
        return self._size

    def _check_serial(self, if_serial: Literal["raise", "ignore", "warn"]) -> None:
        if self._size == 1:
            err_msg = (
                "Execution in serial. For parallel MPI execution, use:\n"
                " $ mpiexec -n <number CPUs> python -m mpi4py.futures"
                " <executable script>"
            )
            if if_serial == "raise":
                raise RuntimeError(err_msg)
            if if_serial == "warn":
                warnings.warn(err_msg, UserWarning, stacklevel=4)

    def _clamp_workers(self, max_workers: int) -> int:
        if max_workers > self._size:
            warnings.warn(
                "max_workers too large, reducing to UNIVERSE_SIZE-1",
                UserWarning,
                stacklevel=3,
            )
        return min(max_workers, self._size - 1)


class PicklableAndCallable(Generic[P, R]):
    """wraps a callable so it is picklable for use with MPI executors"""

    def __init__(self, func: Callable[P, R]) -> None:
        self.func = func

    def __call__(self, *args: P.args, **kw: P.kwargs) -> R:
        return self.func(*args, **kw)


def _resolve_max_workers_local(max_workers: int | None) -> int:
    """resolve max_workers for local (non-MPI) backends"""
    cpu = multiprocessing.cpu_count()
    if not max_workers:
        return cpu
    if max_workers > cpu:
        msg = f"max_workers ({max_workers}) must be less than or equal to CPU count ({cpu})"
        raise ValueError(msg)
    return max_workers


def _clamp_max_workers_local(max_workers: int | None) -> int:
    """clamp max_workers for local as_completed (silent, no raise)"""
    if not max_workers or max_workers > multiprocessing.cpu_count():
        return multiprocessing.cpu_count()
    return max_workers


def _get_rank_local() -> int:
    """return the rank of the current process for local backends"""
    process_name = multiprocessing.current_process().name
    if process_name != "MainProcess":
        return int(process_name.split("-")[-1])
    return 0


def _resolve_chunksize(
    s: Iterable[Any], max_workers: int, chunksize: int | None
) -> int:
    """resolve chunksize, defaulting via get_default_chunksize for Sized inputs"""
    if not chunksize:
        return get_default_chunksize(s, max_workers) if isinstance(s, Sized) else 1
    return chunksize


def _validate_if_serial(
    if_serial: str,
) -> Literal["raise", "ignore", "warn"]:
    """validate and normalise the if_serial parameter"""
    if_serial = if_serial.lower()
    if if_serial not in ("ignore", "raise", "warn"):
        msg = f"invalid choice '{if_serial}'"
        raise ValueError(msg)
    return cast("Literal['raise', 'ignore', 'warn']", if_serial)


def get_default_chunksize(s: Sized, max_workers: int) -> int:
    """compute a stable chunksize for distributing items across workers

    Parameters
    ----------
    s
        a sized collection of work items
    max_workers
        number of worker processes
    """
    chunksize, remainder = divmod(len(s), max_workers * 4)
    if remainder:
        chunksize += 1
    return chunksize


_default_backend: Parallel | None = None
_mpi_backend: MPIBackend | None = None


def set_parallel_backend(
    backend: BackendType | Parallel | None = None,
) -> None:
    """set the default parallel execution backend

    Parameters
    ----------
    backend
        a ``Parallel`` instance, a string literal
        (``"multiprocess"``, ``"loky"``, or ``"mpi"``),
        or ``None`` to reset to the default (``MultiprocessBackend``).
    """
    global _default_backend  # noqa: PLW0603

    if backend is None or isinstance(backend, Parallel):
        _default_backend = backend
    elif backend == "multiprocess":
        _default_backend = MultiprocessBackend()
    elif backend == "loky":
        try:
            import loky  # noqa: F401
        except ImportError:
            msg = "loky is not installed, use pip install scinexus[loky]"
            raise ImportError(msg) from None
        _default_backend = LokyBackend()
    elif backend == "mpi":
        if MPI is None:
            msg = "mpi4py is not installed, use pip install scinexus[mpi]"
            raise ImportError(msg)
        _default_backend = MPIBackend()
    else:
        msg = (
            f"unknown backend {backend!r}, expected 'multiprocess',"
            " 'loky', 'mpi', or a Parallel instance"
        )
        raise ValueError(msg)


def get_parallel_backend() -> Parallel:
    """return the current parallel execution backend

    Returns ``MultiprocessBackend`` if no backend has been set.
    """
    global _default_backend  # noqa: PLW0603
    if _default_backend is None:
        _default_backend = MultiprocessBackend()
    return _default_backend


def _effective_backend() -> Parallel:
    """return the backend for the current process context

    If MPI is active, always returns an ``MPIBackend`` regardless of the
    default -- MPI worker processes don't inherit the parent's backend
    setting, and introspection functions like ``get_rank()`` must use the
    MPI communicator to report correctly.
    """
    global _mpi_backend  # noqa: PLW0603
    if USING_MPI:
        if _mpi_backend is None:
            _mpi_backend = MPIBackend()
        return _mpi_backend
    return get_parallel_backend()


def get_rank() -> int:
    """Returns the rank of the current process"""
    return _effective_backend().get_rank()


def get_size() -> int:
    """Returns the num cpus"""
    return _effective_backend().get_size()


SIZE = (
    COMM.Get_attr(MPI.UNIVERSE_SIZE)  # type: ignore[possibly-undefined]
    if USING_MPI
    else multiprocessing.cpu_count()
)


def is_master_process() -> bool:
    """
    Evaluates if current process is master

    In case of MPI checks whether current process
    is being run on file generated by mpi4py.futures

    In case of Multiprocessing checks if generated
    process name included "ForkProcess" for Windows
    or "SpawnProcess" for POSIX
    """
    return _effective_backend().is_master_process()


def imap(
    f: Callable[[T], R],
    s: Iterable[T],
    max_workers: int | None = None,
    use_mpi: bool = False,
    if_serial: Literal["raise", "ignore", "warn"] = "raise",
    chunksize: int | None = None,
) -> Generator[R]:
    """
    Parameters
    ----------
    f
        function that operates on values in s
    s
        series of inputs to f
    max_workers
        maximum number of workers. Defaults to 1-maximum available.
    use_mpi
        use MPI for parallel execution. Temporarily switches to
        ``MPIBackend`` for the duration of the call.
    if_serial
        action to take if conditions will result in serial execution. Valid
        values are 'raise', 'ignore', 'warn'. Defaults to 'raise'.
    chunksize
        Size of data chunks executed by worker processes. Defaults to None
        where stable chunksize is determined by get_default_chunksize()

    Returns
    -------
    imap and as_completed are generators yielding result of f(s[i]), map returns the result
    series. imap and map return results in the same order as s, as_completed returns results
    in the order completed (which can differ from the order in s).

    Notes
    -----
    To use MPI, you must have openmpi (use conda or your preferred package manager)
    and mpi4py (use pip or conda) installed. In addition, your initial script must
    have a ``if __name__ == '__main__':`` block. You then invoke your program using

    $ mpiexec -n <number CPUs> python3 -m mpi4py.futures <initial script>
    """
    if_serial = _validate_if_serial(if_serial)

    if use_mpi:
        backend = MPIBackend()
        yield from backend.imap(
            f, s, max_workers=max_workers, if_serial=if_serial, chunksize=chunksize
        )
    else:
        yield from get_parallel_backend().imap(
            f, s, max_workers=max_workers, chunksize=chunksize
        )


@extend_docstring_from(imap)
def map(
    f: Callable[[T], R],
    s: Iterable[T],
    max_workers: int | None = None,
    use_mpi: bool = False,
    if_serial: Literal["raise", "ignore", "warn"] = "raise",
    chunksize: int | None = None,
) -> list[R]:
    return list(imap(f, s, max_workers, use_mpi, if_serial, chunksize))


@extend_docstring_from(imap, pre=True)
def as_completed(
    f: Callable[[T], R],
    s: Iterable[T],
    max_workers: int | None = None,
    use_mpi: bool = False,
    if_serial: Literal["raise", "ignore", "warn"] = "raise",
    chunksize: int | None = None,
) -> Generator[R]:
    if_serial = _validate_if_serial(if_serial)

    if use_mpi:
        backend = MPIBackend()
        yield from backend.as_completed(
            f, s, max_workers=max_workers, if_serial=if_serial, chunksize=chunksize
        )
    else:
        yield from get_parallel_backend().as_completed(
            f, s, max_workers=max_workers, chunksize=chunksize
        )

from __future__ import annotations

import concurrent.futures as concurrentfutures
import itertools
import multiprocessing
import numbers
import os
import sys
import threading
import warnings
from abc import ABC, abstractmethod
from collections.abc import Sized
from types import MappingProxyType
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
        from mpi4py import futures as MPIfutures  # noqa: N812
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

BackendType = Literal["multiprocess", "threads", "loky", "mpi"]


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
        _check_chunksize(kwargs.get("chunksize"))
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


class ThreadBackend(Parallel):
    """parallel backend using the stdlib ``concurrent.futures.ThreadPoolExecutor``

    On a free-threaded build the workers run without contending for a global
    lock, and neither the arguments nor the results are pickled, so closures
    and lambdas are accepted where the process backends refuse them.

    ``chunksize`` is checked and then ignored, because a thread pool takes
    one task per item and has no per-item transport cost to amortise.
    ``max_workers`` is validated as it is for the process backends, so
    ``imap`` refuses a value above the CPU count, ``as_completed`` clamps
    one, and both refuse a value below one.

    Notes
    -----
    Workers share the app instance and everything reachable from it. A
    ``main`` that mutates ``self``, or that writes module-level state such as
    the ``numpy.random`` global generator, is a race here where it was
    harmless under the process backends.
    """

    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        max_workers = _resolve_max_workers_local(max_workers)
        _check_chunksize(kwargs.get("chunksize"))
        with concurrentfutures.ThreadPoolExecutor(
            max_workers=max_workers, initializer=_assign_rank_thread
        ) as executor:
            yield from executor.map(f, s)

    def as_completed(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        max_workers = _clamp_max_workers_local(max_workers)
        _check_chunksize(kwargs.get("chunksize"))
        with concurrentfutures.ThreadPoolExecutor(
            max_workers=max_workers, initializer=_assign_rank_thread
        ) as executor:
            to_do = [executor.submit(f, e) for e in s]
            for result in concurrentfutures.as_completed(to_do):
                yield result.result()

    def is_master_process(self) -> bool:
        return multiprocessing.parent_process() is None and _get_rank_thread() == 0

    def get_rank(self) -> int:
        return _get_rank_thread()

    def get_size(self) -> int:
        return multiprocessing.cpu_count()


class LokyBackend(Parallel):
    """parallel backend using the loky library

    loky provides reusable process pools that are more robust than the
    stdlib ``ProcessPoolExecutor``, particularly in Jupyter notebooks.
    Requires ``pip install "scinexus[loky]"``.
    """

    def imap(
        self,
        f: Callable[[T], R],
        s: Iterable[T],
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> Generator[R]:
        import loky  # type: ignore[import-untyped,import-not-found]

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
        import loky  # type: ignore[import-untyped,import-not-found]

        max_workers = _clamp_max_workers_local(max_workers)
        _check_chunksize(kwargs.get("chunksize"))
        with loky.get_reusable_executor(max_workers=max_workers) as executor:
            to_do = [executor.submit(f, e) for e in s]
            for result in concurrentfutures.as_completed(to_do):
                yield result.result()

    def is_master_process(self) -> bool:
        import loky  # type: ignore[import-untyped,import-not-found]

        ctxt = loky.backend.get_context()
        return ctxt.parent_process() is None

    def get_rank(self) -> int:
        return _get_rank_local()

    def get_size(self) -> int:
        return multiprocessing.cpu_count()


class MPIBackend(Parallel):
    """parallel backend using MPI via mpi4py

    Requires ``pip install "scinexus[mpi]"`` and an MPI implementation
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
        return False if process_file == "server.py" else self._comm.Get_rank() == 0

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


BACKEND_TYPES: MappingProxyType[BackendType, type[Parallel]] = MappingProxyType(
    {
        "multiprocess": MultiprocessBackend,
        "threads": ThreadBackend,
        "loky": LokyBackend,
        "mpi": MPIBackend,
    }
)

_THREAD_BACKEND = ThreadBackend()

_thread_state = threading.local()
_rank_lock = threading.Lock()
_rank_counter = itertools.count(1)


def _get_rank_thread() -> int:
    """return the rank of the current thread, 0 for any thread not in a pool

    Rank 0 meaning "not one of our workers" is what lets
    ``ThreadBackend.is_master_process`` keep reporting master for a thread
    the caller created, such as a web request handler or a GUI worker. A
    thread that a worker itself starts is in that group too, so it reports
    master where the equivalent thread inside a worker process does not.
    """
    return int(getattr(_thread_state, "rank", 0))


def _assign_rank_thread() -> None:
    """give the calling thread a rank no other live worker holds

    Used as a ``ThreadPoolExecutor`` initializer. The count runs across
    pools rather than restarting within each, so nested and concurrent
    pools cannot hand one rank to two threads that are alive at once, and
    successive pools number their workers the way successive process pools
    do: ``multiprocessing`` names them ``SpawnProcess-2``, ``-4``, ``-8``
    and never returns to 1. The lock keeps the increment atomic without
    relying on ``itertools.count``, whose atomicity is an implementation
    detail of CPython rather than a guarantee of the language.
    """
    with _rank_lock:
        _thread_state.rank = next(_rank_counter)


def _check_max_workers_local(max_workers: int | None) -> None:
    """raise unless max_workers is None or an int of at least 1"""
    if max_workers is None:
        return
    _check_positive_int(max_workers, "max_workers", "an int or None")


def _resolve_max_workers_local(max_workers: int | None) -> int:
    """resolve max_workers for local (non-MPI) backends"""
    _check_max_workers_local(max_workers)
    cpu = multiprocessing.cpu_count()
    if max_workers is None:
        return cpu
    if max_workers > cpu:
        msg = f"max_workers ({max_workers}) must be less than or equal to CPU count ({cpu})"
        raise ValueError(msg)
    return int(max_workers)


def _clamp_max_workers_local(max_workers: int | None) -> int:
    """clamp max_workers for local as_completed, raising only below one"""
    _check_max_workers_local(max_workers)
    if max_workers is None or max_workers > multiprocessing.cpu_count():
        return multiprocessing.cpu_count()
    return int(max_workers)


def _get_rank_local() -> int:
    """return the rank of the current process for local backends"""
    process_name = multiprocessing.current_process().name
    return int(process_name.split("-")[-1]) if process_name != "MainProcess" else 0


def _check_integral(value: object, name: str, expected: str = "an int") -> None:
    """raise unless value is of an integer type and is not a bool

    ``expected`` names what this caller accepts, since only some take None.
    """
    # bool is Integral and numpy.bool_ is neither a bool nor Integral, so
    # one test alone lets one of them through
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        msg = f"{name} must be {expected}, got {value!r}"
        raise TypeError(msg)


def _check_positive_int(value: object, name: str, expected: str = "an int") -> None:
    """raise unless value is of an integer type and is at least 1"""
    _check_integral(value, name, expected)
    if cast("numbers.Integral", value) < 1:
        msg = f"{name} ({value}) must be greater than 0"
        raise ValueError(msg)


def _check_chunksize(chunksize: int | None) -> None:
    """raise unless chunksize is None or an int of at least 1"""
    if chunksize is None:
        return
    _check_positive_int(chunksize, "chunksize", "an int or None")


def _resolve_chunksize(
    s: Iterable[Any], max_workers: int, chunksize: int | None
) -> int:
    """resolve chunksize, defaulting via get_default_chunksize for Sized inputs"""
    _check_chunksize(chunksize)
    if chunksize is None:
        return get_default_chunksize(s, max_workers) if isinstance(s, Sized) else 1
    return int(chunksize)


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
        number of worker processes, an int of at least 1

    Raises
    ------
    TypeError
        if max_workers is not of an integer type
    ValueError
        if max_workers is below 1
    """
    _check_positive_int(max_workers, "max_workers")
    chunksize, remainder = divmod(len(s), max_workers * 4)
    if remainder:
        chunksize += 1
    # an empty input divides to 0 with no remainder, and the executors that
    # receive this refuse a chunk size of 0
    return int(max(chunksize, 1))


_default_backend: Parallel | None = None
_default_backend_lock = threading.Lock()
_auto_selected = False
_mpi_backend: MPIBackend | None = None


def _replace_locks_after_fork() -> None:
    """give a forked child its own module locks

    A fork copies each lock in whatever state the parent held it, so a child
    that inherits a held one would wait for an owner that does not exist in
    it. Registered below on platforms that can fork.
    """
    global _default_backend_lock, _rank_lock  # noqa: PLW0603
    _default_backend_lock = threading.Lock()
    _rank_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_replace_locks_after_fork)


def _gil_enabled() -> bool:
    """report whether the GIL is currently in force

    ``sys._is_gil_enabled`` arrived in 3.13, so its absence means a build
    that always has the GIL. On a free-threaded build it can still report
    ``True``, because ``PYTHON_GIL=1`` forces the GIL on and because
    importing an extension module that does not declare free-threading
    support re-enables it.
    """
    probe = getattr(sys, "_is_gil_enabled", None)
    return True if probe is None else bool(probe())


def _in_worker_process() -> bool:
    """report whether this process was started by one of the pool backends

    One question covers every backend that starts processes. loky builds its
    workers through a ``multiprocessing`` context, so its contexts expose
    this very function and its workers answer it the same way a spawned
    worker does.
    """
    return multiprocessing.parent_process() is not None


def _auto_backend_type() -> BackendType:
    """the backend to use when the caller has not chosen one

    Threads are worth using only where they can run at the same time, and
    only in the master process. A spawned or loky worker re-imports this
    module with no default set, and a thread backend there would describe
    that worker as rank 0 and as the master, which it is not.
    """
    if _gil_enabled() or _in_worker_process():
        return "multiprocess"
    return "threads"


def _make_backend(backend: BackendType) -> Parallel:
    """create a backend instance from a backend type string"""
    if backend == "loky":
        try:
            import loky  # type: ignore[import-untyped,import-not-found]  # noqa: F401
        except ImportError:
            msg = 'loky is not installed, use pip install "scinexus[loky]"'
            raise ImportError(msg) from None
    elif backend == "mpi" and MPI is None:
        msg = 'mpi4py is not installed, use pip install "scinexus[mpi]"'
        raise ImportError(msg)
    return BACKEND_TYPES[backend]()


def set_parallel_backend(
    backend: BackendType | Parallel | None = None,
) -> None:
    """set the default parallel execution backend

    Parameters
    ----------
    backend
        a ``Parallel`` instance, a string literal
        (``"multiprocess"``, ``"threads"``, ``"loky"``, or ``"mpi"``),
        or ``None`` to choose again according to the interpreter.

    Notes
    -----
    A choice made here is honoured until it is changed, which is how to opt
    out of the thread backend on a free-threaded build. It applies to this
    process alone, since a worker process does not inherit it.
    """
    global _default_backend, _auto_selected  # noqa: PLW0603

    if backend is None or isinstance(backend, Parallel):
        chosen = backend
    elif backend in BACKEND_TYPES:
        chosen = _make_backend(backend)
    else:
        msg = (
            f"unknown backend {backend!r}, expected 'multiprocess',"
            " 'threads', 'loky', 'mpi', or a Parallel instance"
        )
        raise ValueError(msg)

    # the backend is built before the lock is taken, so that a refused name
    # or a missing package leaves the current choice alone, and so that no
    # import runs while the lock is held. the pair is then written together,
    # or a caller reading between the two stores would see this choice as
    # one to replace on the next look at the GIL
    with _default_backend_lock:
        _default_backend = chosen
        _auto_selected = False


def get_parallel_backend(backend: BackendType | None = None) -> Parallel:
    """return the current parallel execution backend

    Parameters
    ----------
    backend
        if provided, return an instance of this backend type without
        changing the global default. This lets a package obtain the
        backend it needs without disrupting the settings of other
        packages.

    Returns
    -------
    When no backend has been set and ``backend is None``, a
    ``ThreadBackend`` if the GIL is not in force and a
    ``MultiprocessBackend`` otherwise.

    Notes
    -----
    Left to choose for itself, this asks about the GIL on every call rather
    than once. Importing an extension module that does not declare
    free-threading support re-enables the GIL, and that can happen after
    the first parallel call, which would otherwise leave threads running
    one at a time for the rest of the process. A backend passed to
    ``set_parallel_backend`` is never replaced.
    """
    if backend is not None:
        return _make_backend(backend)

    global _default_backend, _auto_selected  # noqa: PLW0603
    wanted = _auto_backend_type()
    with _default_backend_lock:
        stale = _auto_selected and not isinstance(
            _default_backend, BACKEND_TYPES[wanted]
        )
        if _default_backend is None or stale:
            _default_backend = _make_backend(wanted)
            _auto_selected = True
        return _default_backend


def _effective_backend() -> Parallel:
    """return the backend for the current process context

    If MPI is active, always returns an ``MPIBackend`` regardless of the
    default -- MPI worker processes don't inherit the parent's backend
    setting, and introspection functions like ``get_rank()`` must use the
    MPI communicator to report correctly.

    A thread one of this module's pools started is described by
    ``ThreadBackend`` for the same reason, whatever the default is. The
    default records what the caller asked for, not what is running the
    current thread, so consulting it would have a worker report itself as
    the master and as rank 0 whenever the pool came from anywhere other
    than ``set_parallel_backend``.
    """
    global _mpi_backend  # noqa: PLW0603
    if USING_MPI:
        if _mpi_backend is None:
            _mpi_backend = MPIBackend()
        return _mpi_backend
    if _get_rank_thread():
        return _THREAD_BACKEND
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

    In case of threads, a thread the pool created is not the master, and any
    other thread is. Callers gate the creation of shared resources on this,
    so a caller's own thread has to keep reporting master.
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
        maximum number of workers, an int of at least 1. Defaults to None,
        meaning every available CPU. A bool, and anything else that is not
        of an integer type, is refused.
    use_mpi
        use MPI for parallel execution. Temporarily switches to
        ``MPIBackend`` for the duration of the call.
    if_serial
        action to take if conditions will result in serial execution. Valid
        values are 'raise', 'ignore', 'warn'. Defaults to 'raise'.
    chunksize
        Size of data chunks executed by worker processes, an int of at least
        1. Defaults to None, where a stable chunksize is determined by
        get_default_chunksize(). Checked wherever it is accepted, but only
        imap on the process and MPI backends chunks the work by it.

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

    `$ mpiexec -n <number CPUs> python3 -m mpi4py.futures <initial script>`
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

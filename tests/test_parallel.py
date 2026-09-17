import multiprocessing
import os
import sys
import threading
import time
from collections.abc import Generator
from unittest.mock import patch

import numpy
import pytest

from scinexus import parallel
from scinexus.parallel import (
    BACKEND_TYPES,
    LokyBackend,
    MPIBackend,
    MultiprocessBackend,
    Parallel,
    PicklableAndCallable,
    ThreadBackend,
    _clamp_max_workers_local,
    _effective_backend,
    _get_rank_thread,
    _gil_enabled,
    _resolve_chunksize,
    _resolve_max_workers_local,
    _resolve_max_workers_mpi,
    _worker_budget,
    as_completed,
    get_default_chunksize,
    get_parallel_backend,
    get_size,
    set_parallel_backend,
)


@pytest.fixture(autouse=True)
def _reset_backend():
    """Reset the module-level default after each test."""
    yield
    set_parallel_backend(None)


def get_process_value(n):
    # Sleep to accommodate Windows process creation overhead
    time.sleep(1)
    return (parallel.get_rank(), n)


def get_ranint(n):
    numpy.random.seed(n)
    return numpy.random.randint(1, 10)


def check_is_master_process(n):
    return parallel.is_master_process()


def _double(x):
    return x * 2


def _getpid(_):
    return os.getpid()


def test_parallel_backend_abc_cannot_instantiate():
    """Parallel cannot be instantiated directly"""
    with pytest.raises(TypeError):
        Parallel()


def test_parallel_backend_abc_missing_methods():
    """incomplete subclass raises TypeError"""

    class Incomplete(Parallel):
        def imap(self, f, s, max_workers=None, **kwargs):
            yield from ()

    with pytest.raises(TypeError):
        Incomplete()


def test_set_parallel_backend_multiprocess():
    """setting 'multiprocess' returns MultiprocessBackend"""
    set_parallel_backend("multiprocess")
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


def test_set_parallel_backend_loky():
    """setting 'loky' returns LokyBackend"""
    set_parallel_backend("loky")
    assert isinstance(get_parallel_backend(), LokyBackend)


def test_set_parallel_backend_none_resets(monkeypatch):
    """None resets to default

    The probe is pinned so this tests the reset rather than which build is
    running the suite.
    """
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    set_parallel_backend("loky")
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


def test_set_parallel_backend_custom_instance():
    """accepts a Parallel instance"""

    class Custom(Parallel):
        def imap(self, f, s, max_workers=None, **kwargs):
            yield from ()

        def as_completed(self, f, s, max_workers=None, **kwargs):
            yield from ()

        def is_master_process(self):
            return True

        def get_rank(self):
            return 0

        def get_size(self):
            return 1

    custom = Custom()
    set_parallel_backend(custom)
    assert get_parallel_backend() is custom


def test_set_parallel_backend_invalid_string():
    """invalid string raises ValueError"""
    with pytest.raises(ValueError, match="unknown backend"):
        set_parallel_backend("invalid")  # type: ignore


def test_set_parallel_backend_loky_not_installed():
    """set_parallel_backend('loky') raises ImportError when loky is missing"""
    with patch.dict("sys.modules", {"loky": None}), pytest.raises(ImportError):
        set_parallel_backend("loky")


def test_set_parallel_backend_mpi_not_available():
    """set_parallel_backend('mpi') raises ImportError when mpi4py is missing"""
    with patch.object(parallel, "MPI", None), pytest.raises(ImportError):
        set_parallel_backend("mpi")


def test_get_parallel_backend_default(monkeypatch):
    """returns MultiprocessBackend when nothing set and the GIL is in force"""
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


def test_get_parallel_backend_caches():
    """get_parallel_backend caches the default instance"""
    set_parallel_backend(None)
    b1 = get_parallel_backend()
    b2 = get_parallel_backend()
    assert b1 is b2


def test_get_parallel_backend_with_backend_multiprocess():
    """returns a MultiprocessBackend when backend='multiprocess'"""
    assert isinstance(get_parallel_backend(backend="multiprocess"), MultiprocessBackend)


def test_get_parallel_backend_with_backend_loky():
    """returns a LokyBackend when backend='loky'"""
    assert isinstance(get_parallel_backend(backend="loky"), LokyBackend)


def test_get_parallel_backend_with_backend_does_not_modify_default():
    """passing backend does not change the global default"""
    set_parallel_backend("multiprocess")
    default_before = get_parallel_backend()
    get_parallel_backend(backend="loky")
    default_after = get_parallel_backend()
    assert default_before is default_after
    assert isinstance(default_after, MultiprocessBackend)


def test_get_parallel_backend_with_backend_none():
    """backend=None returns the current default"""
    set_parallel_backend("loky")
    assert isinstance(get_parallel_backend(backend=None), LokyBackend)


def test_effective_backend_caches_mpi():
    """_effective_backend caches the MPIBackend instance"""
    mock_mpi = type("FakeMPI", (), {"COMM_WORLD": None})
    with (
        patch.object(parallel, "USING_MPI", True),
        patch.object(parallel, "MPI", mock_mpi),
        patch.object(parallel, "_mpi_backend", None),
        patch.object(MPIBackend, "__init__", lambda self: None),
    ):
        b1 = _effective_backend()
        b2 = _effective_backend()
        assert b1 is b2
        assert isinstance(b1, MPIBackend)


def test_multiprocess_imap():
    """MultiprocessBackend.imap returns ordered results"""
    backend = MultiprocessBackend()
    data = list(range(10))
    result = list(backend.imap(_double, data, max_workers=1))
    assert result == [x * 2 for x in data]


def test_multiprocess_as_completed():
    """MultiprocessBackend.as_completed returns all results"""
    backend = MultiprocessBackend()
    data = list(range(10))
    result = sorted(backend.as_completed(_double, data))
    assert result == sorted(x * 2 for x in data)


def test_multiprocess_is_master_process():
    """MultiprocessBackend.is_master_process returns True in main"""
    backend = MultiprocessBackend()
    assert backend.is_master_process()


def test_multiprocess_get_rank():
    """MultiprocessBackend.get_rank returns 0 in main process"""
    backend = MultiprocessBackend()
    assert backend.get_rank() == 0


def test_multiprocess_get_size():
    """MultiprocessBackend.get_size returns cpu_count"""
    backend = MultiprocessBackend()
    assert backend.get_size() == multiprocessing.cpu_count()


def test_multiprocess_max_workers_too_large():
    """max_workers > cpu_count raises ValueError"""
    backend = MultiprocessBackend()
    n = multiprocessing.cpu_count() + 1
    with pytest.raises(ValueError, match="max_workers"):
        list(backend.imap(_double, [1], max_workers=n))


def test_multiprocess_non_sized_iterable():
    """imap with a generator defaults chunksize to 1"""
    backend = MultiprocessBackend()

    def gen():
        yield from range(4)

    result = list(backend.imap(_double, gen(), max_workers=1))
    assert sorted(result) == [0, 2, 4, 6]


def test_multiprocess_as_completed_max_workers_clamped():
    """large max_workers gets clamped"""
    backend = MultiprocessBackend()
    data = list(range(4))
    result = sorted(backend.as_completed(_double, data, max_workers=9999))
    assert result == sorted(x * 2 for x in data)


def test_clamp_max_workers_local_valid():
    """valid max_workers is returned unchanged"""
    result = _clamp_max_workers_local(1)
    assert result == 1


def test_clamp_max_workers_local_too_large():
    """max_workers exceeding cpu_count is clamped to cpu_count"""
    cpu = multiprocessing.cpu_count()
    result = _clamp_max_workers_local(cpu + 1)
    assert result == cpu


@pytest.mark.parametrize(
    "resolve", [_resolve_max_workers_local, _clamp_max_workers_local]
)
@pytest.mark.parametrize("max_workers", [-1, 0])
def test_max_workers_below_one_refused(resolve, max_workers):
    """a worker count below one is named in the message we raise"""
    with pytest.raises(ValueError, match=rf"max_workers \({max_workers}\)"):
        resolve(max_workers)


@pytest.mark.parametrize(
    "resolve", [_resolve_max_workers_local, _clamp_max_workers_local]
)
def test_max_workers_none_is_the_default(resolve):
    """None still asks for one worker per cpu"""
    assert resolve(None) == multiprocessing.cpu_count()


@pytest.mark.parametrize(
    "resolve", [_resolve_max_workers_local, _clamp_max_workers_local]
)
@pytest.mark.parametrize("max_workers", [True, False])
def test_max_workers_bool_refused(resolve, max_workers):
    """a bool is not a worker count, and None is how to ask for every cpu"""
    with pytest.raises(TypeError, match="must be an int or None"):
        resolve(max_workers)


@pytest.mark.parametrize(
    "resolve", [_resolve_max_workers_local, _clamp_max_workers_local]
)
@pytest.mark.parametrize("max_workers", [numpy.True_, numpy.False_, 2.5])
def test_max_workers_non_integer_refused(resolve, max_workers):
    """what is not a whole number is refused as a type, not as a range"""
    with pytest.raises(TypeError, match="must be an int or None"):
        resolve(max_workers)


@pytest.mark.parametrize(
    "resolve", [_resolve_max_workers_local, _clamp_max_workers_local]
)
def test_max_workers_numpy_integer_accepted(resolve):
    """a numpy integer is a worker count, and reaches the executor as an int"""
    got = resolve(numpy.int64(1))
    assert got == 1
    assert type(got) is int


@pytest.mark.parametrize(("world_size", "expect"), [(4, 3), (8, 7), (2, 1), (1, 1)])
def test_worker_budget(world_size, expect):
    """the budget is the ranks launched, less the master

    These values come from mpi4py's own num_workers, which this test does
    not consult. test_mpi_get_size_is_the_pool_the_job_has does.
    """
    assert _worker_budget(world_size) == expect


def test_resolve_max_workers_mpi_none_takes_the_pool():
    """None accepts the pool the job was launched with"""
    assert _resolve_max_workers_mpi(None, 7) == 7
    assert _resolve_max_workers_mpi(None, 1) == 1


def test_resolve_max_workers_mpi_matching_request_is_quiet():
    """asking for exactly what the job has is not worth a warning"""
    assert _resolve_max_workers_mpi(7, 7) == 7


@pytest.mark.parametrize("max_workers", [3, 8])
def test_resolve_max_workers_mpi_reports_a_request_it_cannot_meet(max_workers):
    """a count that is not the pool size is reported, above or below it"""
    with pytest.warns(UserWarning, match="this request is not used"):
        assert _resolve_max_workers_mpi(max_workers, 7) == 7


@pytest.mark.parametrize("max_workers", [-1, 0])
def test_resolve_max_workers_mpi_below_one_refused(max_workers):
    """MPI refuses a worker count below one, as the local backends do"""
    with pytest.raises(ValueError, match=rf"max_workers \({max_workers}\)"):
        _resolve_max_workers_mpi(max_workers, 8)


@pytest.mark.parametrize("max_workers", [True, numpy.True_, 2.5])
def test_resolve_max_workers_mpi_non_integer_refused(max_workers):
    """MPI refuses what is not a worker count, as the local backends do"""
    with pytest.raises(TypeError, match="max_workers must be an int or None"):
        _resolve_max_workers_mpi(max_workers, 8)


def test_resolve_max_workers_mpi_accepts_a_numpy_count():
    """a numpy integer equal to the pool size is a matching request"""
    # numpy.int64(8) != 8 is numpy.False_, which must be falsy rather than
    # merely not True for the warning to stay quiet
    assert _resolve_max_workers_mpi(numpy.int64(8), 8) == 8


@pytest.mark.free_threaded
def test_thread_imap():
    """ThreadBackend.imap returns ordered results"""
    backend = ThreadBackend()
    data = list(range(10))
    result = list(backend.imap(_double, data, max_workers=2))
    assert result == [x * 2 for x in data]


@pytest.mark.free_threaded
def test_thread_as_completed():
    """ThreadBackend.as_completed returns all results"""
    backend = ThreadBackend()
    data = list(range(10))
    result = sorted(backend.as_completed(_double, data))
    assert result == sorted(x * 2 for x in data)


@pytest.mark.free_threaded
def test_thread_accepts_closure():
    """a closure is callable on threads, where pickling would refuse it"""
    factor = 3

    def scale(x):
        return x * factor

    backend = ThreadBackend()
    assert list(backend.imap(scale, [1, 2, 3], max_workers=2)) == [3, 6, 9]


@pytest.mark.free_threaded
def test_thread_max_workers_too_large():
    """max_workers above cpu_count raises, as it does for processes"""
    backend = ThreadBackend()
    n = multiprocessing.cpu_count() + 1
    with pytest.raises(ValueError, match="must be less than or equal to"):
        list(backend.imap(_double, [1], max_workers=n))


@pytest.mark.free_threaded
def test_thread_as_completed_max_workers_clamped():
    """large max_workers makes no more workers than there are cpus

    The pool starts a thread per submitted task until it reaches its limit,
    so the number of distinct ranks over more tasks than cpus reports what
    the limit actually was.
    """
    backend = ThreadBackend()
    cpu = multiprocessing.cpu_count()
    barrier = threading.Barrier(cpu, timeout=30)

    def blocked(_):
        barrier.wait()
        return parallel.get_rank()

    ranks = set(backend.as_completed(blocked, range(cpu * 3), max_workers=9999))
    assert len(ranks) == cpu


@pytest.mark.free_threaded
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2, reason="requires at least 2 CPU cores"
)
def test_thread_max_workers_limits_the_pool():
    """max_workers reaches the executor rather than only being validated

    Without it the executor uses its own default, which is larger, so the
    number of distinct ranks over many tasks would exceed what was asked
    for.
    """
    backend = ThreadBackend()
    barrier = threading.Barrier(2, timeout=30)

    def blocked(_):
        barrier.wait()
        return parallel.get_rank()

    ranks = set(backend.imap(blocked, range(20), max_workers=2))
    assert len(ranks) == 2


@pytest.mark.free_threaded
def test_thread_non_sized_iterable():
    """a generator input works, having no length to chunk by"""
    backend = ThreadBackend()

    def gen():
        yield from range(5)

    result = list(backend.imap(_double, gen(), max_workers=2))
    assert result == [x * 2 for x in range(5)]


@pytest.mark.free_threaded
def test_thread_get_size():
    """ThreadBackend.get_size returns cpu_count"""
    backend = ThreadBackend()
    assert backend.get_size() == multiprocessing.cpu_count()


@pytest.mark.free_threaded
def test_thread_get_rank_main_thread():
    """the main thread is rank 0, as the master is for the process backends"""
    backend = ThreadBackend()
    assert backend.get_rank() == 0


@pytest.mark.free_threaded
def test_thread_is_master_process_main_thread():
    """the main thread reports master"""
    backend = ThreadBackend()
    assert backend.is_master_process()


@pytest.mark.free_threaded
def test_thread_is_master_process_foreign_thread():
    """a thread scinexus did not create still reports master

    Someone driving scinexus from a web request handler or a GUI worker is
    the master, so anything gated on this must keep happening for them.
    """
    backend = ThreadBackend()
    got = []
    thread = threading.Thread(target=lambda: got.append(backend.is_master_process()))
    thread.start()
    thread.join()
    assert got == [True]


@pytest.mark.free_threaded
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2, reason="requires at least 2 CPU cores"
)
def test_thread_workers_have_distinct_ranks():
    """concurrent workers report distinct ranks above 0"""
    backend = ThreadBackend()
    barrier = threading.Barrier(2, timeout=30)

    def blocked(_):
        barrier.wait()
        return parallel.get_rank()

    ranks = set(backend.imap(blocked, [1, 2], max_workers=2))
    assert len(ranks) == 2
    assert min(ranks) > 0


@pytest.mark.free_threaded
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2, reason="requires at least 2 CPU cores"
)
def test_thread_ranks_are_not_reused_by_a_later_pool():
    """a second pool's workers hold ranks the first pool's did not

    Two pools can be alive at once, nested or side by side, so a rank
    restarting at 1 for each pool would be held by two live threads. The
    process backends never return to 1 either.
    """
    backend = ThreadBackend()

    def collect():
        barrier = threading.Barrier(2, timeout=30)

        def blocked(_):
            barrier.wait()
            return parallel.get_rank()

        return set(backend.imap(blocked, [1, 2], max_workers=2))

    first = collect()
    second = collect()
    assert len(first) == len(second) == 2
    assert not (first & second)


@pytest.mark.free_threaded
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 4, reason="requires at least 4 CPU cores"
)
def test_thread_nested_pool_ranks_differ_from_the_outer_worker():
    """a pool started inside a worker does not reuse the outer rank"""
    backend = ThreadBackend()

    def inner(_):
        return parallel.get_rank()

    def outer(_):
        return (parallel.get_rank(), set(backend.imap(inner, [1, 2], max_workers=2)))

    for outer_rank, inner_ranks in backend.imap(outer, [1, 2], max_workers=2):
        assert outer_rank not in inner_ranks


@pytest.mark.free_threaded
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2, reason="requires at least 2 CPU cores"
)
def test_thread_workers_are_not_master():
    """a worker thread does not report master, so it creates nothing

    The default is left at the process backend on purpose. What describes a
    worker is the pool that started it, not what the caller registered.
    """
    set_parallel_backend("multiprocess")
    backend = ThreadBackend()
    barrier = threading.Barrier(2, timeout=30)

    def blocked(_):
        barrier.wait()
        return (parallel.is_master_process(), parallel.get_rank())

    got = list(backend.imap(blocked, [1, 2], max_workers=2))
    assert [is_master for is_master, _ in got] == [False, False]
    assert all(rank > 0 for _, rank in got)


@pytest.mark.free_threaded
def test_set_parallel_backend_threads():
    """setting 'threads' returns ThreadBackend"""
    set_parallel_backend("threads")
    assert isinstance(get_parallel_backend(), ThreadBackend)


@pytest.mark.free_threaded
def test_get_parallel_backend_with_backend_threads():
    """returns a ThreadBackend when backend='threads' without changing default"""
    set_parallel_backend("multiprocess")
    assert isinstance(get_parallel_backend(backend="threads"), ThreadBackend)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


@pytest.mark.free_threaded
def test_get_rank_thread_unknown_thread():
    """a thread with no rank assigned to it reports 0"""
    got = []
    thread = threading.Thread(target=lambda: got.append(_get_rank_thread()))
    thread.start()
    thread.join()
    assert got == [0]


@pytest.mark.free_threaded
def test_gil_enabled_reports_the_probe(monkeypatch):
    """the probe reports what sys._is_gil_enabled says

    raising=False because 3.11 and 3.12 have no such attribute to replace.
    """
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert _gil_enabled() is False
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    assert _gil_enabled() is True


@pytest.mark.free_threaded
def test_gil_enabled_without_the_probe(monkeypatch):
    """an interpreter with no probe always has the GIL

    sys._is_gil_enabled arrived in 3.13, so its absence is the answer.
    """
    monkeypatch.delattr(sys, "_is_gil_enabled", raising=False)
    assert _gil_enabled() is True


@pytest.mark.free_threaded
def test_default_is_threads_without_the_gil(monkeypatch):
    """the default is a thread backend when the GIL is off"""
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), ThreadBackend)


@pytest.mark.free_threaded
def test_default_is_processes_with_the_gil(monkeypatch):
    """the default is a process backend when the GIL is on"""
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


@pytest.mark.free_threaded
def test_default_is_processes_in_a_worker_process(monkeypatch):
    """a worker process does not pick threads

    A spawned worker re-imports this module with no default set. A thread
    backend there would report the worker as rank 0 and as the master.
    """
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    monkeypatch.setattr(multiprocessing, "parent_process", lambda: object())
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


@pytest.mark.free_threaded
def test_default_follows_the_gil_being_switched_back_on(monkeypatch):
    """the probe runs on each call, not once

    Importing an extension without free-threading support re-enables the
    GIL, which can happen after the first parallel call.
    """
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), ThreadBackend)
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


@pytest.mark.free_threaded
def test_chosen_backend_survives_the_gil_being_switched_on(monkeypatch):
    """an explicit choice is never replaced by the probe"""
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    set_parallel_backend("loky")
    assert isinstance(get_parallel_backend(), LokyBackend)
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    assert isinstance(get_parallel_backend(), LokyBackend)


@pytest.mark.free_threaded
def test_default_matches_this_interpreter():
    """the unpatched default follows the GIL state of the running build

    Every other test here pins the probe, so this is the only one that
    fails if the choice is wired to the interpreter the wrong way round.
    """
    set_parallel_backend(None)
    gil_on = getattr(sys, "_is_gil_enabled", None) is None or sys._is_gil_enabled()
    expected = MultiprocessBackend if gil_on else ThreadBackend
    assert isinstance(get_parallel_backend(), expected)


@pytest.mark.free_threaded
def test_refused_backend_leaves_the_choice_to_be_made_again(monkeypatch):
    """a call that raises does not stop the GIL being looked at again

    Clearing the automatic flag before the name is validated would leave a
    thread backend installed and unreplaceable after the GIL came back.
    """
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), ThreadBackend)

    with pytest.raises(ValueError, match="unknown backend"):
        set_parallel_backend("thread")  # type: ignore[arg-type]

    monkeypatch.setattr(parallel, "_gil_enabled", lambda: True)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


@pytest.mark.free_threaded
def test_locks_are_replaced_after_a_fork():
    """a child gets its own locks rather than the ones the parent held"""
    before = (parallel._default_backend_lock, parallel._rank_lock)
    parallel._replace_locks_after_fork()
    after = (parallel._default_backend_lock, parallel._rank_lock)
    assert all(new is not old for new, old in zip(after, before, strict=True))
    assert not any(lock.locked() for lock in after)


@pytest.mark.free_threaded
def test_module_map_runs_in_one_process_without_the_gil(monkeypatch):
    """the automatic default really runs the work on threads

    Threads share the interpreter, so every worker reports the caller's pid
    where a process backend would report its own.
    """
    monkeypatch.setattr(parallel, "_gil_enabled", lambda: False)
    set_parallel_backend(None)
    assert parallel.map(_getpid, range(4)) == [os.getpid()] * 4


def test_loky_imap():
    """LokyBackend.imap returns ordered results"""
    backend = LokyBackend()
    data = list(range(10))
    result = list(backend.imap(_double, data, max_workers=1))
    assert result == [x * 2 for x in data]


def test_loky_as_completed():
    """LokyBackend.as_completed returns all results"""
    backend = LokyBackend()
    data = list(range(10))
    result = sorted(backend.as_completed(_double, data))
    assert result == sorted(x * 2 for x in data)


def test_loky_is_master_process():
    """LokyBackend.is_master_process returns True in main"""
    backend = LokyBackend()
    assert backend.is_master_process()


def test_loky_get_rank():
    """LokyBackend.get_rank returns 0 in main process"""
    backend = LokyBackend()
    assert backend.get_rank() == 0


def test_loky_get_size():
    """LokyBackend.get_size returns cpu_count"""
    backend = LokyBackend()
    assert backend.get_size() == multiprocessing.cpu_count()


@pytest.mark.slow
@pytest.mark.skipif(
    multiprocessing.cpu_count() < 2, reason="requires at least 2 CPU cores"
)
def test_create_processes():
    """Processor pool should create multiple distinct processes"""
    set_parallel_backend("multiprocess")
    max_worker_count = multiprocessing.cpu_count() - 1
    index = list(range(max_worker_count))
    result = parallel.map(get_process_value, index, max_workers=None)
    result_processes = [v[0] for v in result]
    result_values = [v[1] for v in result]
    assert sorted(result_values) == index
    assert len(set(result_processes)) == max_worker_count


def test_random_seeding():
    """Random seed should be set every function call"""
    index1 = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    index2 = [2, 2, 2, 2, 2, 2, 2, 2, 2]
    result1 = parallel.map(get_ranint, index1, max_workers=1)
    result2 = parallel.map(get_ranint, index2, max_workers=1)
    assert result1[0] == result2[0]
    assert result1 != result2


def _get_rank(_x):
    return parallel.get_rank()


def test_get_rank():
    """get_rank() should return 0 on master, > 0 on workers"""
    assert parallel.get_rank() == 0
    index = list(range(1, 5))
    ranks = list(parallel.imap(_get_rank, index))
    assert all(r > 0 for r in ranks)


def test_is_master_process():
    """is_master_process() should return True on master, False on workers"""
    assert parallel.is_master_process()
    index = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    master_processes = sum(
        bool(result) for result in parallel.imap(check_is_master_process, index)
    )
    assert master_processes == 0


def test_as_completed():
    """as_completed should return all results"""
    data = list(range(10))
    result = sorted(as_completed(_double, data))
    assert result == sorted(x * 2 for x in data)


def test_get_size():
    """get_size returns cpu_count when not using MPI"""
    assert get_size() == multiprocessing.cpu_count()


def test_get_default_chunksize_exact():
    """chunksize with no remainder"""
    assert get_default_chunksize(range(16), 4) == 1


def test_get_default_chunksize_remainder():
    """chunksize rounds up when there is a remainder"""
    assert get_default_chunksize(range(17), 4) == 2


def test_get_default_chunksize_empty():
    """an empty input gives a chunksize an executor will accept"""
    assert get_default_chunksize([], 1) == 1
    assert get_default_chunksize([], 6) == 1


@pytest.mark.parametrize("max_workers", [-1, 0])
def test_get_default_chunksize_below_one_refused(max_workers):
    """a worker count below one is named rather than divided by"""
    with pytest.raises(ValueError, match=rf"max_workers \({max_workers}\)"):
        get_default_chunksize(range(16), max_workers)


@pytest.mark.parametrize("max_workers", [True, numpy.True_, 2.5, None])
def test_get_default_chunksize_non_integer_refused(max_workers):
    """the public helper refuses what the resolvers refuse, None included"""
    with pytest.raises(TypeError, match=r"max_workers must be an int, got"):
        get_default_chunksize(range(16), max_workers)


def test_get_default_chunksize_numpy_integer_accepted():
    """a numpy worker count does not make a numpy chunk size"""
    got = get_default_chunksize(range(16), numpy.int64(2))
    assert got == 2
    assert type(got) is int


def test_resolve_chunksize_empty():
    """the empty case reaches the executor as 1 rather than 0"""
    assert _resolve_chunksize([], 6, None) == 1


def test_resolve_chunksize_keeps_what_the_caller_asked_for():
    """a valid explicit chunk size is passed through untouched"""
    assert _resolve_chunksize([1, 2], 6, 3) == 3


@pytest.mark.parametrize("chunksize", [-1, 0])
def test_chunksize_below_one_refused(chunksize):
    """a chunk size below one is named in the message we raise"""
    with pytest.raises(ValueError, match=rf"chunksize \({chunksize}\)"):
        _resolve_chunksize([1, 2], 6, chunksize)


@pytest.mark.parametrize("chunksize", [True, False])
def test_chunksize_bool_refused(chunksize):
    """a bool is not a chunk size, and None is how to ask for the default"""
    with pytest.raises(TypeError, match="must be an int or None"):
        _resolve_chunksize([1, 2], 6, chunksize)


@pytest.mark.parametrize("chunksize", [numpy.True_, numpy.False_, 2.5])
def test_chunksize_non_integer_refused(chunksize):
    """what is not a whole number is refused as a type, not as a range"""
    with pytest.raises(TypeError, match="must be an int or None"):
        _resolve_chunksize([1, 2], 6, chunksize)


def test_chunksize_numpy_integer_accepted():
    """a numpy integer is a chunk size, and reaches the executor as an int"""
    got = _resolve_chunksize([1, 2], 6, numpy.int64(3))
    assert got == 3
    assert type(got) is int


_LOCAL_BACKENDS = [
    "multiprocess",
    "loky",
    pytest.param("threads", marks=pytest.mark.free_threaded),
]


@pytest.mark.parametrize("backend_name", _LOCAL_BACKENDS)
def test_backend_empty_input(backend_name):
    """an empty input yields nothing rather than raising"""
    backend = BACKEND_TYPES[backend_name]()
    assert list(backend.imap(_double, [])) == []
    assert list(backend.as_completed(_double, [])) == []


@pytest.mark.parametrize("backend_name", _LOCAL_BACKENDS)
def test_module_empty_input(backend_name):
    """the module functions take an empty input on every local backend"""
    set_parallel_backend(backend_name)
    assert list(parallel.imap(_double, [])) == []
    assert parallel.map(_double, []) == []
    assert list(parallel.as_completed(_double, [])) == []


@pytest.mark.parametrize("backend_name", _LOCAL_BACKENDS)
def test_backend_chunksize_below_one(backend_name):
    """every call that accepts a chunk size refuses a bad one

    Only imap on the process backends chunks the work, but as_completed and
    the thread backend take the argument, so they check it too.
    """
    backend = BACKEND_TYPES[backend_name]()
    with pytest.raises(ValueError, match=r"chunksize \(-1\)"):
        list(backend.imap(_double, [1], chunksize=-1))
    with pytest.raises(ValueError, match=r"chunksize \(-1\)"):
        list(backend.as_completed(_double, [1], chunksize=-1))


@pytest.mark.parametrize("backend_name", _LOCAL_BACKENDS)
def test_backend_max_workers_below_one(backend_name):
    """our message reaches the caller rather than the executor's own"""
    backend = BACKEND_TYPES[backend_name]()
    with pytest.raises(ValueError, match=r"max_workers \(-1\)"):
        list(backend.imap(_double, [1], max_workers=-1))
    with pytest.raises(ValueError, match=r"max_workers \(-1\)"):
        list(backend.as_completed(_double, [1], max_workers=-1))


def test_picklable_and_callable():
    """PicklableAndCallable wraps and delegates calls"""
    wrapped = PicklableAndCallable(_double)
    assert wrapped(5) == 10


def test_imap_invalid_if_serial():
    """invalid if_serial raises ValueError"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(parallel.imap(_double, [1], if_serial="invalid"))


def test_imap_max_workers_too_large():
    """max_workers > cpu_count raises ValueError"""
    n = multiprocessing.cpu_count() + 1
    with pytest.raises(ValueError, match="max_workers"):
        list(parallel.imap(_double, [1], max_workers=n))


def test_as_completed_invalid_if_serial():
    """invalid if_serial raises ValueError in as_completed"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(as_completed(_double, [1], if_serial="invalid"))


def test_as_completed_max_workers_clamped():
    """large max_workers gets clamped"""
    data = list(range(4))
    result = sorted(as_completed(_double, data, max_workers=9999))
    assert result == sorted(x * 2 for x in data)


def test_imap_use_mpi_when_unavailable():
    """imap(use_mpi=True) raises RuntimeError when MPI unavailable"""
    with patch.object(parallel, "MPI", None):
        with pytest.raises(RuntimeError, match="Cannot use MPI"):
            list(parallel.imap(_double, [1], use_mpi=True))


def test_imap_non_sized_iterable():
    """imap with a generator (non-Sized) defaults chunksize to 1"""

    def gen():
        yield from range(4)

    result = list(parallel.imap(_double, gen(), max_workers=1))
    assert sorted(result) == [0, 2, 4, 6]


def test_get_rank_worker_process():
    """get_rank parses rank from worker process name"""
    mock_process = type("FakeProcess", (), {"name": "LokyProcess-3"})()
    with patch("multiprocessing.current_process", return_value=mock_process):
        backend = MultiprocessBackend()
        assert backend.get_rank() == 3


def test_dont_use_mpi_env_var():
    """DONT_USE_MPI env var disables MPI import"""
    import importlib

    with patch.dict("os.environ", {"DONT_USE_MPI": "1"}):
        importlib.reload(parallel)
        assert parallel.MPI is None
        assert parallel.USING_MPI is False
    # reload to restore original state
    os.environ.pop("DONT_USE_MPI", None)
    importlib.reload(parallel)


def test_mpi_import_error_fallback():
    """MPI is None when mpi4py cannot be imported"""
    import importlib

    import scinexus.parallel as par

    with patch.dict("sys.modules", {"mpi4py": None, "mpi4py.futures": None}):
        importlib.reload(par)
        assert par.MPI is None
        assert par.USING_MPI is False
    # reload to restore original state
    importlib.reload(par)


def test_module_imap_delegates_to_backend():
    """module-level imap delegates to the current backend"""
    set_parallel_backend("multiprocess")
    data = list(range(5))
    result = list(parallel.imap(_double, data, max_workers=1))
    assert result == [x * 2 for x in data]


def test_module_as_completed_delegates_to_backend():
    """module-level as_completed delegates to the current backend"""
    set_parallel_backend("multiprocess")
    data = list(range(5))
    result = sorted(parallel.as_completed(_double, data))
    assert result == sorted(x * 2 for x in data)


def test_module_map_returns_list():
    """module-level map returns a list"""
    data = list(range(5))
    result = parallel.map(_double, data, max_workers=1)
    assert isinstance(result, list)
    assert result == [x * 2 for x in data]


def test_imap_returns_generator():
    """module-level imap returns a generator"""
    result = parallel.imap(_double, [1, 2, 3], max_workers=1)
    assert isinstance(result, Generator)


def test_as_completed_returns_generator():
    """module-level as_completed returns a generator"""
    result = parallel.as_completed(_double, [1, 2, 3])
    assert isinstance(result, Generator)

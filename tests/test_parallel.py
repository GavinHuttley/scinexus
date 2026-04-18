import multiprocessing
import os
import time
from collections.abc import Generator
from unittest.mock import patch

import numpy
import pytest

from scinexus import parallel
from scinexus.parallel import (
    LokyBackend,
    MultiprocessBackend,
    Parallel,
    PicklableAndCallable,
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


def test_set_parallel_backend_none_resets():
    """None resets to default"""
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
        set_parallel_backend("invalid")


def test_set_parallel_backend_loky_not_installed():
    """set_parallel_backend('loky') raises ImportError when loky is missing"""
    with patch.dict("sys.modules", {"loky": None}):
        with pytest.raises(ImportError, match="pip install scinexus"):
            set_parallel_backend("loky")


def test_set_parallel_backend_mpi_not_available():
    """set_parallel_backend('mpi') raises ImportError when mpi4py is missing"""
    with patch.object(parallel, "MPI", None):
        with pytest.raises(ImportError, match="pip install scinexus"):
            set_parallel_backend("mpi")


def test_get_parallel_backend_default():
    """returns MultiprocessBackend when nothing set"""
    set_parallel_backend(None)
    assert isinstance(get_parallel_backend(), MultiprocessBackend)


def test_get_parallel_backend_caches():
    """get_parallel_backend caches the default instance"""
    set_parallel_backend(None)
    b1 = get_parallel_backend()
    b2 = get_parallel_backend()
    assert b1 is b2


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
    """max_workers >= cpu_count raises ValueError"""
    backend = MultiprocessBackend()
    n = multiprocessing.cpu_count()
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


@pytest.mark.slow
def test_create_processes():
    """Processor pool should create multiple distinct processes"""
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


def test_picklable_and_callable():
    """PicklableAndCallable wraps and delegates calls"""
    wrapped = PicklableAndCallable(_double)
    assert wrapped(5) == 10


def test_imap_invalid_if_serial():
    """invalid if_serial raises ValueError"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(parallel.imap(_double, [1], if_serial="invalid"))


def test_imap_max_workers_too_large():
    """max_workers >= cpu_count raises ValueError"""
    n = multiprocessing.cpu_count()
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

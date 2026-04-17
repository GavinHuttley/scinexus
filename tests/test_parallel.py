import multiprocessing
import os
import time
from unittest.mock import patch

import numpy
import pytest

from scinexus import parallel
from scinexus.parallel import (
    PicklableAndCallable,
    as_completed,
    get_default_chunksize,
    get_size,
)


def get_process_value(n):
    # Sleep to accommodate Windows process creation overhead
    time.sleep(1)
    return (parallel.get_rank(), n)


def get_ranint(n):
    numpy.random.seed(n)
    return numpy.random.randint(1, 10)


def check_is_master_process(n):
    return parallel.is_master_process()


@pytest.mark.slow
def test_create_processes():
    """Procressor pool should create multiple distingue processes"""
    max_worker_count = multiprocessing.cpu_count() - 1
    index = list(range(max_worker_count))
    result = parallel.map(get_process_value, index, max_workers=None, use_mpi=False)
    result_processes = [v[0] for v in result]
    result_values = [v[1] for v in result]
    assert sorted(result_values) == index
    assert len(set(result_processes)) == max_worker_count


def test_random_seeding():
    """Random seed should be set every function call"""
    # On Windows process ids are not guaranteed to be sequential(1,2,3,4...)
    # thus they cannot be used for reproducibility
    index1 = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    index2 = [2, 2, 2, 2, 2, 2, 2, 2, 2]
    result1 = parallel.map(get_ranint, index1, max_workers=1, use_mpi=False)
    result2 = parallel.map(get_ranint, index2, max_workers=1, use_mpi=False)
    assert result1[0] == result2[0]
    assert result1 != result2


def test_get_rank():
    """get_rank() should return 0 on master, > 0 on workers"""
    assert parallel.get_rank() == 0
    index = list(range(1, 5))
    ranks = list(parallel.imap(lambda _: parallel.get_rank(), index, use_mpi=False))
    assert all(r > 0 for r in ranks)


def test_is_master_process():
    """
    is_master_process() should return False
    for all child processes
    """
    assert parallel.is_master_process()  # this should be master!
    index = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # but workers should not
    master_processes = sum(
        bool(result)
        for result in parallel.imap(check_is_master_process, index, use_mpi=False)
    )
    assert master_processes == 0


def _double(x):
    return x * 2


def test_as_completed():
    """as_completed should return all results"""
    data = list(range(10))
    result = sorted(as_completed(_double, data, use_mpi=False))
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
    """large max_workers in _as_completed_mproc gets clamped"""
    data = list(range(4))
    result = sorted(as_completed(_double, data, max_workers=9999, use_mpi=False))
    assert result == sorted(x * 2 for x in data)


def test_imap_use_mpi_when_unavailable():
    """imap(use_mpi=True) raises RuntimeError when MPI unavailable"""
    with patch.object(parallel, "USING_MPI", False):
        with pytest.raises(RuntimeError, match="Cannot use MPI"):
            list(parallel.imap(_double, [1], use_mpi=True))


def test_imap_non_sized_iterable():
    """imap with a generator (non-Sized) defaults chunksize to 1"""

    def gen():
        yield from range(4)

    result = list(parallel.imap(_double, gen(), max_workers=1, use_mpi=False))
    assert sorted(result) == [0, 2, 4, 6]


def test_get_rank_worker_process():
    """get_rank parses rank from worker process name"""
    mock_process = type("FakeProcess", (), {"name": "LokyProcess-3"})()
    with patch("multiprocessing.current_process", return_value=mock_process):
        assert parallel.get_rank() == 3


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


def test_as_completed_use_loky_false():
    """as_completed with use_loky=False returns all results"""
    data = list(range(10))
    result = sorted(as_completed(_double, data, use_mpi=False, use_loky=False))
    assert result == sorted(x * 2 for x in data)


def test_imap_use_loky_false():
    """imap with use_loky=False returns results in order"""
    data = list(range(10))
    result = list(parallel.imap(_double, data, max_workers=1, use_loky=False))
    assert result == [x * 2 for x in data]


def test_map_use_loky_false():
    """map with use_loky=False returns results in order"""
    data = list(range(10))
    result = parallel.map(_double, data, max_workers=1, use_loky=False)
    assert result == [x * 2 for x in data]


def test_as_completed_mproc_max_workers_clamped():
    """large max_workers in _as_completed_mproc gets clamped"""
    data = list(range(4))
    result = sorted(
        as_completed(_double, data, max_workers=9999, use_mpi=False, use_loky=False)
    )
    assert result == sorted(x * 2 for x in data)


def test_imap_use_loky_false_non_sized():
    """imap with use_loky=False and a generator defaults chunksize to 1"""

    def gen():
        yield from range(4)

    result = list(
        parallel.imap(_double, gen(), max_workers=1, use_mpi=False, use_loky=False)
    )
    assert sorted(result) == [0, 2, 4, 6]


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

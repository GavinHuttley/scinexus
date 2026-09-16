import sys
from unittest.mock import patch

import pytest

from scinexus import parallel
from scinexus.parallel import (
    SIZE,
    as_completed,
    get_rank,
    imap,
    is_master_process,
    map,
)

pytestmark = pytest.mark.mpi


def _get_rank(n):  # noqa: ARG001
    return get_rank()


def _is_master(n):  # noqa: ARG001
    return is_master_process()


@pytest.mark.mpi
def test_get_rank_mpi():
    """get_rank() should return 0 on master, > 0 on MPI workers"""
    assert get_rank() == 0
    ranks = list(imap(_get_rank, list(range(1, 5)), use_mpi=True))
    assert all(r > 0 for r in ranks)


@pytest.mark.mpi
def test_is_master_process():
    """is_master_process() should return False for all child processes"""
    assert is_master_process()
    index = list(range(1, 11))
    master_processes = sum(
        bool(result) for result in imap(_is_master, index, use_mpi=True)
    )
    assert master_processes == 0


def _double(x):
    return x * 2


@pytest.mark.mpi
def test_imap_mpi():
    """imap with use_mpi should process all items"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True))
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
def test_map_mpi():
    """map with use_mpi should return correct results"""
    data = list(range(10))
    result = map(_double, data, use_mpi=True)
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
def test_as_completed_mpi():
    """as_completed with use_mpi should return all results"""
    data = list(range(10))
    result = sorted(as_completed(_double, data, use_mpi=True))
    assert result == sorted(x * 2 for x in data)


@pytest.mark.mpi
def test_imap_mpi_with_chunksize():
    """imap with explicit chunksize under MPI"""
    data = list(range(20))
    result = list(imap(_double, data, use_mpi=True, chunksize=5))
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
def test_imap_mpi_max_workers_warning():
    """a worker count the job cannot change says so and still runs

    mpiexec -n fixed the pool before the program started, so the request
    cannot be met and the work runs on the pool regardless.
    """
    data = list(range(10))
    with pytest.warns(UserWarning, match="this request is not used"):
        result = list(imap(_double, data, use_mpi=True, max_workers=SIZE + 10))
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
@pytest.mark.parametrize("max_workers", [-1, 0])
def test_mpi_max_workers_below_one_refused(max_workers):
    """our message reaches the caller rather than the executor's own"""
    # imap and as_completed are generators, so an unconsumed call never
    # runs the body and never raises
    with pytest.raises(ValueError, match=rf"max_workers \({max_workers}\)"):
        list(imap(_double, [1], use_mpi=True, max_workers=max_workers))
    with pytest.raises(ValueError, match=rf"max_workers \({max_workers}\)"):
        list(as_completed(_double, [1], use_mpi=True, max_workers=max_workers))


@pytest.mark.mpi
def test_mpi_max_workers_bool_refused():
    """a bool is not a worker count under MPI either"""
    with pytest.raises(TypeError, match="max_workers must be an int or None"):
        list(imap(_double, [1], use_mpi=True, max_workers=True))
    with pytest.raises(TypeError, match="max_workers must be an int or None"):
        list(as_completed(_double, [1], use_mpi=True, max_workers=True))


@pytest.mark.mpi
def test_imap_mpi_if_serial_warn():
    """if_serial='warn' should be accepted without error when SIZE > 1"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True, if_serial="warn"))
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
def test_imap_mpi_if_serial_ignore():
    """if_serial='ignore' should be accepted without error"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True, if_serial="ignore"))
    assert result == [x * 2 for x in data]


@pytest.mark.mpi
def test_imap_mpi_invalid_if_serial():
    """invalid if_serial value should raise ValueError"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(imap(_double, [1], use_mpi=True, if_serial="invalid"))


@pytest.mark.mpi
def test_imap_mpi_if_serial_raise_size_1():
    """if_serial='raise' with SIZE==1 raises RuntimeError"""
    backend = parallel.MPIBackend()
    backend._size = 1
    with pytest.raises(RuntimeError, match="Execution in serial"):
        list(backend.imap(_double, [1], if_serial="raise"))


@pytest.mark.mpi
def test_imap_mpi_if_serial_warn_size_1():
    """if_serial='warn' with SIZE==1 warns, and _size alone selects it"""
    backend = parallel.MPIBackend()
    backend._size = 1
    with pytest.warns(UserWarning, match="Execution in serial"):
        assert list(backend.imap(_double, [1], if_serial="warn")) == [2]


@pytest.mark.mpi
def test_imap_mpi_non_sized_iterable():
    """imap with a generator under MPI defaults chunksize to 1"""

    def gen():
        yield from range(4)

    result = list(imap(_double, gen(), use_mpi=True))
    assert sorted(result) == [0, 2, 4, 6]


@pytest.mark.mpi
def test_as_completed_mpi_invalid_if_serial():
    """invalid if_serial raises ValueError via as_completed with MPI"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(as_completed(_double, [1], use_mpi=True, if_serial="invalid"))


@pytest.mark.mpi
def test_as_completed_mpi_max_workers_warning():
    """max_workers > SIZE emits warning in _as_completed_mpi"""
    data = list(range(10))
    with pytest.warns(UserWarning, match="this request is not used"):
        result = sorted(
            as_completed(_double, data, use_mpi=True, max_workers=SIZE + 10)
        )
    assert result == sorted(x * 2 for x in data)


@pytest.mark.mpi
def test_as_completed_mpi_if_serial_raise_size_1():
    """_as_completed_mpi with SIZE==1 and if_serial='raise' raises RuntimeError"""
    backend = parallel.MPIBackend()
    backend._size = 1
    with pytest.raises(RuntimeError, match="Execution in serial"):
        list(backend.as_completed(_double, [1], if_serial="raise"))


@pytest.mark.mpi
def test_as_completed_mpi_if_serial_warn_size_1():
    """_as_completed_mpi with SIZE==1 and if_serial='warn' emits warning"""
    backend = parallel.MPIBackend()
    backend._size = 1
    with pytest.warns(UserWarning, match="Execution in serial"):
        got = sorted(backend.as_completed(_double, list(range(4)), if_serial="warn"))
    assert got == [0, 2, 4, 6]


@pytest.mark.mpi
def test_as_completed_mpi_if_serial_ignore_size_1():
    """_as_completed_mpi with SIZE==1 and if_serial='ignore' does not raise serial error"""
    backend = parallel.MPIBackend()
    backend._size = 1
    got = sorted(backend.as_completed(_double, list(range(4)), if_serial="ignore"))
    assert got == [0, 2, 4, 6]


def test_as_completed_mpi_not_using_mpi():
    """MPIBackend raises RuntimeError when MPI is None"""
    with patch.object(parallel, "MPI", None):
        with pytest.raises(RuntimeError, match="Cannot use MPI"):
            list(as_completed(_double, [1], use_mpi=True))


@pytest.mark.mpi
def test_as_completed_mpi_works_out_no_chunk_size(monkeypatch):
    """as_completed submits one item at a time, so there is nothing to chunk"""

    def fail(*args, **kwargs):
        msg = "as_completed has no use for a chunk size"
        raise AssertionError(msg)

    monkeypatch.setattr(parallel, "get_default_chunksize", fail)
    assert sorted(as_completed(_double, list(range(4)), use_mpi=True)) == [0, 2, 4, 6]


@pytest.mark.mpi
@pytest.mark.parametrize("chunksize", [None, 5])
def test_as_completed_mpi_sends_no_chunk_size(monkeypatch, chunksize):
    """nothing reaches the executor under a name it has no parameter for"""
    # MPIPoolExecutor keeps unrecognised keywords in _options rather than
    # refusing them, so passing one is invisible without looking there
    seen = []
    real_executor = parallel.MPIfutures.MPIPoolExecutor

    class Spy(real_executor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            seen.append(self._options)

    monkeypatch.setattr(parallel.MPIfutures, "MPIPoolExecutor", Spy)
    got = sorted(
        as_completed(_double, list(range(4)), use_mpi=True, chunksize=chunksize)
    )
    assert got == [0, 2, 4, 6]
    assert seen
    assert all("chunksize" not in options for options in seen)


@pytest.mark.mpi
def test_as_completed_mpi_chunksize_still_checked():
    """a bad chunk size is refused even though a good one is ignored"""
    with pytest.raises(ValueError, match=r"chunksize \(-1\)"):
        list(as_completed(_double, [1], use_mpi=True, chunksize=-1))


@pytest.mark.mpi
def test_as_completed_mpi_non_sized_iterable():
    """_as_completed_mpi takes a generator, having no length to ask for"""

    def gen():
        yield from range(4)

    result = sorted(as_completed(_double, gen(), use_mpi=True))
    assert result == [0, 2, 4, 6]


@pytest.mark.mpi
def test_mpi_get_size():
    """MPIBackend.get_size returns the module-level SIZE"""
    backend = parallel.MPIBackend()
    assert backend.get_size() == SIZE


@pytest.mark.mpi
def test_mpi_get_size_is_the_pool_the_job_has():
    """get_size agrees with mpi4py's num_workers, the ground truth here"""
    backend = parallel.MPIBackend()
    with backend._futures.MPIPoolExecutor() as executor:
        assert backend.get_size() == executor.num_workers


@pytest.mark.mpi
def test_is_master_process_mpi_server():
    """is_master_process returns False when argv[0] is server.py"""
    backend = parallel.MPIBackend()
    with patch.object(sys, "argv", ["server.py"]):
        assert backend.is_master_process() is False

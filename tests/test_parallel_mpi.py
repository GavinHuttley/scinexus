import pytest

from scinexus.parallel import (
    SIZE,
    USING_MPI,
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


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_get_rank_mpi():
    """get_rank() should return 0 on master, > 0 on MPI workers"""
    assert get_rank() == 0
    ranks = list(imap(_get_rank, list(range(1, 5)), use_mpi=True))
    assert all(r > 0 for r in ranks)


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
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


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi():
    """imap with use_mpi should process all items"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True))
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_map_mpi():
    """map with use_mpi should return correct results"""
    data = list(range(10))
    result = map(_double, data, use_mpi=True)
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_as_completed_mpi():
    """as_completed with use_mpi should return all results"""
    data = list(range(10))
    result = sorted(as_completed(_double, data, use_mpi=True))
    assert result == sorted(x * 2 for x in data)


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi_with_chunksize():
    """imap with explicit chunksize under MPI"""
    data = list(range(20))
    result = list(imap(_double, data, use_mpi=True, chunksize=5))
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi_max_workers_warning():
    """max_workers exceeding SIZE should emit a warning"""
    data = list(range(10))
    with pytest.warns(UserWarning, match="max_workers too large"):
        result = list(imap(_double, data, use_mpi=True, max_workers=SIZE + 10))
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi_if_serial_warn():
    """if_serial='warn' should be accepted without error when SIZE > 1"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True, if_serial="warn"))
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi_if_serial_ignore():
    """if_serial='ignore' should be accepted without error"""
    data = list(range(10))
    result = list(imap(_double, data, use_mpi=True, if_serial="ignore"))
    assert result == [x * 2 for x in data]


@pytest.mark.skipif(not USING_MPI, reason="Not using MPI")
def test_imap_mpi_invalid_if_serial():
    """invalid if_serial value should raise ValueError"""
    with pytest.raises(ValueError, match="invalid choice"):
        list(imap(_double, [1], use_mpi=True, if_serial="invalid"))

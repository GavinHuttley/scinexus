import pytest

from scinexus.parallel import USING_MPI, imap, is_master_process

pytestmark = pytest.mark.mpi


def _is_master(n):
    return is_master_process()


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

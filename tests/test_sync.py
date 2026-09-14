import copy
import pickle
import threading
from typing import Self

from scinexus._sync import LockMixin


class _Owner(LockMixin):
    """minimal owner, following the contract of making the lock in __new__"""

    def __new__(cls, value: int = 0) -> Self:
        obj = object.__new__(cls)
        obj._cache_lock = cls.new_lock()
        obj.value = value
        return obj


def test_new_lock_is_reentrant():
    """the guarded regions re-enter each other, so a plain lock would hang"""
    lock = LockMixin.new_lock()
    with lock, lock:
        assert True


def test_lock_is_created_by_new():
    owner = _Owner(3)
    assert isinstance(owner._cache_lock, type(LockMixin.new_lock()))


def test_getstate_drops_the_lock():
    owner = _Owner(3)
    state = owner.__getstate__()
    assert "_cache_lock" not in state
    assert state["value"] == 3


def test_getstate_tolerates_a_missing_lock():
    """an instance built without __new__ has no lock to drop"""
    owner = object.__new__(_Owner)
    owner.value = 1
    assert owner.__getstate__() == {"value": 1}


def test_pickle_roundtrip_restores_a_usable_lock():
    owner = _Owner(7)
    restored = pickle.loads(pickle.dumps(owner))
    assert restored.value == 7
    with restored._cache_lock:
        assert True


def test_deepcopy_restores_a_usable_lock():
    """apps deepcopy their arguments on every call, so this is a hot path"""
    owner = _Owner(7)
    restored = copy.deepcopy(owner)
    assert restored.value == 7
    with restored._cache_lock:
        assert True


def test_locks_are_per_instance():
    first = _Owner(1)
    second = _Owner(2)
    assert first._cache_lock is not second._cache_lock


def test_lock_actually_excludes():
    """two threads must not be inside the guarded region at once"""
    owner = _Owner()
    inside = 0
    overlapped = False

    def enter():
        nonlocal inside, overlapped
        with owner._cache_lock:
            inside += 1
            if inside > 1:
                overlapped = True
            inside -= 1

    threads = [threading.Thread(target=enter) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not overlapped

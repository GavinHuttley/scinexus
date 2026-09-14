"""synchronisation helpers

The package is currently free of threading primitives -- every parallel
backend in :mod:`scinexus.parallel` is process based. These helpers exist
to facilitate usage in free-threaded CPython.
"""

import threading
from typing import Any


class LockMixin:
    """gives an object a reentrant lock that is kept out of its pickled state

    Owning classes create the lock in ``__new__`` rather than ``__init__``,
    so that instances built by ``__new__`` alone still have one. Creating it
    on first use instead would need a guard of its own, which is the problem
    this class exists to solve.

    Notes
    -----
    ``__getstate__`` returning a falsy mapping makes pickle skip
    ``__setstate__`` entirely. That is harmless here because the lock is
    restored by ``__new__``, not by ``__setstate__``.
    """

    _cache_lock: threading.RLock

    @staticmethod
    def new_lock() -> threading.RLock:
        """return a lock suitable for guarding lazily cached state

        Reentrant because the guarded regions re-enter each other: reading a
        cached list can run through ``__contains__``, which reads the same
        cached list again.
        """
        return threading.RLock()

    def __getstate__(self) -> dict[str, Any]:
        """return ``__dict__`` without the lock, which cannot be pickled"""
        state = self.__dict__.copy()
        state.pop("_cache_lock", None)
        return state

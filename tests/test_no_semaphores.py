"""Fast tests of the no-semaphore fallback, without running dbt.

AWS Lambda has no /dev/shm, so no multiprocessing lock can be created there.
dbt and Python's ThreadPool both create them anyway, although dbt only ever
shares them between threads. ``_use_thread_locks_if_no_semaphores`` detects that
and substitutes thread locks. These tests check the fallback itself; the end-to-
end build under the same conditions is in test_transform.py.
"""

import multiprocessing
import threading
from multiprocessing.pool import ThreadPool

import multiprocessing.synchronize as mp_sync

from eskom_grid.transform import _use_thread_locks_if_no_semaphores

THREAD_LOCK = type(threading.Lock())
THREAD_RLOCK = type(threading.RLock())


def test_without_semaphores_multiprocessing_locks_become_thread_locks(no_semaphores):
    assert _use_thread_locks_if_no_semaphores() is True

    # Through the context, which is how dbt and ThreadPool actually ask for them.
    ctx = multiprocessing.get_context()
    assert isinstance(ctx.Lock(), THREAD_LOCK)
    assert isinstance(ctx.RLock(), THREAD_RLOCK)


def test_without_semaphores_a_thread_pool_can_be_created_once_patched(no_semaphores):
    """The second call site: Pool.__init__ builds a SimpleQueue guarded by ctx.Lock().

    This is the one dbt's --single-threaded flag cannot avoid, because dbt
    creates the pool either way.
    """
    _use_thread_locks_if_no_semaphores()

    pool = ThreadPool(1)
    try:
        assert pool.apply(sum, ([1, 2, 3],)) == 6
    finally:
        pool.close()
        pool.join()


def test_a_second_call_changes_nothing(no_semaphores):
    """A warm Lambda calls run_transform - and so this - on every invocation."""
    _use_thread_locks_if_no_semaphores()
    patched_lock, patched_rlock = mp_sync.Lock, mp_sync.RLock

    assert _use_thread_locks_if_no_semaphores() is False
    assert mp_sync.Lock is patched_lock
    assert mp_sync.RLock is patched_rlock


def test_with_semaphores_multiprocessing_is_left_alone():
    """On a laptop, in Docker, under Dagster: the standard library is untouched."""
    original_lock, original_rlock = mp_sync.Lock, mp_sync.RLock

    assert _use_thread_locks_if_no_semaphores() is False
    assert mp_sync.Lock is original_lock
    assert mp_sync.RLock is original_rlock

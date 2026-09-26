"""Fixtures shared across test modules."""

import multiprocessing.synchronize as mp_sync

import pytest


@pytest.fixture
def no_semaphores(monkeypatch):
    """Make this process behave like AWS Lambda, which has no /dev/shm.

    Every multiprocessing lock, semaphore, event and condition is built on
    ``SemLock``, so refusing to create one here refuses them all - the way the
    real environment does. Patching only ``Lock`` and ``RLock`` would be easier,
    but the test would then share the fix's assumption that those are the only
    two that matter, and could never catch the day that stops being true.

    ``Lock`` and ``RLock`` are registered with monkeypatch first, unchanged, so
    that if the code under test swaps them for thread locks, the swap is undone
    after the test instead of leaking into every test that follows.
    """
    monkeypatch.setattr(mp_sync, "Lock", mp_sync.Lock)
    monkeypatch.setattr(mp_sync, "RLock", mp_sync.RLock)

    def refuse(self, *args, **kwargs):
        # The exact error Lambda raised: sem_open finds no /dev/shm.
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(mp_sync.SemLock, "__init__", refuse)

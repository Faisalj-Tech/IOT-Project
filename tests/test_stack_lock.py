"""The lockfile that stops two pytest sessions from sharing one live stack.

Phase 2 lost four result files to overlapping Docker operations from concurrent
terminals. These tests need no stack and no Docker.

Every test drives a lock file under tmp_path, never the real LOCK_PATH. By the
time this module runs in the default suite the session-scoped `stack` fixture is
already holding the real lock, and unlinking or overwriting it here would
disable the guard for the rest of the run.
"""

import os
import subprocess
import sys
import time

import pytest

from tests.conftest import _pid_alive, acquire_stack_lock, release_stack_lock


@pytest.fixture
def lock(tmp_path):
    return tmp_path / "stack.lock"


def test_acquire_writes_our_pid(lock):
    acquire_stack_lock(lock)
    assert lock.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_release_removes_the_lock(lock):
    acquire_stack_lock(lock)
    release_stack_lock(lock)
    assert not lock.exists()


def test_release_is_safe_when_no_lock_exists(lock):
    release_stack_lock(lock)  # must not raise


def test_a_live_foreign_lock_is_refused(lock):
    """A real child process, not a well-known PID.

    PID 4 (Windows System) and PID 1 (POSIX init) are tempting but depend on what
    tasklist shows an unelevated user, which would silently invert this test.
    """
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        lock.write_text(str(child.pid), encoding="utf-8")
        with pytest.raises(RuntimeError) as excinfo:
            acquire_stack_lock(lock)
        assert str(lock) in str(excinfo.value), "the error must name the file to delete"
        assert str(child.pid) in str(excinfo.value), "the error must name the holder"
    finally:
        child.kill()
        child.wait(timeout=10)


def test_a_stale_lock_is_reclaimed(lock):
    """A PID that was alive and is not any more."""
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait(timeout=10)
    deadline = time.time() + 10
    while _pid_alive(child.pid) and time.time() < deadline:
        time.sleep(0.2)

    lock.write_text(str(child.pid), encoding="utf-8")
    acquire_stack_lock(lock)
    assert lock.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_a_corrupt_lock_is_reclaimed(lock):
    lock.write_text("not-a-pid", encoding="utf-8")
    acquire_stack_lock(lock)
    assert lock.read_text(encoding="utf-8").strip() == str(os.getpid())


def test_pid_alive_reports_this_process():
    assert _pid_alive(os.getpid()) is True

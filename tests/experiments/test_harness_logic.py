"""Harness logic tests that need no stack, no Docker, and no broker.

Everything here is driven by hand-constructed sample payloads. These run in the
default suite, unlike test_recorder.py and test_drain_guard.py, which are both
marked stack+experiment and are deselected by pytest.ini's addopts.
"""

import threading
import time

import pytest

from tests.experiments.conftest import GaugeRecorder, _online_from_quorum


def _recorder(samples):
    """A GaugeRecorder that never starts its thread, pre-loaded with samples."""
    recorder = GaugeRecorder(rabbit_get_node=None, nodes=(1, 2, 3))
    recorder.samples = samples
    return recorder


def _sample(t, nodes_dict):
    return {"t": t, "nodes": nodes_dict}


def test_node_window_returns_a_genuine_false_for_reachable():
    """The guard and the queried key were the same field, so False was unreachable.

    An unreachable minority node is the finding experiments H and I exist to
    capture; a harness that can only answer True-or-None cannot report it.
    Audit finding H-3, ADR-0018.
    """
    recorder = _recorder([
        _sample(100.0, {2: {"reachable": True, "source": "exec"}}),
        _sample(110.0, {2: {"reachable": False, "exec_error": "timeout"}}),
    ])
    assert recorder.node_window(2, "reachable", since=105.0) is False


def test_node_window_returns_none_when_the_window_holds_no_sample():
    recorder = _recorder([
        _sample(100.0, {2: {"reachable": False, "exec_error": "timeout"}}),
    ])
    assert recorder.node_window(2, "reachable", since=200.0) is None


def test_node_window_still_gates_ordinary_keys_on_reachability():
    """An unreachable sample has no gauge values; it must not shadow a good one."""
    recorder = _recorder([
        _sample(100.0, {2: {"reachable": True, "telemetry_ready": 42}}),
        _sample(110.0, {2: {"reachable": False, "exec_error": "timeout"}}),
    ])
    assert recorder.node_window(2, "telemetry_ready", since=50.0) == 42


def test_node_latest_returns_a_genuine_false_for_reachable():
    recorder = _recorder([
        _sample(100.0, {2: {"reachable": True, "source": "http"}}),
        _sample(110.0, {2: {"reachable": False, "exec_error": "timeout"}}),
    ])
    assert recorder.node_latest(2, "reachable") is False


def test_online_excludes_peers_not_in_a_live_raft_state():
    """online aliased members, so a partitioned node reported every peer online.

    Exec is the only read path for a detached node, which made the split-brain
    signal structurally unrepresentable. Audit finding H-2.
    """
    quorum = [
        {"Node Name": "rabbit@rabbit1", "Raft State": "leader"},
        {"Node Name": "rabbit@rabbit2", "Raft State": "follower"},
        {"Node Name": "rabbit@rabbit3", "Raft State": "noproc"},
    ]
    assert _online_from_quorum(quorum) == ["rabbit@rabbit1", "rabbit@rabbit2"]


def test_online_equals_members_on_a_healthy_group():
    quorum = [
        {"Node Name": "rabbit@rabbit1", "Raft State": "leader"},
        {"Node Name": "rabbit@rabbit2", "Raft State": "follower"},
        {"Node Name": "rabbit@rabbit3", "Raft State": "follower"},
    ]
    assert len(_online_from_quorum(quorum)) == 3


def test_online_treats_a_missing_raft_state_as_not_live():
    """A row with no Raft State is not evidence the peer is up."""
    quorum = [
        {"Node Name": "rabbit@rabbit1", "Raft State": "leader"},
        {"Node Name": "rabbit@rabbit2"},
    ]
    assert _online_from_quorum(quorum) == ["rabbit@rabbit1"]


def test_await_sample_after_returns_when_a_later_sample_lands():
    """The window _views() reads must be guaranteed to contain a fresh sample.

    Without this the floor assertions can read None on a run where the partition
    genuinely bit, which is the second of ADR-0018's two named causes.
    """
    recorder = _recorder([_sample(100.0, {1: {"reachable": True}})])
    marked = 100.0

    def append_later():
        time.sleep(0.2)
        with recorder._new_sample:
            recorder.samples.append(_sample(101.0, {1: {"reachable": True}}))
            recorder._new_sample.notify_all()

    threading.Thread(target=append_later, daemon=True).start()
    assert recorder.await_sample_after(marked, timeout_s=5.0) == 101.0


def test_await_sample_after_returns_immediately_if_one_already_landed():
    recorder = _recorder([
        _sample(100.0, {1: {"reachable": True}}),
        _sample(101.0, {1: {"reachable": True}}),
    ])
    assert recorder.await_sample_after(100.0, timeout_s=5.0) == 101.0


def test_await_sample_after_raises_when_sampling_has_stopped():
    """A recorder that stopped sampling is the failure the floor exists to catch.

    It must fail loudly rather than degrade into an ambiguous None.
    """
    recorder = _recorder([_sample(100.0, {1: {"reachable": True}})])
    with pytest.raises(AssertionError, match="stopped sampling"):
        recorder.await_sample_after(100.0, timeout_s=0.3)

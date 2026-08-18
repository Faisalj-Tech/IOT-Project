"""Harness logic tests that need no stack, no Docker, and no broker.

Everything here is driven by hand-constructed sample payloads. These run in the
default suite, unlike test_recorder.py and test_drain_guard.py, which are both
marked stack+experiment and are deselected by pytest.ini's addopts.
"""

from tests.experiments.conftest import GaugeRecorder


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

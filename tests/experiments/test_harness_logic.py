"""Harness logic tests that need no stack, no Docker, and no broker.

Everything here is driven by hand-constructed sample payloads. These run in the
default suite, unlike test_recorder.py and test_drain_guard.py, which are both
marked stack+experiment and are deselected by pytest.ini's addopts.
"""

import threading
import time

import pytest

from tests.conftest import consumer_files, detect_profile_mismatch
from tests.experiments.conftest import DrainResult, GaugeRecorder, _online_from_quorum, drain_and_fetch


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


class _StalledRecorder:
    """A recorder whose broker still reports work in flight."""

    def node_latest(self, node, key):
        return 5 if key == "telemetry_unacked" else 0


def test_drain_does_not_give_up_early_while_the_recorder_reports_work_in_flight():
    """The row-count fallback is documented as a no-recorder path but ran always.

    A stalled Influx row count plus a genuinely undrained broker made it exit
    short, and sequence_report then reported loss that had not happened — the
    exact ADR-0012 regression. Audit finding H-1.
    """
    result = drain_and_fetch(
        lambda flux: [], "no-such-run", "-5m", expected_total=10,
        timeout_s=1.0, stable_polls_limit=1, gauge_recorder=_StalledRecorder(),
    )
    assert result.exit_condition == "timeout", (
        f"gave up on the row-count heuristic despite a live recorder: "
        f"{result.exit_condition!r}"
    )


def test_drain_still_falls_back_when_no_recorder_was_supplied():
    result = drain_and_fetch(
        lambda flux: [], "no-such-run", "-5m", expected_total=10,
        timeout_s=60, stable_polls_limit=2,
    )
    assert result.exit_condition == "row-count-gave-up"


def test_drain_result_fields_distinguish_completion_from_giving_up():
    result = DrainResult(
        rows=[{"_value": 1}], elapsed_s=12.5, exit_condition="row-count-gave-up",
        ready_at_exit=7, unacked_at_exit=3, expected_total=10,
    )
    assert result.as_result_fields() == {
        "drain_elapsed_s": 12.5,
        "drain_exit_condition": "row-count-gave-up",
        "queue_ready_at_drain_exit": 7,
        "queue_unacked_at_drain_exit": 3,
        "drain_rows_at_exit": 1,
        "drain_expected_total": 10,
    }


def test_consumer_files_follows_the_single_node_profile(monkeypatch):
    monkeypatch.delenv("IOT_CLUSTER", raising=False)
    assert consumer_files() == ("compose.yml", "compose.consumer.yml")


def test_consumer_files_follows_the_cluster_profile(monkeypatch):
    """The bug this replaces: a hardcoded single-node set made compose evaluate
    `consumer`'s `depends_on: rabbitmq` against compose.yml alone, which recreates
    iot-rabbitmq onto the empty single-node volume mid-run. ADR-0026, bite #16."""
    monkeypatch.setenv("IOT_CLUSTER", "1")
    assert consumer_files() == (
        "compose.yml",
        "compose.cluster.yml",
        "compose.consumer.yml",
    )


def test_docker_control_routes_the_consumer_through_the_derived_set(monkeypatch):
    from tests.experiments.conftest import DockerControl

    monkeypatch.setenv("IOT_CLUSTER", "1")
    control = DockerControl()
    assert control._files("consumer") == (
        "compose.yml",
        "compose.cluster.yml",
        "compose.consumer.yml",
    )
    assert control._files("rabbitmq") == ("compose.yml", "compose.cluster.yml")


MISMATCHED_DRY_RUN = """\
 Found orphan containers ([iot-rabbitmq2 iot-rabbitmq3]) for this project. If you removed or renamed this service in your compose file, you can run this command with the --remove-orphans flag to clean it up.
 Volume iot-messaging_rabbitmq-data  Creating
 Volume iot-messaging_rabbitmq-data  Created
 Container iot-rabbitmq  Recreate
 Container iot-influxdb  Running
"""

MATCHING_DRY_RUN = """\
 Container iot-rabbitmq  Running
 Container iot-rabbitmq2  Running
 Container iot-rabbitmq3  Running
 Container iot-influxdb  Running
 Container iot-telegraf  Running
"""

CLEAN_FIRST_RUN = """\
 Network iot-messaging_core  Creating
 Network iot-messaging_core  Created
 Volume iot-messaging_rabbitmq-data  Creating
 Volume iot-messaging_rabbitmq-data  Created
 Container iot-rabbitmq  Creating
 Container iot-rabbitmq  Created
"""


def test_mismatch_is_detected_from_a_recreate_and_orphans():
    message = detect_profile_mismatch(MISMATCHED_DRY_RUN)
    assert message is not None
    assert "iot-rabbitmq" in message
    assert "iot-rabbitmq2" in message


def test_a_matching_stack_is_not_flagged():
    assert detect_profile_mismatch(MATCHING_DRY_RUN) is None


def test_a_clean_first_bring_up_is_not_flagged():
    """`Creating` on a container is a first bring-up; `Recreate` is a reconcile.
    Confusing the two would fail every clean session."""
    assert detect_profile_mismatch(CLEAN_FIRST_RUN) is None

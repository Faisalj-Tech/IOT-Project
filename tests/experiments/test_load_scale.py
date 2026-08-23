"""L1 - how many concurrent devices can the containerised swarm actually hold?

Recorded, not asserted (ADR-0004): a connection-count threshold that passes on
this host is a false guarantee on any other. The single assertion is the one the
containerised design exists to make - that the swarm exceeds the ~512 ceiling the
HOST process dies at (spec 2.1, `ValueError: too many file descriptors in
select()`), which is why the swarm runs in Linux containers at all.
"""

import pytest

from tests.conftest import load_mode
from tests.experiments.conftest import write_result
from tests.experiments.load import Swarm, broker_memory_bytes, host_envelope

pytestmark = [pytest.mark.load, pytest.mark.stack]

HOST_PROCESS_CEILING = 512  # measured, spec 2.1
STEPS = [(2, 50), (4, 100), (8, 150), (12, 200)]  # (replicas, devices each)


def test_swarm_connection_ceiling(stack):
    swarm = Swarm()
    swarm.clear_reports()
    steps = []
    peak = 0
    try:
        for replicas, devices in STEPS:
            swarm.scale(replicas, devices=devices, duration_s=600)
            observed = swarm.connection_count()
            steps.append({
                "replicas": replicas,
                "devices_each": devices,
                "target_connections": replicas * devices,
                "observed_connections": observed,
                "broker_memory_bytes": broker_memory_bytes(),
            })
            peak = max(peak, observed)
    finally:
        swarm.stop()

    write_result("L1-swarm-ceiling", {
        "run_id": "L1",
        "host_envelope": host_envelope(),
        "steps": steps,
        "peak_connections": peak,
        "host_process_ceiling": HOST_PROCESS_CEILING,
    })

    assert peak > HOST_PROCESS_CEILING, (
        f"swarm peaked at {peak}, at or below the {HOST_PROCESS_CEILING} ceiling a "
        "single host process dies at - the containerised design bought nothing"
    )

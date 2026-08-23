"""Pure helpers for the device message contract.

Nothing here touches the network, so the contract is unit-testable on its own.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class DeviceSpec:
    region: str
    plant: str
    device: str
    metric: str
    unit: str
    baseline: float
    jitter: float


def topic_for(spec: DeviceSpec) -> str:
    return f"region/{spec.region}/{spec.plant}/{spec.device}/{spec.metric}"


def routing_key_for(spec: DeviceSpec) -> str:
    return topic_for(spec).replace("/", ".")


def _format_timestamp(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def build_payload(
    spec: DeviceSpec,
    seq: int,
    run_id: str,
    value: float,
    now: datetime | None = None,
) -> dict:
    if now is None:
        now = datetime.now(timezone.utc)
    return {
        "ts": _format_timestamp(now),
        "region": spec.region,
        "plant": spec.plant,
        "device": spec.device,
        "metric": spec.metric,
        "value": value,
        "unit": spec.unit,
        "seq": seq,
        "run_id": run_id,
    }


def default_specs(count: int, region: str, plant: str,
                  prefix: str = "press", width: int = 2) -> list[DeviceSpec]:
    """A small SCADA-flavoured fleet: temperature sensors on numbered presses.

    `prefix` and `width` exist for the Phase 6 swarm, whose replicas must not share
    device names: `docker compose --scale` gives every replica an identical command
    line, so each one self-assigns from its container hostname instead (spec 4.2).
    The defaults reproduce the Phase 1-5 names exactly - press-01, press-02, ... -
    because four phases of tests assert on them.

    width=3 is what the swarm passes; width=2 would collide past 99 devices per
    replica.
    """
    return [
        DeviceSpec(
            region=region,
            plant=plant,
            device=f"{prefix}-{index:0{width}d}",
            metric="temp",
            unit="C",
            baseline=70.0,
            jitter=2.0,
        )
        for index in range(1, count + 1)
    ]

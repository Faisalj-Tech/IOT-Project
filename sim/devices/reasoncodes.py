"""PUBACK reason-code accounting for the device swarm.

Why this module exists, measured on 2026-08-22 (spec 2.6):

A QoS-1 publish into a queue whose x-overflow is reject-publish is REJECTED by the
broker. Over MQTT 3.1.1 the broker sends nothing at all - no PUBACK, no error, no
disconnect - and the withheld PUBACK never arrives, even after the queue drains.
Over MQTT 5 the same rejection arrives in 0.2s as 0x97 "Quota exceeded".

aiomqtt does not surface that code: it reported the rejected v5 publish as a
successful publish. So the swarm has to read it at the paho layer, which is what
attach_reason_code_observer does. This is the third instance of the
library-hides-the-signal shape in this project, after ADR-0039 and ADR-0040.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PublishAccounting:
    """What the swarm actually observed, with rejections kept separate.

    The separation matters: aiomqtt raises MqttError for both a timed-out publish
    and a genuine disconnect, and runner.py's reconnect loop catches MqttError as
    a disconnect. Without this split an overflow rejection is indistinguishable
    from connection churn in the results.
    """

    attempted: int = 0
    puback: int = 0
    rejected: int = 0
    timed_out: int = 0
    reconnects: int = 0
    reason_codes: dict[str, int] = field(default_factory=dict)

    def record_attempt(self) -> None:
        self.attempted += 1

    def record_reason_code(self, code: str, is_failure: bool) -> None:
        self.reason_codes[code] = self.reason_codes.get(code, 0) + 1
        if is_failure:
            self.rejected += 1
        else:
            self.puback += 1

    def record_timeout(self) -> None:
        self.timed_out += 1

    def record_reconnect(self) -> None:
        self.reconnects += 1

    def as_dict(self) -> dict:
        return {
            "attempted": self.attempted,
            "puback": self.puback,
            "rejected": self.rejected,
            "timed_out": self.timed_out,
            "reconnects": self.reconnects,
            "reason_codes": dict(self.reason_codes),
        }


def attach_reason_code_observer(client, accounting: PublishAccounting) -> None:
    """Chain a reason-code observer onto an aiomqtt client's paho on_publish.

    Reaches into client._client, a private attribute, because aiomqtt exposes no
    public hook for PUBACK reason codes. tests/test_sim_load.py pins that
    assumption so a future aiomqtt cannot silently break the accounting.

    The original handler is always called afterwards: aiomqtt uses it to resolve
    the publish future, so dropping it would hang every publish.
    """
    paho_client = client._client
    original = paho_client.on_publish

    def on_publish(cli, userdata, mid, reason_code=None, properties=None):
        if reason_code is not None:
            accounting.record_reason_code(
                str(reason_code), bool(getattr(reason_code, "is_failure", False))
            )
        if original is not None:
            original(cli, userdata, mid, reason_code, properties)

    paho_client.on_publish = on_publish

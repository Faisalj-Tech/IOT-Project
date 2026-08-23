"""A Locust User that speaks MQTT.

Locust ships no MQTT client, so this wraps paho and fires
environment.events.request by hand - that is what feeds Locust's statistics and
its HTML report.

The important detail is the failure accounting. A QoS-1 publish into a queue under
x-overflow=reject-publish is REJECTED, and over MQTT 5 that arrives as PUBACK
reason code 0x97 "Quota exceeded" (spec 2.6). A naive wrapper reports the publish
as successful because the PUBACK arrived at all - the same trap aiomqtt fell into.
This class reports a rejection as a FAILED request, so a Locust ramp against a
bounded queue shows the rejection rate instead of a clean green wall of successes.
"""

from __future__ import annotations

import time

import paho.mqtt.client as mqtt
from locust import User, events, task
from paho.mqtt.client import CallbackAPIVersion, MQTTv5


class MQTTDeviceUser(User):
    """One simulated device. Publishes a telemetry payload on each task."""

    abstract = False

    def __init__(self, environment):
        super().__init__(environment)
        self.client: mqtt.Client | None = None
        self._pending: dict[int, float] = {}

    def on_start(self) -> None:
        client_id = f"locust-{id(self):x}"
        self.client = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=client_id, protocol=MQTTv5
        )
        self.client.username_pw_set(
            self.environment.parsed_options.mqtt_user or "device",
            self.environment.parsed_options.mqtt_password or "devicepass",
        )
        self.client.on_publish = self._on_publish
        self.client.connect(self.host or "rabbitmq", 1883, 30)
        self.client.loop_start()

    def on_stop(self) -> None:
        if self.client is not None:
            self.client.disconnect()
            self.client.loop_stop()

    def _on_publish(self, client, userdata, mid, reason_code=None, properties=None):
        started = self._pending.pop(mid, None)
        if started is None:
            return
        elapsed_ms = (time.perf_counter() - started) * 1000
        failed = bool(getattr(reason_code, "is_failure", False))
        events.request.fire(
            request_type="MQTT",
            name="publish qos1",
            response_time=elapsed_ms,
            response_length=0,
            exception=RuntimeError(str(reason_code)) if failed else None,
        )

    @task
    def publish_telemetry(self) -> None:
        assert self.client is not None
        payload = (
            '{"ts":"2026-01-01T00:00:00.000Z","region":"eu","plant":"plant1",'
            '"device":"locust","metric":"temp","value":70.0,"unit":"C",'
            '"seq":1,"run_id":"locust"}'
        )
        info = self.client.publish(
            "region/eu/plant1/locust/temp", payload=payload.encode(), qos=1
        )
        self._pending[info.mid] = time.perf_counter()

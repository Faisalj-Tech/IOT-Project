"""Structured ramp profiles.

Locust owns ramps and its own HTML report. It never produces the numbers the phase
report quotes as headline results - those come from the asyncio swarm (spec
decision 5). Keeping that split is what stops two generators reporting the same
metric two different ways.
"""

from locust import LoadTestShape, events

from loadtest.mqtt_user import MQTTDeviceUser  # noqa: F401  (registers the User)


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--mqtt-user", default="device")
    parser.add_argument("--mqtt-password", default="devicepass")


class StepRamp(LoadTestShape):
    """Hold each level long enough for the pipeline to reach steady state.

    60s per step: Telegraf's flush_interval is 10s and RabbitMQ's stats interval is
    ~5s, so a shorter step would be measuring transients rather than a plateau.
    """

    steps = [
        {"duration": 60, "users": 50, "spawn_rate": 10},
        {"duration": 120, "users": 150, "spawn_rate": 20},
        {"duration": 180, "users": 300, "spawn_rate": 30},
        {"duration": 240, "users": 600, "spawn_rate": 50},
    ]

    def tick(self):
        run_time = self.get_run_time()
        for step in self.steps:
            if run_time < step["duration"]:
                return step["users"], step["spawn_rate"]
        return None

import pytest

pytestmark = pytest.mark.stack


def test_telemetry_queue_is_durable_quorum_with_dlx(rabbit_get):
    response = rabbit_get("/queues/%2F/telemetry.q")
    assert response.status_code == 200, response.text

    queue = response.json()
    assert queue["durable"] is True
    assert queue["arguments"]["x-queue-type"] == "quorum"
    assert queue["arguments"]["x-dead-letter-exchange"] == "dlx"


def test_dlq_exists_and_is_quorum(rabbit_get):
    queue = rabbit_get("/queues/%2F/dlq").json()
    assert queue["durable"] is True
    assert queue["arguments"]["x-queue-type"] == "quorum"


def test_telemetry_queue_bound_to_amq_topic(rabbit_get):
    bindings = rabbit_get("/queues/%2F/telemetry.q/bindings").json()
    pairs = {(b["source"], b["routing_key"]) for b in bindings}
    assert ("amq.topic", "region.#") in pairs


def test_dlq_bound_to_dlx(rabbit_get):
    bindings = rabbit_get("/queues/%2F/dlq/bindings").json()
    pairs = {(b["source"], b["routing_key"]) for b in bindings}
    assert ("dlx", "#") in pairs


def test_expected_users_exist_with_correct_tags(rabbit_get):
    users = {u["name"]: u for u in rabbit_get("/users").json()}
    assert set(users) >= {"admin", "device", "telegraf"}
    assert "administrator" in users["admin"]["tags"]
    assert "monitoring" in users["telegraf"]["tags"]
    assert users["device"]["tags"] == []

"""Static assertions about the region profile's configuration.

No Docker, no stack: these run in the default suite so a config mistake is caught
before anyone spends a bring-up on it. Spec section 6, R1.
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLUSTER_DEFS = ROOT / "config" / "rabbitmq" / "definitions.cluster.json"
REGION_DEFS = ROOT / "config" / "rabbitmq" / "definitions.region.json"
REGION_CONF = ROOT / "config" / "rabbitmq" / "rabbitmq.region.conf"

REGIONS = ("eu", "us")


def _defs(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _default_vhost_slice(definitions: dict) -> dict:
    """Everything the definitions file says about vhost '/'.

    definitions.region.json duplicates the cluster file's '/' objects because
    RabbitMQ 4.3.4 refuses to merge multiple definitions files (spec 2.3). This
    is the test that pays for that duplication.
    """
    keys = ("permissions", "queues", "bindings", "exchanges", "topic_permissions")
    slice_ = {k: [o for o in definitions.get(k, []) if o.get("vhost") == "/"] for k in keys}
    slice_["users"] = sorted(
        (u["name"], u["password_hash"], tuple(u.get("tags", [])))
        for u in definitions["users"]
        if u["name"] in {"admin", "device", "telegraf"}
    )
    return slice_


def test_the_default_vhost_is_identical_to_the_cluster_definitions():
    assert _default_vhost_slice(_defs(REGION_DEFS)) == _default_vhost_slice(_defs(CLUSTER_DEFS))


def test_every_region_queue_declares_a_three_member_quorum_group():
    for queue in _defs(REGION_DEFS)["queues"]:
        args = queue["arguments"]
        assert args["x-queue-type"] == "quorum", queue["name"]
        assert args["x-quorum-initial-group-size"] == 3, (
            f"{queue['vhost']}/{queue['name']} would be a one-member Raft group; "
            "see Phase 3 spec section 2"
        )


def test_region_device_users_are_confined_to_their_own_routing_keys():
    topic_permissions = {
        (t["user"], t["vhost"]): t for t in _defs(REGION_DEFS)["topic_permissions"]
    }
    for region in REGIONS:
        entry = topic_permissions[(f"device-{region}", region)]
        assert entry["exchange"] == "amq.topic"
        assert entry["write"] == rf"^region\.{region}\..*"
        assert entry["read"] == rf"^region\.{region}\..*"


def test_region_device_users_hold_no_permissions_outside_their_region():
    for permission in _defs(REGION_DEFS)["permissions"]:
        user = permission["user"]
        if user.startswith(("device-", "telegraf-")):
            assert permission["vhost"] == user.split("-")[-1], permission


def test_region_consumers_are_read_only():
    """Spec 2.10: with binding_key omitted, amqp_consumer needs no write at all.
    Granting write here would be an unearned privilege in the one file whose
    purpose is proving least privilege."""
    for permission in _defs(REGION_DEFS)["permissions"]:
        if permission["user"].startswith("telegraf-"):
            assert permission["configure"] == "^$", permission
            assert permission["write"] == "^$", permission


def test_regions_carry_distinct_policies():
    policies = {p["name"]: p for p in _defs(REGION_DEFS)["policies"]}
    assert set(policies) == {"eu-limits", "us-limits"}
    assert policies["eu-limits"]["vhost"] == "eu"
    assert policies["us-limits"]["vhost"] == "us"
    assert policies["eu-limits"]["definition"] != policies["us-limits"]["definition"]
    assert policies["eu-limits"]["definition"]["max-length"] == 100000
    assert policies["us-limits"]["definition"]["max-length"] == 10000


def test_the_port_to_vhost_mapping_matches_the_listeners():
    mapping = {
        p["name"]: p["value"] for p in _defs(REGION_DEFS)["global_parameters"]
    }["mqtt_port_to_vhost_mapping"]
    assert mapping == {"1893": "eu", "1993": "us"}
    conf = REGION_CONF.read_text(encoding="utf-8")
    for port, region in mapping.items():
        assert f"mqtt.listeners.tcp.{region} = " in conf
        assert f":{port}" in conf
    assert "mqtt.listeners.tcp.default = 1883" in conf, (
        "1883 must stay unmapped on vhost / so every Phase 1-3 experiment keeps working"
    )


REGION_COMPOSE = ROOT / "compose.region.yml"

EXPECTED_ADDRESSES = {"eu": "172.28.1.10", "us": "172.28.2.10"}


def test_listener_addresses_match_the_compose_static_ips():
    """Two files must agree or a region listener silently binds nothing.

    Parsed as text rather than YAML so this test needs no new dependency and
    fails with a readable diff.
    """
    compose_text = REGION_COMPOSE.read_text(encoding="utf-8")
    conf_text = REGION_CONF.read_text(encoding="utf-8")
    for region, address in EXPECTED_ADDRESSES.items():
        assert f"ipv4_address: {address}" in compose_text, region
        assert f"mqtt.listeners.tcp.{region} = {address}:" in conf_text, region


def test_region_subnets_contain_their_broker_addresses():
    compose_text = REGION_COMPOSE.read_text(encoding="utf-8")
    for address in EXPECTED_ADDRESSES.values():
        network_prefix = address.rsplit(".", 1)[0]
        assert f"subnet: {network_prefix}.0/24" in compose_text, address


REGION_TELEGRAF = ROOT / "config" / "telegraf" / "telegraf.region.d"


def test_region_inputs_are_bound_to_one_vhost_each():
    for region in REGIONS:
        text = (REGION_TELEGRAF / f"{region}.conf").read_text(encoding="utf-8")
        assert f'brokers = ["amqp://rabbitmq:5672/{region}"]' in text
        assert f'queue = "telemetry.{region}.q"' in text
        assert f'region_src = "{region}"' in text


def test_region_inputs_do_not_bind_the_queue():
    """Spec 2.10: binding_key forces a queue.bind, which needs write on the queue.
    The binding is already declared in definitions.region.json, so setting the key
    would cost the consumer its read-only identity and fail Telegraf at startup
    with 403 ACCESS_REFUSED."""
    for region in REGIONS:
        text = (REGION_TELEGRAF / f"{region}.conf").read_text(encoding="utf-8")
        assert "binding_key" not in text
        assert "queue_passive = true" in text
        assert "exchange_passive = true" in text

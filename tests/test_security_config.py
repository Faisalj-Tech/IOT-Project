"""Harness-level tests for the Phase 5 security axis. No Docker.

The ordering assertion matters: compose.region-security.yml carries the region
TLS listeners, which cannot bind under base+security (the addresses do not
exist) and cannot boot under region-alone (no advanced.config, so a TLS listener
has no ssl_options). It is valid only for the both-axes-on combination.
"""

import json
import pytest

from tests import conftest as ct
from tests.conftest import ROOT


@pytest.fixture
def axes(monkeypatch):
    def _set(cluster=False, region=False, security=False):
        for name, on in (("IOT_CLUSTER", cluster),
                         ("IOT_REGION", region),
                         ("IOT_SECURITY", security)):
            monkeypatch.setenv(name, "1") if on else monkeypatch.delenv(name, raising=False)
    return _set


def test_the_default_profile_is_unchanged(axes):
    axes()
    assert ct.compose_files() == ("compose.yml",)


def test_security_alone_appends_only_the_security_overlay(axes):
    axes(security=True)
    assert ct.compose_files() == ("compose.yml", "compose.security.yml")


def test_region_and_security_together_append_the_combination_file(axes):
    axes(region=True, security=True)
    assert ct.compose_files() == (
        "compose.yml",
        "compose.region.yml",
        "compose.security.yml",
        "compose.region-security.yml",
    )


def test_region_alone_never_pulls_in_a_security_file(axes):
    axes(region=True)
    assert ct.compose_files() == ("compose.yml", "compose.region.yml")


def test_security_mode_reads_its_own_variable(axes):
    axes(security=True)
    assert ct.security_mode() is True
    axes()
    assert ct.security_mode() is False


def test_the_profile_guard_uses_the_derived_file_set():
    """_fail_on_profile_mismatch must not hardcode axes, or a new one bypasses it."""
    import inspect
    source = inspect.getsource(ct._fail_on_profile_mismatch)
    assert "compose_files()" in source


def test_the_region_port_map_covers_both_plaintext_and_tls_ports():
    """One definitions file serves region and region+security. A mapping entry
    for a port with no live listener was verified harmless, which is what makes
    that possible without a second file (spec 2.13)."""
    definitions = json.loads(
        (ROOT / "config" / "rabbitmq" / "definitions.region.json").read_text(encoding="utf-8"))
    mapping = next(
        p["value"] for p in definitions["global_parameters"]
        if p["name"] == "mqtt_port_to_vhost_mapping")
    assert mapping == {"1893": "eu", "1993": "us", "9883": "eu", "9993": "us"}


def test_the_region_tls_listeners_avoid_the_all_interfaces_port():
    """8883 is bound on all interfaces by 10-security.conf; a specific-address
    bind on the same port would collide with it."""
    conf = (ROOT / "config" / "rabbitmq" / "conf.d" / "15-region-security.conf").read_text(
        encoding="utf-8")
    assert "172.28.1.10:9883" in conf
    assert "172.28.2.10:9993" in conf
    assert ":8883" not in conf


def test_the_security_plugin_list_is_the_base_list_plus_oauth2():
    """Two plugin files means drift. This is the ADR-0029 pattern: pay for the
    duplication with a test rather than with a surprise."""
    base = (ROOT / "config" / "rabbitmq" / "enabled_plugins").read_text(encoding="utf-8")
    security = (ROOT / "config" / "rabbitmq" / "enabled_plugins.security").read_text(
        encoding="utf-8")

    def parse(text: str) -> set[str]:
        return set(text.strip().strip("[].").split(","))

    assert parse(security) == parse(base) | {"rabbitmq_auth_backend_oauth2"}

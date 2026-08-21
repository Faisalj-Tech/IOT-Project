"""Harness-level tests for the Phase 5 security axis. No Docker.

The ordering assertion matters: compose.region-security.yml carries the region
TLS listeners, which cannot bind under base+security (the addresses do not
exist) and cannot boot under region-alone (no advanced.config, so a TLS listener
has no ssl_options). It is valid only for the both-axes-on combination.
"""

import pytest

from tests import conftest as ct


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

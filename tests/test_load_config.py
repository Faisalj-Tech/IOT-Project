"""Unit tests for the Phase 6 load axis. No Docker required."""

import re

from tests.conftest import ROOT, compose_files, load_mode

PROFILE_VARS = ("IOT_LOAD", "IOT_CLUSTER", "IOT_REGION", "IOT_SECURITY")


def _clear(monkeypatch) -> None:
    for var in PROFILE_VARS:
        monkeypatch.delenv(var, raising=False)


def test_load_mode_off_by_default(monkeypatch):
    _clear(monkeypatch)
    assert load_mode() is False


def test_load_mode_on_with_env(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("IOT_LOAD", "1")
    assert load_mode() is True


def test_load_overlay_applied_last_on_the_default_profile(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("IOT_LOAD", "1")
    assert compose_files() == ("compose.yml", "compose.load.yml")


def test_load_overlay_applied_after_cluster(monkeypatch):
    """L6 runs the load overlay on top of the cluster overlay."""
    _clear(monkeypatch)
    monkeypatch.setenv("IOT_LOAD", "1")
    monkeypatch.setenv("IOT_CLUSTER", "1")
    assert compose_files() == (
        "compose.yml",
        "compose.cluster.yml",
        "compose.load.yml",
    )


def test_other_profiles_unchanged_when_load_is_off(monkeypatch):
    _clear(monkeypatch)
    assert compose_files() == ("compose.yml",)
    monkeypatch.setenv("IOT_CLUSTER", "1")
    assert compose_files() == ("compose.yml", "compose.cluster.yml")


def test_pytest_ini_registers_the_load_marker():
    ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
    assert re.search(r"^\s+load:", ini, re.MULTILINE), "load marker not registered"


def test_pytest_ini_deselects_load_by_default():
    """Registering the marker is only HALF the edit.

    addopts carries the deselection expression. Registering `load` without adding
    it there leaves every load test running in the default suite - and the
    full-suite regression run at the end of every task would then recurse into the
    load experiments. This is carried-forward bite #22's exact shape: a shared
    config file where the obvious edit is not the complete edit.
    """
    ini = (ROOT / "pytest.ini").read_text(encoding="utf-8")
    addopts = next(line for line in ini.splitlines() if line.startswith("addopts"))
    assert "not load" in addopts

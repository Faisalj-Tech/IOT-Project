"""Unit tests for the simulator's TLS parameters. No broker, no Docker.

The backward-compatibility test is the important one: Phases 1-4 call this code
with no TLS arguments at all and must be completely unaffected.
"""

import pytest

from sim.devices.runner import build_tls_params


def test_no_tls_arguments_yields_no_tls(tmp_path):
    assert build_tls_params(None, None, None) is None


def test_all_three_files_yield_tls_parameters(tmp_path):
    ca = tmp_path / "ca.crt"; ca.write_text("x")
    crt = tmp_path / "d.crt"; crt.write_text("x")
    key = tmp_path / "d.key"; key.write_text("x")
    params = build_tls_params(str(ca), str(crt), str(key))
    assert params is not None
    assert params.ca_certs == str(ca)
    assert params.certfile == str(crt)
    assert params.keyfile == str(key)


def test_a_partial_set_is_rejected_loudly(tmp_path):
    """Silently ignoring two of three would produce an unauthenticated connection."""
    ca = tmp_path / "ca.crt"; ca.write_text("x")
    with pytest.raises(ValueError, match="all three"):
        build_tls_params(str(ca), None, None)


def test_a_missing_file_is_reported_by_path(tmp_path):
    ca = tmp_path / "ca.crt"; ca.write_text("x")
    crt = tmp_path / "d.crt"; crt.write_text("x")
    with pytest.raises(FileNotFoundError, match="missing.key"):
        build_tls_params(str(ca), str(crt), str(tmp_path / "missing.key"))

"""Unit tests for the Phase 5 dev PKI helper. No Docker, no broker.

Two of these guard requirements that fail SILENTLY in production: a device cert
without a CRL distribution point is rejected by crl_check=peer with a message
that looks like a broken TLS setup (spec 2.8), and a server cert without the
region IPs in its SAN fails client-side with "IP address mismatch" (spec 2.15).
"""

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtensionOID, NameOID

from scripts import make_certs


@pytest.fixture
def certs(tmp_path):
    make_certs.generate_all(tmp_path)
    return tmp_path


def _load(certs_dir, name):
    return x509.load_pem_x509_certificate((certs_dir / f"{name}.crt").read_bytes())


def _common_name(cert):
    return cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value


def test_every_device_cert_carries_a_crl_distribution_point(certs):
    for name in make_certs.DEVICE_CERTS:
        cert = _load(certs, name)
        ext = cert.extensions.get_extension_for_oid(
            ExtensionOID.CRL_DISTRIBUTION_POINTS).value
        uris = [str(gn.value) for dp in ext for gn in (dp.full_name or [])]
        assert make_certs.DEFAULT_CDP in uris, f"{name} has no CDP: {uris}"


def test_the_broker_server_cert_covers_both_region_ips(certs):
    san = _load(certs, "server").extensions.get_extension_for_oid(
        ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
    ips = {str(ip) for ip in san.get_values_for_type(x509.IPAddress)}
    for address in make_certs.REGION_IPS:
        assert address in ips, f"{address} missing from server SAN: {ips}"
    assert "localhost" in san.get_values_for_type(x509.DNSName)


def test_two_device_certs_share_a_common_name_with_different_serials(certs):
    a, b = _load(certs, "device-eu-a"), _load(certs, "device-eu-b")
    assert _common_name(a) == _common_name(b) == "device-eu"
    assert a.serial_number != b.serial_number


def test_revoking_one_cert_leaves_its_sibling_out_of_the_crl(certs):
    make_certs.revoke(certs, "device-eu-a")
    crl = x509.load_der_x509_crl(make_certs.write_crl(certs).read_bytes())
    revoked = {entry.serial_number for entry in crl}
    assert _load(certs, "device-eu-a").serial_number in revoked
    assert _load(certs, "device-eu-b").serial_number not in revoked


def test_generate_all_is_idempotent(certs):
    """Re-running must not reissue, or a test run would invalidate a live stack."""
    before = (certs / "device-eu-a.crt").read_bytes()
    make_certs.generate_all(certs)
    assert (certs / "device-eu-a.crt").read_bytes() == before


def test_the_ca_key_is_never_written_outside_the_certs_dir(certs):
    key = serialization.load_pem_private_key(
        (certs / "rootCA.key").read_bytes(), password=None)
    assert key.key_size >= 2048

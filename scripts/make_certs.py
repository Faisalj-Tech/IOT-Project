"""Generate the Phase 5 development PKI into main/certs/ (gitignored).

Idempotent by design: re-running reuses existing material, so a test that calls
generate_all() never invalidates a stack that is already running against it.

Two requirements here were measured against a live broker, not assumed, and both
fail in ways that look like something else entirely:

  - Every device cert MUST carry crlDistributionPoints. Under crl_check = peer a
    cert without one is refused with {bad_crls,no_relevant_crls} — identical to
    the message you get when the CRL server is unreachable (spec 2.8, 2.4).
  - The broker server cert MUST carry the region IPs in its SAN, because ADR-0027
    has devices connect to an IP-bound listener by address (spec 2.15).

CLI:
    python -m scripts.make_certs                     # generate everything
    python -m scripts.make_certs --revoke device-a   # revoke and rewrite the CRL
    python -m scripts.make_certs --regenerate-crl
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import json
from collections.abc import Sequence
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

ROOT = Path(__file__).resolve().parents[1]
CERTS_DIR = ROOT / "certs"
STATE_NAME = "ca-state.json"

# Must match the crl service's name and port in compose.security.yml. The broker
# fetches the CRL from this URI; if it cannot, it rejects EVERY cert (spec 2.4).
DEFAULT_CDP = "http://crl:8080/root.crl"

REGION_IPS = ("172.28.1.10", "172.28.2.10")

# name -> CN. The same-CN pairs are what make S3 a test of certificate-scoped
# revocation rather than of account deletion. One pair per profile, because a CN
# must name a user in the definitions that profile actually loads (spec 4.2).
DEVICE_CERTS = {
    "device-a": "device",          # base+security pair
    "device-b": "device",
    "device-eu-a": "device-eu",    # region+security pair
    "device-eu-b": "device-eu",
    "device-us-a": "device-us",
    "unknown-cn": "nosuchuser",    # S1 negative control: valid cert, no such user
}

SERVER_CERTS = {
    "server": {
        "dns": ("iot-rabbitmq", "rabbit1", "localhost"),
        "ips": ("127.0.0.1",) + REGION_IPS,
    },
    "keycloak": {
        "dns": ("keycloak", "localhost"),
        "ips": ("127.0.0.1",),
    },
}

_DAY = dt.timedelta(days=1)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _load_state(certs_dir: Path) -> dict:
    path = certs_dir / STATE_NAME
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"next_serial": 1000, "issued": {}, "revoked": []}


def _save_state(certs_dir: Path, state: dict) -> None:
    (certs_dir / STATE_NAME).write_text(
        json.dumps(state, indent=2), encoding="utf-8")


def _new_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def ensure_ca(certs_dir: Path) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    """Return the dev CA, creating it on first call."""
    certs_dir.mkdir(parents=True, exist_ok=True)
    crt_path, key_path = certs_dir / "rootCA.crt", certs_dir / "rootCA.key"
    if crt_path.exists() and key_path.exists():
        return (
            x509.load_pem_x509_certificate(crt_path.read_bytes()),
            serialization.load_pem_private_key(key_path.read_bytes(), password=None),
        )
    key = _new_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "iot-dev-ca")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_now() - _DAY)
        .not_valid_after(_now() + 3650 * _DAY)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False, content_commitment=False,
                key_encipherment=False, data_encipherment=False,
                key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False),
            critical=True)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    _write_cert(crt_path, cert)
    _write_key(key_path, key)
    _save_state(certs_dir, _load_state(certs_dir))
    return cert, key


def _next_serial(certs_dir: Path, name: str) -> int:
    state = _load_state(certs_dir)
    if name in state["issued"]:
        return state["issued"][name]
    serial = state["next_serial"]
    state["next_serial"] = serial + 1
    state["issued"][name] = serial
    _save_state(certs_dir, state)
    return serial


def _base_builder(certs_dir: Path, name: str, common_name: str,
                  ca_cert: x509.Certificate, key: rsa.RSAPrivateKey):
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(_next_serial(certs_dir, name))
        .not_valid_before(_now() - _DAY)
        .not_valid_after(_now() + 825 * _DAY)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=True, data_encipherment=False,
                key_agreement=False, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False),
            critical=True)
    )


def issue_server_cert(certs_dir: Path, name: str,
                      dns_names: Sequence[str], ip_addresses: Sequence[str]) -> Path:
    crt_path = certs_dir / f"{name}.crt"
    if crt_path.exists():
        return crt_path
    ca_cert, ca_key = ensure_ca(certs_dir)
    key = _new_key()
    san = [x509.DNSName(d) for d in dns_names]
    san += [x509.IPAddress(ipaddress.ip_address(a)) for a in ip_addresses]
    cert = (
        _base_builder(certs_dir, name, name, ca_cert, key)
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False)
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    _write_cert(crt_path, cert)
    _write_key(certs_dir / f"{name}.key", key)
    return crt_path


def issue_device_cert(certs_dir: Path, name: str, common_name: str,
                      cdp_url: str = DEFAULT_CDP) -> Path:
    crt_path = certs_dir / f"{name}.crt"
    if crt_path.exists():
        return crt_path
    ca_cert, ca_key = ensure_ca(certs_dir)
    key = _new_key()
    cdp = x509.DistributionPoint(
        full_name=[x509.UniformResourceIdentifier(cdp_url)],
        relative_name=None, reasons=None, crl_issuer=None)
    cert = (
        _base_builder(certs_dir, name, common_name, ca_cert, key)
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False)
        .add_extension(x509.CRLDistributionPoints([cdp]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    _write_cert(crt_path, cert)
    _write_key(certs_dir / f"{name}.key", key)
    return crt_path


def revoke(certs_dir: Path, name: str) -> None:
    state = _load_state(certs_dir)
    serial = state["issued"].get(name)
    if serial is None:
        raise KeyError(f"no certificate named {name!r} has been issued")
    if serial not in state["revoked"]:
        state["revoked"].append(serial)
    _save_state(certs_dir, state)


def unrevoke_all(certs_dir: Path) -> None:
    """Clear the revocation list. Used by test teardown so one test's revocation
    does not leak into the next; there is no production analogue."""
    state = _load_state(certs_dir)
    state["revoked"] = []
    _save_state(certs_dir, state)


def write_crl(certs_dir: Path) -> Path:
    """Write the CRL in DER. Erlang's ssl_crl_cache HTTP fetch expects DER."""
    ca_cert, ca_key = ensure_ca(certs_dir)
    state = _load_state(certs_dir)
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(ca_cert.subject)
        .last_update(_now() - dt.timedelta(minutes=1))
        .next_update(_now() + 3650 * _DAY)
    )
    for serial in state["revoked"]:
        builder = builder.add_revoked_certificate(
            x509.RevokedCertificateBuilder()
            .serial_number(serial)
            .revocation_date(_now() - dt.timedelta(minutes=1))
            .build()
        )
    crl = builder.sign(ca_key, hashes.SHA256())
    path = certs_dir / "root.crl"
    path.write_bytes(crl.public_bytes(serialization.Encoding.DER))
    return path


def generate_all(certs_dir: Path = CERTS_DIR) -> None:
    certs_dir.mkdir(parents=True, exist_ok=True)
    ensure_ca(certs_dir)
    for name, cfg in SERVER_CERTS.items():
        issue_server_cert(certs_dir, name, cfg["dns"], cfg["ips"])
    for name, common_name in DEVICE_CERTS.items():
        issue_device_cert(certs_dir, name, common_name)
    write_crl(certs_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the Phase 5 dev PKI")
    parser.add_argument("--certs-dir", type=Path, default=CERTS_DIR)
    parser.add_argument("--revoke", metavar="NAME", default=None)
    parser.add_argument("--regenerate-crl", action="store_true")
    args = parser.parse_args(argv)

    generate_all(args.certs_dir)
    if args.revoke:
        revoke(args.certs_dir, args.revoke)
    if args.revoke or args.regenerate_crl:
        write_crl(args.certs_dir)
    print(f"PKI ready in {args.certs_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

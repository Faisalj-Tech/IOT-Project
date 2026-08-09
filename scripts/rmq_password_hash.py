"""Generate RabbitMQ password hashes for definitions.json.

RabbitMQ's rabbit_password_hashing_sha256 algorithm is:
    base64(salt || sha256(salt || utf8(password)))
where salt is 4 random bytes.

Usage:
    python scripts/rmq_password_hash.py adminpass devicepass telegrafpass
"""

from __future__ import annotations

import base64
import hashlib
import os
import sys


def rmq_password_hash(password: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = os.urandom(4)
    if len(salt) != 4:
        raise ValueError("RabbitMQ expects a 4-byte salt")
    digest = hashlib.sha256(salt + password.encode("utf-8")).digest()
    return base64.b64encode(salt + digest).decode("ascii")


def main(passwords: list[str]) -> int:
    if not passwords:
        print(__doc__)
        return 1
    for password in passwords:
        print(f"{password}\t{rmq_password_hash(password)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

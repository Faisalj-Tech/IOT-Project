import base64
import hashlib

from scripts.rmq_password_hash import rmq_password_hash


def test_hash_is_salt_plus_sha256_of_salt_and_password():
    salt = b"\x01\x02\x03\x04"
    encoded = rmq_password_hash("hunter2", salt=salt)

    raw = base64.b64decode(encoded)
    assert raw[:4] == salt
    assert raw[4:] == hashlib.sha256(salt + b"hunter2").digest()
    assert len(raw) == 36


def test_random_salt_produces_different_hashes_for_same_password():
    assert rmq_password_hash("hunter2") != rmq_password_hash("hunter2")

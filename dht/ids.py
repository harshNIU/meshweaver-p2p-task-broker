"""160-bit Kademlia identities and XOR distance (SHA-1 is not authentication)."""

import hashlib

BITS = 160
SPACE = 1 << BITS


def as_int(value: int | str) -> int:
    if isinstance(value, str):
        if len(value) != 40 or any(c not in "0123456789abcdefABCDEF" for c in value):
            raise ValueError("node ID must be 40 hexadecimal characters")
        value = int(value, 16)
    if type(value) is not int or not 0 <= value < SPACE:
        raise ValueError("node ID must be an unsigned 160-bit integer")
    return value


def normalize_id(value: int | str) -> str:
    return f"{as_int(value):040x}"


def make_node_id(name: str) -> str:
    return hashlib.sha1(name.encode("utf-8"), usedforsecurity=False).hexdigest()


def distance(a: int | str, b: int | str) -> int:
    return as_int(a) ^ as_int(b)


def bucket_index(my_id: int | str, other_id: int | str) -> int:
    """Return -1 for self; callers must never insert self into a bucket."""
    return distance(my_id, other_id).bit_length() - 1

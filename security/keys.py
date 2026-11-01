
"""Ed25519 key generation, secure storage, and loading."""

from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def generate_key_pair() -> tuple[Ed25519PrivateKey, Ed25519PublicKey]:
    """Generate a new Ed25519 private and public key pair."""
    private_key = Ed25519PrivateKey.generate()
    return private_key, private_key.public_key()


def save_private_key(
    private_key: Ed25519PrivateKey,
    path: str | Path,
    password: bytes,
) -> None:
    """Save a private key as password-encrypted PEM."""
    if not password:
        raise ValueError("Password must not be empty")

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(password),
    )
    Path(path).write_bytes(pem)


def load_private_key(
    path: str | Path,
    password: bytes,
) -> Ed25519PrivateKey:
    """Load a password-protected Ed25519 private key."""
    key = serialization.load_pem_private_key(
        Path(path).read_bytes(),
        password=password,
    )
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("File does not contain an Ed25519 private key")
    return key


def save_public_key(
    public_key: Ed25519PublicKey,
    path: str | Path,
) -> None:
    """Save a public key in PEM format."""
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    Path(path).write_bytes(pem)


def load_public_key(path: str | Path) -> Ed25519PublicKey:
    """Load an Ed25519 public key from PEM."""
    key = serialization.load_pem_public_key(Path(path).read_bytes())
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError("File does not contain an Ed25519 public key")
    return key

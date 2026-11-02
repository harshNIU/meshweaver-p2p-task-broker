"""Reusable TLS context helpers for MeshWeaver."""

import ssl
from pathlib import Path


def create_server_context(
    cert_path: str | Path,
    key_path: str | Path,
) -> ssl.SSLContext:
    """Create a TLS server context using its certificate and private key."""
    cert_file = Path(cert_path)
    key_file = Path(key_path)

    if not cert_file.is_file():
        raise FileNotFoundError(f"TLS certificate not found: {cert_file}")

    if not key_file.is_file():
        raise FileNotFoundError(f"TLS private key not found: {key_file}")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(
        certfile=str(cert_file),
        keyfile=str(key_file),
    )

    return context


def create_client_context(
    ca_cert_path: str | Path,
) -> ssl.SSLContext:
    """Create a TLS client context that verifies the server certificate."""
    ca_file = Path(ca_cert_path)

    if not ca_file.is_file():
        raise FileNotFoundError(f"CA certificate not found: {ca_file}")

    context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH,
        cafile=str(ca_file),
    )
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    return context

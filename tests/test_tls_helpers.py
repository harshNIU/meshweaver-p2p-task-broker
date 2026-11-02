"""Tests for reusable TLS context helpers."""

import ssl

import pytest

from scripts.gen_certs import generate_certificate
from security.tls import create_client_context, create_server_context


def test_server_context_loads_certificate_and_key(tmp_path):
    cert_path, key_path = generate_certificate(tmp_path)

    context = create_server_context(cert_path, key_path)

    assert isinstance(context, ssl.SSLContext)
    assert context.protocol == ssl.PROTOCOL_TLS_SERVER
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2


def test_client_context_verifies_server_certificate(tmp_path):
    cert_path, _ = generate_certificate(tmp_path)

    context = create_client_context(cert_path)

    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2


def test_server_context_rejects_missing_certificate(tmp_path):
    key_path = tmp_path / "missing-key.pem"
    key_path.write_text("dummy key")

    with pytest.raises(FileNotFoundError):
        create_server_context(tmp_path / "missing-cert.pem", key_path)


def test_server_context_rejects_missing_private_key(tmp_path):
    cert_path = tmp_path / "dummy-cert.pem"
    cert_path.write_text("dummy certificate")

    with pytest.raises(FileNotFoundError):
        create_server_context(cert_path, tmp_path / "missing-key.pem")


def test_client_context_rejects_missing_ca_certificate(tmp_path):
    with pytest.raises(FileNotFoundError):
        create_client_context(tmp_path / "missing-ca.pem")


def test_generated_certificate_files_exist(tmp_path):
    cert_path, key_path = generate_certificate(tmp_path)

    assert cert_path.is_file()
    assert key_path.is_file()
    assert cert_path.stat().st_size > 0
    assert key_path.stat().st_size > 0

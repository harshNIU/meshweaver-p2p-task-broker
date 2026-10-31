"""Generate self-signed TLS certificates for local development."""

import argparse
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def generate_certificate(output_dir: Path, hostname: str = "localhost"):
    """Generate a development certificate and private key."""
    output_dir.mkdir(parents=True, exist_ok=True)

    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, hostname),
        ]
    )

    now = datetime.datetime.now(datetime.UTC)

    san_entries = [x509.DNSName(hostname)]

    if hostname == "localhost":
        san_entries.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))

    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(san_entries),
            critical=False,
        )
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )

    cert_path = output_dir / "localhost-cert.pem"
    key_path = output_dir / "localhost-key.pem"

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))

    # Restrict private-key access where supported.
    try:
        key_path.chmod(0o600)
    except OSError:
        pass

    return cert_path, key_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate a self-signed TLS certificate for development."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("certs"),
        help="Directory where certificate files will be created.",
    )
    parser.add_argument(
        "--hostname",
        default="localhost",
        help="Hostname included in the certificate.",
    )

    args = parser.parse_args()

    cert_path, key_path = generate_certificate(
        args.output_dir,
        args.hostname,
    )

    print(f"Certificate created: {cert_path}")
    print(f"Private key created: {key_path}")
    print("WARNING: Development certificate only. Do not use in production.")


if __name__ == "__main__":
    main()

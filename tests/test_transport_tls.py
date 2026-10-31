
"""Integration tests for optional TLS in the task transport."""

import datetime
import ipaddress
import ssl
import tempfile
import unittest
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from execution.engine import ExecutionEngine
from execution.models import Task
from gossip.state import NodeInfo
from tasks.transport import TaskServer, send_task


def create_test_certificate(directory):
    """Generate a temporary self-signed localhost certificate."""
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])

    now = datetime.datetime.now(datetime.UTC)

    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    key_path = Path(directory) / "localhost-key.pem"
    cert_path = Path(directory) / "localhost-cert.pem"

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    cert_path.write_bytes(
        certificate.public_bytes(serialization.Encoding.PEM)
    )

    return cert_path, key_path


class TestTransportTLS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        cert_path, key_path = create_test_certificate(self.temp_dir.name)

        self.server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.server_context.load_cert_chain(
            certfile=str(cert_path),
            keyfile=str(key_path),
        )

        self.client_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=str(cert_path),
        )

        self.engine = ExecutionEngine()
        self.engine.register("square", lambda value: value * value)

        self.server = await TaskServer(self.engine).start(
            host="127.0.0.1",
            port=0,
            ssl_context=self.server_context,
        )

        host, port = self.server.address
        self.peer = NodeInfo(
            "tls-worker",
            host,
            port,
            cpu=10,
            ram=10,
        )

    async def asyncTearDown(self):
        await self.server.close()
        self.temp_dir.cleanup()

    async def test_trusted_client_executes_task_over_tls(self):
        result = await send_task(
            self.peer,
            Task("square", args=(7,)),
            ssl_context=self.client_context,
        )

        self.assertEqual(result.value, 49)
        self.assertEqual(result.status.value, "completed")

    async def test_untrusted_client_is_rejected(self):
        untrusted_context = ssl.create_default_context()

        with self.assertRaises((ssl.SSLError, OSError)):
            await send_task(
                self.peer,
                Task("square", args=(7,)),
                ssl_context=untrusted_context,
            )


if __name__ == "__main__":
    unittest.main()

"""TLS integration tests for remote callable task execution."""

import datetime
import ipaddress
import tempfile
import unittest
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from execution.engine import ExecutionEngine
from execution.models import Task, TaskStatus
from net.tcp_node import send_callable_task, start_tcp_server
from security.tls import create_client_context, create_server_context

TEST_SECRET = b"meshweaver-test-secret-32-bytes!!"


def create_test_certificate(directory):
    """Generate a temporary certificate trusted by the test client."""
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]
    )
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
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    cert_path = Path(directory) / "localhost-cert.pem"
    key_path = Path(directory) / "localhost-key.pem"

    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    return cert_path, key_path


class TestTCPCallableTLS(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        cert_path, key_path = create_test_certificate(self.temp_dir.name)

        self.server_context = create_server_context(cert_path, key_path)
        self.client_context = create_client_context(cert_path)

        self.engine = ExecutionEngine(max_workers=2)
        self.server, self.received_messages = await start_tcp_server(
            host="127.0.0.1",
            port=0,
            execution_engine=self.engine,
            secret=TEST_SECRET,
            ssl_context=self.server_context,
        )
        self.host, self.port = self.server.sockets[0].getsockname()[:2]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        self.engine.shutdown()
        self.temp_dir.cleanup()

    async def test_authenticated_callable_executes_over_tls(self):
        def multiply_and_add(a, b, bonus=0):
            return a * b + bonus

        task = Task(
            task_name="multiply_and_add",
            args=(10, 20),
            kwargs={"bonus": 5},
        )

        result = await send_callable_task(
            self.host,
            self.port,
            task,
            multiply_and_add,
            secret=TEST_SECRET,
            ssl_context=self.client_context,
        )

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.value, 205)
        self.assertEqual(result.attempts, 1)

        self.assertTrue(
            any(
                message.type == "TASK_SUBMIT"
                for message in self.received_messages
            )
        )


if __name__ == "__main__":
    unittest.main()

"""Tests for authenticated MeshWeaver messages."""

import unittest

from net.messages import Message
from security.message_signing import (
    MessageAuthenticationError,
    sign_message,
    verify_message,
)

TEST_SECRET = b"meshweaver-test-secret-32-bytes!!"


class MessageSigningTests(unittest.TestCase):
    def make_message(self):
        return Message(
            type="TASK_REQUEST",
            sender_id="node-a",
            seq=101,
            payload={"task": {"task_id": "task-1", "task_name": "add"}},
            ts=1700000000.0,
        )

    def test_valid_signature_is_accepted(self):
        message = self.make_message()

        sign_message(message, TEST_SECRET)

        self.assertTrue(verify_message(message, TEST_SECRET))

    def test_unsigned_message_is_rejected(self):
        message = self.make_message()

        with self.assertRaises(MessageAuthenticationError):
            verify_message(message, TEST_SECRET)

    def test_tampered_payload_is_rejected(self):
        message = self.make_message()
        sign_message(message, TEST_SECRET)

        message.payload["task"]["task_name"] = "delete"

        with self.assertRaises(MessageAuthenticationError):
            verify_message(message, TEST_SECRET)

    def test_wrong_secret_is_rejected(self):
        message = self.make_message()
        sign_message(message, TEST_SECRET)

        with self.assertRaises(MessageAuthenticationError):
            verify_message(message, b"another-test-secret-32-bytes-long")

    def test_changed_sender_is_rejected(self):
        message = self.make_message()
        sign_message(message, TEST_SECRET)

        message.sender_id = "attacker"

        with self.assertRaises(MessageAuthenticationError):
            verify_message(message, TEST_SECRET)

    def test_changed_sequence_is_rejected(self):
        message = self.make_message()
        sign_message(message, TEST_SECRET)

        message.seq += 1

        with self.assertRaises(MessageAuthenticationError):
            verify_message(message, TEST_SECRET)

    def test_short_secret_is_rejected(self):
        message = self.make_message()

        with self.assertRaises(MessageAuthenticationError):
            sign_message(message, b"too-short")


if __name__ == "__main__":
    unittest.main()

import unittest

from net.messages import PING, PONG, Message


class TestMessage(unittest.TestCase):

    def test_ping_message_round_trip(self):
        original = Message(
            type=PING,
            sender_id="node-1",
            seq=1,
            payload={"target": "node-2"},
        )

        encoded = original.encode()
        decoded = Message.decode(encoded)

        self.assertEqual(decoded.type, PING)
        self.assertEqual(decoded.sender_id, "node-1")
        self.assertEqual(decoded.seq, 1)
        self.assertEqual(decoded.payload, {"target": "node-2"})
        self.assertGreater(decoded.ts, 0)

    def test_pong_message_round_trip(self):
        original = Message(
            type=PONG,
            sender_id="node-2",
            seq=2,
            payload={"status": "alive"},
        )

        decoded = Message.decode(original.encode())

        self.assertEqual(decoded.type, PONG)
        self.assertEqual(decoded.sender_id, "node-2")
        self.assertEqual(decoded.payload, {"status": "alive"})


if __name__ == "__main__":
    unittest.main()

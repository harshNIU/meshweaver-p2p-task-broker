"""Run a real UDP peer that sends PING messages to node-8000."""

import asyncio
import uuid

from net.messages import PING, PONG, Message
from net.udp_node import start_udp_node


async def main():
    # A unique sender ID prevents collisions with messages from earlier runs.
    session_id = uuid.uuid4().hex[:8]
    peer_id = f"node-8001-{session_id}"

    transport, protocol = await start_udp_node(
        host="127.0.0.1",
        port=8001,
        node_id=peer_id,
    )

    sequence = 0

    try:
        print(f"Real peer {peer_id} running on 127.0.0.1:8001")
        print("Sending PING messages to node-8000 on port 8000.")
        print("Press Ctrl+C to stop.")

        while True:
            sequence += 1

            ping = Message(
                type=PING,
                sender_id=peer_id,
                seq=sequence,
                payload={"target": "node-8000"},
            )

            protocol.send_message(ping, ("127.0.0.1", 8000))

            # Give the receiving node time to send its response.
            await asyncio.sleep(1)

            received_pong = any(
                message.type == PONG
                and message.sender_id == "node-8000"
                and message.seq == sequence
                for message, _address in protocol.received_envelopes
            )

            if received_pong:
                print(f"PING {sequence}: PONG received from node-8000")
            else:
                print(f"PING {sequence}: waiting for PONG")

            await asyncio.sleep(2)

    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPeer test stopped.")

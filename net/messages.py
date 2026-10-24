
"""Standard message format for MeshWeaver."""

import json
import time
from dataclasses import asdict, dataclass

# Message types used for peer communication.
PING = "PING"
PONG = "PONG"
PEER_LIST = "PEER_LIST"
TASK_REQUEST = "TASK_REQUEST"
TASK_RESULT = "TASK_RESULT"


@dataclass
class Message:
    """A standard envelope for messages exchanged between nodes."""

    type: str
    sender_id: str
    seq: int
    payload: dict
    ts: float = 0.0
    sig: str = ""

    def encode(self) -> bytes:
        """Convert the message into JSON bytes."""
        data = asdict(self)

        if not data["ts"]:
            data["ts"] = time.time()

        return json.dumps(data).encode("utf-8")

    @staticmethod
    def decode(raw: bytes) -> "Message":
        """Convert JSON bytes back into a Message object."""
        data = json.loads(raw.decode("utf-8"))
        return Message(**data)

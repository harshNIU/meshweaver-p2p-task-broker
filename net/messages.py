
"""Standard message format for MeshWeaver."""

from dataclasses import asdict, dataclass
import json
import time


# Message types used for peer communication
PING = "PING"
PONG = "PONG"


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

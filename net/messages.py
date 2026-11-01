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
TASK_ASSIGN = "TASK_ASSIGN"
TASK_ACK = "TASK_ACK"


@dataclass(init=False)
class Message:
    """A standard envelope for messages exchanged between nodes."""

    type: str
    sender_id: str
    seq: int
    payload: dict
    ts: float = 0.0
    sig: str = ""

    def __init__(
        self,
        type: str,
        sender_id: str,
        seq: int,
        payload: dict,
        ts: float = 0.0,
        sig: str = "",
        correlation_id: str = "",
    ) -> None:
        """Initialize a message with optional correlation metadata."""
        self.type = type
        self.sender_id = sender_id
        self.seq = seq
        self.payload = payload
        self.ts = ts
        self.sig = sig
        self._correlation_id = correlation_id

    @property
    def correlation_id(self) -> str:
        """Return the optional request/response correlation ID."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: str) -> None:
        """Set the optional request/response correlation ID."""
        self._correlation_id = value

    def encode(self) -> bytes:
        """Convert the message into JSON bytes."""
        data = asdict(self)

        if not data["ts"]:
            data["ts"] = time.time()

        # Keep the original six-field contract for ordinary messages.
        if self.correlation_id:
            data["correlation_id"] = self.correlation_id

        return json.dumps(data).encode("utf-8")

    @staticmethod
    def decode(raw: bytes) -> "Message":
        """Convert JSON bytes back into a Message object."""
        data = json.loads(raw.decode("utf-8"))

        if not isinstance(data, dict):
            raise TypeError("Message JSON must be an object")

        # Extract optional metadata before constructing the message.
        correlation_id = data.pop("correlation_id", "")

        return Message(**data, correlation_id=correlation_id)

"""Track asynchronous request/response correlation IDs and timeouts."""

import asyncio
import uuid
from typing import Any


class RequestTimeoutError(TimeoutError):
    """Raised when a correlated request does not receive a response in time."""


class CorrelationManager:
    """Match responses to pending requests using unique correlation IDs."""

    def __init__(self) -> None:
        """Initialize the pending-request registry."""
        self._pending: dict[str, asyncio.Future[Any]] = {}

    @property
    def pending_count(self) -> int:
        """Return the number of requests currently awaiting responses."""
        return len(self._pending)

    def create_request(self) -> tuple[str, asyncio.Future[Any]]:
        """Create a unique correlation ID and register its response future."""
        loop = asyncio.get_running_loop()
        correlation_id = uuid.uuid4().hex

        future: asyncio.Future[Any] = loop.create_future()
        self._pending[correlation_id] = future

        return correlation_id, future

    async def wait_for_response(
        self,
        correlation_id: str,
        timeout: float,
    ) -> Any:
        """Wait for a response, raising RequestTimeoutError on timeout."""
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        future = self._pending.get(correlation_id)
        if future is None:
            raise KeyError(f"Unknown correlation ID: {correlation_id}")

        try:
            return await asyncio.wait_for(
                asyncio.shield(future),
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise RequestTimeoutError(
                f"Request {correlation_id} timed out after {timeout} seconds"
            ) from exc
        finally:
            self._pending.pop(correlation_id, None)
            if not future.done():
                future.cancel()

    def deliver_response(self, correlation_id: str, response: Any) -> bool:
        """Deliver a response to its matching pending request.

        Returns True if a pending request received the response, or False
        if the correlation ID is unknown or the request is already complete.
        """
        future = self._pending.get(correlation_id)

        if future is None or future.done():
            return False

        future.set_result(response)
        return True

    def cancel_request(self, correlation_id: str) -> bool:
        """Cancel and remove a pending request.

        Returns True if a pending request was cancelled, otherwise False.
        """
        future = self._pending.pop(correlation_id, None)

        if future is None:
            return False

        if not future.done():
            future.cancel()

        return True

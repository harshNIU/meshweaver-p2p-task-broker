"""Configure a compatible asyncio event loop for Windows tests."""

import asyncio
import sys


def pytest_configure(config):
    """Use the Selector event loop on Windows for the test process."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

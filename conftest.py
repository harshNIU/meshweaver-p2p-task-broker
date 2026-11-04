"""Configure the test environment and Windows asyncio event loop."""

import asyncio
import os
import sys


def pytest_configure(config):
    """Configure test-only settings before tests execute."""

    os.environ.setdefault(
        "MESHWEAVER_SHARED_SECRET",
        "meshweaver-pytest-only-secret-2026-secure",
    )

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )
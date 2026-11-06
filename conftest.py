"""Configure the test environment."""

import os


def pytest_configure(config):
    """Configure test-only settings before tests execute."""

    os.environ.setdefault(
        "MESHWEAVER_SHARED_SECRET",
        "meshweaver-pytest-only-secret-2026-secure",
    )

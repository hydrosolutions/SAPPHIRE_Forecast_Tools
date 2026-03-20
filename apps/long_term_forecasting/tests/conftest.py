import pytest


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Prevent test pollution from real env files.

    Sets SAPPHIRE_TEST_ENV=True so tests don't attempt real DB connections
    or file I/O unless explicitly configured.
    """
    monkeypatch.setenv("SAPPHIRE_TEST_ENV", "True")

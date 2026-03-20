"""Root test configuration — ensures correct test ordering and event loop handling."""
import pytest


def pytest_collection_modifyitems(config, items):
    """Ensure unit tests run before browser tests to avoid event loop conflicts."""
    unit_tests = []
    browser_tests = []
    other_tests = []

    for item in items:
        if "/unit/" in str(item.fspath) or "\\unit\\" in str(item.fspath):
            unit_tests.append(item)
        elif "/browser/" in str(item.fspath) or "\\browser\\" in str(item.fspath):
            browser_tests.append(item)
        else:
            other_tests.append(item)

    items[:] = unit_tests + other_tests + browser_tests

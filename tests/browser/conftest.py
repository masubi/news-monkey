"""Playwright browser test fixtures for News Monkey."""
import os
import sys
import socket
import tempfile
import threading
import time

import pytest
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Set test DB before importing app
_tmpdb = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["NEWS_MONKEY_DB"] = _tmpdb.name
os.environ["ENABLE_INGESTION"] = "false"
os.environ["OLLAMA_BASE_URL"] = "http://127.0.0.1:1"  # unreachable — skip LLM in tests


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def server_url():
    """Start a real FastAPI server on a random port for browser tests."""
    from app import app  # noqa: E402

    port = _find_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for server to be ready
    base_url = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            import httpx
            resp = httpx.get(f"{base_url}/api/stats", timeout=1.0)
            if resp.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("Server did not start in time")

    yield base_url


@pytest.fixture
def page(browser, server_url):
    """Create a new browser page pointed at the test server."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    pg.goto(server_url)
    pg.wait_for_load_state("networkidle")
    # News feed is expanded by default per user feedback
    yield pg
    pg.close()
    context.close()


@pytest.fixture
def markets_page(browser, server_url):
    """Create a new browser page pointed at the markets page."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    pg.goto(f"{server_url}/markets")
    pg.wait_for_load_state("networkidle")
    yield pg
    pg.close()
    context.close()


@pytest.fixture
def gaps_page(browser, server_url):
    """Create a new browser page pointed at the gaps page."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    pg.goto(f"{server_url}/gaps")
    pg.wait_for_load_state("networkidle")
    yield pg
    pg.close()
    context.close()


@pytest.fixture
def sources_page(browser, server_url):
    """Create a new browser page pointed at the sources page."""
    context = browser.new_context(viewport={"width": 1280, "height": 800})
    pg = context.new_page()
    pg.goto(f"{server_url}/sources")
    pg.wait_for_load_state("networkidle")
    yield pg
    pg.close()
    context.close()

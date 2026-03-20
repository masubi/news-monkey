"""Load local .env configuration for News Monkey.

Import this module early (in app.py) to populate os.environ
from a project-local .env file. Environment variables always
take precedence over .env values.
"""
from pathlib import Path
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).parent
_ENV_FILE = _PROJECT_ROOT / ".env"

load_dotenv(_ENV_FILE, override=False)

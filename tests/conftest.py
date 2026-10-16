import os

# Settings.from_env() requires these; tests never talk to real services.
os.environ.setdefault("TELEGRAM_TOKEN", "123:test")
os.environ.setdefault("DB_URL", "postgresql://unused")
os.environ.setdefault("GEMINI_API_KEY", "test")

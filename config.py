from pathlib import Path

from pydantic import BaseSettings

class Settings(BaseSettings):
    ALLOWED_ORIGINS: list = ["http://localhost:3000", "http://127.0.0.1:3000"]
    DATABASE_NAME: str = "inflation_database.db"

    class Config:
        env_file = ".env"

settings = Settings()

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / settings.DATABASE_NAME

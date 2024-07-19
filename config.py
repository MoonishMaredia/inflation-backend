from pydantic import BaseSettings

class Settings(BaseSettings):
    ALLOWED_ORIGINS: list = ["http://localhost:3000", "https://localhost:3000", "https://www.us-cpi.com"]
    DATABASE_NAME: str = "inflation_database.db"

    class Config:
        env_file = ".env"

settings = Settings()
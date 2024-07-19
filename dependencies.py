from config import Settings

settings = Settings()

async def get_db():
    return settings.DATABASE_NAME
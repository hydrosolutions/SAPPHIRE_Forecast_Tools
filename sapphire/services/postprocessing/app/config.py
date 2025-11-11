# config.py (new file)
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Postprocessing Service API"
    version: str = "1.0.0"
    database_url: str = "postgresql://postgresql:password@postprocessing-db-service:5432/postprocessing_db"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()

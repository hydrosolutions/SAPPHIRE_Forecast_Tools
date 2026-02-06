from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    app_name: str = "Preprocessing Service API"
    version: str = "1.0.0"
    database_url: str = "postgresql://postgresql:password@preprocessing-db-service:5432/preprocessing_db"
    log_level: str = "INFO"
    api_base_url: str = "http://preprocessing-api:8002"
    batch_size: int = 1000
    csv_folder: str = "/intermediate_data/"

    class Config:
        env_file = Path(__file__).parent.parent / ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = False


settings = Settings()

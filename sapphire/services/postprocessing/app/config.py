from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    app_name: str = "Postprocessing Service API"
    version: str = "1.0.0"
    database_url: str
    log_level: str
    api_base_url: str
    batch_size: int
    csv_folder: str

settings = Settings()

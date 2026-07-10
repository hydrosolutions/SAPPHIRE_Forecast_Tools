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
    config_folder: str
    # Default keeps the service booting without extra config; override per
    # deployment with the public HTTPS gateway host for real third-party sharing.
    public_bulletin_base_url: str = "http://localhost:8000"

settings = Settings()

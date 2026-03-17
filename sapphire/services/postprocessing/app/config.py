from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent / ".env",
        env_file_encoding='utf-8',
        case_sensitive=False,
    )

    app_name: str = "Postprocessing Service API"
    version: str = "1.0.0"
    database_url: str
    log_level: str
    api_base_url: str
    batch_size: int
    csv_folder: str

settings = Settings()

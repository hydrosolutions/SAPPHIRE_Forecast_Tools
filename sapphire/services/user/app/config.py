from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "User Service API"
    version: str = "1.0.0"
    database_url: str
    log_level: str

settings = Settings()

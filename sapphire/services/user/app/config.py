from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "User Service API"
    version: str = "1.0.0"
    database_url: str = "postgresql://postgres:password@user-db:5432/user_db"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()

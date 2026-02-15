from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    app_name: str = "Auth Service API"
    version: str = "1.0.0"
    database_url: str = "postgresql://postgres:password@auth-db:5432/auth_db"
    user_service_url: str = "http://user-api:8004"
    jwt_secret_key: str = "your-super-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    log_level: str = "INFO"

settings = Settings()

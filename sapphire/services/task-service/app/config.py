from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Application
    app_name: str = "Task Management Service"
    version: str = "1.0.0"
    debug: bool = False
    environment: str = "development"

    # Database
    database_url: str = "postgresql://postgres:password@localhost:5432/taskdb"
    test_database_url: str = "postgresql://postgres:password@localhost:5432/taskdb_test"

    # Database pool settings
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_pre_ping: bool = True

    # API
    api_prefix: str = "/api/v1"
    max_page_size: int = 100
    default_page_size: int = 20

    # Logging
    log_level: str = "INFO"
    log_format: str = "text"

    # CORS
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]
    cors_credentials: bool = True
    cors_methods: list[str] = ["*"]
    cors_headers: list[str] = ["*"]

    # Security
    api_key_header: str = "X-API-Key"
    rate_limit_per_minute: int = 60

    class Config:
        env_file = ".env"
        case_sensitive = False

    def get_database_url(self, is_test: bool = False) -> str:
        """Get the appropriate database URL"""
        if is_test:
            # Ensure test database is different from main database
            test_url = self.test_database_url
            if test_url == self.database_url:
                raise ValueError("Test database URL must be different from main database URL")
            return test_url
        return self.database_url

    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment.lower() == "production"


@lru_cache()
def get_settings() -> Settings:
    """Cache settings to avoid reading .env multiple times"""
    return Settings()

# Validate settings on import
_settings = get_settings()
if not _settings.database_url:
    raise ValueError("DATABASE_URL must be set in environment or .env file")

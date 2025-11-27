from pydantic_settings import BaseSettings
from typing import Dict


class Settings(BaseSettings):
    """
    Gateway configuration settings
    Load from environment variables
    """

    # Gateway settings
    gateway_title: str = "SAPPHIRE API Gateway"
    gateway_version: str = "1.0.0"

    # Timeout settings (in seconds)
    request_timeout: int = 30
    health_check_timeout: int = 5

    # Authentication
    api_key_enabled: bool = False
    api_key: str = "your-secret-api-key"

    # Rate limiting (requests per minute per IP)
    rate_limit_enabled: bool = False
    rate_limit: int = 100

    class Config:
        env_file = ".env"

    @property
    def services(self) -> Dict[str, str]:
        """Get all service URLs as a dictionary."""
        return {
            "preprocessing": "http://preprocessing-api:8002",
            "postprocessing": "http://postprocessing-api:8003",
        }

    def get_service_url(self, service_name: str) -> str:
        """Get the URL of a specific service by name."""
        return self.services.get(service_name)


settings = Settings()

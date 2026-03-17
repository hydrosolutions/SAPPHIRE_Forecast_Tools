from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict


class Settings(BaseSettings):
    """
    Gateway configuration settings
    Load from environment variables
    """
    model_config = SettingsConfigDict(env_file=".env")

    # Gateway settings
    gateway_title: str = "SAPPHIRE API Gateway"
    gateway_version: str = "1.0.0"

    # Timeout settings (in seconds)
    request_timeout: int
    health_check_timeout: int

    # Authentication
    api_key_enabled: bool
    api_key: str

    # Rate limiting (requests per minute per IP)
    rate_limit_enabled: bool
    rate_limit: int

    # Service URLs
    preprocessing_api_url: str
    postprocessing_api_url: str
    user_api_url: str
    auth_api_url: str

    @property
    def services(self) -> Dict[str, str]:
        """Get all service URLs as a dictionary."""
        return {
            "preprocessing": self.preprocessing_api_url,
            "postprocessing": self.postprocessing_api_url,
            "user": self.user_api_url,
            "auth": self.auth_api_url,
        }

    def get_service_url(self, service_name: str) -> str:
        """Get the URL of a specific service by name."""
        return self.services.get(service_name)


settings = Settings()

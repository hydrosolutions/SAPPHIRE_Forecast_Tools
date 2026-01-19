"""
Base client with retry logic and common functionality.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, cast

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


class SapphireAPIError(Exception):
    """Exception raised when API operations fail after all retries."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class SapphireAPIClient:
    """
    Base client for SAPPHIRE API with retry logic.

    Provides common functionality for preprocessing and postprocessing clients:
    - Automatic retry with exponential backoff on transient failures
    - Batch posting for large datasets
    - Strict error handling (fails on persistent errors)
    - Optional authentication via Bearer token

    Args:
        base_url: API base URL (default: http://localhost:8000)
        auth_token: Optional Bearer token for authentication. When provided,
            included in Authorization header for all requests.
        max_retries: Maximum number of retry attempts (default: 3)
        batch_size: Number of records per batch for bulk writes (default: 1000)
        timeout: Request timeout in seconds (default: 30)
    """

    # HTTP status codes that should trigger a retry
    RETRYABLE_STATUS_CODES = {502, 503, 504}

    # Service prefix for API routing (override in subclasses)
    # e.g., "/api/preprocessing" or "/api/postprocessing"
    SERVICE_PREFIX = ""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        auth_token: Optional[str] = None,
        max_retries: int = 3,
        batch_size: int = 1000,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.timeout = timeout
        self.session = requests.Session()

        # Set up authentication header if token provided
        if auth_token:
            self.session.headers["Authorization"] = f"Bearer {auth_token}"

    def __repr__(self) -> str:
        """Return string representation without exposing auth token."""
        auth_status = "authenticated" if self.auth_token else "unauthenticated"
        return f"{self.__class__.__name__}(base_url={self.base_url!r}, {auth_status})"

    @property
    def is_authenticated(self) -> bool:
        """Check if client has an auth token configured."""
        return self.auth_token is not None

    def _get_full_url(self, endpoint: str) -> str:
        """
        Build full URL with service prefix.

        Args:
            endpoint: API endpoint (e.g., "/runoff/")

        Returns:
            Full URL (e.g., "http://localhost:8000/api/preprocessing/runoff/")
        """
        return f"{self.base_url}{self.SERVICE_PREFIX}{endpoint}"

    def _get_retry_decorator(self) -> Callable[[Callable[[], requests.Response]], Callable[[], requests.Response]]:
        """Create a retry decorator with configured settings."""
        return retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be appended to base_url with service prefix)
            params: Query parameters
            json: JSON body for POST requests

        Returns:
            Response object

        Raises:
            SapphireAPIError: If request fails after all retries
        """
        url = self._get_full_url(endpoint)

        @self._get_retry_decorator()
        def _do_request() -> requests.Response:
            resp = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.timeout,
            )

            # Retry on certain status codes
            if resp.status_code in self.RETRYABLE_STATUS_CODES:
                raise requests.ConnectionError(
                    f"Server returned {resp.status_code}, will retry"
                )

            return resp

        try:
            response: requests.Response = _do_request()
        except (requests.ConnectionError, requests.Timeout) as e:
            raise SapphireAPIError(
                f"Failed to connect to {url} after {self.max_retries} attempts: {e}"
            )

        # Check for non-retryable errors
        if response.status_code == 401:
            raise SapphireAPIError(
                "Authentication required. Provide a valid auth_token.",
                status_code=401,
                response=response.text,
            )
        if response.status_code == 403:
            raise SapphireAPIError(
                "Access denied. Insufficient permissions for this resource.",
                status_code=403,
                response=response.text,
            )
        if response.status_code >= 400:
            raise SapphireAPIError(
                f"API request failed: {response.status_code}",
                status_code=response.status_code,
                response=response.text,
            )

        return response

    def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Make a GET request and return JSON response.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            List of records from API
        """
        response = self._make_request("GET", endpoint, params=params)
        return cast(List[Dict[str, Any]], response.json())

    def _post(
        self,
        endpoint: str,
        data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Make a POST request and return JSON response.

        Args:
            endpoint: API endpoint
            data: Request body

        Returns:
            List of created/updated records
        """
        response = self._make_request("POST", endpoint, json=data)
        return cast(List[Dict[str, Any]], response.json())

    def _post_batched(
        self,
        endpoint: str,
        records: List[Dict[str, Any]],
    ) -> int:
        """
        Post records in batches.

        Args:
            endpoint: API endpoint
            records: List of records to post

        Returns:
            Total number of records successfully posted

        Raises:
            SapphireAPIError: If any batch fails
        """
        if not records:
            return 0

        total_posted = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(records) + self.batch_size - 1) // self.batch_size

            logger.info(f"Posting batch {batch_num}/{total_batches} ({len(batch)} records)")

            try:
                result = self._post(endpoint, {"data": batch})
                total_posted += len(result)
            except SapphireAPIError as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                raise SapphireAPIError(
                    f"Failed at batch {batch_num}/{total_batches}: {e}",
                    status_code=e.status_code,
                    response=e.response,
                )

        logger.info(f"Successfully posted {total_posted} records to {endpoint}")
        return total_posted

    def health_check(self) -> bool:
        """
        Check if the API is healthy.

        Returns:
            True if API is healthy, False otherwise
        """
        try:
            response = self._make_request("GET", "/health")
            data = cast(Dict[str, Any], response.json())
            return data.get("status") == "healthy"
        except SapphireAPIError:
            return False

    def readiness_check(self) -> bool:
        """
        Check if the API is ready (including database connection).

        Returns:
            True if API is ready, False otherwise
        """
        try:
            response = self._make_request("GET", "/health/ready")
            data = cast(Dict[str, Any], response.json())
            return data.get("status") == "ready"
        except SapphireAPIError:
            return False

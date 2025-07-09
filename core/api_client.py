"""
API client module for the data processing framework.
Provides unified REST API client with authentication, rate limiting, and error handling.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import aiohttp
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .exceptions import APIClientException, handle_exception, handle_async_exception
from .logging_config import get_logger, log_api_request
from .models import APIRequest, APIResponse, ToolConfig

logger = get_logger(__name__)


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, max_requests: int = 100, time_window: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed in the time window
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        async with self._lock:
            now = time.time()
            
            # Remove old requests outside the time window
            self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
            
            # Check if we can make a request
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0])
                if sleep_time > 0:
                    logger.warning(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
                    await asyncio.sleep(sleep_time)
                    await self.acquire()  # Retry
            
            # Record the request
            self.requests.append(now)
    
    def acquire_sync(self) -> None:
        """Synchronous version of acquire."""
        now = time.time()
        
        # Remove old requests outside the time window
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        # Check if we can make a request
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.acquire_sync()  # Retry
        
        # Record the request
        self.requests.append(now)


class AuthManager:
    """Authentication manager for different auth types."""
    
    def __init__(self, auth_type: str, credentials: Dict[str, Any]):
        """
        Initialize authentication manager.
        
        Args:
            auth_type: Type of authentication (api_key, oauth, basic, bearer, service_account)
            credentials: Authentication credentials
        """
        self.auth_type = auth_type
        self.credentials = credentials
        self.token = None
        self.token_expires_at = None
    
    @handle_exception
    def get_headers(self) -> Dict[str, str]:
        """
        Get authentication headers.
        
        Returns:
            Dictionary of headers for authentication
            
        Raises:
            APIClientException: If authentication fails
        """
        headers = {}
        
        if self.auth_type == "api_key":
            api_key = self.credentials.get("api_key")
            key_header = self.credentials.get("key_header", "X-API-Key")
            if not api_key:
                raise APIClientException(
                    "API key not provided",
                    details={"auth_type": self.auth_type}
                )
            headers[key_header] = api_key
            
        elif self.auth_type == "bearer":
            token = self.credentials.get("token")
            if not token:
                raise APIClientException(
                    "Bearer token not provided",
                    details={"auth_type": self.auth_type}
                )
            headers["Authorization"] = f"Bearer {token}"
            
        elif self.auth_type == "oauth":
            # OAuth implementation would go here
            # For now, assume token is provided
            token = self.credentials.get("access_token")
            if not token:
                raise APIClientException(
                    "OAuth access token not provided",
                    details={"auth_type": self.auth_type}
                )
            headers["Authorization"] = f"Bearer {token}"
            
        elif self.auth_type == "basic":
            username = self.credentials.get("username")
            password = self.credentials.get("password")
            if not username or not password:
                raise APIClientException(
                    "Basic auth credentials not provided",
                    details={"auth_type": self.auth_type}
                )
            import base64
            auth_string = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {auth_string}"
            
        elif self.auth_type == "service_account":
            # Service account implementation would go here
            # For now, assume token is provided
            token = self.credentials.get("access_token")
            if not token:
                raise APIClientException(
                    "Service account access token not provided",
                    details={"auth_type": self.auth_type}
                )
            headers["Authorization"] = f"Bearer {token}"
        
        return headers


class APIClient:
    """Unified API client for external tool APIs."""
    
    def __init__(self, config: ToolConfig, credentials: Dict[str, Any]):
        """
        Initialize API client.
        
        Args:
            config: Tool configuration
            credentials: Authentication credentials
        """
        self.config = config
        self.credentials = credentials
        self.base_url = config.api_base_url
        self.rate_limiter = RateLimiter(config.rate_limit, 60)
        self.auth_manager = AuthManager(config.auth_type, credentials)
        
        # Setup requests session
        self.session = requests.Session()
        self._setup_session()
        
        logger.info(f"APIClient initialized for {config.name}", 
                   base_url=self.base_url, 
                   auth_type=config.auth_type,
                   rate_limit=config.rate_limit)
    
    def _setup_session(self) -> None:
        """Setup requests session with retry strategy."""
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST", "PUT", "DELETE", "PATCH"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            "User-Agent": f"DataProcessingFramework/{self.config.name}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
    
    def _get_full_url(self, endpoint: str) -> str:
        """Get full URL for an endpoint."""
        return urljoin(self.base_url, endpoint.lstrip('/'))
    
    @handle_exception
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> APIResponse:
        """
        Make HTTP request with error handling and logging.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            headers: Additional headers
            
        Returns:
            API response object
            
        Raises:
            APIClientException: If request fails
        """
        # Rate limiting
        self.rate_limiter.acquire_sync()
        
        # Prepare request
        url = self._get_full_url(endpoint)
        request_headers = self.auth_manager.get_headers()
        
        if headers:
            request_headers.update(headers)
        
        start_time = time.time()
        response = None
        error_message = None
        
        try:
            # Make the request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=request_headers,
                timeout=self.config.timeout
            )
            
            response_time = time.time() - start_time
            
                         # Parse response
             response_data = None
             if response.headers.get('content-type', '').startswith('application/json'):
                 try:
                     response_data = response.json()
                 except ValueError:
                     response_data = response.text
             else:
                 response_data = response.text
             
                          # Create response object
             api_response = APIResponse(
                 status_code=response.status_code,
                 headers=dict(response.headers),
                 data=response_data if isinstance(response_data, dict) else {"content": response_data},
                 response_time=response_time
             )
            
            # Check for HTTP errors
            if not response.ok:
                error_message = f"HTTP {response.status_code}: {response.reason}"
                api_response.error = error_message
                
                raise APIClientException(
                    f"API request failed: {error_message}",
                    status_code=response.status_code,
                    response_data=response_data,
                    endpoint=endpoint,
                    method=method,
                    details={
                        "url": url,
                        "response_time": response_time
                    }
                )
            
            # Log successful request
            log_api_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                response_status=response.status_code,
                response_time=response_time
            )
            
            return api_response
            
        except APIClientException:
            # Log failed request
            log_api_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                response_status=response.status_code if response else None,
                response_time=time.time() - start_time,
                error=error_message
            )
            raise
            
        except Exception as e:
            response_time = time.time() - start_time
            error_message = str(e)
            
            # Log failed request
            log_api_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                response_status=None,
                response_time=response_time,
                error=error_message
            )
            
            raise APIClientException(
                f"Request failed: {error_message}",
                endpoint=endpoint,
                method=method,
                details={
                    "url": url,
                    "response_time": response_time
                },
                original_exception=e
            )
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make GET request."""
        return self._make_request("GET", endpoint, params=params, **kwargs)
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make POST request."""
        return self._make_request("POST", endpoint, data=data, **kwargs)
    
    def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make PUT request."""
        return self._make_request("PUT", endpoint, data=data, **kwargs)
    
    def delete(self, endpoint: str, **kwargs) -> APIResponse:
        """Make DELETE request."""
        return self._make_request("DELETE", endpoint, **kwargs)
    
    def patch(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make PATCH request."""
        return self._make_request("PATCH", endpoint, data=data, **kwargs)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def close(self):
        """Close the session."""
        self.session.close()


class AsyncAPIClient:
    """Asynchronous API client for external tool APIs."""
    
    def __init__(self, config: ToolConfig, credentials: Dict[str, Any]):
        """
        Initialize async API client.
        
        Args:
            config: Tool configuration
            credentials: Authentication credentials
        """
        self.config = config
        self.credentials = credentials
        self.base_url = config.api_base_url
        self.rate_limiter = RateLimiter(config.rate_limit, 60)
        self.auth_manager = AuthManager(config.auth_type, credentials)
        self.session = None
        
        logger.info(f"AsyncAPIClient initialized for {config.name}", 
                   base_url=self.base_url, 
                   auth_type=config.auth_type,
                   rate_limit=config.rate_limit)
    
    async def _ensure_session(self) -> None:
        """Ensure aiohttp session exists."""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": f"DataProcessingFramework/{self.config.name}",
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                }
            )
    
    def _get_full_url(self, endpoint: str) -> str:
        """Get full URL for an endpoint."""
        return urljoin(self.base_url, endpoint.lstrip('/'))
    
    @handle_async_exception
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> APIResponse:
        """
        Make async HTTP request with error handling and logging.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            headers: Additional headers
            
        Returns:
            API response object
            
        Raises:
            APIClientException: If request fails
        """
        await self._ensure_session()
        
        # Rate limiting
        await self.rate_limiter.acquire()
        
        # Prepare request
        url = self._get_full_url(endpoint)
        request_headers = self.auth_manager.get_headers()
        
        if headers:
            request_headers.update(headers)
        
        start_time = time.time()
        response = None
        error_message = None
        
        try:
            # Make the request
            async with self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=request_headers
            ) as response:
                
                response_time = time.time() - start_time
                
                # Parse response
                response_data = None
                content_type = response.headers.get('content-type', '')
                
                if content_type.startswith('application/json'):
                    try:
                        response_data = await response.json()
                    except ValueError:
                        response_data = await response.text()
                else:
                    response_data = await response.text()
                
                # Create response object
                api_response = APIResponse(
                    status_code=response.status,
                    headers=dict(response.headers),
                    data=response_data,
                    response_time=response_time
                )
                
                # Check for HTTP errors
                if not response.ok:
                    error_message = f"HTTP {response.status}: {response.reason}"
                    api_response.error = error_message
                    
                    raise APIClientException(
                        f"API request failed: {error_message}",
                        status_code=response.status,
                        response_data=response_data,
                        endpoint=endpoint,
                        method=method,
                        details={
                            "url": url,
                            "response_time": response_time
                        }
                    )
                
                # Log successful request
                log_api_request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    response_status=response.status,
                    response_time=response_time
                )
                
                return api_response
                
        except APIClientException:
            # Log failed request
            log_api_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                response_status=response.status if response else None,
                response_time=time.time() - start_time,
                error=error_message
            )
            raise
            
        except Exception as e:
            response_time = time.time() - start_time
            error_message = str(e)
            
            # Log failed request
            log_api_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                response_status=None,
                response_time=response_time,
                error=error_message
            )
            
            raise APIClientException(
                f"Request failed: {error_message}",
                endpoint=endpoint,
                method=method,
                details={
                    "url": url,
                    "response_time": response_time
                },
                original_exception=e
            )
    
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make async GET request."""
        return await self._make_request("GET", endpoint, params=params, **kwargs)
    
    async def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make async POST request."""
        return await self._make_request("POST", endpoint, data=data, **kwargs)
    
    async def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make async PUT request."""
        return await self._make_request("PUT", endpoint, data=data, **kwargs)
    
    async def delete(self, endpoint: str, **kwargs) -> APIResponse:
        """Make async DELETE request."""
        return await self._make_request("DELETE", endpoint, **kwargs)
    
    async def patch(self, endpoint: str, data: Optional[Dict[str, Any]] = None, **kwargs) -> APIResponse:
        """Make async PATCH request."""
        return await self._make_request("PATCH", endpoint, data=data, **kwargs)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def close(self):
        """Close the session."""
        if self.session:
            await self.session.close()
            self.session = None


class APIClientFactory:
    """Factory for creating API clients."""
    
    @staticmethod
    def create_client(config: ToolConfig, credentials: Dict[str, Any]) -> APIClient:
        """
        Create synchronous API client.
        
        Args:
            config: Tool configuration
            credentials: Authentication credentials
            
        Returns:
            API client instance
        """
        return APIClient(config, credentials)
    
    @staticmethod
    def create_async_client(config: ToolConfig, credentials: Dict[str, Any]) -> AsyncAPIClient:
        """
        Create asynchronous API client.
        
        Args:
            config: Tool configuration
            credentials: Authentication credentials
            
        Returns:
            Async API client instance
        """
        return AsyncAPIClient(config, credentials) 
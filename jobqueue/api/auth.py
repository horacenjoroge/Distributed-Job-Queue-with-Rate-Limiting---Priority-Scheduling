"""
Authentication middleware for API using API keys.
"""
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from typing import Optional
from jobqueue.utils.logger import log
from config import settings


# API Key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class APIKeyAuth:
    """
    API Key authentication handler.
    """
    
    def __init__(self, api_keys: Optional[list] = None):
        """
        Initialize API key authentication.
        
        Args:
            api_keys: List of valid API keys (default: from settings)
        """
        self.api_keys = api_keys or getattr(settings, 'api_keys', [])
        
        # If no API keys configured, allow all requests (development mode)
        if not self.api_keys:
            log.warning("No API keys configured - API is open (development mode)")
    
    def verify_api_key(self, api_key: Optional[str] = None) -> bool:
        """
        Verify API key.
        
        Args:
            api_key: API key to verify
            
        Returns:
            True if valid, False otherwise
        """
        # If no API keys configured, allow all requests
        if not self.api_keys:
            return True
        
        if not api_key:
            return False
        
        return api_key in self.api_keys
    
    async def get_current_user(self, api_key: Optional[str] = Security(api_key_header)):
        """
        Get current user from API key.
        
        Args:
            api_key: API key from header
            
        Returns:
            User identifier (API key)
            
        Raises:
            HTTPException if API key is invalid
        """
        if not self.verify_api_key(api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key",
                headers={"WWW-Authenticate": "ApiKey"},
            )
        
        return api_key or "anonymous"


# Global API key auth instance
api_key_auth = APIKeyAuth()


def get_api_key_auth() -> APIKeyAuth:
    """Get API key authentication instance."""
    return api_key_auth


def require_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Dependency to require API key authentication.
    
    Args:
        api_key: API key from header
        
    Returns:
        API key if valid
        
    Raises:
        HTTPException if API key is invalid
    """
    return api_key_auth.get_current_user(api_key)

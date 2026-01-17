"""
Rate limiting middleware for API endpoints.
"""
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class APIRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware for API requests.
    Uses Redis to track request counts per IP/API key.
    """
    
    def __init__(
        self,
        app,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
        key_prefix: str = "api:ratelimit"
    ):
        """
        Initialize API rate limit middleware.
        
        Args:
            app: FastAPI application
            requests_per_minute: Max requests per minute (default: 60)
            requests_per_hour: Max requests per hour (default: 1000)
            key_prefix: Redis key prefix (default: "api:ratelimit")
        """
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.key_prefix = key_prefix
    
    def _get_client_id(self, request: Request) -> str:
        """
        Get client identifier for rate limiting.
        Uses API key if available, otherwise IP address.
        
        Args:
            request: FastAPI request
            
        Returns:
            Client identifier
        """
        # Try to get API key from header
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"apikey:{api_key}"
        
        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"
    
    def _check_rate_limit(self, client_id: str, window: str, limit: int) -> tuple[bool, int]:
        """
        Check rate limit for a client in a time window.
        
        Args:
            client_id: Client identifier
            window: Time window ("minute" or "hour")
            limit: Request limit
            
        Returns:
            Tuple of (allowed, remaining)
        """
        try:
            now = datetime.utcnow()
            
            if window == "minute":
                window_key = now.strftime("%Y%m%d%H%M")
                ttl = 60  # 1 minute
            else:  # hour
                window_key = now.strftime("%Y%m%d%H")
                ttl = 3600  # 1 hour
            
            redis_key = f"{self.key_prefix}:{window}:{client_id}:{window_key}"
            
            # Get current count
            current_count = redis_broker.client.get(redis_key)
            if current_count:
                current_count = int(current_count)
            else:
                current_count = 0
            
            # Check if limit exceeded
            if current_count >= limit:
                return False, 0
            
            # Increment count
            pipe = redis_broker.client.pipeline()
            pipe.incr(redis_key)
            pipe.expire(redis_key, ttl)
            pipe.execute()
            
            remaining = limit - (current_count + 1)
            return True, remaining
            
        except Exception as e:
            log.error(f"Error checking rate limit: {e}")
            # On error, allow request (fail open)
            return True, limit
    
    async def dispatch(self, request: Request, call_next: Callable):
        """
        Process request with rate limiting.
        
        Args:
            request: FastAPI request
            call_next: Next middleware/endpoint
            
        Returns:
            Response
        """
        # Skip rate limiting for health check and docs
        if request.url.path in ["/health", "/docs", "/openapi.json", "/redoc"]:
            return await call_next(request)
        
        # Get client identifier
        client_id = self._get_client_id(request)
        
        # Check rate limits
        minute_allowed, minute_remaining = self._check_rate_limit(
            client_id, "minute", self.requests_per_minute
        )
        
        hour_allowed, hour_remaining = self._check_rate_limit(
            client_id, "hour", self.requests_per_hour
        )
        
        # If either limit exceeded, return 429
        if not minute_allowed or not hour_allowed:
            log.warning(
                f"Rate limit exceeded for {client_id}",
                extra={
                    "client_id": client_id,
                    "minute_remaining": minute_remaining,
                    "hour_remaining": hour_remaining,
                    "path": request.url.path
                }
            )
            
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Rate limit exceeded",
                    "message": "Too many requests. Please try again later.",
                    "retry_after": 60  # Retry after 60 seconds
                },
                headers={
                    "X-RateLimit-Limit-Minute": str(self.requests_per_minute),
                    "X-RateLimit-Remaining-Minute": str(minute_remaining),
                    "X-RateLimit-Limit-Hour": str(self.requests_per_hour),
                    "X-RateLimit-Remaining-Hour": str(hour_remaining),
                    "Retry-After": "60"
                }
            )
        
        # Add rate limit headers to response
        response = await call_next(request)
        response.headers["X-RateLimit-Limit-Minute"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining-Minute"] = str(minute_remaining)
        response.headers["X-RateLimit-Limit-Hour"] = str(self.requests_per_hour)
        response.headers["X-RateLimit-Remaining-Hour"] = str(hour_remaining)
        
        return response

"""
VOXMILL RATE LIMITER
====================
Redis-based token bucket rate limiting
Prevents spam, abuse, and runaway costs
"""

import os
import logging
from datetime import datetime, timezone
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

# ============================================================
# UPSTASH REDIS CONNECTION
# ============================================================

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
redis_client = None
redis_available = False

try:
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        import httpx
        
        class UpstashRedisClient:
            """REST API client for Upstash Redis"""
            
            def __init__(self, url: str, token: str):
                self.url = url.rstrip('/')
                self.token = token
                self.client = httpx.Client(timeout=5.0)
            
            def _execute(self, command: list):
                """Execute Redis command via REST API"""
                try:
                    response = self.client.post(
                        self.url,
                        headers={"Authorization": f"Bearer {self.token}"},
                        json=command
                    )
                    response.raise_for_status()
                    result = response.json()
                    return result.get("result")
                except Exception as e:
                    logger.debug(f"Upstash command failed: {e}")
                    return None
            
            def incr(self, key: str):
                """Increment counter"""
                return self._execute(["INCR", key])
            
            def expire(self, key: str, seconds: int):
                """Set expiry"""
                return self._execute(["EXPIRE", key, seconds])
            
            def ttl(self, key: str):
                """Get TTL"""
                return self._execute(["TTL", key])
            
            def get(self, key: str):
                """Get value"""
                return self._execute(["GET", key])
        
        redis_client = UpstashRedisClient(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN)
        redis_available = True
        logger.info("✅ Rate limiter: Redis connected")
    else:
        logger.warning("⚠️ Rate limiter: Redis not configured - rate limiting DISABLED")
except Exception as e:
    logger.error(f"❌ Rate limiter: Redis connection failed: {e}")


class RateLimiter:
    """Token bucket rate limiter"""
    
    DEFAULT_LIMIT = 100  # messages per hour
    WINDOW_SECONDS = 3600  # 1 hour
    
    @classmethod
    def check_rate_limit(cls, client_id: str, client_profile: dict = None) -> Tuple[bool, int, int]:
        """
        Check if client has exceeded rate limit
        
        Returns: (allowed, current_count, limit)
        """
        
        # If Redis not available, allow (fail open for now)
        if not redis_available or not redis_client:
            logger.warning("⚠️ Rate limiting unavailable (Redis down) - allowing request")
            return True, 0, cls.DEFAULT_LIMIT
        
        try:
            # Get client-specific limit from profile
            max_queries = cls.DEFAULT_LIMIT
            if client_profile:
                max_queries = client_profile.get('max_queries_per_hour', cls.DEFAULT_LIMIT)
            
            # Generate Redis key (hour-based bucket)
            current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
            rate_key = f"voxmill:rate:{client_id}:{current_hour}"
            
            # Increment counter
            count = redis_client.incr(rate_key)
            
            # Set expiry on first increment
            if count == 1:
                redis_client.expire(rate_key, cls.WINDOW_SECONDS)
            
            # Check if over limit
            if count > max_queries:
                logger.warning(f"🚫 RATE LIMIT EXCEEDED: {client_id} ({count}/{max_queries})")
                return False, count, max_queries
            
            logger.debug(f"✅ Rate limit OK: {client_id} ({count}/{max_queries})")
            return True, count, max_queries
            
        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            # Fail open (allow request if rate limiter breaks)
            return True, 0, cls.DEFAULT_LIMIT
    
@classmethod
    def get_reset_time(cls, client_id: str) -> Optional[int]:
        """
        Get seconds until rate limit resets
        
        Returns: seconds or None
        """
        
        if not redis_available or not redis_client:
            return None
        
        try:
            current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
            rate_key = f"voxmill:rate:{client_id}:{current_hour}"
            
            ttl = redis_client.ttl(rate_key)
            
            # ✅ FIX: Check for None before comparison
            if ttl is not None and ttl > 0:
                return ttl
            else:
                return 3600  # Default to 1 hour
                
        except Exception as e:
            logger.error(f"Get reset time error: {e}")
            return None
    
    @classmethod
    def get_current_usage(cls, client_id: str) -> int:
        """
        Get current usage count for this hour
        
        Returns: message count
        """
        
        if not redis_available or not redis_client:
            return 0
        
        try:
            current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
            rate_key = f"voxmill:rate:{client_id}:{current_hour}"
            
            count = redis_client.get(rate_key)
            return int(count) if count else 0
            
        except Exception as e:
            logger.error(f"Get current usage error: {e}")
            return 0

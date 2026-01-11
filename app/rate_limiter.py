"""
VOXMILL RATE LIMITER - WORLD CLASS
====================================
Multi-layer spam protection and rate limiting

LAYERS:
0. Idempotency (duplicate detection)
1. Hard protocol gates (FSM - handled in whatsapp.py)
2. Token bucket per identity (core limiter)
3. Burst protection (anti-flood)
4. Abuse scoring (anomaly detection)
5. Challenge gates (verification)
6. Global budget (infrastructure protection)
"""

import os
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict

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
            
            def incrby(self, key: str, amount: int):
                """Increment by amount"""
                return self._execute(["INCRBY", key, amount])
            
            def expire(self, key: str, seconds: int):
                """Set expiry"""
                return self._execute(["EXPIRE", key, seconds])
            
            def ttl(self, key: str):
                """Get TTL"""
                return self._execute(["TTL", key])
            
            def get(self, key: str):
                """Get value"""
                return self._execute(["GET", key])
            
            def set(self, key: str, value: str):
                """Set value"""
                return self._execute(["SET", key, value])
            
            def setex(self, key: str, seconds: int, value: str):
                """Set with expiry"""
                return self._execute(["SETEX", key, seconds, value])
            
            def setnx(self, key: str, value: str):
                """Set if not exists"""
                result = self._execute(["SETNX", key, value])
                return result == 1
        
        redis_client = UpstashRedisClient(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN)
        redis_available = True
        logger.info("✅ Rate limiter: Redis connected")
    else:
        logger.warning("⚠️ Rate limiter: Redis not configured - rate limiting DISABLED")
except Exception as e:
    logger.error(f"❌ Rate limiter: Redis connection failed: {e}")


class RateLimiter:
    """
    World-class multi-layer rate limiter
    
    CHATGPT SPEC: Token bucket + burst protection + abuse scoring
    """
    
    # ================================================================
    # TIER-BASED CONFIGURATION
    # ================================================================
    
    TOKEN_BUCKET_CONFIG = {
        'tier_1': {
            'capacity': 8,           # tokens
            'refill_rate': 0.133,    # tokens per second (≈8/min)
            'refill_interval': 7.5   # seconds between refills
        },
        'tier_2': {
            'capacity': 12,
            'refill_rate': 0.200,    # ≈12/min
            'refill_interval': 5.0
        },
        'tier_3': {
            'capacity': 24,
            'refill_rate': 0.400,    # ≈24/min
            'refill_interval': 2.5
        },
        'trial': {
            'capacity': 6,
            'refill_rate': 0.100,    # ≈6/min
            'refill_interval': 10.0
        }
    }
    
    # Operation costs (in tokens)
    OPERATION_COSTS = {
        'message': 1,
        'data_load': 2,
        'llm_call': 4,
        'pdf_gen': 8,
        'portfolio_mutation': 3,
        'comparison': 5
    }
    
    # Burst limits
    BURST_WINDOW = 3      # seconds
    BURST_LIMIT = 5       # messages
    
    # Abuse scoring
    ABUSE_SCORE_WINDOW = 600  # 10 minutes
    ABUSE_THRESHOLDS = {
        'soft': 10,   # Slow down
        'hard': 20,   # Block 1 hour
        'lock': 30    # Require re-verification
    }
    
    # Global limits (protect infrastructure)
    GLOBAL_LLM_LIMIT = 100       # calls per minute
    GLOBAL_DATASET_LIMIT = 50    # loads per minute
    
    # ================================================================
    # LAYER 0: IDEMPOTENCY (DUPLICATE DETECTION)
    # ================================================================
    
    @classmethod
    def check_duplicate(cls, sender: str, message_text: str) -> Tuple[bool, Optional[str]]:
        """
        Check if message is a duplicate (Twilio retry)
        
        Returns: (is_duplicate, cached_response)
        """
        
        if not redis_available or not redis_client:
            return False, None
        
        try:
            # Normalize message
            normalized = message_text.lower().strip()
            normalized = ''.join(c for c in normalized if c.isalnum() or c.isspace())
            normalized = ' '.join(normalized.split())
            
            # Create fingerprint (60-second bucket)
            timestamp_bucket = int(time.time()) // 60
            fingerprint_input = f"{sender}:{timestamp_bucket}:{normalized}"
            fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()[:16]
            
            dedup_key = f"voxmill:dedup:{fingerprint}"
            
            # Check if seen
            is_duplicate = not redis_client.setnx(dedup_key, "1")
            
            if is_duplicate:
                logger.warning(f"🔁 DUPLICATE MESSAGE DETECTED: {sender} (fingerprint: {fingerprint})")
                
                # Try to get cached response
                cache_key = f"voxmill:response_cache:{fingerprint}"
                cached_response = redis_client.get(cache_key)
                
                return True, cached_response
            else:
                # First time seeing this - set expiry
                redis_client.expire(dedup_key, 60)
                return False, None
        
        except Exception as e:
            logger.error(f"Duplicate check error: {e}")
            return False, None
    
    @classmethod
    def cache_response(cls, sender: str, message_text: str, response: str):
        """Cache response for duplicate detection"""
        
        if not redis_available or not redis_client:
            return
        
        try:
            normalized = message_text.lower().strip()
            normalized = ''.join(c for c in normalized if c.isalnum() or c.isspace())
            normalized = ' '.join(normalized.split())
            
            timestamp_bucket = int(time.time()) // 60
            fingerprint_input = f"{sender}:{timestamp_bucket}:{normalized}"
            fingerprint = hashlib.sha256(fingerprint_input.encode()).hexdigest()[:16]
            
            cache_key = f"voxmill:response_cache:{fingerprint}"
            redis_client.setex(cache_key, 60, response)
            
        except Exception as e:
            logger.error(f"Response cache error: {e}")
    
    # ================================================================
    # LAYER 2: TOKEN BUCKET (CORE LIMITER)
    # ================================================================
    
    @classmethod
    def check_token_bucket(cls, client_id: str, operation: str = 'message', 
                          client_tier: str = 'tier_1') -> Tuple[bool, int, int]:
        """
        Token bucket rate limiter with operation costs
        
        Returns: (allowed, current_tokens, capacity)
        """
        
        if not redis_available or not redis_client:
            return True, 0, 0
        
        try:
            # Get tier config
            config = cls.TOKEN_BUCKET_CONFIG.get(client_tier, cls.TOKEN_BUCKET_CONFIG['tier_1'])
            
            capacity = config['capacity']
            refill_rate = config['refill_rate']
            
            # Get operation cost
            cost = cls.OPERATION_COSTS.get(operation, 1)
            
            bucket_key = f"voxmill:bucket:{client_id}"
            last_refill_key = f"voxmill:bucket_time:{client_id}"
            
            # Get current tokens and last refill time
            current_tokens_str = redis_client.get(bucket_key)
            last_refill_str = redis_client.get(last_refill_key)
            
            now = time.time()
            
            if current_tokens_str is None:
                # First time - start with full capacity
                current_tokens = capacity
                last_refill = now
            else:
                current_tokens = int(current_tokens_str)
                last_refill = float(last_refill_str) if last_refill_str else now
            
            # Refill tokens based on time elapsed
            time_elapsed = now - last_refill
            tokens_to_add = int(time_elapsed * refill_rate)
            
            if tokens_to_add > 0:
                current_tokens = min(capacity, current_tokens + tokens_to_add)
                last_refill = now
                
                # Update Redis
                redis_client.setex(bucket_key, 3600, str(current_tokens))
                redis_client.setex(last_refill_key, 3600, str(last_refill))
            
            # Check if enough tokens
            if current_tokens >= cost:
                # Deduct tokens
                current_tokens -= cost
                redis_client.setex(bucket_key, 3600, str(current_tokens))
                
                logger.debug(f"✅ Token bucket OK: {client_id} ({current_tokens}/{capacity} after {operation})")
                return True, current_tokens, capacity
            else:
                logger.warning(f"🚫 TOKEN BUCKET DEPLETED: {client_id} ({current_tokens}/{capacity}, need {cost})")
                return False, current_tokens, capacity
        
        except Exception as e:
            logger.error(f"Token bucket error: {e}")
            return True, 0, 0
    
    # ================================================================
    # LAYER 3: BURST PROTECTION
    # ================================================================
    
    @classmethod
    def check_burst_limit(cls, client_id: str) -> Tuple[bool, int]:
        """
        Check if client is flooding (>5 messages in 3 seconds)
        
        Returns: (allowed, count_in_window)
        """
        
        if not redis_available or not redis_client:
            return True, 0
        
        try:
            burst_key = f"voxmill:burst:{client_id}"
            
            # Increment counter
            count = redis_client.incr(burst_key)
            
            if count is None:
                count = 0
            
            # Set expiry on first message
            if count == 1:
                redis_client.expire(burst_key, cls.BURST_WINDOW)
            
            # Check threshold
            if count > cls.BURST_LIMIT:
                logger.warning(f"🚫 BURST FLOOD: {client_id} ({count} in {cls.BURST_WINDOW}s)")
                return False, count
            
            return True, count
        
        except Exception as e:
            logger.error(f"Burst check error: {e}")
            return True, 0
    
    # ================================================================
    # LAYER 4: ABUSE SCORING (ANOMALY DETECTION)
    # ================================================================
    
    @classmethod
    def update_abuse_score(cls, client_id: str, event: str, delta: int):
        """
        Update rolling abuse score
        
        Events:
        - repeated_message: +3
        - short_spam: +2
        - command_spam: +4
        - invalid_confirm: +2
        - failed_parse: +1
        - successful_pin: -5
        - paid_account: -10
        """
        
        if not redis_available or not redis_client:
            return
        
        try:
            score_key = f"voxmill:abuse_score:{client_id}"
            
            # Get current score
            current_score_str = redis_client.get(score_key)
            current_score = int(current_score_str) if current_score_str else 0
            
            # Update score
            new_score = max(0, current_score + delta)
            
            # Store with 10-minute expiry (rolling window)
            redis_client.setex(score_key, cls.ABUSE_SCORE_WINDOW, str(new_score))
            
            logger.debug(f"📊 Abuse score: {client_id} = {new_score} (event: {event} {delta:+d})")
            
        except Exception as e:
            logger.error(f"Abuse score update error: {e}")
    
    @classmethod
    def get_abuse_score(cls, client_id: str) -> int:
        """Get current abuse score"""
        
        if not redis_available or not redis_client:
            return 0
        
        try:
            score_key = f"voxmill:abuse_score:{client_id}"
            score_str = redis_client.get(score_key)
            return int(score_str) if score_str else 0
        except Exception as e:
            logger.error(f"Abuse score get error: {e}")
            return 0
    
    @classmethod
    def check_abuse_threshold(cls, client_id: str) -> Tuple[bool, str, int]:
        """
        Check if abuse score exceeds threshold
        
        Returns: (blocked, action, score)
        - action: 'none', 'soft_throttle', 'hard_block', 'require_verification'
        """
        
        score = cls.get_abuse_score(client_id)
        
        if score >= cls.ABUSE_THRESHOLDS['lock']:
            return True, 'require_verification', score
        elif score >= cls.ABUSE_THRESHOLDS['hard']:
            return True, 'hard_block', score
        elif score >= cls.ABUSE_THRESHOLDS['soft']:
            return False, 'soft_throttle', score
        else:
            return False, 'none', score
    
    # ================================================================
    # LAYER 5: CHALLENGE GATES
    # ================================================================
    
    @classmethod
    def set_challenge_required(cls, client_id: str, challenge_type: str = 'verify'):
        """Mark client as requiring challenge"""
        
        if not redis_available or not redis_client:
            return
        
        try:
            challenge_key = f"voxmill:challenge:{client_id}"
            redis_client.setex(challenge_key, 3600, challenge_type)  # 1 hour
            logger.warning(f"🔐 CHALLENGE REQUIRED: {client_id} ({challenge_type})")
        except Exception as e:
            logger.error(f"Challenge set error: {e}")
    
    @classmethod
    def is_challenge_required(cls, client_id: str) -> Tuple[bool, Optional[str]]:
        """Check if challenge required"""
        
        if not redis_available or not redis_client:
            return False, None
        
        try:
            challenge_key = f"voxmill:challenge:{client_id}"
            challenge_type = redis_client.get(challenge_key)
            
            if challenge_type:
                return True, challenge_type
            return False, None
        except Exception as e:
            logger.error(f"Challenge check error: {e}")
            return False, None
    
    @classmethod
    def clear_challenge(cls, client_id: str):
        """Clear challenge requirement"""
        
        if not redis_available or not redis_client:
            return
        
        try:
            challenge_key = f"voxmill:challenge:{client_id}"
            redis_client.expire(challenge_key, 0)
            logger.info(f"✅ Challenge cleared: {client_id}")
        except Exception as e:
            logger.error(f"Challenge clear error: {e}")
    
    # ================================================================
    # LAYER 6: GLOBAL BUDGET (INFRASTRUCTURE PROTECTION)
    # ================================================================
    
    @classmethod
    def check_global_budget(cls, resource: str) -> bool:
        """
        Check global resource budget (LLM calls, dataset loads)
        
        Args:
            resource: 'llm' or 'dataset'
        
        Returns: allowed
        """
        
        if not redis_available or not redis_client:
            return True
        
        try:
            current_minute = int(time.time()) // 60
            budget_key = f"voxmill:global_{resource}:{current_minute}"
            
            # Get limit
            limit = cls.GLOBAL_LLM_LIMIT if resource == 'llm' else cls.GLOBAL_DATASET_LIMIT
            
            # Increment counter
            count = redis_client.incr(budget_key)
            
            if count is None:
                count = 0
            
            # Set expiry
            if count == 1:
                redis_client.expire(budget_key, 60)
            
            # Check limit
            if count > limit:
                logger.error(f"🚫 GLOBAL {resource.upper()} BUDGET EXCEEDED: {count}/{limit} per minute")
                return False
            
            return True
        
        except Exception as e:
            logger.error(f"Global budget check error: {e}")
            return True
    
    # ================================================================
    # LEGACY COMPATIBILITY (OLD METHODS)
    # ================================================================
    
    @classmethod
    def check_rate_limit(cls, client_id: str, client_profile: dict = None) -> Tuple[bool, int, int]:
        """
        Legacy hourly rate limit (for backward compatibility)
        
        Returns: (allowed, current_count, limit)
        """
        
        if not redis_available or not redis_client:
            return True, 0, 100
        
        try:
            # Get tier
            tier = client_profile.get('tier', 'tier_1') if client_profile else 'tier_1'
            
            # Map to hourly limit
            hourly_limits = {
                'tier_1': 50,
                'tier_2': 100,
                'tier_3': 200,
                'trial': 30
            }
            
            limit = hourly_limits.get(tier, 100)
            
            # Count messages this hour
            current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
            rate_key = f"voxmill:rate:{client_id}:{current_hour}"
            
            count = redis_client.incr(rate_key)
            
            if count is None:
                count = 0
            
            if count == 1:
                redis_client.expire(rate_key, 3600)
            
            if count > limit:
                return False, count, limit
            
            return True, count, limit
        
        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            return True, 0, 100
    
    @classmethod
    def get_reset_time(cls, client_id: str) -> Optional[int]:
        """Get seconds until rate limit resets"""
        
        if not redis_available or not redis_client:
            return None
        
        try:
            current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
            rate_key = f"voxmill:rate:{client_id}:{current_hour}"
            
            ttl = redis_client.ttl(rate_key)
            
            if ttl is not None and ttl > 0:
                return ttl
            else:
                return 3600
        
        except Exception as e:
            logger.error(f"Get reset time error: {e}")
            return None
    
    @classmethod
    def get_current_usage(cls, client_id: str) -> int:
        """Get current usage count for this hour"""
        
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

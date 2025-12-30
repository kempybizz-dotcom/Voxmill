"""
VOXMILL CACHE MANAGER V2
=========================
Redis-based caching with in-memory fallback for 99.9% uptime
Target: 70% cost reduction via intelligent caching

FEATURES:
- Dual-layer caching: Redis (primary) + in-memory (fallback)
- Graceful degradation if Redis unavailable
- Thread-safe in-memory cache with automatic expiry cleanup
- Zero downtime even if Redis crashes mid-operation
"""

import os
import json
import logging
import hashlib
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# ============================================================
# UPSTASH REDIS CONNECTION (REST API)
# ============================================================

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
redis_client = None
redis_available = False

try:
    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        # Upstash uses HTTP REST API, not standard Redis protocol
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
            
            def ping(self):
                """Test connection"""
                result = self._execute(["PING"])
                return result == "PONG"
            
            def get(self, key: str):
                """Get value"""
                return self._execute(["GET", key])
            
            def setex(self, key: str, seconds: int, value: str):
                """Set value with expiry"""
                result = self._execute(["SETEX", key, seconds, value])
                return result == "OK"
            
            def delete(self, *keys):
                """Delete keys"""
                if not keys:
                    return 0
                return self._execute(["DEL", *keys])
            
            def exists(self, key: str):
                """Check if key exists"""
                result = self._execute(["EXISTS", key])
                return result == 1
            
            def keys(self, pattern: str):
                """Get keys matching pattern"""
                result = self._execute(["KEYS", pattern])
                return result if result else []
            
            def dbsize(self):
                """Get database size"""
                result = self._execute(["DBSIZE"])
                return result if result else 0
            
            def info(self, section: str = 'stats'):
                """Get info (limited in Upstash)"""
                # Upstash doesn't support full INFO, return minimal stats
                return {
                    'keyspace_hits': 0,
                    'keyspace_misses': 0,
                    'used_memory_human': 'Unknown (Upstash)'
                }
        
        redis_client = UpstashRedisClient(UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN)
        
        # Test connection
        if redis_client.ping():
            redis_available = True
            logger.info("✅ Upstash Redis connected successfully (REST API)")
        else:
            redis_client = None
            logger.warning("⚠️ Upstash Redis ping failed")
    else:
        logger.warning("⚠️ UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN not configured - using in-memory cache only")
except ImportError:
    logger.error("❌ httpx library not installed (required for Upstash)")
    logger.warning("⚠️ Falling back to in-memory cache only")
except Exception as e:
    logger.error(f"❌ Upstash Redis connection failed: {e}")
    logger.warning("⚠️ Falling back to in-memory cache only")

# ============================================================
# IN-MEMORY CACHE (Fallback)
# ============================================================

_memory_cache = {}  # {cache_key: {'data': ..., 'expiry': timestamp}}
_memory_cache_lock = threading.Lock()
_last_cleanup = time.time()
CLEANUP_INTERVAL = 300  # Clean expired entries every 5 minutes

def _cleanup_expired_entries():
    """Remove expired entries from memory cache (housekeeping)"""
    global _last_cleanup
    
    now = time.time()
    
    # Only run cleanup every CLEANUP_INTERVAL seconds
    if now - _last_cleanup < CLEANUP_INTERVAL:
        return
    
    with _memory_cache_lock:
        expired_keys = [
            key for key, entry in _memory_cache.items() 
            if now > entry['expiry']
        ]
        
        for key in expired_keys:
            del _memory_cache[key]
        
        _last_cleanup = now
        
        if expired_keys:
            logger.debug(f"🗑️ Cleaned {len(expired_keys)} expired memory cache entries")


class CacheManager:
    """
    Intelligent caching with automatic failover
    
    Cache hierarchy:
    1. Redis (distributed, survives restarts)
    2. In-memory (process-local, fast fallback)
    3. Database/API (original source)
    """
    
    # Cache TTL settings (in seconds)
    RESPONSE_CACHE_TTL = 300   # 5 minutes for GPT-4 responses
    DATASET_CACHE_TTL = 1800   # 30 minutes for datasets (CRITICAL FOR PERFORMANCE)
    CLIENT_PROFILE_TTL = 600   # 10 minutes for client profiles
    DEDUPLICATION_TTL = 60     # 1 minute for webhook deduplication
    
    @classmethod
    def _generate_cache_key(cls, prefix: str, *args) -> str:
        """
        Generate consistent cache key from arguments
        
        Example: _generate_cache_key("dataset", "Mayfair", "real_estate")
        Returns: "voxmill:dataset:a3f2c9b1e8d4"
        """
        key_data = ":".join(str(arg) for arg in args)
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"voxmill:{prefix}:{key_hash}"
    
    # ============================================================
    # RESPONSE CACHE (GPT-4 responses)
    # ============================================================
    
    @classmethod
    def get_response_cache(cls, query: str, region: str, client_tier: str) -> Optional[str]:
        """
        Get cached GPT-4 response
        
        Args:
            query: User query (normalized)
            region: Market region
            client_tier: Client tier (affects response depth)
        
        Returns: Cached response text or None
        """
        cache_key = cls._generate_cache_key("response", query.lower().strip(), region, client_tier)
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                cached_data = redis_client.get(cache_key)
                
                if cached_data:
                    result = json.loads(cached_data)
                    cached_time = datetime.fromisoformat(result['cached_at'])
                    age_seconds = (datetime.now(timezone.utc) - cached_time).total_seconds()
                    
                    logger.info(f"✅ REDIS CACHE HIT: Response ({int(age_seconds)}s old, saved GPT-4 call)")
                    return result['response']
                
            except Exception as e:
                logger.warning(f"Redis read failed: {e}, trying memory cache")
        
        # FALLBACK TO IN-MEMORY CACHE
        _cleanup_expired_entries()
        
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                entry = _memory_cache[cache_key]
                
                if time.time() < entry['expiry']:
                    age_seconds = int(time.time() - (entry['expiry'] - cls.RESPONSE_CACHE_TTL))
                    logger.info(f"✅ MEMORY CACHE HIT: Response ({age_seconds}s old)")
                    return entry['data']['response']
                else:
                    # Expired
                    del _memory_cache[cache_key]
        
        return None
    
    @classmethod
    def set_response_cache(cls, query: str, region: str, client_tier: str, 
                          category: str, response_text: str, metadata: Dict) -> bool:
        """Cache GPT-4 response in Redis + memory"""
        cache_key = cls._generate_cache_key("response", query.lower().strip(), region, client_tier)
        
        cache_data = {
            "query": query,
            "region": region,
            "client_tier": client_tier,
            "category": category,
            "response": response_text,
            "metadata": metadata,
            "cached_at": datetime.now(timezone.utc).isoformat()
        }
        
        success = False
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                redis_client.setex(
                    cache_key,
                    cls.RESPONSE_CACHE_TTL,
                    json.dumps(cache_data, default=str)
                )
                logger.info(f"💾 Response cached in REDIS ({cls.RESPONSE_CACHE_TTL}s TTL)")
                success = True
            except Exception as e:
                logger.warning(f"Redis write failed: {e}, using memory cache only")
        
        # ALWAYS CACHE IN MEMORY AS BACKUP
        with _memory_cache_lock:
            _memory_cache[cache_key] = {
                'data': cache_data,
                'expiry': time.time() + cls.RESPONSE_CACHE_TTL
            }
            logger.info(f"💾 Response cached in MEMORY ({cls.RESPONSE_CACHE_TTL}s TTL)")
            success = True
        
        return success
    
    # ============================================================
    # DATASET CACHE (Critical for performance)
    # ============================================================
    
    @classmethod
    def get_dataset_cache(cls, area: str, vertical: str = "real_estate") -> Optional[Dict]:
        """
        Get cached dataset - CRITICAL FOR PERFORMANCE
        
        Without this working, every query loads fresh data (10-15s latency)
        With this working, repeated queries take <1s
        
        Args:
            area: Region name (e.g., "Mayfair")
            vertical: Market vertical (default: "real_estate")
        
        Returns: Cached dataset dict or None
        """
        cache_key = cls._generate_cache_key("dataset", area, vertical)
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                cached_data = redis_client.get(cache_key)
                
                if cached_data:
                    result = json.loads(cached_data)
                    cached_time = datetime.fromisoformat(result['cached_at'])
                    age_seconds = (datetime.now(timezone.utc) - cached_time).total_seconds()
                    age_minutes = int(age_seconds / 60)
                    
                    logger.info(f"✅ REDIS CACHE HIT: Dataset for {area} ({age_minutes}m old, saved 10-15s load)")
                    return result['dataset']
                
            except Exception as e:
                logger.warning(f"Redis read failed: {e}, trying memory cache")
        
        # FALLBACK TO IN-MEMORY CACHE
        _cleanup_expired_entries()
        
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                entry = _memory_cache[cache_key]
                
                if time.time() < entry['expiry']:
                    age_seconds = time.time() - (entry['expiry'] - cls.DATASET_CACHE_TTL)
                    age_minutes = int(age_seconds / 60)
                    logger.info(f"✅ MEMORY CACHE HIT: Dataset for {area} ({age_minutes}m old)")
                    return entry['data']
                else:
                    # Expired
                    del _memory_cache[cache_key]
        
        # MISS ON BOTH
        logger.info(f"❌ CACHE MISS: Dataset for {area} (will load fresh, ~15s)")
        return None
    
    @classmethod
    def set_dataset_cache(cls, area: str, dataset: Dict, vertical: str = "real_estate") -> bool:
        """
        Cache dataset in Redis + memory
        
        CRITICAL: This must succeed for performance
        If this fails, every query triggers 15s load
        """
        cache_key = cls._generate_cache_key("dataset", area, vertical)
        
        success = False
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                cache_data = {
                    "area": area,
                    "vertical": vertical,
                    "dataset": dataset,
                    "cached_at": datetime.now(timezone.utc).isoformat()
                }
                
                redis_client.setex(
                    cache_key,
                    cls.DATASET_CACHE_TTL,
                    json.dumps(cache_data, default=str)  # default=str handles datetime/ObjectId
                )
                
                ttl_minutes = int(cls.DATASET_CACHE_TTL / 60)
                logger.info(f"💾 Dataset cached in REDIS for {ttl_minutes}m (distributed cache)")
                success = True
                
            except Exception as e:
                logger.warning(f"Redis write failed: {e}, using memory cache only")
        
        # ALWAYS CACHE IN MEMORY AS BACKUP
        with _memory_cache_lock:
            _memory_cache[cache_key] = {
                'data': dataset,
                'expiry': time.time() + cls.DATASET_CACHE_TTL
            }
            ttl_minutes = int(cls.DATASET_CACHE_TTL / 60)
            logger.info(f"💾 Dataset cached in MEMORY for {ttl_minutes}m (process-local)")
            success = True
        
        return success
    
    # ============================================================
    # CLIENT PROFILE CACHE
    # ============================================================
    
    @classmethod
    def get_client_profile_cache(cls, whatsapp_number: str) -> Optional[Dict]:
        """Get cached client profile"""
        cache_key = cls._generate_cache_key("profile", whatsapp_number)
        
        # TRY REDIS FIRST
        if redis_available and redis_client:
            try:
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    logger.info(f"✅ REDIS CACHE HIT: Client profile")
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Redis read failed: {e}")
        
        # FALLBACK TO MEMORY
        _cleanup_expired_entries()
        
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                entry = _memory_cache[cache_key]
                if time.time() < entry['expiry']:
                    logger.info(f"✅ MEMORY CACHE HIT: Client profile")
                    return entry['data']
                else:
                    del _memory_cache[cache_key]
        
        return None
    
    @classmethod
    def set_client_profile_cache(cls, whatsapp_number: str, profile: Dict) -> bool:
        """Cache client profile"""
        cache_key = cls._generate_cache_key("profile", whatsapp_number)
        
        # Remove MongoDB _id before caching
        if '_id' in profile:
            profile = profile.copy()
            del profile['_id']
        
        success = False
        
        # TRY REDIS
        if redis_available and redis_client:
            try:
                redis_client.setex(
                    cache_key,
                    cls.CLIENT_PROFILE_TTL,
                    json.dumps(profile, default=str)
                )
                success = True
            except Exception as e:
                logger.warning(f"Redis write failed: {e}")
        
        # MEMORY BACKUP
        with _memory_cache_lock:
            _memory_cache[cache_key] = {
                'data': profile,
                'expiry': time.time() + cls.CLIENT_PROFILE_TTL
            }
            success = True
        
        return success
    
    @classmethod
    def invalidate_client_cache(cls, whatsapp_number: str):
        """Invalidate client profile cache (e.g., after preference update)"""
        cache_key = cls._generate_cache_key("profile", whatsapp_number)
        
        # Clear from Redis
        if redis_available and redis_client:
            try:
                redis_client.delete(cache_key)
                logger.info(f"🗑️ Redis cache invalidated for client")
            except Exception as e:
                logger.warning(f"Redis delete failed: {e}")
        
        # Clear from memory
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                del _memory_cache[cache_key]
                logger.info(f"🗑️ Memory cache invalidated for client")

    @classmethod
    def clear_dataset_cache(cls, area: str, vertical: str = "real_estate"):
        """
        Clear dataset cache for a specific region
        
        Use this after preference changes to force fresh data load
        
        Args:
            area: Region name (e.g., "Mayfair")
            vertical: Market vertical (default: "real_estate")
        """
        cache_key = cls._generate_cache_key("dataset", area, vertical)
        
        # Clear from Redis
        if redis_available and redis_client:
            try:
                redis_client.delete(cache_key)
                logger.info(f"🗑️ Redis dataset cache cleared for {area}")
            except Exception as e:
                logger.warning(f"Redis delete failed: {e}")
        
        # Clear from memory
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                del _memory_cache[cache_key]
                logger.info(f"🗑️ Memory dataset cache cleared for {area}")
    
    # ============================================================
    # WEBHOOK DEDUPLICATION
    # ============================================================
    
    @classmethod
    def check_webhook_duplicate(cls, message_sid: str) -> bool:
        """
        Check if webhook has already been processed
        
        Returns: True if duplicate (already processed)
        """
        cache_key = f"voxmill:webhook:{message_sid}"
        
        # CHECK REDIS FIRST
        if redis_available and redis_client:
            try:
                if redis_client.exists(cache_key):
                    logger.warning(f"⚠️ DUPLICATE WEBHOOK (Redis): {message_sid}")
                    return True
                
                # Mark as processed
                redis_client.setex(cache_key, cls.DEDUPLICATION_TTL, "1")
                return False
            except Exception as e:
                logger.warning(f"Redis duplicate check failed: {e}, using memory")
        
        # FALLBACK TO MEMORY
        with _memory_cache_lock:
            if cache_key in _memory_cache:
                entry = _memory_cache[cache_key]
                if time.time() < entry['expiry']:
                    logger.warning(f"⚠️ DUPLICATE WEBHOOK (Memory): {message_sid}")
                    return True
            
            # Mark as processed
            _memory_cache[cache_key] = {
                'data': True,
                'expiry': time.time() + cls.DEDUPLICATION_TTL
            }
            return False
    
    # ============================================================
    # CACHE MANAGEMENT & DIAGNOSTICS
    # ============================================================
    
    @classmethod
    def get_cache_stats(cls) -> Dict:
        """Get cache performance statistics"""
        stats = {
            "redis_available": redis_available,
            "memory_cache_entries": len(_memory_cache),
        }
        
        if redis_available and redis_client:
            try:
                info = redis_client.info('stats')
                
                hits = info.get('keyspace_hits', 0)
                misses = info.get('keyspace_misses', 0)
                total = hits + misses
                
                stats.update({
                    "redis_connected": True,
                    "redis_keyspace_hits": hits,
                    "redis_keyspace_misses": misses,
                    "redis_hit_rate_pct": round((hits / total * 100) if total > 0 else 0, 2),
                    "redis_total_keys": redis_client.dbsize(),
                    "redis_memory_used": info.get('used_memory_human', 'Unknown')
                })
            except Exception as e:
                stats["redis_error"] = str(e)
        else:
            stats["redis_connected"] = False
            stats["redis_reason"] = "REDIS_URL not configured or connection failed"
        
        return stats
    
    @classmethod
    def warm_cache_for_region(cls, area: str):
        """
        Pre-warm cache for a region
        Useful before high-traffic periods or after deployment
        """
        try:
            from app.dataset_loader import load_dataset
            
            logger.info(f"🔥 Cache warming started for {area}...")
            dataset = load_dataset(area=area)
            
            if dataset and not dataset.get('metadata', {}).get('is_fallback'):
                cls.set_dataset_cache(area, dataset)
                logger.info(f"✅ Cache warmed for {area}")
                return True
            else:
                logger.warning(f"⚠️ Cache warming failed for {area} (no valid dataset)")
                return False
                
        except Exception as e:
            logger.error(f"❌ Cache warming error for {area}: {e}")
            return False
    
    @classmethod
    def clear_all_caches(cls):
        """
        Clear all Voxmill caches (emergency use only)
        
        WARNING: This will cause latency spike as all queries reload fresh data
        """
        # Clear Redis
        if redis_available and redis_client:
            try:
                keys = redis_client.keys("voxmill:*")
                
                if keys:
                    redis_client.delete(*keys)
                    logger.warning(f"🗑️ Cleared {len(keys)} Redis cache keys")
                else:
                    logger.info("No Redis cache keys to clear")
            except Exception as e:
                logger.error(f"Redis clear failed: {e}")
        
        # Clear memory
        with _memory_cache_lock:
            count = len(_memory_cache)
            _memory_cache.clear()
            logger.warning(f"🗑️ Cleared {count} memory cache entries")
    
    @classmethod
    def get_memory_cache_size(cls) -> Dict:
        """Get memory cache statistics"""
        import sys
        
        with _memory_cache_lock:
            total_entries = len(_memory_cache)
            
            # Estimate memory usage (rough)
            try:
                memory_bytes = sys.getsizeof(_memory_cache)
                for key, value in _memory_cache.items():
                    memory_bytes += sys.getsizeof(key)
                    memory_bytes += sys.getsizeof(value)
                
                memory_mb = memory_bytes / (1024 * 1024)
            except:
                memory_mb = 0
            
            return {
                "total_entries": total_entries,
                "estimated_memory_mb": round(memory_mb, 2)
            }


# ============================================================
# CACHE METRICS (Optional - for cost tracking)
# ============================================================

class CacheMetrics:
    """Track cache performance for cost analysis"""
    
    @classmethod
    def log_cache_hit(cls, cache_type: str, details: Dict = None):
        """Log cache hit for analytics (saves API costs)"""
        try:
            from pymongo import MongoClient
            
            MONGODB_URI = os.getenv("MONGODB_URI")
            if MONGODB_URI:
                mongo_client = MongoClient(MONGODB_URI)
                db = mongo_client['Voxmill']
                
                db['cache_metrics'].insert_one({
                    "event_type": "cache_hit",
                    "cache_type": cache_type,
                    "timestamp": datetime.now(timezone.utc),
                    "details": details or {}
                })
        except Exception as e:
            logger.debug(f"Cache metrics logging skipped: {e}")
    
    @classmethod
    def get_cost_savings(cls, days: int = 7) -> Dict:
        """
        Calculate approximate cost savings from caching
        
        Assumptions:
        - GPT-4 Turbo: ~$0.01 per 1K input tokens, ~$0.03 per 1K output tokens
        - Average query: ~8K input tokens, ~500 output tokens
        - Average cost per query: ~$0.095
        """
        try:
            from pymongo import MongoClient
            
            MONGODB_URI = os.getenv("MONGODB_URI")
            if not MONGODB_URI:
                return {"error": "MongoDB not connected"}
            
            mongo_client = MongoClient(MONGODB_URI)
            db = mongo_client['Voxmill']
            
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            
            cache_hits = db['cache_metrics'].count_documents({
                "event_type": "cache_hit",
                "timestamp": {"$gte": cutoff_date}
            })
            
            COST_PER_GPT4_QUERY = 0.095
            estimated_savings = cache_hits * COST_PER_GPT4_QUERY
            
            return {
                "period_days": days,
                "cache_hits": cache_hits,
                "estimated_savings_usd": round(estimated_savings, 2),
                "avg_savings_per_day": round(estimated_savings / days, 2)
            }
            
        except Exception as e:
            logger.error(f"Cost savings calculation error: {e}")
            return {"error": str(e)}

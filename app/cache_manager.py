"""
VOXMILL CACHE MANAGER
======================
Redis-based caching for GPT-4 responses and dataset queries
Target: 70% cost reduction via intelligent caching
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
import redis

logger = logging.getLogger(__name__)

# Redis connection
REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None


class CacheManager:
    """Intelligent caching for Voxmill intelligence queries"""
    
    # Cache TTL settings (in seconds)
    RESPONSE_CACHE_TTL = 300  # 5 minutes for GPT-4 responses
    DATASET_CACHE_TTL = 1800  # 30 minutes for MongoDB datasets
    CLIENT_PROFILE_TTL = 600  # 10 minutes for client profiles
    DEDUPLICATION_TTL = 60    # 1 minute for webhook deduplication
    
    @classmethod
    def _generate_cache_key(cls, prefix: str, *args) -> str:
        """Generate consistent cache key from arguments"""
        # Create deterministic hash from arguments
        key_data = ":".join(str(arg) for arg in args)
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"voxmill:{prefix}:{key_hash}"
    
    @classmethod
    def get_response_cache(cls, query: str, region: str, client_tier: str) -> Optional[Dict]:
        """
        Get cached GPT-4 response
        
        Args:
            query: User query (normalized)
            region: Market region
            client_tier: Client tier (affects response depth)
        
        Returns: Cached response dict or None
        """
        if not redis_client:
            return None
        
        try:
            cache_key = cls._generate_cache_key("response", query.lower().strip(), region, client_tier)
            cached_data = redis_client.get(cache_key)
            
            if cached_data:
                result = json.loads(cached_data)
                
                # Check freshness
                cached_time = datetime.fromisoformat(result['cached_at'])
                age_seconds = (datetime.now(timezone.utc) - cached_time).total_seconds()
                
                logger.info(f"✅ Cache HIT: Response cached {int(age_seconds)}s ago (saved GPT-4 call)")
                
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"Cache retrieval error: {e}")
            return None
    
    @classmethod
    def set_response_cache(cls, query: str, region: str, client_tier: str, 
                          category: str, response_text: str, metadata: Dict) -> bool:
        """
        Cache GPT-4 response
        
        Returns: True if cached successfully
        """
        if not redis_client:
            return False
        
        try:
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
            
            redis_client.setex(
                cache_key,
                cls.RESPONSE_CACHE_TTL,
                json.dumps(cache_data)
            )
            
            logger.info(f"💾 Response cached for {cls.RESPONSE_CACHE_TTL}s")
            return True
            
        except Exception as e:
            logger.error(f"Cache storage error: {e}")
            return False
    
    @classmethod
    def get_dataset_cache(cls, area: str, vertical: str = "real_estate") -> Optional[Dict]:
        """
        Get cached dataset from MongoDB query
        Reduces MongoDB load
        """
        if not redis_client:
            return None
        
        try:
            cache_key = cls._generate_cache_key("dataset", area, vertical)
            cached_data = redis_client.get(cache_key)
            
            if cached_data:
                result = json.loads(cached_data)
                
                cached_time = datetime.fromisoformat(result['cached_at'])
                age_seconds = (datetime.now(timezone.utc) - cached_time).total_seconds()
                
                logger.info(f"✅ Cache HIT: Dataset cached {int(age_seconds)}s ago (saved MongoDB query)")
                
                return result['dataset']
            
            return None
            
        except Exception as e:
            logger.error(f"Dataset cache retrieval error: {e}")
            return None
    
    @classmethod
    def set_dataset_cache(cls, area: str, dataset: Dict, vertical: str = "real_estate") -> bool:
        """Cache dataset from MongoDB"""
        if not redis_client:
            return False
        
        try:
            cache_key = cls._generate_cache_key("dataset", area, vertical)
            
            cache_data = {
                "area": area,
                "vertical": vertical,
                "dataset": dataset,
                "cached_at": datetime.now(timezone.utc).isoformat()
            }
            
            redis_client.setex(
                cache_key,
                cls.DATASET_CACHE_TTL,
                json.dumps(cache_data)
            )
            
            logger.info(f"💾 Dataset cached for {cls.DATASET_CACHE_TTL}s")
            return True
            
        except Exception as e:
            logger.error(f"Dataset cache storage error: {e}")
            return False
    
    @classmethod
    def get_client_profile_cache(cls, whatsapp_number: str) -> Optional[Dict]:
        """Get cached client profile"""
        if not redis_client:
            return None
        
        try:
            cache_key = cls._generate_cache_key("profile", whatsapp_number)
            cached_data = redis_client.get(cache_key)
            
            if cached_data:
                logger.info(f"✅ Cache HIT: Client profile")
                return json.loads(cached_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Profile cache retrieval error: {e}")
            return None
    
    @classmethod
    def set_client_profile_cache(cls, whatsapp_number: str, profile: Dict) -> bool:
        """Cache client profile"""
        if not redis_client:
            return False
        
        try:
            cache_key = cls._generate_cache_key("profile", whatsapp_number)
            
            # Remove MongoDB _id before caching
            if '_id' in profile:
                profile = profile.copy()
                del profile['_id']
            
            redis_client.setex(
                cache_key,
                cls.CLIENT_PROFILE_TTL,
                json.dumps(profile, default=str)  # default=str handles datetime
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Profile cache storage error: {e}")
            return False
    
    @classmethod
    def invalidate_client_cache(cls, whatsapp_number: str):
        """Invalidate client profile cache (e.g., after preference update)"""
        if not redis_client:
            return
        
        try:
            cache_key = cls._generate_cache_key("profile", whatsapp_number)
            redis_client.delete(cache_key)
            logger.info(f"🗑️  Client profile cache invalidated")
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
    
    @classmethod
    def check_webhook_duplicate(cls, message_sid: str) -> bool:
        """
        Check if webhook has already been processed
        
        Returns: True if duplicate (already processed)
        """
        if not redis_client:
            return False
        
        try:
            cache_key = f"voxmill:webhook:{message_sid}"
            
            # Check if exists
            if redis_client.exists(cache_key):
                logger.warning(f"⚠️  Duplicate webhook: {message_sid}")
                return True
            
            # Mark as processed
            redis_client.setex(cache_key, cls.DEDUPLICATION_TTL, "1")
            return False
            
        except Exception as e:
            logger.error(f"Webhook deduplication error: {e}")
            return False
    
    @classmethod
    def get_cache_stats(cls) -> Dict:
        """Get cache performance statistics"""
        if not redis_client:
            return {"error": "Redis not connected"}
        
        try:
            info = redis_client.info('stats')
            
            return {
                "connected": True,
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "hit_rate": round(
                    info.get('keyspace_hits', 0) / 
                    max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1) * 100, 
                    2
                ),
                "total_keys": redis_client.dbsize(),
                "memory_used": info.get('used_memory_human', 'Unknown')
            }
            
        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return {"error": str(e)}
    
    @classmethod
    def warm_cache_for_region(cls, area: str):
        """
        Pre-warm cache for a region
        Useful before high-traffic periods
        """
        if not redis_client:
            return
        
        try:
            from app.dataset_loader import load_dataset
            
            # Load and cache dataset
            dataset = load_dataset(area=area)
            if dataset and not dataset.get('error'):
                cls.set_dataset_cache(area, dataset)
                logger.info(f"🔥 Cache warmed for {area}")
        except Exception as e:
            logger.error(f"Cache warming error: {e}")
    
    @classmethod
    def clear_all_caches(cls):
        """Clear all Voxmill caches (emergency use only)"""
        if not redis_client:
            return
        
        try:
            # Get all Voxmill keys
            keys = redis_client.keys("voxmill:*")
            
            if keys:
                redis_client.delete(*keys)
                logger.warning(f"🗑️  Cleared {len(keys)} cache keys")
            else:
                logger.info("No cache keys to clear")
                
        except Exception as e:
            logger.error(f"Cache clearing error: {e}")


class CacheMetrics:
    """Track cache performance metrics"""
    
    @classmethod
    def log_cache_event(cls, event_type: str, details: Dict = None):
        """Log cache events for analytics"""
        try:
            from pymongo import MongoClient
            
            MONGODB_URI = os.getenv("MONGODB_URI")
            if MONGODB_URI:
                mongo_client = MongoClient(MONGODB_URI)
                db = mongo_client['Voxmill']
                
                cache_log = {
                    "event_type": event_type,
                    "timestamp": datetime.now(timezone.utc),
                    "details": details or {}
                }
                
                db['cache_metrics'].insert_one(cache_log)
        except Exception as e:
            logger.debug(f"Cache metrics logging error: {e}")
    
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
            
            # Cost per GPT-4 query
            COST_PER_QUERY = 0.095
            
            estimated_savings = cache_hits * COST_PER_QUERY
            
            return {
                "period_days": days,
                "cache_hits": cache_hits,
                "estimated_savings_usd": round(estimated_savings, 2),
                "avg_savings_per_day": round(estimated_savings / days, 2)
            }
            
        except Exception as e:
            logger.error(f"Cost savings calculation error: {e}")
            return {"error": str(e)}

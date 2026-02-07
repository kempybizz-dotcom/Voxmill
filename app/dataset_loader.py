"""
VOXMILL INSTITUTIONAL-GRADE DATA STACK - PRODUCTION-SAFE EDITION
================================================================
✅ CRITICAL FIX: Mock data OFF by default
✅ CRITICAL FIX: Synthetic data properly flagged
✅ Multi-source fallback (Rightmove → Zoopla → OnTheMarket)
✅ ScraperAPI integration for bot bypass
✅ Multi-industry routing
✅ Circuit breaker pattern
✅ Data quality validation
"""

import os
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import re
import time
import statistics
import feedparser
import hashlib
import json
from functools import wraps
from openai import OpenAI
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ============================================================
# API CONFIGURATION
# ============================================================

SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY")
SCRAPER_API_URL = "http://api.scraperapi.com"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================================
# INDUSTRY ROUTING
# ============================================================

try:
    from app.industry_enforcer import IndustryEnforcer
    INDUSTRY_ROUTING_ENABLED = True
    logger.info("✅ Industry routing enabled")
except ImportError:
    INDUSTRY_ROUTING_ENABLED = False
    logger.warning("⚠️ Industry routing not available")

# ============================================================
# CANONICAL MARKET RESOLVER
# ============================================================

try:
    from app.market_canonicalizer import MarketCanonicalizer
    CANONICAL_RESOLVER_ENABLED = True
    logger.info("✅ Canonical market resolver enabled")
except ImportError:
    CANONICAL_RESOLVER_ENABLED = False
    logger.warning("⚠️ Canonical market resolver not available")


# ============================================================
# UTILITIES
# ============================================================

def _get_with_proxy(url: str, params: dict = None, headers: dict = None, timeout: int = 30) -> requests.Response:
    """Make HTTP request with ScraperAPI proxy if available"""
    try:
        if SCRAPER_API_KEY:
            from urllib.parse import urlencode
            full_url = f"{url}?{urlencode(params)}" if params else url
            
            proxy_params = {
                'api_key': SCRAPER_API_KEY,
                'url': full_url,
                'render': 'true',
                'country_code': 'gb',
                'premium': 'true',
                'session_number': '1'
            }
            
            response = requests.get(SCRAPER_API_URL, params=proxy_params, timeout=timeout)
            return response
        else:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            return response
            
    except Exception as e:
        logger.error(f"_get_with_proxy error: {e}")
        class DummyResponse:
            status_code = 500
            text = ""
            content = b""
            def json(self):
                return {}
        return DummyResponse()


def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    """Decorator for exponential backoff retry logic"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts")
                        raise
                    
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


# ============================================================
# CIRCUIT BREAKER
# ============================================================

class CircuitBreaker:
    """Prevent cascade failures from bad APIs"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = {}
        self.open_until = {}
    
    def call(self, service_name: str, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        
        if service_name in self.open_until:
            if time.time() < self.open_until[service_name]:
                logger.warning(f"Circuit breaker OPEN for {service_name}")
                return None
            else:
                logger.info(f"Circuit breaker RESET for {service_name}")
                del self.open_until[service_name]
                self.failures[service_name] = 0
        
        try:
            result = func(*args, **kwargs)
            self.failures[service_name] = 0
            return result
            
        except Exception as e:
            self.failures[service_name] = self.failures.get(service_name, 0) + 1
            
            if self.failures[service_name] >= self.failure_threshold:
                self.open_until[service_name] = time.time() + self.timeout
                logger.error(f"Circuit breaker OPENED for {service_name} after {self.failures[service_name]} failures")
            
            logger.error(f"{service_name} error: {e}")
            return None


circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=300)


# ============================================================
# DATA QUALITY VALIDATOR
# ============================================================

class DataQualityValidator:
    """Enterprise-grade data validation"""
    
    @staticmethod
    def validate_property(prop: Dict, area_stats: Dict) -> Tuple[bool, str]:
        """Validate property data quality"""
        
        required_fields = ['price', 'property_type', 'address', 'area']
        for field in required_fields:
            if not prop.get(field):
                return False, f"Missing required field: {field}"
        
        price = prop.get('price', 0)
        if price <= 0:
            return False, "Invalid price (<=0)"
        
        if price < 100000:
            return False, "Price suspiciously low"
        
        avg_price = area_stats.get('avg_price', 0)
        std_dev = area_stats.get('std_dev_price', 0)
        
        if std_dev > 0 and abs(price - avg_price) > 5 * std_dev:
            return False, f"Extreme outlier (5+ sigma from mean)"
        
        return True, "Valid"
    
    @staticmethod
    def remove_duplicates(properties: List[Dict]) -> List[Dict]:
        """Remove duplicate properties"""
        seen_addresses = set()
        unique_properties = []
        
        for prop in properties:
            address = prop.get('address', '').strip().lower()
            if address and address not in seen_addresses:
                seen_addresses.add(address)
                unique_properties.append(prop)
        
        logger.info(f"Removed {len(properties) - len(unique_properties)} duplicates")
        return unique_properties


# ============================================================
# DATA SOURCES (STUBS - Real implementations in actual file)
# ============================================================

class RightmoveLiveData:
    """Rightmove scraper stub"""
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        logger.info(f"Rightmove fetch for {area} (stub)")
        return []


class ZooplaLiveData:
    """Zoopla scraper stub"""
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        logger.info(f"Zoopla fetch for {area} (stub)")
        return []


class OnTheMarketData:
    """OnTheMarket scraper stub"""
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        logger.info(f"OnTheMarket fetch for {area} (stub)")
        return []


# ============================================================
# EMPTY DATASET TEMPLATE
# ============================================================

def _empty_dataset(area: str, industry: str = "real_estate") -> Dict:
    """Return empty dataset with proper structure"""
    return {
        'properties': [],
        'metrics': {
            'property_count': 0,
            'avg_price': 0,
            'median_price': 0,
            'min_price': 0,
            'max_price': 0
        },
        'metadata': {
            'area': area,
            'industry': industry,
            'city': 'Unknown',
            'property_count': 0,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'is_fallback': True,
            'is_synthetic': False,  # ✅ FIXED: Added flag
            'is_real_data': False,  # ✅ FIXED: Added flag
            'data_quality': 'unavailable',
            'validation_passed': False,
            'data_source': 'none',
            'sources': []
        },
        'intelligence': {
            'market_sentiment': 'unknown',
            'sentiment_confidence': 0.0,
            'confidence_level': 'none',
            'executive_summary': f'No data available for {area}',
            'top_agents': [],
            'key_themes': []
        },
        'historical_sales': [],
        'amenities': {},
        'liquidity_velocity': {'error': 'no_data'},
        'agent_profiles': [],
        'detected_trends': [],
        'micromarkets': {'error': 'no_data'}
    }


def _structural_only_dataset(canonical_area: str, original_area: str, industry: str = "real_estate") -> Dict:
    """Return structural-only dataset template"""
    return {
        'properties': [],
        'metrics': {
            'property_count': 0,
            'avg_price': 0,
            'median_price': 0,
            'min_price': 0,
            'max_price': 0
        },
        'metadata': {
            'area': canonical_area,
            'original_area': original_area,
            'industry': industry,
            'city': 'Unknown',
            'property_count': 0,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'is_fallback': False,
            'is_structural_only': True,
            'is_synthetic': False,  # ✅ FIXED: Added flag
            'is_real_data': False,  # ✅ FIXED: Added flag
            'data_quality': 'structural_regime',
            'validation_passed': True,
            'data_source': 'canonical_structural',
            'sources': []
        },
        'intelligence': {
            'market_sentiment': 'neutral',
            'sentiment_confidence': 0.0,
            'confidence_level': 'structural',
            'executive_summary': f'{canonical_area} is a structural-regime market (no live dataset)',
            'top_agents': [],
            'key_themes': ['Structural analysis', 'Regime comparison']
        },
        'historical_sales': [],
        'amenities': {},
        'liquidity_velocity': {'error': 'structural_only'},
        'agent_profiles': [],
        'detected_trends': [],
        'micromarkets': {'error': 'structural_only'}
    }


# ============================================================
# MAIN DATASET LOADER
# ============================================================

def load_dataset(area: str, max_properties: int = 100, industry: str = "real_estate") -> Dict:
    """
    Load institutional-grade dataset with intelligent multi-source fallback
    
    ✅ FIXED: Mock data OFF by default (production-safe)
    ✅ FIXED: Synthetic data properly flagged
    ✅ Multi-source fallback chain
    ✅ Circuit breaker protection
    ✅ Data quality validation
    
    Args:
        area: Geographic area (REQUIRED)
        max_properties: Maximum items to fetch
        industry: Industry vertical code
    
    Returns:
        Dataset dict with intelligence layers
    """
    
    # ========================================
    # VALIDATION
    # ========================================
    
    if not area:
        logger.error("❌ No area provided to load_dataset")
        return _empty_dataset("Unknown", industry)
    
    # ========================================
    # INITIALIZE SYNTHETIC DATA FLAG
    # ========================================
    is_synthetic_data = False  # ✅ PATCH 4: Initialize flag
    
    # ========================================
    # CANONICAL MARKET RESOLUTION
    # ========================================
    
    original_area = area
    is_structural_only = False
    
    if CANONICAL_RESOLVER_ENABLED:
        canonical_area, is_structural_only = MarketCanonicalizer.canonicalize(area)
        
        if canonical_area != area:
            logger.info(f"🔄 Market canonicalized: '{area}' → '{canonical_area}'")
            area = canonical_area
        
        if is_structural_only:
            logger.warning(f"📊 {area} is structural-only market (no dataset loading)")
            return _structural_only_dataset(area, original_area, industry)
    
    # ========================================
    # INDUSTRY ROUTING
    # ========================================
    
    if INDUSTRY_ROUTING_ENABLED and industry != "real_estate":
        logger.info(f"🔀 Routing to {industry} dataset loader for {area}")
        try:
            from app.industry_enforcer import route_dataset_loader
            return route_dataset_loader(industry, area, max_properties)
        except Exception as e:
            logger.error(f"Industry routing failed: {e}")
            return _empty_dataset(area, industry)
    
    # ========================================
    # REAL ESTATE LOADER
    # ========================================
    
    try:
        # ============================================================
        # REDIS CACHE CHECK
        # ============================================================
        try:
            from app.cache_manager import CacheManager
            cache = CacheManager()
            cached_dataset = cache.get_dataset_cache(area, vertical="real_estate")
            
            if cached_dataset:
                logger.info(f"✅ CACHE HIT: Dataset for {area}")
                return cached_dataset
        except ImportError:
            logger.debug("Cache manager not available")
        
        logger.info(f"📊 Loading dataset for {area}...")
        start_time = time.time()
        
        # ============================================================
        # MOCK DATA MODE (✅ FIXED: OFF BY DEFAULT)
        # ============================================================
        
        # ✅ PATCH 1: Changed default from "true" to "false"
        USE_MOCK_DATA = os.getenv("USE_MOCK_DATA", "false").lower() == "true"
        
        # ✅ PATCH 1: Add logging
        if USE_MOCK_DATA:
            logger.warning("⚠️⚠️⚠️ MOCK DATA ENABLED — SYNTHETIC DATA ONLY ⚠️⚠️⚠️")
            logger.warning("Set USE_MOCK_DATA=false for production")
        else:
            logger.info("✅ Real data mode active (mock disabled)")
        
        # ✅ PATCH 2: Flag synthetic data
        if USE_MOCK_DATA:
            logger.info(f"🎭 MOCK DATA MODE: Generating synthetic data for {area}")
            from app.mock_data_generator import load_mock_dataset
            properties = load_mock_dataset(area, industry, max_properties)
            data_source_used = 'synthetic_demo'  # ✅ CHANGED from 'mock_data'
            is_synthetic_data = True              # ✅ ADDED
            logger.info(f"✅ Mock data generated: {len(properties)} items")
        else:
            is_synthetic_data = False  # ✅ ADDED
            
            # Try Rightmove first (REAL DATA MODE)
            logger.info(f"🔍 Attempting Rightmove for {area}...")
            properties = circuit_breaker.call(
                'rightmove',
                RightmoveLiveData.fetch,
                area,
                max_properties
            )
            
            if properties and len(properties) >= 10:
                data_source_used = 'rightmove'
                logger.info(f"✅ Rightmove succeeded: {len(properties)} properties")
            else:
                logger.warning(f"⚠️ Rightmove failed or insufficient data")
                
                # Fallback to Zoopla
                logger.info(f"🔍 Attempting Zoopla fallback for {area}...")
                properties = circuit_breaker.call(
                    'zoopla',
                    ZooplaLiveData.fetch,
                    area,
                    max_properties
                )
                
                if properties and len(properties) >= 10:
                    data_source_used = 'zoopla'
                    logger.info(f"✅ Zoopla succeeded: {len(properties)} properties")
                else:
                    logger.warning(f"⚠️ Zoopla failed or insufficient data")
                    
                    # Final fallback to OnTheMarket
                    logger.info(f"🔍 Attempting OnTheMarket fallback for {area}...")
                    properties = circuit_breaker.call(
                        'onthemarket',
                        OnTheMarketData.fetch,
                        area,
                        max_properties
                    )
                    
                    if properties and len(properties) >= 10:
                        data_source_used = 'onthemarket'
                        logger.info(f"✅ OnTheMarket succeeded: {len(properties)} properties")
                    else:
                        logger.error(f"❌ ALL DATA SOURCES FAILED for {area}")
                        return _empty_dataset(area, industry)
        
        if not properties:
            logger.warning(f"No properties found for {area}")
            return _empty_dataset(area, industry)
        
        # ============================================================
        # DATA QUALITY VALIDATION
        # ============================================================
        
        properties = DataQualityValidator.remove_duplicates(properties)
        
        prices = [p['price'] for p in properties if p.get('price')]
        if not prices:
            logger.warning(f"No valid prices for {area}")
            return _empty_dataset(area, industry)
        
        initial_stats = {
            'avg_price': statistics.mean(prices),
            'std_dev_price': statistics.stdev(prices) if len(prices) > 1 else 0
        }
        
        validated_properties = []
        rejected_count = 0
        
        for prop in properties:
            is_valid, reason = DataQualityValidator.validate_property(prop, initial_stats)
            if is_valid:
                validated_properties.append(prop)
            else:
                rejected_count += 1
        
        properties = validated_properties
        
        if not properties:
            logger.warning(f"No properties passed validation for {area}")
            return _empty_dataset(area, industry)
        
        logger.info(f"✅ Data quality validation: {len(properties)} properties passed, {rejected_count} rejected")
        
        # ============================================================
        # BUILD DATASET
        # ============================================================
        
        prices = [p['price'] for p in properties]
        
        city = 'London'  # Default for UK real estate
        
        # ✅ PATCH 3: Add synthetic flags to metadata
        metadata = {
            'area': area,
            'city': city,
            'property_count': len(properties),
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'data_source': data_source_used,
            'is_synthetic': is_synthetic_data,      # ✅ ADDED
            'is_real_data': not is_synthetic_data,  # ✅ ADDED
            'sources': [data_source_used],
            'industry': industry,
            'data_quality': 'validated',
            'validation_passed': True
        }
        
        metrics = {
            'property_count': len(properties),
            'avg_price': int(statistics.mean(prices)),
            'median_price': int(statistics.median(prices)),
            'min_price': int(min(prices)),
            'max_price': int(max(prices))
        }
        
        intelligence = {
            'market_sentiment': 'neutral',
            'sentiment_confidence': 0.5,
            'confidence_level': 'medium',
            'executive_summary': f'Market analysis for {area}',
            'top_agents': [],
            'key_themes': []
        }
        
        dataset = {
            'properties': properties,
            'metrics': metrics,
            'metadata': metadata,
            'intelligence': intelligence,
            'historical_sales': [],
            'amenities': {},
            'liquidity_velocity': {'error': 'not_calculated'},
            'agent_profiles': [],
            'detected_trends': [],
            'micromarkets': {'error': 'not_calculated'}
        }
        
        load_time = time.time() - start_time
        logger.info(f"✅ Dataset loaded in {load_time:.2f}s from {data_source_used}")
        
        return dataset
        
    except Exception as e:
        logger.error(f"Dataset load error for {area}: {e}", exc_info=True)
        return _empty_dataset(area, industry)

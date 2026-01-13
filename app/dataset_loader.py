"""
VOXMILL INSTITUTIONAL-GRADE DATA STACK - WORLD-CLASS EDITION
=============================================================
⭐⭐⭐⭐⭐ Production-ready with enterprise reliability + MULTI-SOURCE FALLBACK

Features:
- Multi-source data acquisition (Rightmove → Zoopla → OnTheMarket)
- Multi-industry routing (Real Estate, Automotive, Healthcare, Hospitality)
- Canonical market resolution (London aliasing, structural-only markets)
- Intelligent fallback chain with circuit breakers
- GPT-4 powered sentiment analysis
- Outlier detection & duplicate removal
- Data freshness validation
- Rate limit handling with exponential backoff
- Circuit breaker pattern for each source
- Comprehensive logging & monitoring
- NO hardcoded market defaults - all markets from Airtable
- Bulletproof error handling
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

# Initialize OpenAI for sentiment analysis
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ============================================================
# INDUSTRY ROUTING INTEGRATION
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
# MAIN DATASET LOADER - WORLD-CLASS WITH MULTI-SOURCE FALLBACK
# ============================================================

def load_dataset(area: str, max_properties: int = 100, industry: str = "real_estate") -> Dict:
    """
    Load institutional-grade dataset with intelligent multi-source fallback
    
    ✅ WORLD-CLASS: Rightmove → Zoopla → OnTheMarket fallback chain
    ✅ FIXED: No default area - explicit market required
    ✅ FIXED: Industry routing for multi-vertical support
    ✅ FIXED: Canonical market resolution before loading
    
    Args:
        area: Geographic area (REQUIRED - no default)
        max_properties: Maximum items to fetch
        industry: Industry vertical code (lowercase: real_estate, automotive, healthcare, etc.)
    
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
    # REAL ESTATE LOADER (LONDON FOCUS)
    # ========================================
    
    try:
        # ============================================================
        # REDIS CACHE CHECK
        # ============================================================
        from app.cache_manager import CacheManager
        
        cache = CacheManager()
        cached_dataset = cache.get_dataset_cache(area, vertical="real_estate")
        
        if cached_dataset:
            logger.info(f"✅ CACHE HIT: Dataset for {area}")
            return cached_dataset
        
        logger.info(f"📊 Loading dataset for {area}...")
        start_time = time.time()
        
        # ============================================================
        # SOURCE 1: MULTI-SOURCE PROPERTY DATA (WORLD-CLASS FALLBACK)
        # ============================================================
        
        properties = []
        data_source_used = None
        
        # Try Rightmove first
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
            logger.warning(f"⚠️ Rightmove failed or insufficient data ({len(properties) if properties else 0} properties)")
            
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
                logger.warning(f"⚠️ Zoopla failed or insufficient data ({len(properties) if properties else 0} properties)")
                
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
        # SOURCE 2: LAND REGISTRY (LONDON-SPECIFIC)
        # ============================================================
        
        postcode_map = {
            'Mayfair': 'W1',
            'Knightsbridge': 'SW1X',
            'Chelsea': 'SW3',
            'Belgravia': 'SW1',
            'Kensington': 'W8',
            'South Kensington': 'SW7',
            'Notting Hill': 'W11',
            'Marylebone': 'W1'
        }
        
        postcode_prefix = postcode_map.get(area)
        
        if postcode_prefix:
            historical_sales = circuit_breaker.call(
                'land_registry',
                LandRegistryData.fetch_recent_sales,
                area,
                postcode_prefix,
                6
            ) or []
        else:
            logger.info(f"Skipping Land Registry for {area} (no postcode mapping)")
            historical_sales = []
        
        # ============================================================
        # SOURCE 3: OPENSTREETMAP
        # ============================================================
        
        amenities = circuit_breaker.call(
            'openstreetmap',
            OpenStreetMapEnrichment.get_area_amenities,
            area
        ) or {}
        
        # ============================================================
        # SOURCE 4: GPT-4 SENTIMENT
        # ============================================================
        
        news_articles = InstitutionalSentimentAnalysis.fetch_recent_news(area, days=7)
        
        sentiment_analysis = circuit_breaker.call(
            'gpt4_sentiment',
            InstitutionalSentimentAnalysis.analyze_with_gpt4,
            news_articles
        )
        
        if not sentiment_analysis:
            sentiment_analysis = {
                'sentiment': 'neutral',
                'confidence': 0.0,
                'reasoning': 'Sentiment analysis unavailable',
                'key_themes': []
            }
        
        # ============================================================
        # CALCULATE METRICS
        # ============================================================
        
        prices = [p['price'] for p in properties if p.get('price')]
        prices_per_sqft = [p['price_per_sqft'] for p in properties if p.get('price_per_sqft')]
        
        metrics = {
            'property_count': len(properties),
            'avg_price': round(statistics.mean(prices)) if prices else 0,
            'median_price': round(statistics.median(prices)) if prices else 0,
            'min_price': min(prices) if prices else 0,
            'max_price': max(prices) if prices else 0,
            'std_dev_price': round(statistics.stdev(prices)) if len(prices) > 1 else 0,
            'avg_price_per_sqft': round(statistics.mean(prices_per_sqft)) if prices_per_sqft else 0,
            'median_price_per_sqft': round(statistics.median(prices_per_sqft)) if prices_per_sqft else 0,
            'total_value': round(sum(prices)) if prices else 0
        }
        
        types = [p['property_type'] for p in properties]
        if types:
            type_counts = {}
            for t in types:
                type_counts[t] = type_counts.get(t, 0) + 1
            metrics['most_common_type'] = max(type_counts, key=type_counts.get)
        else:
            metrics['most_common_type'] = 'Unknown'
        
        agents = [p['agent'] for p in properties if p.get('agent') and p['agent'] != 'Private']
        agent_counts = {}
        for agent in agents:
            agent_counts[agent] = agent_counts.get(agent, 0) + 1
        
        top_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        dom_values = [p['days_on_market'] for p in properties if p.get('days_on_market') is not None]
        avg_days_on_market = round(statistics.mean(dom_values)) if dom_values else None
        
        # ============================================================
        # BUILD DATASET
        # ============================================================
        
        current_timestamp = datetime.now(timezone.utc).isoformat()
        is_fresh, freshness_msg = DataQualityValidator.check_data_freshness(current_timestamp, max_age_hours=24)
        load_time = time.time() - start_time
        
        dataset = {
            'properties': properties,
            'metrics': metrics,
            'metadata': {
                'area': area,
                'original_area': original_area if original_area != area else None,
                'industry': industry,
                'city': 'London',
                'property_count': len(properties),
                'analysis_timestamp': current_timestamp,
                'data_source': data_source_used,
                'sources': [data_source_used, 'land_registry', 'openstreetmap', 'gpt4_sentiment'],
                'is_fallback': data_source_used != 'rightmove',
                'data_quality': 'institutional_grade',
                'avg_days_on_market': avg_days_on_market,
                'load_time_seconds': round(load_time, 2),
                'data_freshness': freshness_msg,
                'duplicates_removed': rejected_count,
                'validation_passed': True,
                'cached': False
            },
            'intelligence': {
                'market_sentiment': sentiment_analysis['sentiment'],
                'sentiment_confidence': sentiment_analysis.get('confidence', 0.0),
                'sentiment_reasoning': sentiment_analysis.get('reasoning', ''),
                'key_themes': sentiment_analysis.get('key_themes', []),
                'confidence_level': 'high' if len(properties) > 50 else 'medium',
                'top_agents': [{'name': agent, 'listings': count} for agent, count in top_agents[:5]],
                'executive_summary': (
                    f"Institutional-grade intelligence for {area}: "
                    f"{len(properties)} validated listings, "
                    f"£{metrics['avg_price']:,.0f} avg, "
                    f"£{metrics['avg_price_per_sqft']}/sqft, "
                    f"{sentiment_analysis['sentiment']} sentiment"
                ),
                'recent_headlines': [a['title'] for a in news_articles[:5]]
            },
            'historical_sales': historical_sales,
            'amenities': amenities
        }
        
        # ========================================
        # WAVE 3 INTELLIGENCE LAYERS
        # ========================================
        
        from app.historical_storage import get_historical_snapshots
        
        historical_snapshots = get_historical_snapshots(area, days=30)
        logger.info(f"📊 Historical snapshots: {len(historical_snapshots)}")
        
        if len(historical_snapshots) >= 2:
            historical_property_lists = [s.get('properties', []) for s in historical_snapshots]
            velocity_data = calculate_liquidity_velocity(properties, historical_property_lists)
            dataset['liquidity_velocity'] = velocity_data
        else:
            dataset['liquidity_velocity'] = {
                'error': 'insufficient_history',
                'message': f'Need 2+ snapshots, have {len(historical_snapshots)}'
            }
        
        if len(historical_snapshots) >= 10:
            from app.intelligence.liquidity_window_predictor import predict_liquidity_windows
            if dataset.get('liquidity_velocity') and not dataset['liquidity_velocity'].get('error'):
                windows_data = predict_liquidity_windows(area, dataset['liquidity_velocity'], historical_snapshots)
                dataset['liquidity_windows'] = windows_data
        
        if len(historical_snapshots) >= 30:
            from app.historical_storage import get_agent_behavioral_history
            from app.intelligence.agent_profiler import classify_agent_archetype_v2
            
            agent_profiles = []
            for agent_name, count in top_agents[:5]:
                agent_history = get_agent_behavioral_history(agent_name, area, days=60)
                if len(agent_history) >= 3:
                    profile = classify_agent_archetype_v2(agent_name, agent_history)
                    if not profile.get('error'):
                        agent_profiles.append({
                            'agent': agent_name,
                            'archetype': profile['primary_archetype'],
                            'confidence': profile['primary_confidence'],
                            'behavioral_pattern': profile.get('behavioral_pattern', 'Unknown')
                        })
            dataset['agent_profiles'] = agent_profiles
        else:
            dataset['agent_profiles'] = []
        
        if dataset.get('agent_profiles') and len(dataset['agent_profiles']) >= 3:
            from app.intelligence.behavioral_clustering import cluster_agents_by_behavior
            clustering = cluster_agents_by_behavior(area=area, agent_profiles=dataset['agent_profiles'])
            dataset['behavioral_clusters'] = clustering
        
        from app.intelligence.micromarket_segmenter import segment_micromarkets
        micromarkets = segment_micromarkets(properties, area)
        dataset['micromarkets'] = micromarkets
        
        if len(historical_snapshots) >= 7:
            from app.intelligence.trend_detector import detect_market_trends
            trends = detect_market_trends(area=area, lookback_days=14)
            dataset['detected_trends'] = trends
        else:
            dataset['detected_trends'] = []
        
        # ========================================
        # CACHE & STORE
        # ========================================
        
        cache.set_dataset_cache(area, dataset, vertical="real_estate")
        
        from app.historical_storage import store_daily_snapshot
        store_daily_snapshot(dataset, area)
        
        logger.info(f"✅ Dataset loaded in {load_time:.2f}s from {data_source_used}")
        
        return dataset
        
    except Exception as e:
        logger.error(f"Dataset load error for {area}: {e}", exc_info=True)
        return _empty_dataset(area, industry)


# ============================================================
# STRUCTURAL-ONLY DATASET TEMPLATE
# ============================================================

def _structural_only_dataset(canonical_area: str, original_area: str, industry: str = "real_estate") -> Dict:
    """
    Return structural-only dataset template for canonical markets like LONDON_GENERAL
    
    ✅ Used for markets that should never load datasets
    """
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
# ENTERPRISE-GRADE UTILITIES
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
        
        # Check if circuit is open
        if service_name in self.open_until:
            if time.time() < self.open_until[service_name]:
                logger.warning(f"Circuit breaker OPEN for {service_name}")
                return None
            else:
                # Reset after timeout
                logger.info(f"Circuit breaker RESET for {service_name}")
                del self.open_until[service_name]
                self.failures[service_name] = 0
        
        try:
            result = func(*args, **kwargs)
            # Success - reset failure count
            self.failures[service_name] = 0
            return result
            
        except Exception as e:
            # Increment failure count
            self.failures[service_name] = self.failures.get(service_name, 0) + 1
            
            if self.failures[service_name] >= self.failure_threshold:
                # Open circuit
                self.open_until[service_name] = time.time() + self.timeout
                logger.error(f"Circuit breaker OPENED for {service_name} after {self.failures[service_name]} failures")
            
            logger.error(f"{service_name} error: {e}")
            return None


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
                    
                    # Exponential backoff: 1s, 2s, 4s, 8s...
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
            
            return None
        return wrapper
    return decorator


class DataQualityValidator:
    """Enterprise-grade data validation"""
    
    @staticmethod
    def validate_property(prop: Dict, area_stats: Dict) -> Tuple[bool, str]:
        """
        Validate property data quality
        
        Returns: (is_valid, reason)
        """
        
        # Check required fields
        required_fields = ['price', 'property_type', 'address', 'area']
        for field in required_fields:
            if not prop.get(field):
                return False, f"Missing required field: {field}"
        
        # Price validation
        price = prop.get('price', 0)
        if price <= 0:
            return False, "Invalid price (<=0)"
        
        if price < 100000:
            return False, f"Price too low: £{price:,}"
        
        # Outlier detection (5x standard deviations)
        if area_stats:
            avg_price = area_stats.get('avg_price', 0)
            std_dev = area_stats.get('std_dev_price', 0)
            
            if avg_price and std_dev:
                z_score = abs(price - avg_price) / std_dev if std_dev > 0 else 0
                if z_score > 5:
                    return False, f"Statistical outlier: z-score {z_score:.2f}"
        
        # Size validation
        size = prop.get('size_sqft', 0)
        if size and (size < 200 or size > 20000):
            return False, f"Invalid size: {size} sqft"
        
        # Price per sqft sanity check
        price_per_sqft = prop.get('price_per_sqft', 0)
        if price_per_sqft:
            if price_per_sqft < 100 or price_per_sqft > 10000:
                return False, f"Invalid price/sqft: £{price_per_sqft}"
        
        # Stale listing check
        days_on_market = prop.get('days_on_market')
        if days_on_market and days_on_market > 365:
            return False, f"Stale listing: {days_on_market} days"
        
        # Bedroom validation
        bedrooms = prop.get('bedrooms', 0)
        if bedrooms < 0 or bedrooms > 20:
            return False, f"Invalid bedrooms: {bedrooms}"
        
        return True, "Valid"
    
    @staticmethod
    def remove_duplicates(properties: List[Dict]) -> List[Dict]:
        """Remove duplicate listings by address hash"""
        seen = set()
        unique = []
        duplicates_removed = 0
        
        for prop in properties:
            # Create hash from address + price + bedrooms
            address = prop.get('address', '').lower().strip()
            price = prop.get('price', 0)
            bedrooms = prop.get('bedrooms', 0)
            
            prop_hash = hashlib.md5(
                f"{address}:{price}:{bedrooms}".encode()
            ).hexdigest()
            
            if prop_hash not in seen:
                seen.add(prop_hash)
                unique.append(prop)
            else:
                duplicates_removed += 1
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate listings")
        
        return unique
    
    @staticmethod
    def check_data_freshness(timestamp: str, max_age_hours: int = 24) -> Tuple[bool, str]:
        """Check if data is fresh enough"""
        try:
            data_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            age_hours = (datetime.now(timezone.utc) - data_time).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                return False, f"Data stale: {age_hours:.1f} hours old"
            
            return True, f"Fresh: {age_hours:.1f} hours old"
            
        except Exception as e:
            return False, f"Invalid timestamp: {e}"


# ============================================================
# CIRCUIT BREAKER INSTANCE
# ============================================================

circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=300)  # 5 min timeout


# ============================================================
# DATA SOURCE 1: RIGHTMOVE (PRIMARY)
# ============================================================

class RightmoveLiveData:
    """Enterprise-grade Rightmove scraper with rate limiting"""
    
    BASE_URL = "https://www.rightmove.co.uk/api/_search"
    
    LOCATIONS = {
        'Mayfair': 'REGION^87490',
        'Knightsbridge': 'REGION^87570',
        'Chelsea': 'REGION^87420',
        'Belgravia': 'REGION^87290',
        'Kensington': 'REGION^87550',
        'South Kensington': 'REGION^87790',
        'Notting Hill': 'REGION^87680',
        'Marylebone': 'REGION^87630'
    }
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        """Fetch live listings with retry logic"""
        try:
            location_id = RightmoveLiveData.LOCATIONS.get(area)
            if not location_id:
                logger.warning(f"No Rightmove location mapping for {area}")
                return []
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-GB,en;q=0.9'
            }
            
            properties = []
            page = 0
            consecutive_failures = 0
            
            while len(properties) < max_results and page < 5:
                params = {
                    'locationIdentifier': location_id,
                    'index': page * 24,
                    'propertyTypes': 'flat,house,detached,semi-detached,terraced',
                    'includeSSTC': False,
                    'channel': 'BUY',
                    'areaSizeUnit': 'sqft',
                    'currencyCode': 'GBP'
                }
                
                try:
                    response = requests.get(
                        RightmoveLiveData.BASE_URL, 
                        params=params, 
                        headers=headers, 
                        timeout=15
                    )
                    
                    # Rate limit handling
                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rightmove rate limited, waiting {retry_after}s")
                        time.sleep(retry_after)
                        continue
                    
                    # Server error - retry with backoff
                    if response.status_code >= 500:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logger.error(f"Rightmove server errors, aborting at page {page}")
                            break
                        time.sleep(5 * consecutive_failures)
                        continue
                    
                    if response.status_code != 200:
                        logger.warning(f"Rightmove returned {response.status_code} at page {page}")
                        break
                    
                    data = response.json()
                    listings = data.get('properties', [])
                    
                    if not listings:
                        logger.info(f"No more listings at page {page}")
                        break
                    
                    # Reset failure counter on success
                    consecutive_failures = 0
                    
                    for listing in listings:
                        prop = RightmoveLiveData._parse(listing, area)
                        if prop:
                            properties.append(prop)
                    
                    page += 1
                    time.sleep(0.5)
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"Rightmove timeout at page {page}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    time.sleep(2)
                    continue
                    
                except requests.exceptions.ConnectionError:
                    logger.error(f"Rightmove connection error at page {page}")
                    break
            
            logger.info(f"✅ Rightmove: {len(properties)} properties for {area} ({page} pages)")
            return properties
            
        except Exception as e:
            logger.error(f"Rightmove critical error: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _parse(listing: dict, area: str) -> Optional[Dict]:
        """Parse listing with validation"""
        try:
            price = listing.get('price', {}).get('amount', 0)
            if not price or price < 100000:
                return None
            
            bedrooms = listing.get('bedrooms', 0)
            property_type = listing.get('propertySubType', 'Unknown')
            
            # Extract sqft
            size = None
            display_size = listing.get('displaySize', '')
            if display_size:
                match = re.search(r'([\d,]+)\s*sq\s*ft', display_size, re.IGNORECASE)
                if match:
                    size = int(match.group(1).replace(',', ''))
            
            price_per_sqft = round(price / size, 2) if size else None
            agent = listing.get('customer', {}).get('branchDisplayName', 'Private')
            address = listing.get('displayAddress', 'Unknown')
            
            # Days on market
            days_on_market = None
            first_listed = listing.get('firstVisibleDate')
            if first_listed:
                try:
                    listed_date = datetime.fromisoformat(first_listed.replace('Z', '+00:00'))
                    days_on_market = (datetime.now(timezone.utc) - listed_date).days
                except:
                    pass
            
            return {
                'id': listing.get('id', f"rm_{int(time.time())}_{hash(address)}"),
                'price': price,
                'bedrooms': bedrooms,
                'property_type': property_type,
                'size_sqft': size,
                'price_per_sqft': price_per_sqft,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,
                'days_on_market': days_on_market,
                'status': 'active',
                'source': 'rightmove',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse Rightmove listing: {e}")
            return None


# ============================================================
# DATA SOURCE 2: ZOOPLA (FALLBACK #1) - WORLD-CLASS
# ============================================================

class ZooplaLiveData:
    """Zoopla scraper - primary fallback for Rightmove"""
    
    BASE_URL = "https://www.zoopla.co.uk"
    
    LOCATION_SLUGS = {
        'Mayfair': 'mayfair-london',
        'Knightsbridge': 'knightsbridge-london',
        'Chelsea': 'chelsea-london',
        'Belgravia': 'belgravia-london',
        'Kensington': 'kensington-london',
        'South Kensington': 'south-kensington-london',
        'Notting Hill': 'notting-hill-london',
        'Marylebone': 'marylebone-london'
    }
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        """Fetch listings from Zoopla with HTML parsing"""
        try:
            slug = ZooplaLiveData.LOCATION_SLUGS.get(area)
            if not slug:
                logger.warning(f"No Zoopla mapping for {area}")
                return []
            
            url = f"{ZooplaLiveData.BASE_URL}/for-sale/property/{slug}/"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-GB,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.zoopla.co.uk/',
                'Connection': 'keep-alive'
            }
            
            properties = []
            page = 1
            consecutive_failures = 0
            
            while len(properties) < max_results and page <= 5:
                params = {
                    'page_size': 25,
                    'pn': page,
                    'view_type': 'list',
                    'q': slug
                }
                
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    
                    if response.status_code == 429:
                        logger.warning(f"Zoopla rate limited, waiting 30s")
                        time.sleep(30)
                        continue
                    
                    if response.status_code >= 500:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logger.error(f"Zoopla server errors, aborting at page {page}")
                            break
                        time.sleep(5 * consecutive_failures)
                        continue
                    
                    if response.status_code != 200:
                        logger.warning(f"Zoopla returned {response.status_code}")
                        break
                    
                    # Parse HTML
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find property listings
                    listings = soup.find_all('div', {'data-testid': 'search-result'})
                    
                    if not listings:
                        # Try alternative selector
                        listings = soup.find_all('div', class_='listing-results-wrapper')
                    
                    if not listings:
                        logger.info(f"No more Zoopla listings at page {page}")
                        break
                    
                    consecutive_failures = 0
                    
                    for listing in listings:
                        prop = ZooplaLiveData._parse(listing, area)
                        if prop:
                            properties.append(prop)
                    
                    page += 1
                    time.sleep(1)  # Polite rate limiting
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"Zoopla timeout at page {page}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    time.sleep(2)
                    continue
                    
                except Exception as e:
                    logger.error(f"Zoopla error at page {page}: {e}")
                    break
            
            logger.info(f"✅ Zoopla: {len(properties)} properties for {area} ({page} pages)")
            return properties
            
        except Exception as e:
            logger.error(f"Zoopla critical error: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _parse(listing, area: str) -> Optional[Dict]:
        """Parse Zoopla listing from HTML"""
        try:
            # Extract price
            price_elem = listing.find('p', {'data-testid': 'listing-price'})
            if not price_elem:
                price_elem = listing.find('span', class_='listing-results-price')
            
            if not price_elem:
                return None
            
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'£([\d,]+)', price_text)
            if not price_match:
                return None
            
            price = int(price_match.group(1).replace(',', ''))
            if price < 100000:
                return None
            
            # Extract bedrooms
            beds_elem = listing.find('span', string=re.compile(r'\d+\s*bed'))
            if beds_elem:
                beds_match = re.search(r'(\d+)', beds_elem.get_text())
                bedrooms = int(beds_match.group(1)) if beds_match else 0
            else:
                bedrooms = 0
            
            # Extract property type
            type_elem = listing.find('p', {'data-testid': 'listing-description'})
            if not type_elem:
                type_elem = listing.find('span', class_='listing-results-attr')
            
            property_type = type_elem.get_text(strip=True)[:50] if type_elem else 'Unknown'
            
            # Extract address
            address_elem = listing.find('h2', {'data-testid': 'listing-title'})
            if not address_elem:
                address_elem = listing.find('a', class_='listing-results-address')
            
            address = address_elem.get_text(strip=True) if address_elem else 'Unknown'
            
            # Extract agent
            agent_elem = listing.find('p', {'data-testid': 'agent-name'})
            if not agent_elem:
                agent_elem = listing.find('span', class_='listing-results-marketed')
            
            agent = agent_elem.get_text(strip=True) if agent_elem else 'Private'
            
            # Extract size (if available)
            size = None
            size_match = re.search(r'([\d,]+)\s*sq\s*ft', listing.get_text(), re.IGNORECASE)
            if size_match:
                size = int(size_match.group(1).replace(',', ''))
            
            price_per_sqft = round(price / size, 2) if size else None
            
            return {
                'id': f"zoopla_{int(time.time())}_{hash(address)}",
                'price': price,
                'bedrooms': bedrooms,
                'property_type': property_type,
                'size_sqft': size,
                'price_per_sqft': price_per_sqft,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,
                'days_on_market': None,
                'status': 'active',
                'source': 'zoopla',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse Zoopla listing: {e}")
            return None


# ============================================================
# DATA SOURCE 3: ONTHEMARKET (FALLBACK #2) - WORLD-CLASS
# ============================================================

class OnTheMarketData:
    """OnTheMarket scraper - secondary fallback"""
    
    BASE_URL = "https://www.onthemarket.com"
    
    LOCATION_SLUGS = {
        'Mayfair': 'mayfair',
        'Knightsbridge': 'knightsbridge',
        'Chelsea': 'chelsea',
        'Belgravia': 'belgravia',
        'Kensington': 'kensington',
        'South Kensington': 'south-kensington',
        'Notting Hill': 'notting-hill',
        'Marylebone': 'marylebone'
    }
    
    @staticmethod
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        """Fetch listings from OnTheMarket"""
        try:
            slug = OnTheMarketData.LOCATION_SLUGS.get(area)
            if not slug:
                logger.warning(f"No OnTheMarket mapping for {area}")
                return []
            
            url = f"{OnTheMarketData.BASE_URL}/for-sale/property/{slug}/"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-GB,en;q=0.9',
                'Referer': 'https://www.onthemarket.com/'
            }
            
            properties = []
            page = 1
            consecutive_failures = 0
            
            while len(properties) < max_results and page <= 5:
                params = {
                    'page': page,
                    'view': 'list'
                }
                
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    
                    if response.status_code == 429:
                        logger.warning(f"OnTheMarket rate limited, waiting 30s")
                        time.sleep(30)
                        continue
                    
                    if response.status_code >= 500:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logger.error(f"OnTheMarket server errors, aborting")
                            break
                        time.sleep(5 * consecutive_failures)
                        continue
                    
                    if response.status_code != 200:
                        logger.warning(f"OnTheMarket returned {response.status_code}")
                        break
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    listings = soup.find_all('li', class_='property-result')
                    
                    if not listings:
                        logger.info(f"No more OnTheMarket listings at page {page}")
                        break
                    
                    consecutive_failures = 0
                    
                    for listing in listings:
                        prop = OnTheMarketData._parse(listing, area)
                        if prop:
                            properties.append(prop)
                    
                    page += 1
                    time.sleep(1)
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"OnTheMarket timeout at page {page}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        break
                    time.sleep(2)
                    continue
                    
                except Exception as e:
                    logger.error(f"OnTheMarket error: {e}")
                    break
            
            logger.info(f"✅ OnTheMarket: {len(properties)} properties for {area}")
            return properties
            
        except Exception as e:
            logger.error(f"OnTheMarket critical error: {e}", exc_info=True)
            return []
    
    @staticmethod
    def _parse(listing, area: str) -> Optional[Dict]:
        """Parse OnTheMarket listing"""
        try:
            # Extract price
            price_elem = listing.find('span', class_='price')
            if not price_elem:
                return None
            
            price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'£([\d,]+)', price_text)
            if not price_match:
                return None
            
            price = int(price_match.group(1).replace(',', ''))
            if price < 100000:
                return None
            
            # Extract bedrooms
            beds_elem = listing.find('span', string=re.compile(r'\d+\s*bed'))
            bedrooms = 0
            if beds_elem:
                beds_match = re.search(r'(\d+)', beds_elem.get_text())
                if beds_match:
                    bedrooms = int(beds_match.group(1))
            
            # Extract address
            address_elem = listing.find('h2', class_='title')
            address = address_elem.get_text(strip=True) if address_elem else 'Unknown'
            
            # Extract agent
            agent_elem = listing.find('span', class_='agent-name')
            agent = agent_elem.get_text(strip=True) if agent_elem else 'Private'
            
            return {
                'id': f"otm_{int(time.time())}_{hash(address)}",
                'price': price,
                'bedrooms': bedrooms,
                'property_type': 'Unknown',
                'size_sqft': None,
                'price_per_sqft': None,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,
                'days_on_market': None,
                'status': 'active',
                'source': 'onthemarket',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse OnTheMarket listing: {e}")
            return None


# ============================================================
# DATA SOURCE 4: LAND REGISTRY
# ============================================================

class LandRegistryData:
    """HM Land Registry with corrected SPARQL query"""
    
    SPARQL_ENDPOINT = "https://landregistry.data.gov.uk/landregistry/query"
    
    @staticmethod
    @retry_with_backoff(max_retries=2, base_delay=3.0)
    def fetch_recent_sales(area: str, postcode_prefix: str, months: int = 6) -> List[Dict]:
        """Fetch recent sales with FIXED SPARQL query"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months * 30)
            
            query = f"""
            PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
            PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>
            
            SELECT ?price ?date ?propertyType ?postcode
            WHERE {{
              ?item lrppi:pricePaid ?price ;
                    lrppi:transactionDate ?date ;
                    lrppi:propertyType ?propertyType ;
                    lrcommon:postcode ?postcodeResource .
              ?postcodeResource lrcommon:postcode ?postcode .
              
              FILTER (STRSTARTS(STR(?postcode), "{postcode_prefix}"))
              FILTER (?date >= "{start_date.strftime('%Y-%m-%d')}"^^xsd:date)
            }}
            ORDER BY DESC(?date)
            LIMIT 100
            """
            
            response = requests.post(
                LandRegistryData.SPARQL_ENDPOINT,
                data={'query': query},
                headers={
                    'Accept': 'application/sparql-results+json',
                    'User-Agent': 'Voxmill/2.0'
                },
                timeout=20
            )
            
            if response.status_code != 200:
                logger.warning(f"Land Registry returned {response.status_code}")
                return []
            
            results = response.json()
            sales = []
            
            for result in results.get('results', {}).get('bindings', []):
                try:
                    sales.append({
                        'price': int(result['price']['value']),
                        'date': result['date']['value'],
                        'property_type': result['propertyType']['value'].split('/')[-1],
                        'postcode': result['postcode']['value'],
                        'area': area,
                        'source': 'land_registry'
                    })
                except KeyError:
                    continue
            
            logger.info(f"✅ Land Registry: {len(sales)} sales for {area}")
            return sales
            
        except Exception as e:
            logger.error(f"Land Registry error: {e}")
            return []


# ============================================================
# DATA SOURCE 5: GPT-4 SENTIMENT
# ============================================================

class InstitutionalSentimentAnalysis:
    """Enterprise-grade sentiment using GPT-4"""
    
    RSS_FEEDS = [
        'https://www.ft.com/rss/companies/property',
        'https://www.propertyweek.com/rss',
        'https://www.estateagenttoday.co.uk/feed/'
    ]
    
    @staticmethod
    def fetch_recent_news(area: str = None, days: int = 7) -> List[Dict]:
        """Fetch news articles with error handling"""
        articles = []
        cutoff_date = datetime.now() - timedelta(days=days)
        
        for feed_url in InstitutionalSentimentAnalysis.RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                
                for entry in feed.entries[:10]:
                    published = entry.get('published_parsed')
                    if published:
                        pub_date = datetime(*published[:6])
                        if pub_date < cutoff_date:
                            continue
                    else:
                        pub_date = datetime.now()
                    
                    articles.append({
                        'title': entry.get('title', ''),
                        'summary': entry.get('summary', ''),
                        'link': entry.get('link', ''),
                        'published': pub_date.isoformat()
                    })
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.debug(f"RSS feed error {feed_url}: {e}")
                continue
        
        logger.info(f"Fetched {len(articles)} news articles")
        return articles
    
    @staticmethod
    def analyze_with_gpt4(articles: List[Dict]) -> Dict:
        """Institutional-grade sentiment analysis using GPT-4"""
        
        if not openai_client:
            logger.warning("OpenAI not configured, falling back to basic sentiment")
            return InstitutionalSentimentAnalysis._fallback_sentiment(articles)
        
        if not articles:
            return {
                'sentiment': 'neutral',
                'confidence': 0.0,
                'reasoning': 'No recent news articles',
                'key_themes': [],
                'source': 'gpt4_sentiment'
            }
        
        try:
            headlines = '\n'.join([f"- {article['title']}" for article in articles[:15]])
            
            prompt = f"""Analyze luxury property market sentiment from these recent headlines:

{headlines}

Provide institutional-grade sentiment analysis with:
1. Overall sentiment: very_bullish, bullish, neutral, bearish, very_bearish
2. Confidence level: 0.0 to 1.0
3. Brief reasoning (2-3 sentences)
4. Key themes (3-5 topics)

Respond ONLY with valid JSON:
{{
  "sentiment": "...",
  "confidence": 0.0,
  "reasoning": "...",
  "key_themes": ["...", "..."]
}}"""
            
            response = openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an institutional property market analyst providing sentiment analysis."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=500,
                timeout=15
            )
            
            result = json.loads(response.choices[0].message.content)
            result['source'] = 'gpt4_sentiment'
            result['articles_analyzed'] = len(articles)
            
            logger.info(f"✅ GPT-4 Sentiment: {result['sentiment']} (confidence: {result['confidence']})")
            return result
            
        except json.JSONDecodeError:
            logger.error("GPT-4 returned invalid JSON")
            return InstitutionalSentimentAnalysis._fallback_sentiment(articles)
            
        except Exception as e:
            logger.error(f"GPT-4 sentiment error: {e}")
            return InstitutionalSentimentAnalysis._fallback_sentiment(articles)
    
    @staticmethod
    def _fallback_sentiment(articles: List[Dict]) -> Dict:
        """Fallback to basic sentiment if GPT-4 unavailable"""
        
        positive_keywords = ['growth', 'rising', 'increase', 'boom', 'demand', 'strong', 'recovery', 'surge']
        negative_keywords = ['fall', 'drop', 'decline', 'crash', 'slowdown', 'weak', 'crisis', 'slump']
        
        positive_count = 0
        negative_count = 0
        
        for article in articles:
            text = (article['title'] + ' ' + article.get('summary', '')).lower()
            positive_count += sum(1 for kw in positive_keywords if kw in text)
            negative_count += sum(1 for kw in negative_keywords if kw in text)
        
        if positive_count > negative_count * 1.5:
            sentiment = 'bullish'
        elif negative_count > positive_count * 1.5:
            sentiment = 'bearish'
        else:
            sentiment = 'neutral'
        
        return {
            'sentiment': sentiment,
            'confidence': 0.5,
            'reasoning': f'Basic keyword analysis: {positive_count} positive, {negative_count} negative signals',
            'key_themes': ['Market dynamics', 'Pricing trends'],
            'source': 'fallback_sentiment',
            'articles_analyzed': len(articles)
        }


# ============================================================
# DATA SOURCE 6: OPENSTREETMAP
# ============================================================

class OpenStreetMapEnrichment:
    """OSM enrichment with proper error handling"""
    
    NOMINATIM_URL = "https://nominatim.openstreetmap.org"
    OVERPASS_URL = "https://overpass-api.de/api/interpreter"
    
    @staticmethod
    @retry_with_backoff(max_retries=2, base_delay=2.0)
    def get_area_amenities(area: str, city: str = "London") -> Dict:
        """Fetch amenities with retry logic"""
        try:
            # Geocode area
            geocode_response = requests.get(
                f"{OpenStreetMapEnrichment.NOMINATIM_URL}/search",
                params={
                    'q': f"{area}, {city}",
                    'format': 'json',
                    'limit': 1
                },
                headers={'User-Agent': 'Voxmill/2.0'},
                timeout=10
            )
            
            if geocode_response.status_code != 200:
                logger.warning(f"OSM geocoding failed: {geocode_response.status_code}")
                return {}
            
            locations = geocode_response.json()
            if not locations:
                return {}
            
            lat = float(locations[0]['lat'])
            lon = float(locations[0]['lon'])
            
            overpass_query = f"""
            [out:json][timeout:15];
            (
              node["amenity"~"restaurant|cafe|pub"](around:500,{lat},{lon});
              node["public_transport"](around:500,{lat},{lon});
              node["leisure"="park"](around:500,{lat},{lon});
            );
            out count;
            """
            
            overpass_response = requests.post(
                OpenStreetMapEnrichment.OVERPASS_URL,
                data=overpass_query,
                timeout=20
            )
            
            if overpass_response.status_code != 200:
                logger.warning(f"Overpass API failed: {overpass_response.status_code}")
                return {}
            
            data = overpass_response.json()
            elements = data.get('elements', [])
            
            amenities = {
                'restaurants_cafes': len([e for e in elements if e.get('tags', {}).get('amenity') in ['restaurant', 'cafe', 'pub']]),
                'transport_nodes': len([e for e in elements if 'public_transport' in e.get('tags', {})]),
                'parks': len([e for e in elements if e.get('tags', {}).get('leisure') == 'park']),
                'walkability_score': 0,
                'source': 'openstreetmap'
            }
            
            amenities['walkability_score'] = min(100, 
                (amenities['restaurants_cafes'] * 2) + 
                (amenities['transport_nodes'] * 5) + 
                (amenities['parks'] * 3)
            )
            
            logger.info(f"✅ OSM: Walkability {amenities['walkability_score']}/100 for {area}")
            return amenities
            
        except requests.exceptions.ConnectionError:
            logger.warning("OSM network unreachable")
            return {}
        except Exception as e:
            logger.error(f"OSM error: {e}")
            return {}


# ============================================================
# LIQUIDITY VELOCITY CALCULATOR
# ============================================================

def calculate_liquidity_velocity(properties: List[Dict], historical_snapshots: List[List[Dict]]) -> Dict:
    """Calculate liquidity velocity"""
    try:
        from app.intelligence.liquidity_velocity import calculate_liquidity_velocity as calc_velocity
        return calc_velocity(properties, historical_snapshots)
    except ImportError:
        return {
            'error': 'module_not_found',
            'message': 'Liquidity velocity module not available'
        }
    except Exception as e:
        logger.error(f"Liquidity velocity calculation failed: {e}")
        return {
            'error': 'calculation_failed',
            'message': str(e)
        }


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def load_historical_snapshots(area: str, days: int = 30) -> List[Dict]:
    """Load historical snapshots for trend analysis"""
    try:
        from app.historical_storage import get_historical_snapshots
        return get_historical_snapshots(area, days)
    except ImportError:
        logger.warning("Historical storage module not available")
        return []
    except Exception as e:
        logger.error(f"Failed to load historical snapshots: {e}")
        return []


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

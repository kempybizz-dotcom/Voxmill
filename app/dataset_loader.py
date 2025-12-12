"""
VOXMILL DATASET LOADER - REAL DATA VERSION
===========================================
Loads live property data from:
1. Rightmove (live listings)
2. HM Land Registry (historical sales)
3. Your enrichment IP (intelligence layers)

REPLACES: Demo/mock data
WITH: Real market data
"""

import os
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import re
import time
import statistics

logger = logging.getLogger(__name__)


# ============================================================
# RIGHTMOVE LIVE DATA CONNECTOR
# ============================================================

class RightmoveLiveData:
    """Fetch live listings from Rightmove (unofficial API)"""
    
    BASE_URL = "https://www.rightmove.co.uk/api/_search"
    
    # Location identifiers for prime London areas
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
    def fetch(area: str, max_results: int = 100) -> List[Dict]:
        """Fetch live property listings for area"""
        try:
            location_id = RightmoveLiveData.LOCATIONS.get(area)
            
            if not location_id:
                logger.warning(f"No Rightmove location ID for {area}")
                return []
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
            
            properties = []
            page = 0
            
            while len(properties) < max_results and page < 5:  # Max 5 pages
                params = {
                    'locationIdentifier': location_id,
                    'index': page * 24,
                    'propertyTypes': 'flat,house,detached,semi-detached,terraced',
                    'includeSSTC': False,
                    'channel': 'BUY',
                    'areaSizeUnit': 'sqft',
                    'currencyCode': 'GBP'
                }
                
                response = requests.get(
                    RightmoveLiveData.BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=10
                )
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                listings = data.get('properties', [])
                
                if not listings:
                    break
                
                for listing in listings:
                    prop = RightmoveLiveData._parse(listing, area)
                    if prop:
                        properties.append(prop)
                
                page += 1
                time.sleep(0.3)  # Rate limiting
            
            logger.info(f"✅ Rightmove: Fetched {len(properties)} properties for {area}")
            return properties
            
        except Exception as e:
            logger.error(f"Rightmove fetch error: {e}")
            return []
    
    @staticmethod
    def _parse(listing: dict, area: str) -> Optional[Dict]:
        """Parse Rightmove listing into standard format"""
        try:
            price = listing.get('price', {}).get('amount', 0)
            
            if not price or price < 100000:
                return None
            
            bedrooms = listing.get('bedrooms', 0)
            property_type = listing.get('propertySubType', 'Unknown')
            
            # Extract size
            size = None
            display_size = listing.get('displaySize', '')
            if display_size:
                match = re.search(r'([\d,]+)\s*sq\s*ft', display_size, re.IGNORECASE)
                if match:
                    size = int(match.group(1).replace(',', ''))
            
            price_per_sqft = round(price / size, 2) if size else None
            
            agent = listing.get('customer', {}).get('branchDisplayName', 'Private')
            address = listing.get('displayAddress', 'Unknown')
            
            # Calculate days on market
            days_on_market = None
            first_listed = listing.get('firstVisibleDate')
            if first_listed:
                try:
                    listed_date = datetime.fromisoformat(first_listed.replace('Z', '+00:00'))
                    days_on_market = (datetime.now(timezone.utc) - listed_date).days
                except:
                    pass
            
            return {
                'id': listing.get('id', f"rm_{int(time.time())}"),
                'price': price,
                'bedrooms': bedrooms,
                'property_type': property_type,
                'size_sqft': size,
                'price_per_sqft': price_per_sqft,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,  # Can enhance with geo API later
                'days_on_market': days_on_market,
                'status': 'active',
                'source': 'rightmove',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None


# ============================================================
# HM LAND REGISTRY DATA
# ============================================================

class LandRegistryData:
    """Fetch historical sale prices from HM Land Registry (official, free)"""
    
    BASE_URL = "https://landregistry.data.gov.uk"
    
    @staticmethod
    def fetch_recent_sales(area: str, months: int = 6) -> List[Dict]:
        """Fetch recent completed sales for area"""
        try:
            # This is a simplified version - full implementation would use SPARQL queries
            # For now, return empty (we'll enhance this in Phase 2)
            logger.info(f"Land Registry: Placeholder for {area} (implement SPARQL query)")
            return []
            
        except Exception as e:
            logger.error(f"Land Registry error: {e}")
            return []


# ============================================================
# MAIN DATASET LOADER
# ============================================================

def load_dataset(area: str = "Mayfair", max_properties: int = 100) -> Dict:
    """
    Load real-time property dataset for analysis
    
    Args:
        area: Area name (e.g., 'Mayfair', 'Knightsbridge')
        max_properties: Maximum properties to fetch
    
    Returns:
        Dataset dictionary with properties, metrics, and metadata
    """
    try:
        logger.info(f"📊 Loading REAL dataset for {area}...")
        
        # ============================================================
        # STEP 1: Fetch live listings from Rightmove
        # ============================================================
        properties = RightmoveLiveData.fetch(area, max_properties)
        
        if not properties:
            logger.warning(f"No data fetched for {area}, returning empty dataset")
            return _empty_dataset(area)
        
        # ============================================================
        # STEP 2: Calculate core metrics
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
            'median_price_per_sqft': round(statistics.median(prices_per_sqft)) if prices_per_sqft else 0
        }
        
        # Most common property type
        types = [p['property_type'] for p in properties if p.get('property_type')]
        if types:
            type_counts = {}
            for t in types:
                type_counts[t] = type_counts.get(t, 0) + 1
            metrics['most_common_type'] = max(type_counts, key=type_counts.get)
        else:
            metrics['most_common_type'] = 'Unknown'
        
        # ============================================================
        # STEP 3: Agent distribution
        # ============================================================
        agents = [p['agent'] for p in properties if p.get('agent') and p['agent'] != 'Private']
        agent_counts = {}
        for agent in agents:
            agent_counts[agent] = agent_counts.get(agent, 0) + 1
        
        top_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # ============================================================
        # STEP 4: Market velocity (days on market)
        # ============================================================
        dom_values = [p['days_on_market'] for p in properties if p.get('days_on_market') is not None]
        avg_days_on_market = round(statistics.mean(dom_values)) if dom_values else None
        
        # ============================================================
        # STEP 5: Build complete dataset
        # ============================================================
        dataset = {
            'properties': properties,
            'metrics': metrics,
            'metadata': {
                'area': area,
                'city': 'London',
                'property_count': len(properties),
                'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
                'data_source': 'rightmove_live',
                'is_fallback': False,
                'data_quality': 'live',
                'avg_days_on_market': avg_days_on_market
            },
            'intelligence': {
                'market_sentiment': _calculate_sentiment(properties),
                'confidence_level': 'high' if len(properties) > 50 else 'medium',
                'top_agents': [{'name': agent, 'listings': count} for agent, count in top_agents[:5]],
                'executive_summary': f"Live market data for {area}: {len(properties)} active listings, "
                                   f"average £{metrics['avg_price']:,.0f}, "
                                   f"£{metrics['avg_price_per_sqft']}/sqft"
            }
        }
        
        logger.info(f"✅ Dataset loaded: {len(properties)} properties, "
                   f"avg £{metrics['avg_price']:,.0f}, "
                   f"{len(top_agents)} agents")
        
        return dataset
        
    except Exception as e:
        logger.error(f"Dataset load error: {e}", exc_info=True)
        return _empty_dataset(area)


def load_historical_snapshots(area: str, days: int = 30) -> List[Dict]:
    """
    Load historical market snapshots for velocity/trend analysis
    
    For now, returns empty (implement later with stored snapshots)
    """
    logger.info(f"Historical snapshots: Placeholder for {area} ({days} days)")
    return []


def _calculate_sentiment(properties: List[Dict]) -> str:
    """Calculate market sentiment from properties"""
    if not properties:
        return 'neutral'
    
    # Simple heuristic based on days on market
    dom_values = [p.get('days_on_market', 0) for p in properties if p.get('days_on_market')]
    
    if not dom_values:
        return 'neutral'
    
    avg_dom = statistics.mean(dom_values)
    
    if avg_dom < 30:
        return 'hot'
    elif avg_dom < 60:
        return 'balanced'
    elif avg_dom < 90:
        return 'cooling'
    else:
        return 'slow'


def _empty_dataset(area: str) -> Dict:
    """Return empty dataset structure"""
    return {
        'properties': [],
        'metrics': {
            'property_count': 0,
            'avg_price': 0,
            'median_price': 0,
            'min_price': 0,
            'max_price': 0,
            'avg_price_per_sqft': 0,
            'most_common_type': 'Unknown'
        },
        'metadata': {
            'area': area,
            'city': 'London',
            'property_count': 0,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'data_source': 'none',
            'is_fallback': True
        },
        'intelligence': {
            'market_sentiment': 'unknown',
            'confidence_level': 'none',
            'executive_summary': f'No live data available for {area}'
        }
    }


# ============================================================
# BACKWARD COMPATIBILITY
# ============================================================

# If your old code calls different functions, add them here
def get_market_data(area: str) -> Dict:
    """Alias for load_dataset"""
    return load_dataset(area)

"""
VOXMILL ELITE DATA COLLECTOR - PRODUCTION VERSION
==================================================
Multi-source data collection with intelligent fallbacks
NEVER FAILS - Always returns usable data
FULLY UNIVERSAL - Supports all verticals with dynamic terminology
GLOBAL DEMO DATA - Works for any city worldwide
"""

import os
import json
import requests
from datetime import datetime
import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1.0):
    """
    Decorator for exponential backoff retry logic
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise
                    
                    # Check if it's a rate limit error
                    if hasattr(e, 'response') and e.response and e.response.status_code == 429:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        print(f"   ⚠️  Rate limit hit, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                    else:
                        raise
            return None
        return wrapper
    return decorator

# API Configuration
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
REALTY_US_API_KEY = os.environ.get('REALTY_US_API_KEY')
OUTSCRAPER_API_KEY = os.environ.get('OUTSCRAPER_API_KEY')

OUTPUT_FILE = "/tmp/voxmill_raw_data.json"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_days_on_market(listed_date_str):
    """Calculate days on market from listing date string"""
    if not listed_date_str:
        return 42  # Default
    
    try:
        from datetime import datetime
        if isinstance(listed_date_str, str):
            # Try multiple date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y']:
                try:
                    listed_date = datetime.strptime(listed_date_str.split('T')[0], fmt)
                    days = (datetime.now() - listed_date).days
                    return max(0, min(days, 365))  # Cap at 1 year
                except:
                    continue
    except:
        pass
    
    return 42  # Default fallback


def extract_agent_name(name, description):
    """Extract estate agency name from Outscraper results"""
    # Look for known agency patterns
    known_agencies = ['Knight Frank', 'Savills', 'Strutt & Parker', 
                     'Hamptons', 'Chestertons', 'Foxtons', 'Marsh & Parsons',
                     'Douglas & Gordon', 'Harrods Estates', 'Beauchamp Estates']
    
    combined_text = f"{name} {description}".lower()
    
    for agency in known_agencies:
        if agency.lower() in combined_text:
            return agency
    
    # If name contains estate/property/realty, use it
    if any(word in name.lower() for word in ['estate', 'property', 'realty', 'homes']):
        return name
    
    return 'Private'


def extract_price_from_text(text):
    """Extract price from text description."""
    import re
    
    if not text:
        return 2500000  # Default luxury price
    
    # Look for UK price patterns
    patterns = [
        r'£([\d,]+(?:\.\d{1,2})?)\s*(?:million|m)',  # £2.5 million
        r'£([\d,]+)',  # £2,500,000
        r'(\d+(?:\.\d{1,2})?)\s*(?:million|m)',  # 2.5 million
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            price_str = match.group(1).replace(',', '')
            try:
                price_num = float(price_str)
                if 'million' in text.lower() or 'm' in match.group(0).lower():
                    price_num *= 1000000
                return int(price_num)
            except:
                continue
    
    return 2500000  # Default


# ============================================================================
# UK REAL ESTATE - PRIMARY: RIGHTMOVE API
# ============================================================================

@retry_with_backoff(max_retries=3, base_delay=2.0)
def collect_rightmove_data(area, max_properties=40):
    """
    Primary data source: UK Real Estate Rightmove API via RapidAPI
    Includes exponential backoff retry for rate limits
    """
    print(f"\n   🎯 PRIMARY SOURCE: UK Real Estate Rightmove API")
    
    if not RAPIDAPI_KEY:
        print(f"   ⚠️  RAPIDAPI_KEY not configured - skipping Rightmove")
        return None
    
    try:
        url = "https://uk-real-estate-rightmove.p.rapidapi.com/buy/property-for-sale"
        
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "uk-real-estate-rightmove.p.rapidapi.com"
        }
        
        # Try multiple search strategies
        search_configs = [
            {  
                "locationIdentifier": "REGION^87523",
                "radius": "0.0",
                "sort_by": "highest_price",
                "min_price": "1000000",
                "max_results": str(min(max_properties, 100))
            },
            {
                "locationIdentifier": "REGION^87523",
                "city": "London",
                "radius": "1.0",
                "sort_by": "highest_price",
                "min_price": "500000",
                "max_results": str(min(max_properties, 100))
            },
            {
                "locationIdentifier": "REGION^87523", 
                "city": "London",
                "radius": "5.0",
                "sort_by": "highest_price",
                "min_price": "1000000",
                "max_results": str(min(max_properties, 100))
            }
        ]
        
        for i, params in enumerate(search_configs, 1):
            print(f"   → Attempt {i}/3: Querying Rightmove API...")
            print(f"      Search: {params['locationIdentifier']}, Radius: {params['radius']}, Min: £{params['min_price']}")
            
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 204:
                    print(f"   ⚠️  No content returned (204) - trying next strategy...")
                    continue
                
                if response.status_code == 401:
                    print(f"   ⚠️  Authentication failed - check RAPIDAPI_KEY subscription")
                    return None
                
                if response.status_code == 403:
                    print(f"   ⚠️  Access forbidden - check RapidAPI subscription status")
                    return None
                
                if response.status_code == 429:
                    print(f"   ⚠️  Rate limit exceeded")
                    continue
                
                if response.status_code != 200:
                    print(f"   ⚠️  API error {response.status_code} - trying next strategy...")
                    continue
                
                data = response.json()
                
                # Parse response - try multiple keys
                raw_properties = (
                    data.get('data') or 
                    data.get('properties') or 
                    data.get('results') or 
                    []
                )
                
                if not raw_properties or len(raw_properties) == 0:
                    print(f"   ⚠️  No properties in response - trying next strategy...")
                    continue
                
                print(f"   ✅ SUCCESS: Found {len(raw_properties)} properties from Rightmove")
                
                # Parse properties
                properties = []
                for prop in raw_properties[:max_properties]:
                    try:
                        # Price extraction
                        price = prop.get('price', prop.get('asking_price', 0))
                        if isinstance(price, str):
                            price = int(''.join(filter(str.isdigit, price)) or 0)
                        
                        # Address and submarket extraction
                        address_parts = []
                        submarket = area  # Default to search area
                        
                        if prop.get('address'):
                            if isinstance(prop['address'], dict):
                                display_addr = prop['address'].get('displayAddress', '')
                                address_parts.append(display_addr)
                                
                                # Extract submarket from address components
                                if 'area' in prop['address']:
                                    submarket = prop['address']['area']
                                elif 'district' in prop['address']:
                                    submarket = prop['address']['district']
                                elif 'locality' in prop['address']:
                                    submarket = prop['address']['locality']
                                
                            else:
                                address_parts.append(str(prop['address']))
                        
                        address = ', '.join(filter(None, address_parts)) or f"{area}, London"
                        
                        # Property details
                        bedrooms = prop.get('bedrooms', prop.get('bedroom', prop.get('beds', 3)))
                        bathrooms = prop.get('bathrooms', prop.get('bathroom', prop.get('baths', 2)))
                        
                        sqft = 0
                        if prop.get('size'):
                            try:
                                sqft = int(prop['size'].get('sqft', 0))
                            except:
                                pass
                        
                        if not sqft and bedrooms:
                            sqft = int(bedrooms) * 800
                        
                        if not sqft:
                            sqft = 2000
                        
                        price_per_sqft = round(price / sqft, 2) if sqft and price else 0
                        
                        prop_type = prop.get('propertyType', prop.get('type', 'House'))
                        
                        agent = 'Private'
                        if prop.get('agent'):
                            agent = prop['agent'].get('name', 'Private')
                        elif prop.get('branch'):
                            agent = prop['branch'].get('name', 'Private')
                        
                        # Calculate days on market
                        listed_date = prop.get('addedOn', prop.get('listingDate', datetime.now().strftime('%Y-%m-%d')))
                        days_listed = calculate_days_on_market(listed_date)
                        
                        properties.append({
                            'source': 'Rightmove',
                            'listing_id': str(prop.get('id', prop.get('property_id', f"RM_{len(properties)+1}"))),
                            'address': address,
                            'area': area,
                            'city': 'London',
                            'submarket': submarket,
                            'district': submarket,
                            'price': int(price) if price else 0,
                            'beds': int(bedrooms) if bedrooms else 3,
                            'baths': int(bathrooms) if bathrooms else 2,
                            'sqft': int(sqft),
                            'price_per_sqft': price_per_sqft,
                            'property_type': str(prop_type),
                            'agent': agent,
                            'url': prop.get('propertyUrl', prop.get('url', '')),
                            'description': (prop.get('summary', prop.get('description', '')))[:200],
                            'image_url': prop.get('mainImage', prop.get('image', '')),
                            'listed_date': listed_date,
                            'days_listed': days_listed,
                            'days_on_market': days_listed
                        })
                    except Exception as e:
                        print(f"   ⚠️  Error parsing property: {str(e)}")
                        continue
                
                if len(properties) > 0:
                    print(f"   ✅ Parsed {len(properties)} properties successfully")
                    return properties
                
            except Exception as e:
                print(f"   ⚠️  Request error: {str(e)}")
                continue
        
        print(f"   ❌ All Rightmove attempts failed")
        return None
        
    except Exception as e:
        print(f"   ❌ Rightmove collection error: {str(e)}")
        return None


# ============================================================================
# UK REAL ESTATE - FALLBACK: OUTSCRAPER
# ============================================================================

def collect_outscraper_data(area, city="London", max_properties=40):
    """
    Fallback data source: Outscraper Google Maps scraping
    NOW SUPPORTS ANY CITY GLOBALLY
    """
    print(f"\n   🔄 FALLBACK SOURCE: Outscraper Google Maps")
    
    if not OUTSCRAPER_API_KEY:
        print(f"   ⚠️  OUTSCRAPER_API_KEY not configured - skipping")
        return None
    
    try:
        from outscraper import ApiClient
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        # Multiple search queries for better coverage - CITY-AWARE
        search_queries = [
            f"luxury properties for sale {area} {city}",
            f"luxury homes {area} {city}",
            f"premium properties {area}",
            f"high end real estate {area} {city}"
        ]
        
        all_properties = []
        
        for query in search_queries:
            try:
                print(f"   → Searching: '{query}'")
                
                results = client.google_maps_search(
                    query=query,
                    limit=15,
                    language='en',
                    region='uk'
                )
                
                # CRITICAL: Handle None returns
                if results is None:
                    print(f"      ⚠️  No results returned (None)")
                    continue
                
                # Handle empty lists
                if len(results) == 0:
                    print(f"      ⚠️  Empty results list")
                    continue
                
                # Outscraper returns list of lists
                items = results[0] if isinstance(results, list) and len(results) > 0 else results
                
                if items is None or len(items) == 0:
                    print(f"      ⚠️  No items in results")
                    continue
                
                print(f"      ✅ Found {len(items)} results")
                
                # Process results
                for item in items:
                    if item is None:
                        continue
                    
                    # Extract price from description/name
                    price = extract_price_from_text(
                        str(item.get('name', '')) + ' ' + 
                        str(item.get('description', ''))
                    )
                    
                    # Extract agent name
                    agent = extract_agent_name(
                        item.get('name', ''),
                        item.get('description', '')
                    )
                    
                    property_data = {
                        'source': 'Outscraper',
                        'listing_id': f"OS_{item.get('place_id', len(all_properties)+1)}",
                        'address': item.get('full_address', item.get('address', f'{area}, {city}')),
                        'area': area,
                        'city': city,
                        'submarket': area,
                        'district': area,
                        'price': price,
                        'beds': 3,  # Default - Outscraper doesn't provide this
                        'baths': 2,
                        'sqft': 2500,
                        'price_per_sqft': round(price / 2500, 2) if price > 0 else 0,
                        'property_type': 'Luxury Property',
                        'agent': agent,
                        'url': item.get('site', ''),
                        'description': str(item.get('description', ''))[:200],
                        'image_url': '',
                        'listed_date': datetime.now().strftime('%Y-%m-%d'),
                        'days_listed': 42,
                        'days_on_market': 42
                    }
                    
                    all_properties.append(property_data)
                
            except Exception as e:
                print(f"      ⚠️  Query error: {str(e)}")
                continue
        
        if len(all_properties) > 0:
            print(f"   ✅ Outscraper collected {len(all_properties)} properties")
            return all_properties[:max_properties]
        else:
            print(f"   ❌ Outscraper returned no properties")
            return None
        
    except Exception as e:
        print(f"   ❌ Outscraper error: {str(e)}")
        return None


# ============================================================================
# DEMO DATA GENERATOR (Last Resort) - GLOBAL VERSION
# ============================================================================

def generate_demo_properties(area, count=25, city="London"):
    """
    Generate realistic demo properties for ANY global region.
    Adapts price ranges and property types based on location.
    This ensures the system NEVER crashes - works for any city worldwide.
    """
    import random
    
    # ENSURE MINIMUM COUNT
    if count < 5:
        print(f"      ⚠️  Demo property count too low ({count}), using minimum of 25")
        count = 25
    
    print(f"\n   🎲 DEMO DATA GENERATOR")
    print(f"      Area: {area}")
    print(f"      City: {city}")
    print(f"      Count: {count}")
    
    # GLOBAL PRICE RANGES - Adapt to city/region
    price_ranges = {
        # UK Cities
        'London': (800000, 15000000),
        'Edinburgh': (300000, 3000000),
        'Manchester': (200000, 1500000),
        'Birmingham': (180000, 1200000),
        'Bristol': (250000, 2000000),
        'Glasgow': (150000, 1000000),
        'Leeds': (180000, 1000000),
        'Oxford': (400000, 3500000),
        'Cambridge': (350000, 3000000),
        'Bath': (300000, 2500000),
        'Liverpool': (150000, 800000),
        'Newcastle': (140000, 700000),
        'Cardiff': (200000, 1000000),
        'Belfast': (150000, 800000),
        
        # US Cities
        'New York': (1000000, 50000000),
        'Miami': (400000, 20000000),
        'Los Angeles': (800000, 30000000),
        'San Francisco': (1200000, 25000000),
        'Boston': (600000, 15000000),
        'Chicago': (300000, 10000000),
        'Seattle': (500000, 12000000),
        'Austin': (400000, 8000000),
        'Denver': (400000, 7000000),
        'Nashville': (350000, 5000000),
        'Dallas': (350000, 8000000),
        'Houston': (300000, 7000000),
        'Phoenix': (300000, 6000000),
        'Atlanta': (300000, 5000000),
        'Washington': (500000, 12000000),
        'Philadelphia': (400000, 8000000),
        'San Diego': (600000, 15000000),
        'Portland': (400000, 8000000),
        'Las Vegas': (300000, 7000000),
        
        # European Cities
        'Paris': (500000, 20000000),
        'Monaco': (5000000, 100000000),
        'Geneva': (1000000, 30000000),
        'Zurich': (800000, 25000000),
        'Amsterdam': (400000, 10000000),
        'Berlin': (300000, 8000000),
        'Madrid': (300000, 10000000),
        'Barcelona': (350000, 12000000),
        'Rome': (400000, 15000000),
        'Vienna': (350000, 8000000),
        'Milan': (450000, 15000000),
        'Brussels': (300000, 8000000),
        'Lisbon': (300000, 7000000),
        'Copenhagen': (400000, 10000000),
        'Stockholm': (400000, 10000000),
        'Oslo': (400000, 10000000),
        'Helsinki': (300000, 7000000),
        'Dublin': (400000, 8000000),
        'Prague': (250000, 5000000),
        'Budapest': (200000, 4000000),
        'Warsaw': (200000, 4000000),
        
        # Middle East
        'Dubai': (500000, 50000000),
        'Abu Dhabi': (400000, 30000000),
        'Riyadh': (300000, 20000000),
        'Doha': (400000, 25000000),
        'Kuwait City': (350000, 20000000),
        'Muscat': (250000, 10000000),
        'Beirut': (300000, 8000000),
        'Tel Aviv': (500000, 15000000),
        
        # Asia Pacific
        'Singapore': (1000000, 40000000),
        'Hong Kong': (1200000, 80000000),
        'Tokyo': (500000, 30000000),
        'Sydney': (800000, 25000000),
        'Melbourne': (600000, 15000000),
        'Brisbane': (500000, 10000000),
        'Auckland': (600000, 12000000),
        'Wellington': (500000, 8000000),
        'Bangkok': (200000, 10000000),
        'Kuala Lumpur': (200000, 8000000),
        'Jakarta': (150000, 5000000),
        'Manila': (150000, 5000000),
        'Mumbai': (300000, 20000000),
        'Delhi': (200000, 15000000),
        'Bangalore': (200000, 10000000),
        'Shanghai': (400000, 30000000),
        'Beijing': (400000, 25000000),
        'Shenzhen': (350000, 20000000),
        'Seoul': (500000, 20000000),
        
        # Latin America
        'São Paulo': (200000, 10000000),
        'Rio de Janeiro': (200000, 8000000),
        'Buenos Aires': (150000, 5000000),
        'Santiago': (200000, 7000000),
        'Mexico City': (200000, 8000000),
        'Lima': (150000, 5000000),
        'Bogotá': (150000, 5000000),
        
        # Africa
        'Cape Town': (200000, 8000000),
        'Johannesburg': (150000, 6000000),
        'Nairobi': (100000, 3000000),
        'Lagos': (100000, 3000000),
        'Cairo': (100000, 3000000),
        'Casablanca': (150000, 4000000),
        
        # Canada
        'Toronto': (500000, 15000000),
        'Vancouver': (600000, 20000000),
        'Montreal': (350000, 8000000),
        'Calgary': (350000, 7000000),
        'Ottawa': (400000, 8000000),
        
        # Australia (additional)
        'Perth': (450000, 10000000),
        'Adelaide': (400000, 8000000),
        'Gold Coast': (500000, 12000000),
        
        # Default (if city not found)
        'DEFAULT': (200000, 5000000)
    }
    
    # Get price range for this city (fallback to DEFAULT)
    min_price, max_price = price_ranges.get(city, price_ranges['DEFAULT'])
    
    # Adjust prices for area premiums within cities
    area_lower = area.lower()
    premium_keywords = ['west end', 'mayfair', 'knightsbridge', 'chelsea', 'kensington', 
                        'tribeca', 'soho', 'upper', 'downtown', 'centrum', 'centre', 
                        'beach', 'marina', 'bay', 'waterfront', 'park', 'hill']
    
    is_premium_area = any(keyword in area_lower for keyword in premium_keywords)
    
    if is_premium_area:
        # Premium areas: increase prices by 30-50%
        min_price = int(min_price * 1.3)
        max_price = int(max_price * 1.5)
    
    # UK-specific property types vs Global
    uk_cities = ['London', 'Edinburgh', 'Manchester', 'Birmingham', 'Bristol', 'Glasgow', 
                 'Leeds', 'Oxford', 'Cambridge', 'Bath', 'Liverpool', 'Newcastle', 
                 'Cardiff', 'Belfast']
    
    if city in uk_cities:
        property_types = ['Flat', 'Terraced House', 'Semi-Detached House', 'Detached House', 'Penthouse', 'Mews']
        type_weights = [35, 20, 15, 15, 10, 5]
    else:
        # Global property types
        property_types = ['Apartment', 'Condo', 'Villa', 'Penthouse', 'Townhouse', 'Estate']
        type_weights = [35, 20, 15, 15, 10, 5]
    
    # Generate street names appropriate to region
    if city in uk_cities:
        street_suffixes = ['Street', 'Road', 'Avenue', 'Square', 'Gardens', 'Place', 'Lane', 'Mews', 'Terrace', 'Close']
        street_prefixes = ['King', 'Queen', 'Victoria', 'Albert', 'George', 'Elizabeth', 'Park', 'High', 'Station', 
                          'Church', 'Castle', 'Royal', 'Prince', 'Duke', 'Market', 'Bridge', 'Hill', 'Grove']
    elif city in ['New York', 'Los Angeles', 'Chicago', 'Boston', 'Miami', 'San Francisco', 'Seattle', 
                  'Austin', 'Denver', 'Nashville', 'Dallas', 'Houston', 'Phoenix', 'Atlanta']:
        # US style
        street_suffixes = ['Street', 'Avenue', 'Boulevard', 'Drive', 'Way', 'Court', 'Place', 'Lane', 'Road', 'Circle']
        street_prefixes = ['Main', 'Park', 'Ocean', 'Lake', 'Hill', 'Valley', 'Forest', 'River', 'Sunset', 
                          'Harbor', 'Maple', 'Oak', 'Pine', 'Cedar', 'Washington', 'Madison', 'Lincoln']
    elif city in ['Paris', 'Monaco', 'Geneva', 'Zurich', 'Amsterdam', 'Berlin', 'Madrid', 'Barcelona', 
                  'Rome', 'Vienna', 'Milan', 'Brussels', 'Lisbon']:
        # European style
        street_suffixes = ['Street', 'Avenue', 'Boulevard', 'Place', 'Square', 'Way', 'Road', 'Alley']
        street_prefixes = ['Grand', 'Royal', 'Central', 'Republic', 'Liberty', 'Victory', 'Peace', 'Unity',
                          'Triumph', 'Cathedral', 'Palace', 'Museum', 'Opera', 'Theatre', 'Garden']
    else:
        # Generic international
        street_suffixes = ['Street', 'Avenue', 'Boulevard', 'Drive', 'Way', 'Road', 'Place']
        street_prefixes = ['Main', 'Central', 'Park', 'Lake', 'Ocean', 'Harbor', 'Bay', 'Hill', 
                          'Valley', 'River', 'Garden', 'Market', 'Palace', 'Royal']
    
    # Agency names (mix of global and local)
    if city in uk_cities:
        agencies = ['Knight Frank', 'Savills', 'Hamptons', 'Strutt & Parker', 'Chestertons', 
                   'Foxtons', 'Marsh & Parsons', 'John D Wood & Co', 'Douglas & Gordon', 
                   'Winkworth', 'Private']
    elif city in ['New York', 'Los Angeles', 'Miami', 'Chicago', 'San Francisco']:
        agencies = ['Sotheby\'s International Realty', 'Douglas Elliman', 'Compass', 
                   'Corcoran', 'Coldwell Banker', 'Keller Williams', 'RE/MAX', 
                   'The Agency', 'Engel & Völkers', 'Private']
    else:
        agencies = ['Sotheby\'s International Realty', 'Christie\'s Real Estate', 
                   'Engel & Völkers', 'Knight Frank', 'Savills', 'CBRE', 
                   'Cushman & Wakefield', 'JLL', 'Colliers', 'Private']
    
    properties = []
    
    # Generate varied submarkets for testing
    submarkets = [area] * 10 + [f"{area} North", f"{area} South", f"{area} East"] * 5
    
    for i in range(count):
        # Random but realistic data
        prop_type = random.choices(property_types, weights=type_weights)[0]
        
        # Sqft varies by property type
        if prop_type in ['Flat', 'Apartment', 'Condo']:
            sqft = random.randint(500, 2500)
        elif prop_type in ['Penthouse']:
            sqft = random.randint(2000, 8000)
        elif prop_type in ['Villa', 'Estate', 'Detached House']:
            sqft = random.randint(3000, 15000)
        else:
            sqft = random.randint(1000, 4000)
        
        price = random.randint(min_price, max_price)
        
        # Bedrooms based on sqft
        if sqft < 800:
            bedrooms = random.randint(1, 2)
        elif sqft < 1500:
            bedrooms = random.randint(2, 3)
        elif sqft < 3000:
            bedrooms = random.randint(3, 4)
        else:
            bedrooms = random.randint(4, 7)
        
        bathrooms = random.randint(1, bedrooms)
        days_listed = random.randint(14, 90)
        
        # Generate address
        street_prefix = random.choice(street_prefixes)
        street_suffix = random.choice(street_suffixes)
        street_number = random.randint(1, 500)
        address = f"{street_number} {street_prefix} {street_suffix}, {area}"
        
        properties.append({
            'source': 'Demo Data (APIs unavailable)',
            'listing_id': f"DEMO-{area[:3].upper()}-{i+1:04d}",
            'address': address,
            'area': area,
            'city': city,
            'submarket': random.choice(submarkets),
            'district': random.choice(submarkets),
            'price': price,
            'beds': bedrooms,
            'baths': bathrooms,
            'sqft': sqft,
            'price_per_sqft': round(price / sqft, 2),
            'property_type': prop_type,
            'agent': random.choice(agencies),
            'url': 'https://example.com',
            'description': f"Stunning {bedrooms}-bedroom {prop_type.lower()} in prime {area} location with modern finishes and exceptional views.",
            'image_url': '',
            'listed_date': datetime.now().strftime('%Y-%m-%d'),
            'days_listed': days_listed,
            'days_on_market': days_listed,
            'coordinates': {
                'lat': 51.5074 + random.uniform(-0.5, 0.5),  # Approximate
                'lng': -0.1278 + random.uniform(-0.5, 0.5)
            }
        })
    
    print(f"      ✅ Generated {len(properties)} demo properties")
    print(f"      Price range: £{min_price:,} - £{max_price:,}")
    print(f"      Property types: {', '.join(property_types)}")
    print(f"      Agencies: {', '.join(agencies[:3])} (+{len(agencies)-3} more)")
    
    return properties


# ============================================================================
# UK REAL ESTATE - MASTER COLLECTOR
# ============================================================================

def collect_uk_real_estate(area, city="London", max_properties=40):
    """
    Master UK real estate collector with intelligent fallbacks.
    NEVER FAILS - Always returns usable data.
    NOW SUPPORTS ANY CITY GLOBALLY
    
    Strategy:
    1. Try Rightmove API (best data quality)
    2. Fallback to Outscraper (good coverage)
    3. Generate demo data (ensures system never crashes)
    """
    
    print(f"\n🏠 COLLECTING REAL ESTATE DATA")
    print(f"   Target Area: {area}")
    print(f"   Target City: {city}")
    print(f"   Target Count: {max_properties} properties")
    print(f"   Strategy: Multi-source with fallbacks")
    
    # Try primary source (only for London/UK for now)
    if city == "London" or city in ["Edinburgh", "Manchester", "Birmingham", "Bristol"]:
        properties = collect_rightmove_data(area, max_properties)
        
        if properties and len(properties) >= 5:
            print(f"\n   ✅ PRIMARY SOURCE SUCCESS: {len(properties)} properties from Rightmove")
            return properties
    
    # Try fallback source
    print(f"\n   ⚠️  Primary source insufficient, trying fallback...")
    properties = collect_outscraper_data(area, city, max_properties)
    
    if properties and len(properties) >= 5:
        print(f"\n   ✅ FALLBACK SUCCESS: {len(properties)} properties from Outscraper")
        return properties
    
    # Last resort: demo data - CITY-AWARE
    print(f"\n   ⚠️  All API sources failed or returned insufficient data")
    properties = generate_demo_properties(area, max_properties, city)
    
    print(f"\n   ✅ DEMO DATA READY: {len(properties)} properties generated")
    return properties


# ============================================================================
# MIAMI REAL ESTATE
# ============================================================================

def collect_miami_real_estate(area, city="Miami", max_properties=40):
    """Collect Miami luxury real estate data"""
    
    print(f"\n🏠 COLLECTING MIAMI REAL ESTATE DATA")
    print(f"   Area: {area}")
    print(f"   City: {city}")
    
    if not REALTY_US_API_KEY:
        print("   ⚠️  REALTY_US_API_KEY not configured - generating demo data")
        return generate_demo_properties(area, max_properties, city)
    
    try:
        url = "https://realty-mole-property-api.p.rapidapi.com/saleListings"
        
        headers = {
            "X-RapidAPI-Key": REALTY_US_API_KEY,
            "X-RapidAPI-Host": "realty-mole-property-api.p.rapidapi.com"
        }
        
        querystring = {
            "city": city,
            "state": "FL",
            "limit": str(max_properties)
        }
        
        if area and area.lower() != city.lower():
            querystring["neighborhood"] = area
        
        response = requests.get(url, headers=headers, params=querystring, timeout=20)
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code}")
        
        listings = response.json()
        
        properties = []
        for listing in listings[:max_properties]:
            price = listing.get('price', 0)
            sqft = listing.get('squareFootage', 2000)
            listed_date = listing.get('listDate', datetime.now().strftime('%Y-%m-%d'))
            days_listed = calculate_days_on_market(listed_date)
            
            properties.append({
                'source': 'Realty Mole',
                'listing_id': listing.get('id', ''),
                'address': listing.get('formattedAddress', ''),
                'area': area,
                'city': city,
                'submarket': area,
                'district': area,
                'price': price,
                'beds': listing.get('bedrooms', 3),
                'baths': listing.get('bathrooms', 2),
                'sqft': sqft,
                'price_per_sqft': round(price / sqft, 2) if sqft else 0,
                'property_type': listing.get('propertyType', ''),
                'agent': 'Miami Realty',
                'url': listing.get('url', ''),
                'description': '',
                'image_url': '',
                'listed_date': listed_date,
                'days_listed': days_listed,
                'days_on_market': days_listed
            })
        
        print(f"   ✅ Collected {len(properties)} properties")
        return properties
        
    except Exception as e:
        print(f"   ⚠️  Miami API failed: {str(e)} - using demo data")
        return generate_demo_properties(area, max_properties, city)


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def collect_market_data(vertical, area, city, vertical_config_json='{}'):
    """
    Main data collection orchestrator.
    NEVER FAILS - Always returns usable data.
    SUPPORTS ANY CITY GLOBALLY
    """
    
    print("\n" + "="*70)
    print("VOXMILL DATA COLLECTION ENGINE")
    print("="*70)
    print(f"Vertical: {vertical}")
    print(f"Area: {area}")
    print(f"City: {city}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Parse vertical config
    import json as json_lib
    try:
        vertical_config = json_lib.loads(vertical_config_json)
        print(f"✅ Vertical Config: {vertical_config.get('name', 'Unknown')}")
    except:
        vertical_config = {'type': 'real_estate', 'name': 'Real Estate'}
        print(f"⚠️  Using default vertical config")
    
    data = {
        'metadata': {
            'vertical': vertical_config,  # Store full config object
            'area': area,
            'city': city,
            'timestamp': datetime.now().isoformat(),
            'data_source': 'Multi-source with intelligent fallbacks'
        },
        'raw_data': {}
    }
    
    try:
        if vertical == 'uk-real-estate':
            properties = collect_uk_real_estate(area, city)
            data['raw_data']['properties'] = properties
            data['metadata']['property_count'] = len(properties)
            
        elif vertical == 'miami-real-estate':
            properties = collect_miami_real_estate(area, city)
            data['raw_data']['properties'] = properties
            data['metadata']['property_count'] = len(properties)
            
        else:
            raise Exception(f"Vertical '{vertical}' not supported")
        
        # Save to file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        record_count = len(data['raw_data'].get('properties', []))
        
        print(f"\n" + "="*70)
        print("✅ DATA COLLECTION COMPLETE")
        print("="*70)
        print(f"Output File: {OUTPUT_FILE}")
        print(f"Records Collected: {record_count} properties")
        print(f"Data Source: {properties[0]['source'] if properties else 'None'}")
        print(f"Vertical: {vertical_config.get('name', 'Unknown')}")
        print("="*70)
        
        return OUTPUT_FILE
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python data_collector.py <vertical> <area> <city> [vertical_config_json]")
        print("Example: python data_collector.py uk-real-estate Mayfair London")
        print("Example: python data_collector.py uk-real-estate \"West End\" Edinburgh")
        sys.exit(1)
    
    vertical = sys.argv[1]
    area = sys.argv[2]
    city = sys.argv[3]
    vertical_config_json = sys.argv[4] if len(sys.argv) > 4 else '{}'
    
    try:
        result = collect_market_data(vertical, area, city, vertical_config_json)
        print(f"\n✅ SUCCESS: Data saved to {result}")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

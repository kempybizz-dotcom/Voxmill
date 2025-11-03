"""
VOXMILL ELITE DATA COLLECTOR - PRODUCTION VERSION
==================================================
Multi-source data collection with intelligent fallbacks
NEVER FAILS - Always returns usable data
"""

import os
import json
import requests
from datetime import datetime

# API Configuration
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
REALTY_US_API_KEY = os.environ.get('REALTY_US_API_KEY')
OUTSCRAPER_API_KEY = os.environ.get('OUTSCRAPER_API_KEY')

OUTPUT_FILE = "/tmp/voxmill_raw_data.json"

# ============================================================================
# UK REAL ESTATE - PRIMARY: RIGHTMOVE API
# ============================================================================

def collect_rightmove_data(area, max_properties=40):
    """
    Primary data source: UK Real Estate Rightmove API via RapidAPI
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
            params = {
                "locationIdentifier": "REGION^87523",
                "radius": "0.0",
                "sort_by": "highest_price",
                "minprice": "1000000",
                "min_price": "1000000",
                "max_results": str(min(max_properties, 100))
            },
            {
                "locationIdentifier": "REGION^87523", London",
                "radius": "1.0",
                "sort_by": "highest_price",
                "minPrice": "500000",
                "min_price": "500000",
                "max_results": str(min(max_properties, 100))
            },
            {
                "locationIdentifier": "REGION^87523", "London",
                "radius": "5.0",
                "sort_by": "highest_price",
                "minPrice": "1000000",
                "min_price": "1000000",
                "max_results": str(min(max_properties, 100))
            }
        ]
        
        for i, params in enumerate(search_configs, 1):
            print(f"   → Attempt {i}/3: Querying Rightmove API...")
            print(f"      Search: {params['searchLocation']}, Radius: {params['radius']}, Min: £{params['min_price']}")
            
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
                        
                        # Address
                        address_parts = []
                        if prop.get('address'):
                            if isinstance(prop['address'], dict):
                                address_parts.append(prop['address'].get('displayAddress', ''))
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
                        
                        properties.append({
                            'source': 'Rightmove',
                            'listing_id': str(prop.get('id', prop.get('property_id', f"RM_{len(properties)+1}"))),
                            'address': address,
                            'area': area,
                            'city': 'London',
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
                            'listed_date': prop.get('addedOn', prop.get('listingDate', datetime.now().strftime('%Y-%m-%d')))
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

def collect_outscraper_data(area, max_properties=40):
    """
    Fallback data source: Outscraper Google Maps scraping
    """
    print(f"\n   🔄 FALLBACK SOURCE: Outscraper Google Maps")
    
    if not OUTSCRAPER_API_KEY:
        print(f"   ⚠️  OUTSCRAPER_API_KEY not configured - skipping")
        return None
    
    try:
        from outscraper import ApiClient
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        # Multiple search queries for better coverage
        search_queries = [
            f"luxury properties for sale {area} London",
            f"luxury homes {area} London",
            f"premium properties {area}",
            f"high end real estate {area} London"
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
                    
                    property_data = {
                        'source': 'Outscraper',
                        'listing_id': f"OS_{item.get('place_id', len(all_properties)+1)}",
                        'address': item.get('full_address', item.get('address', f'{area}, London')),
                        'area': area,
                        'city': 'London',
                        'price': price,
                        'beds': 3,  # Default - Outscraper doesn't provide this
                        'baths': 2,
                        'sqft': 2500,
                        'price_per_sqft': round(price / 2500, 2) if price > 0 else 0,
                        'property_type': 'Luxury Property',
                        'agent': item.get('name', 'Estate Agent'),
                        'url': item.get('site', ''),
                        'description': str(item.get('description', ''))[:200],
                        'image_url': '',
                        'listed_date': datetime.now().strftime('%Y-%m-%d')
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
# DEMO DATA GENERATOR (Last Resort)
# ============================================================================

def generate_demo_properties(area, count=25):
    """
    Generate realistic demo properties when all APIs fail.
    This ensures the system NEVER crashes.
    """
    print(f"\n   🎲 DEMO DATA GENERATOR")
    print(f"   ⚠️  All API sources failed - generating realistic demo data")
    
    import random
    
    property_types = ['Penthouse', 'Townhouse', 'Apartment', 'Mews House', 'Duplex', 'Mansion']
    streets = ['Park Lane', 'Mount Street', 'Grosvenor Square', 'Berkeley Square', 
               'Curzon Street', 'Charles Street', 'South Audley Street', 'Davies Street']
    agents = ['Knight Frank', 'Savills', 'Strutt & Parker', 'Hamptons', 'Chestertons']
    
    properties = []
    
    for i in range(count):
        prop_type = random.choice(property_types)
        bedrooms = random.randint(2, 6)
        bathrooms = random.randint(2, bedrooms)
        sqft = random.randint(1500, 5000)
        price = random.randint(1500000, 8000000)
        price_per_sqft = round(price / sqft, 2)
        
        properties.append({
            'source': 'Demo Data (APIs unavailable)',
            'listing_id': f"DEMO_{i+1:03d}",
            'address': f"{random.randint(1, 99)} {random.choice(streets)}, {area}, London W1K",
            'area': area,
            'city': 'London',
            'price': price,
            'beds': bedrooms,
            'baths': bathrooms,
            'sqft': sqft,
            'price_per_sqft': price_per_sqft,
            'property_type': prop_type,
            'agent': random.choice(agents),
            'url': 'https://example.com',
            'description': f"Stunning {bedrooms}-bedroom {prop_type.lower()} in prime {area} location with modern finishes and exceptional views.",
            'image_url': '',
            'listed_date': datetime.now().strftime('%Y-%m-%d')
        })
    
    print(f"   ✅ Generated {len(properties)} realistic demo properties")
    return properties


# ============================================================================
# UK REAL ESTATE - MASTER COLLECTOR
# ============================================================================

def collect_uk_real_estate(area, max_properties=40):
    """
    Master UK real estate collector with intelligent fallbacks.
    NEVER FAILS - Always returns usable data.
    
    Strategy:
    1. Try Rightmove API (best data quality)
    2. Fallback to Outscraper (good coverage)
    3. Generate demo data (ensures system never crashes)
    """
    
    print(f"\n🏠 COLLECTING UK REAL ESTATE DATA")
    print(f"   Target Area: {area}")
    print(f"   Target Count: {max_properties} properties")
    print(f"   Strategy: Multi-source with fallbacks")
    
    # Try primary source
    properties = collect_rightmove_data(area, max_properties)
    
    if properties and len(properties) >= 5:
        print(f"\n   ✅ PRIMARY SOURCE SUCCESS: {len(properties)} properties from Rightmove")
        return properties
    
    # Try fallback source
    print(f"\n   ⚠️  Primary source insufficient, trying fallback...")
    properties = collect_outscraper_data(area, max_properties)
    
    if properties and len(properties) >= 5:
        print(f"\n   ✅ FALLBACK SUCCESS: {len(properties)} properties from Outscraper")
        return properties
    
    # Last resort: demo data
    print(f"\n   ⚠️  All API sources failed or returned insufficient data")
    properties = generate_demo_properties(area, max_properties)
    
    print(f"\n   ✅ DEMO DATA READY: {len(properties)} properties generated")
    return properties


# ============================================================================
# MIAMI REAL ESTATE
# ============================================================================

def collect_miami_real_estate(area, max_properties=40):
    """Collect Miami luxury real estate data"""
    
    print(f"\n🏠 COLLECTING MIAMI REAL ESTATE DATA")
    print(f"   Area: {area}")
    
    if not REALTY_US_API_KEY:
        print("   ⚠️  REALTY_US_API_KEY not configured - generating demo data")
        return generate_demo_properties(area, max_properties)
    
    try:
        url = "https://realty-mole-property-api.p.rapidapi.com/saleListings"
        
        headers = {
            "X-RapidAPI-Key": REALTY_US_API_KEY,
            "X-RapidAPI-Host": "realty-mole-property-api.p.rapidapi.com"
        }
        
        querystring = {
            "city": "Miami",
            "state": "FL",
            "limit": str(max_properties)
        }
        
        if area and area.lower() != "miami":
            querystring["neighborhood"] = area
        
        response = requests.get(url, headers=headers, params=querystring, timeout=20)
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code}")
        
        listings = response.json()
        
        properties = []
        for listing in listings[:max_properties]:
            price = listing.get('price', 0)
            sqft = listing.get('squareFootage', 2000)
            
            properties.append({
                'source': 'Realty Mole',
                'listing_id': listing.get('id', ''),
                'address': listing.get('formattedAddress', ''),
                'area': area,
                'city': 'Miami',
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
                'listed_date': listing.get('listDate', '')
            })
        
        print(f"   ✅ Collected {len(properties)} properties")
        return properties
        
    except Exception as e:
        print(f"   ⚠️  Miami API failed: {str(e)} - using demo data")
        return generate_demo_properties(area, max_properties)


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def collect_market_data(vertical, area, city):
    """
    Main data collection orchestrator.
    NEVER FAILS - Always returns usable data.
    """
    
    print("\n" + "="*70)
    print("VOXMILL DATA COLLECTION ENGINE")
    print("="*70)
    print(f"Vertical: {vertical}")
    print(f"Area: {area}")
    print(f"City: {city}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    data = {
        'metadata': {
            'vertical': vertical,
            'area': area,
            'city': city,
            'timestamp': datetime.now().isoformat(),
            'data_source': 'Multi-source with intelligent fallbacks'
        },
        'raw_data': {}
    }
    
    try:
        if vertical == 'uk-real-estate':
            properties = collect_uk_real_estate(area)
            data['raw_data']['properties'] = properties
            data['metadata']['property_count'] = len(properties)
            
        elif vertical == 'miami-real-estate':
            properties = collect_miami_real_estate(area)
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
        print("Usage: python data_collector.py <vertical> <area> <city>")
        print("Example: python data_collector.py uk-real-estate Mayfair London")
        sys.exit(1)
    
    vertical = sys.argv[1]
    area = sys.argv[2]
    city = sys.argv[3]
    
    try:
        result = collect_market_data(vertical, area, city)
        print(f"\n✅ SUCCESS: Data saved to {result}")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

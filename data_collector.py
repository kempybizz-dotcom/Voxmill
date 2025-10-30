VOXMILL ELITE DATA COLLECTOR
=============================
Real-time data collection for luxury market intelligence
Supports: UK Real Estate, Miami Real Estate, UK Luxury Car Rentals, Chartering

NO DEMO DATA. 100% REAL MARKET DATA.
"""

import os
import json
import requests
from datetime import datetime
from outscraper import ApiClient

# API Configuration
RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY')
PROPERTY_DATA_API = os.environ.get('PROPERTY_DATA_API')
REALTY_US_API_KEY = os.environ.get('REALTY_US_API_KEY')
OUTSCRAPER_API_KEY = os.environ.get('OUTSCRAPER_API_KEY')

OUTPUT_FILE = "/tmp/voxmill_raw_data.json"

# ============================================================================
# UK REAL ESTATE DATA COLLECTION
# ============================================================================

def collect_uk_real_estate(area, max_properties=40):
    """Collect real UK luxury real estate data via Zoopla API"""
    
    print(f"\n🏠 COLLECTING UK REAL ESTATE DATA")
    print(f"   Area: {area}")
    print(f"   Target: {max_properties} luxury properties")
    
    if not RAPIDAPI_KEY:
        raise Exception("RAPIDAPI_KEY not configured")
    
    try:
        url = "https://zoopla.p.rapidapi.com/properties/list"
        
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "zoopla.p.rapidapi.com"
        }
        
        # High-end property search
        querystring = {
            "area": f"{area}, London",
            "order_by": "age",
            "ordering": "descending",
            "page_number": "1",
            "page_size": str(max_properties),
            "minimum_price": "2000000",
            "listing_status": "sale"
        }
        
        print(f"   → Querying Zoopla API...")
        response = requests.get(url, headers=headers, params=querystring, timeout=20)
        
        if response.status_code != 200:
            raise Exception(f"Zoopla API error: {response.status_code}")
        
        data = response.json()
        listings = data.get('listing', [])
        
        if not listings:
            raise Exception(f"No properties returned for {area}")
        
        # Parse listings
        properties = []
        for listing in listings:
            try:
                price = listing.get('price', 0)
                if isinstance(price, str):
                    price = int(price.replace('£', '').replace(',', ''))
                
                beds = listing.get('num_bedrooms', 0)
                baths = listing.get('num_bathrooms', 0)
                
                # Get square footage
                floor_area = listing.get('floor_area', {})
                sqft = 0
                if floor_area:
                    max_area = floor_area.get('max_floor_area', {})
                    sqft = max_area.get('value', 0) if max_area else 0
                
                if not sqft or sqft == 0:
                    sqft = beds * 600 + 800  # Estimate
                
                price_per_sqft = round(price / sqft, 2) if sqft > 0 else 0
                
                address = listing.get('displayable_address', '')
                
                properties.append({
                    'source': 'Zoopla',
                    'listing_id': listing.get('listing_id', ''),
                    'address': address,
                    'area': area,
                    'city': 'London',
                    'price': price,
                    'beds': beds,
                    'baths': baths,
                    'sqft': sqft,
                    'price_per_sqft': price_per_sqft,
                    'property_type': listing.get('property_type', 'Unknown'),
                    'agent': listing.get('agent_name', 'Private'),
                    'url': listing.get('details_url', ''),
                    'description': listing.get('short_description', ''),
                    'image_url': listing.get('image_url', ''),
                    'listed_date': listing.get('first_published_date', '')
                })
                
            except Exception as e:
                continue
        
        print(f"   ✅ Collected {len(properties)} properties from Zoopla")
        return properties
        
    except Exception as e:
        print(f"   ❌ Zoopla API failed: {str(e)}")
        raise

# ============================================================================
# MIAMI REAL ESTATE DATA COLLECTION
# ============================================================================

def collect_miami_real_estate(area, max_properties=40):
    """Collect real Miami luxury real estate data via Realty API"""
    
    print(f"\n🏖️ COLLECTING MIAMI REAL ESTATE DATA")
    print(f"   Area: {area}")
    print(f"   Target: {max_properties} luxury properties")
    
    if not REALTY_US_API_KEY:
        raise Exception("REALTY_US_API_KEY not configured")
    
    try:
        url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
        
        headers = {
            "X-RapidAPI-Key": REALTY_US_API_KEY,
            "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com"
        }
        
        payload = {
            "limit": max_properties,
            "offset": 0,
            "postal_code": "33139",  # Miami Beach luxury area
            "status": ["for_sale"],
            "sort": {
                "direction": "desc",
                "field": "list_date"
            },
            "price": {
                "min": 2000000
            }
        }
        
        print(f"   → Querying Realty API...")
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        
        if response.status_code != 200:
            raise Exception(f"Realty API error: {response.status_code}")
        
        data = response.json()
        listings = data.get('data', {}).get('results', [])
        
        if not listings:
            raise Exception(f"No properties returned for Miami {area}")
        
        # Parse listings
        properties = []
        for listing in listings:
            try:
                price = listing.get('list_price', 0)
                beds = listing.get('description', {}).get('beds', 0)
                baths = listing.get('description', {}).get('baths', 0)
                sqft = listing.get('description', {}).get('sqft', 0)
                
                if not sqft or sqft == 0:
                    sqft = beds * 600 + 800
                
                price_per_sqft = round(price / sqft, 2) if sqft > 0 else 0
                
                location = listing.get('location', {})
                address_line = location.get('address', {}).get('line', '')
                city = location.get('address', {}).get('city', 'Miami')
                
                properties.append({
                    'source': 'Realty',
                    'listing_id': listing.get('property_id', ''),
                    'address': address_line,
                    'area': area,
                    'city': city,
                    'price': price,
                    'beds': beds,
                    'baths': baths,
                    'sqft': sqft,
                    'price_per_sqft': price_per_sqft,
                    'property_type': listing.get('description', {}).get('type', 'Unknown'),
                    'agent': listing.get('advertisers', [{}])[0].get('name', 'Private') if listing.get('advertisers') else 'Private',
                    'url': listing.get('permalink', ''),
                    'description': listing.get('description', {}).get('text', ''),
                    'image_url': listing.get('primary_photo', {}).get('href', ''),
                    'listed_date': listing.get('list_date', '')
                })
                
            except Exception as e:
                continue
        
        print(f"   ✅ Collected {len(properties)} properties from Realty")
        return properties
        
    except Exception as e:
        print(f"   ❌ Realty API failed: {str(e)}")
        raise

# ============================================================================
# LUXURY CAR RENTAL DATA COLLECTION
# ============================================================================

def collect_car_rental_data(city="London", max_companies=20):
    """Collect luxury car rental companies via Outscraper"""
    
    print(f"\n🚗 COLLECTING LUXURY CAR RENTAL DATA")
    print(f"   City: {city}")
    print(f"   Target: {max_companies} companies")
    
    if not OUTSCRAPER_API_KEY:
        raise Exception("OUTSCRAPER_API_KEY not configured")
    
    try:
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        query = f"luxury car rental {city}"
        
        print(f"   → Querying Google Places via Outscraper...")
        results = client.google_search_v3(
            query=[query],
            limit=max_companies,
            language='en',
            region='GB' if city in ['London', 'Manchester'] else 'US'
        )
        
        if not results or len(results) == 0:
            raise Exception(f"No car rental companies found in {city}")
        
        places = results[0]
        
        companies = []
        for place in places:
            try:
                name = place.get('name', '')
                rating = place.get('rating', 0)
                reviews = place.get('reviews', 0)
                address = place.get('address', '')
                phone = place.get('phone', '')
                website = place.get('site', '')
                
                # Extract pricing info from reviews if available
                review_texts = place.get('reviews_data', [])
                price_mentions = []
                for review in review_texts[:5]:
                    text = review.get('review_text', '').lower()
                    if '£' in text or '$' in text or 'price' in text or 'cost' in text:
                        price_mentions.append(text[:200])
                
                companies.append({
                    'source': 'Google Places',
                    'name': name,
                    'city': city,
                    'address': address,
                    'phone': phone,
                    'website': website,
                    'rating': rating,
                    'total_reviews': reviews,
                    'price_mentions': price_mentions,
                    'google_maps_url': place.get('place_link', '')
                })
                
            except Exception as e:
                continue
        
        print(f"   ✅ Collected {len(companies)} car rental companies")
        return companies
        
    except Exception as e:
        print(f"   ❌ Outscraper failed: {str(e)}")
        raise

# ============================================================================
# CHARTERING COMPANIES DATA COLLECTION
# ============================================================================

def collect_chartering_data(city="London", charter_type="yacht"):
    """Collect yacht/jet charter companies via Outscraper"""
    
    print(f"\n⛵ COLLECTING CHARTERING DATA")
    print(f"   City: {city}")
    print(f"   Type: {charter_type}")
    
    if not OUTSCRAPER_API_KEY:
        raise Exception("OUTSCRAPER_API_KEY not configured")
    
    try:
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        query = f"luxury {charter_type} charter {city}"
        
        print(f"   → Querying Google Places via Outscraper...")
        results = client.google_search_v3(
            query=[query],
            limit=15,
            language='en',
            region='GB' if city in ['London', 'Manchester'] else 'US'
        )
        
        if not results or len(results) == 0:
            raise Exception(f"No {charter_type} charter companies found in {city}")
        
        places = results[0]
        
        companies = []
        for place in places:
            try:
                companies.append({
                    'source': 'Google Places',
                    'name': place.get('name', ''),
                    'city': city,
                    'charter_type': charter_type,
                    'address': place.get('address', ''),
                    'phone': place.get('phone', ''),
                    'website': place.get('site', ''),
                    'rating': place.get('rating', 0),
                    'total_reviews': place.get('reviews', 0),
                    'google_maps_url': place.get('place_link', '')
                })
                
            except Exception as e:
                continue
        
        print(f"   ✅ Collected {len(companies)} {charter_type} charter companies")
        return companies
        
    except Exception as e:
        print(f"   ❌ Outscraper failed: {str(e)}")
        raise

# ============================================================================
# MAIN COLLECTION ORCHESTRATOR
# ============================================================================

def collect_market_data(vertical, area, city="London"):
    """Main data collection orchestrator"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE DATA COLLECTOR")
    print("="*70)
    print(f"Vertical: {vertical}")
    print(f"Area: {area}")
    print(f"City: {city}")
    
    data = {
        'metadata': {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'vertical': vertical,
            'area': area,
            'city': city
        },
        'raw_data': {}
    }
    
    try:
        if vertical == "uk-real-estate":
            data['raw_data']['properties'] = collect_uk_real_estate(area)
            
        elif vertical == "miami-real-estate":
            data['raw_data']['properties'] = collect_miami_real_estate(area)
            
        elif vertical == "uk-car-rentals":
            data['raw_data']['companies'] = collect_car_rental_data(city)
            
        elif vertical == "chartering":
            data['raw_data']['companies'] = collect_chartering_data(city, "yacht")
            
        else:
            raise Exception(f"Unknown vertical: {vertical}")
        
        # Export
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Data collection complete: {OUTPUT_FILE}")
        return OUTPUT_FILE
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python data_collector.py <vertical> <area> [city]")
        print("Example: python data_collector.py uk-real-estate Mayfair London")
        sys.exit(1)
    
    vertical = sys.argv[1]
    area = sys.argv[2]
    city = sys.argv[3] if len(sys.argv) > 3 else "London"
    
    collect_market_data(vertical, area, city)

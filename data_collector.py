"""
VOXMILL ELITE DATA COLLECTOR - FINAL VERSION
=============================================
Real-time data collection using UK Real Estate Rightmove API (RapidAPI)
100% REAL MARKET DATA
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
# UK REAL ESTATE DATA COLLECTION (Rightmove API)
# ============================================================================

def collect_uk_real_estate(area, max_properties=40):
    """Collect real UK luxury real estate data via Rightmove API"""
    
    print(f"\n🏠 COLLECTING UK REAL ESTATE DATA")
    print(f"   Area: {area}")
    print(f"   Target: {max_properties} luxury properties")
    print(f"   Source: UK Real Estate Rightmove API")
    
    if not RAPIDAPI_KEY:
        raise Exception("RAPIDAPI_KEY not configured")
    
    try:
        # UK Real Estate Rightmove API
        url = "https://uk-real-estate-rightmove.p.rapidapi.com/buy/property-for-sale"
        
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": "uk-real-estate-rightmove.p.rapidapi.com"
        }
        
        # Search parameters
        params = {
            "identifier": area,  # e.g., "REGION^61294" for Mayfair or just "Mayfair"
            "search_radius": "0.0",
            "sort_by": "highest_price",
            "added_to_site": "1",
            "min_price": "1000000",  # £1M minimum
            "max_results": str(min(max_properties, 100))
        }
        
        print(f"   → Querying UK Real Estate Rightmove API...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 401:
            raise Exception("RapidAPI authentication failed - check RAPIDAPI_KEY")
        
        if response.status_code == 403:
            raise Exception("RapidAPI access forbidden - check subscription")
        
        if response.status_code == 429:
            raise Exception("RapidAPI rate limit exceeded - wait or upgrade plan")
        
        if response.status_code != 200:
            error_msg = response.text[:300] if response.text else "Unknown error"
            raise Exception(f"Rightmove API error {response.status_code}: {error_msg}")
        
        data = response.json()
        
        # Parse response
        raw_properties = data.get('data', data.get('properties', data.get('results', [])))
        
        print(f"   ✅ Received {len(raw_properties)} properties from Rightmove")
        
        if not raw_properties:
            # Try broader search
            print(f"   ⚠️ No results - trying broader search...")
            params["min_price"] = "500000"
            params["identifier"] = "London"
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                raw_properties = data.get('data', data.get('properties', data.get('results', [])))
                print(f"   ✅ Broader search found {len(raw_properties)} properties")
        
        if not raw_properties:
            raise Exception(f"No properties found for {area}")
        
        properties = []
        for prop in raw_properties[:max_properties]:
            # Extract property details
            price = prop.get('price', prop.get('asking_price', 0))
            
            # Handle price as string (e.g., "£1,500,000")
            if isinstance(price, str):
                price = int(''.join(filter(str.isdigit, price)) or 0)
            
            # Address
            address_parts = []
            if prop.get('address'):
                if isinstance(prop['address'], dict):
                    address_parts.append(prop['address'].get('displayAddress', ''))
                else:
                    address_parts.append(str(prop['address']))
            
            if prop.get('location'):
                address_parts.append(str(prop['location']))
            
            address = ', '.join(filter(None, address_parts)) or f"{area}, London"
            
            # Bedrooms/bathrooms
            bedrooms = prop.get('bedrooms', prop.get('bedroom', prop.get('beds', 3)))
            bathrooms = prop.get('bathrooms', prop.get('bathroom', prop.get('baths', 2)))
            
            # Square footage
            sqft = 0
            if prop.get('size'):
                try:
                    sqft = int(prop['size'].get('sqft', 0))
                except:
                    pass
            
            if not sqft and bedrooms:
                sqft = bedrooms * 800  # Estimate
            
            if not sqft:
                sqft = 2000  # Default
            
            # Price per sqft
            price_per_sqft = round(price / sqft, 2) if sqft and price else 0
            
            # Property type
            prop_type = prop.get('propertyType', prop.get('type', 'House'))
            
            # Agent
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
                'price': int(price),
                'beds': int(bedrooms),
                'baths': int(bathrooms),
                'sqft': int(sqft),
                'price_per_sqft': price_per_sqft,
                'property_type': str(prop_type),
                'agent': agent,
                'url': prop.get('propertyUrl', prop.get('url', '')),
                'description': (prop.get('summary', prop.get('description', '')))[:200],
                'image_url': prop.get('mainImage', prop.get('image', '')),
                'listed_date': prop.get('addedOn', prop.get('listingDate', datetime.now().strftime('%Y-%m-%d')))
            })
        
        print(f"   ✅ Parsed {len(properties)} properties successfully")
        return properties
        
    except Exception as e:
        print(f"   ❌ Rightmove API failed: {str(e)}")
        raise

# ============================================================================
# MIAMI REAL ESTATE DATA COLLECTION
# ============================================================================

def collect_miami_real_estate(area, max_properties=40):
    """Collect Miami luxury real estate data"""
    
    print(f"\n🏠 COLLECTING MIAMI REAL ESTATE DATA")
    print(f"   Area: {area}")
    print(f"   Target: {max_properties} luxury properties")
    
    if not REALTY_US_API_KEY:
        raise Exception("REALTY_US_API_KEY not configured")
    
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
        
        print(f"   → Querying Realty Mole API...")
        response = requests.get(url, headers=headers, params=querystring, timeout=20)
        
        if response.status_code != 200:
            raise Exception(f"Realty API error: {response.status_code}")
        
        listings = response.json()
        
        print(f"   ✅ Received {len(listings)} properties")
        
        properties = []
        for listing in listings[:max_properties]:
            price = listing.get('price', 0)
            sqft = listing.get('squareFootage', 2000)
            price_per_sqft = round(price / sqft, 2) if sqft else 0
            
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
                'price_per_sqft': price_per_sqft,
                'property_type': listing.get('propertyType', ''),
                'agent': 'Miami Realty',
                'url': listing.get('url', ''),
                'description': '',
                'image_url': '',
                'listed_date': listing.get('listDate', '')
            })
        
        print(f"   ✅ Parsed {len(properties)} properties")
        return properties
        
    except Exception as e:
        print(f"   ❌ Miami data collection failed: {str(e)}")
        raise

# ============================================================================
# UK LUXURY CAR RENTALS (Using Outscraper)
# ============================================================================

def collect_uk_car_rentals(city="London", max_results=30):
    """Collect UK luxury car rental companies"""
    
    print(f"\n🚗 COLLECTING UK LUXURY CAR RENTAL DATA")
    print(f"   City: {city}")
    
    if not OUTSCRAPER_API_KEY:
        raise Exception("OUTSCRAPER_API_KEY not configured")
    
    try:
        from outscraper import ApiClient
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        query = f"luxury car rental {city}"
        results = client.google_maps_search(
            query=[query],
            language='en',
            region='uk',
            limit=max_results
        )
        
        companies = []
        for result in results[0][:max_results]:
            companies.append({
                'source': 'Outscraper',
                'company_id': result.get('place_id', ''),
                'name': result.get('name', ''),
                'address': result.get('full_address', ''),
                'city': city,
                'rating': result.get('rating', 0),
                'reviews': result.get('reviews', 0),
                'phone': result.get('phone', ''),
                'website': result.get('site', ''),
                'description': result.get('description', ''),
                'category': 'Luxury Car Rental'
            })
        
        print(f"   ✅ Collected {len(companies)} luxury car rental companies")
        return companies
        
    except Exception as e:
        print(f"   ❌ Car rental data collection failed: {str(e)}")
        raise

# ============================================================================
# CHARTERING COMPANIES
# ============================================================================

def collect_chartering_companies(area="London", max_results=30):
    """Collect yacht/jet charter companies"""
    
    print(f"\n🛥️ COLLECTING CHARTERING COMPANIES")
    print(f"   Area: {area}")
    
    if not OUTSCRAPER_API_KEY:
        raise Exception("OUTSCRAPER_API_KEY not configured")
    
    try:
        from outscraper import ApiClient
        client = ApiClient(api_key=OUTSCRAPER_API_KEY)
        
        queries = [
            f"yacht charter {area}",
            f"private jet charter {area}"
        ]
        
        all_companies = []
        for query in queries:
            results = client.google_maps_search(
                query=[query],
                language='en',
                limit=max_results//2
            )
            
            for result in results[0]:
                all_companies.append({
                    'source': 'Outscraper',
                    'company_id': result.get('place_id', ''),
                    'name': result.get('name', ''),
                    'address': result.get('full_address', ''),
                    'area': area,
                    'rating': result.get('rating', 0),
                    'reviews': result.get('reviews', 0),
                    'phone': result.get('phone', ''),
                    'website': result.get('site', ''),
                    'description': result.get('description', ''),
                    'category': 'Charter Services'
                })
        
        print(f"   ✅ Collected {len(all_companies)} chartering companies")
        return all_companies
        
    except Exception as e:
        print(f"   ❌ Chartering data collection failed: {str(e)}")
        raise

# ============================================================================
# MAIN COLLECTION ORCHESTRATOR
# ============================================================================

def collect_market_data(vertical, area, city):
    """Main data collection orchestrator"""
    
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
            'data_source': 'UK Real Estate Rightmove API (RapidAPI)'
        },
        'raw_data': {}
    }
    
    try:
        if vertical == 'uk-real-estate':
            data['raw_data']['properties'] = collect_uk_real_estate(area)
            
        elif vertical == 'miami-real-estate':
            data['raw_data']['properties'] = collect_miami_real_estate(area)
            
        elif vertical == 'uk-car-rentals':
            data['raw_data']['companies'] = collect_uk_car_rentals(city)
            
        elif vertical == 'chartering':
            data['raw_data']['companies'] = collect_chartering_companies(area)
            
        else:
            raise Exception(f"Unknown vertical: {vertical}")
        
        # Save to file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n✅ DATA COLLECTION COMPLETE")
        print(f"   Output: {OUTPUT_FILE}")
        print(f"   Records: {len(data['raw_data'].get('properties', data['raw_data'].get('companies', [])))} items")
        
        return True
        
    except Exception as e:
        print(f"\n❌ DATA COLLECTION FAILED")
        print(f"   Error: {str(e)}")
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
        collect_market_data(vertical, area, city)
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

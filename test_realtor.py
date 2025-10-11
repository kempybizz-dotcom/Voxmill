import os
import requests
import json

print("=" * 60)
print("RAPIDAPI REALTOR.COM TEST")
print("=" * 60)

rapidapi_key = os.environ.get('RAPIDAPI_KEY')
print(f"\nAPI Key present: {'✅' if rapidapi_key else '❌'}")
print(f"API Key (first 10 chars): {rapidapi_key[:10] if rapidapi_key else 'N/A'}...")

url = "https://realtor.p.rapidapi.com/properties/v3/list"

headers = {
    "X-RapidAPI-Key": rapidapi_key,
    "X-RapidAPI-Host": "realtor.p.rapidapi.com"
}

# Test Miami luxury market
querystring = {
    "limit": "10",
    "offset": "0",
    "postal_code": "",
    "status": "for_sale",
    "sort": "relevance",
    "city": "Miami",
    "state_code": "FL",
    "price_min": "500000"
}

print(f"\nQuerying: Miami, FL luxury properties (>$500k)")
print("=" * 60)

try:
    response = requests.get(url, headers=headers, params=querystring)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        
        # Check structure
        if 'data' in data and 'home_search' in data['data']:
            results = data['data']['home_search'].get('results', [])
            print(f"✅ Properties found: {len(results)}")
            
            if results:
                print("\n" + "=" * 60)
                print("FIRST 3 PROPERTIES:")
                print("=" * 60)
                
                for i, listing in enumerate(results[:3], 1):
                    description = listing.get('description', {})
                    location = listing.get('location', {})
                    address = location.get('address', {})
                    
                    print(f"\n{i}. ${listing.get('list_price', 'N/A'):,}")
                    print(f"   {address.get('line', 'N/A')}")
                    print(f"   {address.get('city', 'N/A')}, {address.get('state_code', 'N/A')} {address.get('postal_code', 'N/A')}")
                    print(f"   {description.get('beds', 'N/A')} bed | {description.get('baths', 'N/A')} bath | {description.get('sqft', 'N/A')} sqft")
                    print(f"   Type: {description.get('type', 'N/A')}")
                    print(f"   Days on Market: {listing.get('days_on_mls', 'N/A')}")
                    print(f"   Property ID: {listing.get('property_id', 'N/A')}")
                
                # Show all available fields from first property
                print("\n" + "=" * 60)
                print("ALL FIELDS FROM FIRST PROPERTY:")
                print("=" * 60)
                print(json.dumps(results[0], indent=2)[:1000] + "...")
            else:
                print("⚠️ No results in response")
        else:
            print("⚠️ Unexpected response structure")
            print(json.dumps(data, indent=2)[:500])
    else:
        print(f"❌ API Error: {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)

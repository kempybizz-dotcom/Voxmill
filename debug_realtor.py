import os
import requests
import json

print("=" * 70)
print("REALTOR API DEBUG")
print("=" * 70)

rapidapi_key = os.environ.get('RAPIDAPI_KEY')
print(f"\nAPI Key present: {'✅' if rapidapi_key else '❌'}")

url = "https://realtor.p.rapidapi.com/properties/v3/list"

headers = {
    "X-RapidAPI-Key": rapidapi_key,
    "X-RapidAPI-Host": "realtor.p.rapidapi.com"
}

# Try different parameter combinations
test_configs = [
    {
        "name": "TEST 1: Miami with min price",
        "params": {
            "limit": "10",
            "city": "Miami",
            "state_code": "FL",
            "status": "for_sale",
            "price_min": "500000"
        }
    },
    {
        "name": "TEST 2: Miami without min price",
        "params": {
            "limit": "10",
            "city": "Miami",
            "state_code": "FL",
            "status": "for_sale"
        }
    },
    {
        "name": "TEST 3: Specific ZIP code",
        "params": {
            "limit": "10",
            "postal_code": "33156",  # Coral Gables
            "status": "for_sale"
        }
    }
]

for config in test_configs:
    print(f"\n{'=' * 70}")
    print(config['name'])
    print('=' * 70)
    print(f"Parameters: {config['params']}")
    
    try:
        response = requests.get(url, headers=headers, params=config['params'])
        
        print(f"\nStatus Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # Print raw response structure
            print(f"\nResponse keys: {list(data.keys())}")
            
            # Try to navigate to results
            if 'data' in data:
                print(f"Data keys: {list(data['data'].keys())}")
                
                if 'home_search' in data['data']:
                    print(f"Home search keys: {list(data['data']['home_search'].keys())}")
                    
                    if 'results' in data['data']['home_search']:
                        results = data['data']['home_search']['results']
                        print(f"\n✅ FOUND {len(results)} RESULTS")
                        
                        if results and len(results) > 0:
                            print(f"\nFIRST RESULT STRUCTURE:")
                            print(json.dumps(results[0], indent=2)[:1500])
                    else:
                        print("⚠️ No 'results' key in home_search")
                        print(f"Available keys: {list(data['data']['home_search'].keys())}")
                else:
                    print("⚠️ No 'home_search' key in data")
            else:
                print("⚠️ No 'data' key in response")
                print(f"\nFull response (first 500 chars):")
                print(json.dumps(data, indent=2)[:500])
        else:
            print(f"❌ HTTP {response.status_code}")
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 70)
print("DEBUG COMPLETE")
print("=" * 70)

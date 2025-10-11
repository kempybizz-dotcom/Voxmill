import os
from outscraper import ApiClient

print("=" * 60)
print("OUTSCRAPER API TEST")
print("=" * 60)

# Initialize client
api_key = os.environ.get('OUTSCRAPER_API_KEY')
print(f"\nAPI Key present: {'✅' if api_key else '❌'}")
print(f"API Key (first 10 chars): {api_key[:10] if api_key else 'N/A'}...")

client = ApiClient(api_key=api_key)

# Test different search types
test_queries = [
    "luxury real estate Miami Pinecrest",
    "real estate agents Miami Florida",
    "Miami luxury homes for sale",
    "Compass Real Estate Miami"
]

for i, query in enumerate(test_queries, 1):
    print(f"\n{'=' * 60}")
    print(f"TEST {i}: {query}")
    print('=' * 60)
    
    try:
        results = client.google_maps_search(
            query,
            limit=5,
            language='en',
            region='us'
        )
        
        print(f"✅ Results found: {len(results)}")
        
        if results:
            print("\nFirst 3 results:")
            for idx, result in enumerate(results[:3], 1):
                print(f"\n  {idx}. {result.get('name', 'N/A')}")
                print(f"     Address: {result.get('full_address', 'N/A')}")
                print(f"     Category: {result.get('category', 'N/A')}")
                print(f"     Rating: {result.get('rating', 'N/A')}")
                print(f"     Reviews: {result.get('reviews', 0)}")
                print(f"     Website: {result.get('site', 'N/A')}")
                
                # Print ALL available fields for first result
                if idx == 1:
                    print(f"\n     All available fields:")
                    for key in result.keys():
                        print(f"       - {key}: {result.get(key)}")
        else:
            print("⚠️ No results returned")
            
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)

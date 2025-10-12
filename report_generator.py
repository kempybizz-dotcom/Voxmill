import os
import json
import re
from datetime import datetime
import requests
from outscraper import ApiClient
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

DEMO_CLIENT = {
    "name": "Miami Brokers Group",
    "contact": "Mike Diaz",
    "city": "Miami",
    "state": "FL",
    "state_code": "FL",
    "focus_areas": "Pinecrest, Coral Gables, Palmetto Bay"
}

def get_google_sheet():
    print("Connecting to Google Sheets...")
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    return client.open_by_key(sheet_id).sheet1

def try_realty_api(city, state_code, min_price=500000):
    """SOURCE 1: RapidAPI Realty In US"""
    print(f"\n[SOURCE 1] Trying Realty API...")
    try:
        api_key = os.environ.get('REALTY_US_API_KEY')
        if not api_key:
            print("  ⚠️ No API key")
            return []
        
        url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
            "Content-Type": "application/json"
        }
        payload = {
            "limit": 30,
            "city": city,
            "state_code": state_code,
            "status": ["for_sale"],
            "price_min": min_price
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        if response.status_code != 200:
            print(f"  ❌ Status {response.status_code}")
            return []
        
        data = response.json()
        results = data.get('data', {}).get('home_search', {}).get('results', [])
        
        listings = []
        for r in results:
            desc = r.get('description') or {}
            loc = r.get('location', {}).get('address') or {}
            price = r.get('list_price', 0)
            if price > 0:
                listings.append({
                    'source': 'Realty API',
                    'address': loc.get('line', 'N/A'),
                    'city': loc.get('city', city),
                    'price': price,
                    'beds': desc.get('beds', 'N/A'),
                    'baths': desc.get('baths', 'N/A'),
                    'sqft': desc.get('sqft', 'N/A'),
                    'days_on_market': r.get('days_on_mls', 'N/A'),
                    'property_type': desc.get('type', 'Single Family')
                })
        
        print(f"  ✅ Found {len(listings)} listings")
        return listings
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return []

def try_outscraper_search(city, focus_areas):
    """SOURCE 2: Outscraper Google Search"""
    print(f"\n[SOURCE 2] Trying Outscraper Google Search...")
    try:
        client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
        queries = [
            f"site:zillow.com {city} {focus_areas} homes for sale",
            f"site:realtor.com {city} {focus_areas} luxury"
        ]
        
        all_listings = []
        for q in queries:
            try:
                results = client.google_search(q, num=15, language='en')
                if results and isinstance(results[0], list):
                    results = results[0]
                
                for r in results:
                    title = r.get('title', '')
                    snippet = r.get('snippet', '')
                    combined = f"{title} {snippet}"
                    
                    # Extract price
                    price = None
                    price_match = re.search(r'\$([0-9,]+)', combined)
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                    
                    if price and price > 400000:
                        # Extract beds/baths
                        beds = None
                        baths = None
                        bed_match = re.search(r'(\d+)\s*(?:bed|bd)', combined, re.I)
                        bath_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba)', combined, re.I)
                        if bed_match:
                            beds = int(bed_match.group(1))
                        if bath_match:
                            baths = float(bath_match.group(1))
                        
                        all_listings.append({
                            'source': 'Zillow/Realtor',
                            'address': title[:100],
                            'city': city,
                            'price': price,
                            'beds': beds or 'N/A',
                            'baths': baths or 'N/A',
                            'sqft': 'N/A',
                            'days_on_market': 'N/A',
                            'property_type': 'Single Family'
                        })
            except:
                continue
        
        print(f"  ✅ Found {len(all_listings)} listings")
        return all_listings
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return []

def try_outscraper_maps(city, focus_areas):
    """SOURCE 3: Outscraper Google Maps (agents + properties)"""
    print(f"\n[SOURCE 3] Trying Outscraper Google Maps...")
    try:
        client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
        queries = [
            f"{city} {focus_areas} luxury homes for sale",
            f"{city} real estate {focus_areas}"
        ]
        
        all_data = []
        for q in queries:
            try:
                results = client.google_maps_search(q, limit=15, language='en', region='us')
                if results and isinstance(results[0], list):
                    results = results[0]
                
                for r in results:
                    desc = r.get('description', '')
                    price = None
                    price_match = re.search(r'\$([0-9,]+)', desc)
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                    
                    all_data.append({
                        'source': 'Google Maps',
                        'address': r.get('name', 'N/A'),
                        'city': city,
                        'price': price or 0,
                        'beds': 'N/A',
                        'baths': 'N/A',
                        'sqft': 'N/A',
                        'days_on_market': 'N/A',
                        'property_type': r.get('category', 'Real Estate'),
                        'rating': r.get('rating', 'N/A'),
                        'reviews': r.get('reviews', 0)
                    })
            except:
                continue
        
        print(f"  ✅ Found {len(all_data)} entities")
        return all_data
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return []

def aggregate_listings(client_data):
    """Aggregate data from all sources"""
    print("\n" + "=" * 70)
    print("AGGREGATING DATA FROM MULTIPLE SOURCES")
    print("=" * 70)
    
    all_data = []
    
    # Try each source
    realty_data = try_realty_api(client_data['city'], client_data['state_code'])
    all_data.extend(realty_data)
    
    search_data = try_outscraper_search(client_data['city'], client_data['focus_areas'])
    all_data.extend(search_data)
    
    maps_data = try_outscraper_maps(client_data['city'], client_data['focus_areas'])
    all_data.extend(maps_data)
    
    print(f"\n📊 TOTAL DATA COLLECTED: {len(all_data)} items")
    print(f"   - Realty API: {len(realty_data)}")
    print(f"   - Zillow/Realtor Search: {len(search_data)}")
    print(f"   - Google Maps: {len(maps_data)}")
    
    # Sort by price (highest first), filter out zero prices
    listings_with_price = [d for d in all_data if d.get('price', 0) > 0]
    listings_with_price.sort(key=lambda x: x['price'], reverse=True)
    
    return listings_with_price, all_data

def calc_metrics(listings):
    if not listings:
        return {}
    prices = [l['price'] for l in listings if l.get('price', 0) > 0]
    return {
        'total': len(listings),
        'with_pricing': len(prices),
        'avg_price': int(sum(prices) / len(prices)) if prices else 0,
        'median_price': int(sorted(prices)[len(prices)//2]) if prices else 0,
        'min_price': min(prices) if prices else 0,
        'max_price': max(prices) if prices else 0
    }

def gen_insights(city, listings, metrics):
    print("\nGenerating AI insights...")
    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    summary = "\n".join([
        f"- ${l['price']:,} | {l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba | {l['address'][:60]}"
        for l in listings[:20] if l.get('price', 0) > 0
    ])
    
    context = f"""Market: {city}
Total: {metrics['total']} | Priced: {metrics['with_pricing']}
Avg: ${metrics['avg_price']:,} | Median: ${metrics['median_price']:,}
Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}"""
    
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are Voxmill Market Intelligence. Analyze data and provide RAISE / REDUCE / ROTATE insights. Max 20 words each."},
                {"role": "user", "content": f"{context}\n\nListings:\n{summary}\n\nGenerate insights:"}
            ],
            temperature=0.7,
            max_tokens=250
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"

def format_for_sheet(listings, count=10):
    out = []
    for i, l in enumerate(listings[:count], 1):
        bed_bath = f"{l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba" if l.get('beds') != 'N/A' else ""
        out.append(f"{i}. ${l['price']:,} {bed_bath}\n   {l['address']}\n   Source: {l.get('source', 'Unknown')}")
    return "\n\n".join(out)

def write_sheet(ws, client_data, listings, metrics, insights):
    print("\nWriting to Google Sheet...")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary = f"""Total Entities: {metrics['total']}
With Pricing: {metrics['with_pricing']}
Avg Price: ${metrics['avg_price']:,}
Median: ${metrics['median_price']:,}
Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}"""
    
    row = [
        ts,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        summary,
        insights,
        format_for_sheet(listings, 10),
        "Generated"
    ]
    ws.append_row(row)
    print("✅ Done")

def main():
    print("=" * 70)
    print("VOXMILL - MULTI-SOURCE MARKET INTELLIGENCE")
    print("=" * 70)
    
    try:
        ws = get_google_sheet()
        if ws.row_count == 0 or not ws.cell(1, 1).value:
            ws.append_row(["Timestamp", "Client", "Contact", "Market", "Focus Areas", "Metrics", "Insights", "Top Listings", "Status"])
        
        print(f"\nGenerating for: {DEMO_CLIENT['name']}")
        
        # Aggregate from all sources
        priced_listings, all_data = aggregate_listings(DEMO_CLIENT)
        
        if len(priced_listings) < 3:
            print("⚠️ Insufficient data")
            return
        
        metrics = calc_metrics(all_data)
        print(f"\n📊 Final Metrics: {metrics['with_pricing']} priced listings, Avg ${metrics['avg_price']:,}")
        
        insights = gen_insights(DEMO_CLIENT['city'], priced_listings, metrics)
        write_sheet(ws, DEMO_CLIENT, priced_listings, metrics, insights)
        
        print("\n" + "=" * 70)
        print("✅ COMPLETE")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

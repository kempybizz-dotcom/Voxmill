import os
import json
from datetime import datetime
import requests
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

DEMO_CLIENT = {
    "name": "Miami Brokers Group",
    "contact": "Mike Diaz",
    "email": "demo@miamibrokersgroup.com",
    "city": "Miami",
    "state": "FL",
    "state_code": "FL",
    "focus_areas": "Pinecrest, Coral Gables, Palmetto Bay",
    "property_type": "luxury"
}

def get_google_sheet():
    print("Connecting to Google Sheets...")
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.sheet1

def fetch_realty_listings(city, state_code, min_price=500000):
    """Fetch real property listings using Realty In US API"""
    print(f"Fetching real listings from Realty In US API for {city}, {state_code}...")
    
    api_key = os.environ.get('REALTY_US_API_KEY')
    
    url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
    
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    
    payload = {
        "limit": 30,
        "offset": 0,
        "postal_code": "",
        "status": ["for_sale", "ready_to_build"],
        "sort": {"direction": "desc", "field": "list_date"},
        "city": city,
        "state_code": state_code,
        "price_min": min_price
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"  API Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"  ❌ API Error: {response.text[:200]}")
            return []
        
        data = response.json()
        
        # DEBUG: Show response structure
        print(f"  Response type: {type(data)}")
        print(f"  Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        
        if data and isinstance(data, dict):
            if 'data' in data:
                print(f"  Data keys: {list(data['data'].keys())}")
                if 'home_search' in data.get('data', {}):
                    print(f"  Home search keys: {list(data['data']['home_search'].keys())}")
            else:
                print(f"  Full response preview: {json.dumps(data, indent=2)[:500]}")
        
        listings = []
        
        # Try to navigate response
        if data and isinstance(data, dict) and 'data' in data:
            data_section = data.get('data', {})
            if 'home_search' in data_section:
                home_search = data_section['home_search']
                if 'results' in home_search:
                    results = home_search['results']
                    
                    for listing in results:
                        description = listing.get('description', {})
                        location = listing.get('location', {})
                        address = location.get('address', {})
                        
                        property_data = {
                            'address': address.get('line', 'N/A'),
                            'city': address.get('city', 'N/A'),
                            'state': address.get('state_code', 'N/A'),
                            'zip': address.get('postal_code', 'N/A'),
                            'price': listing.get('list_price', 0),
                            'beds': description.get('beds', 'N/A'),
                            'baths': description.get('baths', 'N/A'),
                            'sqft': description.get('sqft', 'N/A'),
                            'lot_sqft': description.get('lot_sqft', 'N/A'),
                            'year_built': description.get('year_built', 'N/A'),
                            'property_type': description.get('type', 'N/A'),
                            'days_on_market': listing.get('days_on_mls', 'N/A'),
                            'status': listing.get('status', 'N/A'),
                            'listing_id': listing.get('property_id', 'N/A')
                        }
                        
                        if property_data['price'] > 0:
                            listings.append(property_data)
                    
                    print(f"  ✅ Found {len(listings)} listings with prices")
        
        return listings
    
    except requests.exceptions.Timeout:
        print("  ❌ Request timeout")
        return []
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def calc_metrics(listings):
    """Calculate comprehensive market metrics"""
    if not listings:
        return {}
    
    prices = [l['price'] for l in listings if isinstance(l['price'], (int, float)) and l['price'] > 0]
    sqfts = [l['sqft'] for l in listings if isinstance(l['sqft'], (int, float)) and l['sqft'] > 0]
    doms = [l['days_on_market'] for l in listings if isinstance(l['days_on_market'], (int, float))]
    beds = [l['beds'] for l in listings if isinstance(l['beds'], (int, float))]
    
    price_per_sqft_list = []
    for l in listings:
        if isinstance(l['price'], (int, float)) and isinstance(l['sqft'], (int, float)) and l['sqft'] > 0:
            price_per_sqft_list.append(l['price'] / l['sqft'])
    
    metrics = {
        'total_listings': len(listings),
        'avg_price': int(sum(prices) / len(prices)) if prices else 0,
        'median_price': int(sorted(prices)[len(prices)//2]) if prices else 0,
        'min_price': min(prices) if prices else 0,
        'max_price': max(prices) if prices else 0,
        'avg_sqft': int(sum(sqfts) / len(sqfts)) if sqfts else 0,
        'avg_beds': round(sum(beds) / len(beds), 1) if beds else 0,
        'avg_dom': int(sum(doms) / len(doms)) if doms else 0,
        'price_per_sqft': int(sum(price_per_sqft_list) / len(price_per_sqft_list)) if price_per_sqft_list else 0
    }
    
    return metrics

def gen_insights(city, state, listings, metrics):
    """Generate AI insights from real listing data"""
    print("Generating strategic insights...")
    
    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    listing_summary = "\n".join([
        f"- ${l['price']:,} | {l['beds']}bed/{l['baths']}bath | {l['sqft']:,} sqft | {l['days_on_market']} days | {l['address']}, {l['city']}"
        for l in listings[:20]
        if isinstance(l['beds'], (int, float)) and isinstance(l['baths'], (int, float))
    ])
    
    market_context = f"""Market Overview for {city}, {state}:
- Total Active Listings: {metrics['total_listings']}
- Average Price: ${metrics['avg_price']:,}
- Median Price: ${metrics['median_price']:,}
- Price Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}
- Avg Property: {metrics['avg_beds']} beds | {metrics['avg_sqft']:,} sqft
- Avg Days on Market: {metrics['avg_dom']} days
- Price per SqFt: ${metrics['price_per_sqft']:,}"""
    
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are Voxmill Market Intelligence — an executive real estate analyst.
                    Analyze REAL MLS listing data and provide strategic insights:
                    
                    RAISE: [Specific underpriced opportunities - cite actual prices and addresses]
                    REDUCE: [Overpriced listings needing adjustment - cite actual prices]
                    ROTATE: [Strategic marketing shifts based on DOM and pricing - cite numbers]
                    
                    Be specific. Use actual dollar amounts and days on market. Each line max 25 words."""
                },
                {
                    "role": "user",
                    "content": f"""{market_context}

Active MLS Listings:
{listing_summary}

Generate data-driven RAISE / REDUCE / ROTATE strategic insights."""
                }
            ],
            temperature=0.7,
            max_tokens=350
        )
        
        print("✅ Insights generated")
        return completion.choices[0].message.content.strip()
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return f"Error generating insights: {e}"

def format_listings(listings, count=10):
    """Format listings for Google Sheet"""
    formatted = []
    
    for i, l in enumerate(listings[:count], 1):
        bed_bath = f"{l['beds']}bed/{l['baths']}bath" if l['beds'] != 'N/A' else "N/A"
        sqft_str = f"{l['sqft']:,} sqft" if isinstance(l['sqft'], (int, float)) else "N/A"
        dom_str = f"{l['days_on_market']} days" if isinstance(l['days_on_market'], (int, float)) else "N/A"
        price_sqft = f"${int(l['price']/l['sqft'])}/sqft" if isinstance(l['sqft'], (int, float)) and l['sqft'] > 0 else ""
        
        formatted.append(
            f"{i}. ${l['price']:,} {price_sqft}\n"
            f"   {bed_bath} | {sqft_str} | {dom_str} on market\n"
            f"   {l['address']}, {l['city']}, {l['state']} {l['zip']}\n"
            f"   {l['property_type']} | Built: {l['year_built']}"
        )
    
    return "\n\n".join(formatted)

def write_sheet(ws, client_data, listings, metrics, insights):
    """Write comprehensive report to Google Sheet"""
    print("Writing to Google Sheet...")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    market_summary = f"""Active Listings: {metrics['total_listings']}
Average Price: ${metrics['avg_price']:,}
Median Price: ${metrics['median_price']:,}
Price Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}
Avg Property: {metrics['avg_beds']} beds | {metrics['avg_sqft']:,} sqft
Avg Days on Market: {metrics['avg_dom']} days
Price per SqFt: ${metrics['price_per_sqft']:,}"""
    
    top_listings = format_listings(listings, 10)
    
    row = [
        timestamp,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        market_summary,
        insights,
        top_listings,
        "Generated"
    ]
    
    ws.append_row(row)
    print("✅ Report written to sheet")

def main():
    print("=" * 75)
    print("VOXMILL MARKET INTELLIGENCE - REALTY IN US API")
    print("=" * 75)
    
    try:
        ws = get_google_sheet()
        
        if ws.row_count == 0 or not ws.cell(1, 1).value:
            ws.append_row([
                "Timestamp", "Client Name", "Contact Person", "Market", 
                "Focus Areas", "Market Metrics", "Strategic Insights", 
                "Top 10 Listings", "Status"
            ])
            print("✅ Headers added")
        
        print(f"\n{'=' * 75}")
        print(f"Generating report for: {DEMO_CLIENT['name']}")
        print(f"Market: {DEMO_CLIENT['city']}, {DEMO_CLIENT['state_code']}")
        print('=' * 75)
        
        listings = fetch_realty_listings(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state_code'],
            min_price=500000
        )
        
        if not listings or len(listings) < 5:
            print("⚠️ Insufficient listings - check API key or parameters")
            return
        
        metrics = calc_metrics(listings)
        print(f"\n📊 Market Metrics:")
        print(f"   Total Listings: {metrics['total_listings']}")
        print(f"   Avg Price: ${metrics['avg_price']:,}")
        print(f"   Median Price: ${metrics['median_price']:,}")
        print(f"   Price/SqFt: ${metrics['price_per_sqft']:,}")
        print(f"   Avg DOM: {metrics['avg_dom']} days")
        
        insights = gen_insights(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            listings,
            metrics
        )
        
        write_sheet(ws, DEMO_CLIENT, listings, metrics, insights)
        
        print("\n" + "=" * 75)
        print("✅ REPORT COMPLETE - REAL MLS DATA")
        print("=" * 75)
        print(f"\nView: https://docs.google.com/spreadsheets/d/{os.environ.get('GOOGLE_SHEET_ID')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

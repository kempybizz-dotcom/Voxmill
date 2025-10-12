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

def gen_enhanced_analysis(city, listings, metrics):
    """Generate 5 types of analysis for comprehensive intelligence"""
    print("\nGenerating comprehensive market analysis...")
    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    # Prepare listing summary
    summary = "\n".join([
        f"- ${l['price']:,} | {l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba | {l['address'][:60]}"
        for l in listings[:20] if l.get('price', 0) > 0
    ])
    
    context = f"""Market: {city}
Total Listings: {metrics['total']} | With Pricing: {metrics['with_pricing']}
Average Price: ${metrics['avg_price']:,}
Median Price: ${metrics['median_price']:,}
Price Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}"""
    
    analyses = {}
    
    try:
        # 1. Market Summary
        prompt1 = f"{context}\n\nProvide a 2-sentence executive summary of this market's current state."
        response1 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a luxury real estate market analyst. Be concise and data-driven."},
                {"role": "user", "content": prompt1}
            ],
            temperature=0.7,
            max_tokens=100
        )
        analyses['market_summary'] = response1.choices[0].message.content.strip()
        
        # 2. Strategic Insights (RAISE/REDUCE/ROTATE)
        prompt2 = f"{context}\n\nTop Listings:\n{summary}\n\nProvide strategic insights in this format:\nRAISE: [pricing opportunity]\nREDUCE: [discount strategy]\nROTATE: [marketing shift]"
        response2 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are Voxmill Market Intelligence. Provide actionable RAISE/REDUCE/ROTATE strategies."},
                {"role": "user", "content": prompt2}
            ],
            temperature=0.7,
            max_tokens=200
        )
        analyses['strategic_insights'] = response2.choices[0].message.content.strip()
        
        # 3. Top Opportunities
        prompt3 = f"{context}\n\nListings:\n{summary}\n\nIdentify the top 3 underpriced opportunities with specific addresses and reasoning. Format as numbered list."
        response3 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an investment analyst. Identify value opportunities."},
                {"role": "user", "content": prompt3}
            ],
            temperature=0.7,
            max_tokens=200
        )
        analyses['opportunities'] = response3.choices[0].message.content.strip()
        
        # 4. Risk Factors
        prompt4 = f"{context}\n\nWhat are the top 3 market risks or concerns for sellers/buyers right now? Be specific about pricing levels or inventory issues."
        response4 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a market risk analyst. Identify concerns and caution areas."},
                {"role": "user", "content": prompt4}
            ],
            temperature=0.7,
            max_tokens=150
        )
        analyses['risk_factors'] = response4.choices[0].message.content.strip()
        
        # 5. Price Trend Analysis
        # Calculate distribution
        high_end = len([l for l in listings if l.get('price', 0) > metrics['avg_price'] * 1.2])
        mid_range = len([l for l in listings if metrics['avg_price'] * 0.8 <= l.get('price', 0) <= metrics['avg_price'] * 1.2])
        lower_end = len([l for l in listings if l.get('price', 0) < metrics['avg_price'] * 0.8])
        
        prompt5 = f"{context}\n\nDistribution: High-end (>{metrics['avg_price']*1.2:,.0f}): {high_end} | Mid-range: {mid_range} | Lower (<{metrics['avg_price']*0.8:,.0f}): {lower_end}\n\nAnalyze the price distribution and what it means for market dynamics."
        response5 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a pricing strategist. Analyze distribution patterns."},
                {"role": "user", "content": prompt5}
            ],
            temperature=0.7,
            max_tokens=150
        )
        analyses['price_trends'] = response5.choices[0].message.content.strip()
        
        # 6. Action Items
        prompt6 = f"Based on this {city} market analysis, provide 3 specific action items for a luxury real estate broker this week."
        response6 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a business strategist. Provide actionable next steps."},
                {"role": "user", "content": prompt6}
            ],
            temperature=0.7,
            max_tokens=150
        )
        analyses['action_items'] = response6.choices[0].message.content.strip()
        
        print("✅ All analyses generated")
        return analyses
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'market_summary': 'Error generating analysis',
            'strategic_insights': 'Error',
            'opportunities': 'Error',
            'risk_factors': 'Error',
            'price_trends': 'Error',
            'action_items': 'Error'
        }

def format_for_sheet(listings, count=10):
    out = []
    for i, l in enumerate(listings[:count], 1):
        bed_bath = f"{l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba" if l.get('beds') != 'N/A' else ""
        out.append(f"{i}. ${l['price']:,} {bed_bath}\n   {l['address']}\n   Source: {l.get('source', 'Unknown')}")
    return "\n\n".join(out)

def write_sheet(ws, client_data, listings, metrics, analyses):
    """Write comprehensive report to Google Sheet with headers"""
    print("\nWriting to Google Sheet...")
    
    # Check if headers exist, if not add them
    if ws.row_count == 0 or not ws.cell(1, 1).value or ws.cell(1, 1).value != "Timestamp":
        headers = [
            "Timestamp",
            "Client Name",
            "Market",
            "Total Listings",
            "Avg Price",
            "Price Range",
            "Market Summary",
            "Strategic Insights (R/R/R)",
            "Top Opportunities",
            "Risk Factors",
            "Price Trend Analysis",
            "Action Items This Week",
            "Top 10 Properties",
            "Status"
        ]
        
        # Clear any existing content and write headers
        if ws.row_count > 0:
            ws.insert_row(headers, 1)
        else:
            ws.append_row(headers)
        
        # Format headers (bold would require additional API calls, skip for now)
        print("  ✅ Headers added")
    
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    row = [
        ts,
        client_data['name'],
        f"{client_data['city']}, {client_data['state']}",
        metrics['total'],
        f"${metrics['avg_price']:,}",
        f"${metrics['min_price']:,} - ${metrics['max_price']:,}",
        analyses.get('market_summary', 'N/A'),
        analyses.get('strategic_insights', 'N/A'),
        analyses.get('opportunities', 'N/A'),
        analyses.get('risk_factors', 'N/A'),
        analyses.get('price_trends', 'N/A'),
        analyses.get('action_items', 'N/A'),
        format_for_sheet(listings, 10),
        "Generated"
    ]
    
    ws.append_row(row)
    print("✅ Report written with full analysis")


def main():
    print("=" * 70)
    print("VOXMILL - ENHANCED MARKET INTELLIGENCE")
    print("=" * 70)
    
    try:
        ws = get_google_sheet()
        
        print(f"\nGenerating enhanced report for: {DEMO_CLIENT['name']}")
        
        # Aggregate from all sources
        priced_listings, all_data = aggregate_listings(DEMO_CLIENT)
        
        if len(priced_listings) < 3:
            print("⚠️ Insufficient data")
            return
        
        metrics = calc_metrics(all_data)
        print(f"\n📊 Metrics: {metrics['with_pricing']} listings, Avg ${metrics['avg_price']:,}")
        
        # Generate comprehensive analysis (6 types)
        analyses = gen_enhanced_analysis(DEMO_CLIENT['city'], priced_listings, metrics)
        
        # Write to sheet with headers
        write_sheet(ws, DEMO_CLIENT, priced_listings, metrics, analyses)
        
        print("\n" + "=" * 70)
        print("✅ ENHANCED REPORT COMPLETE")
        print("=" * 70)
        print(f"\n📊 Analysis Depth:")
        print(f"   - Market Summary: ✅")
        print(f"   - Strategic Insights: ✅")
        print(f"   - Opportunities: ✅")
        print(f"   - Risk Factors: ✅")
        print(f"   - Price Trends: ✅")
        print(f"   - Action Items: ✅")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

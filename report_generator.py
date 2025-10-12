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
            "postal_code": "33156",
            "status": ["for_sale"],
            "price_min": min_price
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=60)
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
            sqft = desc.get('sqft', 0)
            
            # Calculate price per sqft
            price_per_sqft = round(price / sqft, 2) if sqft and sqft > 0 else 0
            
            # Get property URL
            property_url = r.get('href', '')
            if property_url and not property_url.startswith('http'):
                property_url = f"https://www.realtor.com{property_url}"
            
            if price > 0:
                listings.append({
                    'source': 'Realty API',
                    'address': loc.get('line', 'N/A'),
                    'city': loc.get('city', city),
                    'price': price,
                    'beds': desc.get('beds', 'N/A'),
                    'baths': desc.get('baths', 'N/A'),
                    'sqft': sqft if sqft else 'N/A',
                    'price_per_sqft': price_per_sqft,
                    'days_on_market': r.get('days_on_mls', 0),
                    'property_type': desc.get('type', 'Single Family'),
                    'url': property_url
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
                    url = r.get('link', '')
                    
                    # Extract price
                    price = None
                    price_match = re.search(r'\$([0-9,]+)', combined)
                    if price_match:
                        price = int(price_match.group(1).replace(',', ''))
                    
                    if price and price > 400000:
                        # Extract beds/baths/sqft
                        beds = None
                        baths = None
                        sqft = None
                        bed_match = re.search(r'(\d+)\s*(?:bed|bd)', combined, re.I)
                        bath_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:bath|ba)', combined, re.I)
                        sqft_match = re.search(r'([\d,]+)\s*(?:sq\.?\s*ft|sqft)', combined, re.I)
                        
                        if bed_match:
                            beds = int(bed_match.group(1))
                        if bath_match:
                            baths = float(bath_match.group(1))
                        if sqft_match:
                            sqft = int(sqft_match.group(1).replace(',', ''))
                        
                        price_per_sqft = round(price / sqft, 2) if sqft and sqft > 0 else 0
                        
                        all_listings.append({
                            'source': 'Zillow/Realtor',
                            'address': title[:100],
                            'city': city,
                            'price': price,
                            'beds': beds or 'N/A',
                            'baths': baths or 'N/A',
                            'sqft': sqft or 'N/A',
                            'price_per_sqft': price_per_sqft,
                            'days_on_market': 'N/A',
                            'property_type': 'Single Family',
                            'url': url
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
                        'price_per_sqft': 0,
                        'days_on_market': 'N/A',
                        'property_type': r.get('category', 'Real Estate'),
                        'rating': r.get('rating', 'N/A'),
                        'reviews': r.get('reviews', 0),
                        'url': r.get('site', '')
                    })
            except:
                continue
        
        print(f"  ✅ Found {len(all_data)} entities")
        return all_data
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return []

def calculate_deal_scores(listings):
    """Calculate deal score (1-10) for each property"""
    if not listings:
        return listings
    
    # Get median price/sqft for comparison
    valid_ppsf = [l['price_per_sqft'] for l in listings if l.get('price_per_sqft', 0) > 0]
    if not valid_ppsf:
        return listings
    
    median_ppsf = sorted(valid_ppsf)[len(valid_ppsf)//2]
    
    for listing in listings:
        score = 5  # Base score
        ppsf = listing.get('price_per_sqft', 0)
        dom = listing.get('days_on_market', 0)
        
        # Price/sqft factor (below median = better deal)
        if ppsf > 0:
            if ppsf < median_ppsf * 0.8:  # 20% below median
                score += 3
            elif ppsf < median_ppsf * 0.9:  # 10% below median
                score += 2
            elif ppsf < median_ppsf:
                score += 1
            elif ppsf > median_ppsf * 1.2:  # 20% above median
                score -= 2
        
        # Days on market factor (higher = more negotiable)
        if dom != 'N/A' and dom > 0:
            if dom > 90:
                score += 2
            elif dom > 60:
                score += 1
            elif dom < 14:
                score -= 1
        
        # Cap between 1-10
        listing['deal_score'] = max(1, min(10, score))
        
        # Add flag
        if listing['deal_score'] >= 8:
            listing['flag'] = '🔥 HOT DEAL'
        elif dom != 'N/A' and dom > 90:
            listing['flag'] = '❄️ STALE'
        elif listing['deal_score'] <= 3:
            listing['flag'] = '💎 PREMIUM'
        else:
            listing['flag'] = '—'
    
    return listings

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
    
    # Filter and calculate deal scores
    listings_with_price = [d for d in all_data if d.get('price', 0) > 0]
    listings_with_price = calculate_deal_scores(listings_with_price)
    
    # Sort by deal score (highest first)
    listings_with_price.sort(key=lambda x: x.get('deal_score', 0), reverse=True)
    
    return listings_with_price, all_data

def calc_metrics(listings):
    if not listings:
        return {}
    prices = [l['price'] for l in listings if l.get('price', 0) > 0]
    ppsf_values = [l['price_per_sqft'] for l in listings if l.get('price_per_sqft', 0) > 0]
    
    # DOM analysis
    dom_values = [l['days_on_market'] for l in listings if l.get('days_on_market') != 'N/A' and l.get('days_on_market', 0) > 0]
    avg_dom = int(sum(dom_values) / len(dom_values)) if dom_values else 0
    
    return {
        'total': len(listings),
        'with_pricing': len(prices),
        'avg_price': int(sum(prices) / len(prices)) if prices else 0,
        'median_price': int(sorted(prices)[len(prices)//2]) if prices else 0,
        'min_price': min(prices) if prices else 0,
        'max_price': max(prices) if prices else 0,
        'avg_ppsf': int(sum(ppsf_values) / len(ppsf_values)) if ppsf_values else 0,
        'median_ppsf': int(sorted(ppsf_values)[len(ppsf_values)//2]) if ppsf_values else 0,
        'avg_dom': avg_dom,
        'hot_deals': len([l for l in listings if l.get('deal_score', 0) >= 8]),
        'stale_listings': len([l for l in listings if l.get('days_on_market') != 'N/A' and l.get('days_on_market', 0) > 90])
    }

def gen_enhanced_analysis(city, listings, metrics):
    """Generate enhanced analysis with deal alerts"""
    print("\nGenerating comprehensive market analysis...")
    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    # Get top 3 deals
    top_deals = [l for l in listings[:5] if l.get('deal_score', 0) >= 7]
    
    # Prepare listing summary
    summary = "\n".join([
        f"- ${l['price']:,} | {l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba | {l.get('sqft', 'N/A')} sqft | ${l.get('price_per_sqft', 0)}/sqft | Score: {l.get('deal_score', 0)}/10 | {l['address'][:60]}"
        for l in listings[:20] if l.get('price', 0) > 0
    ])
    
    context = f"""Market: {city}
Total Listings: {metrics['total']} | With Pricing: {metrics['with_pricing']}
Average Price: ${metrics['avg_price']:,} | Median: ${metrics['median_price']:,}
Price Range: ${metrics['min_price']:,} - ${metrics['max_price']:,}
Avg Price/SqFt: ${metrics['avg_ppsf']} | Median: ${metrics['median_ppsf']}
Avg Days on Market: {metrics['avg_dom']}
Hot Deals (Score 8+): {metrics['hot_deals']} | Stale (90+ days): {metrics['stale_listings']}"""
    
    analyses = {}
    
    try:
        # 1. Market Summary
        prompt1 = f"{context}\n\nProvide a 2-sentence executive summary focusing on pricing dynamics and deal velocity."
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
        
        # 2. Strategic Insights
        prompt2 = f"{context}\n\nTop Listings:\n{summary}\n\nProvide strategic insights:\nRAISE: [pricing opportunity]\nREDUCE: [discount strategy]\nROTATE: [marketing shift]"
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
        
        # 3. DEAL ALERTS - Most important upgrade
        deal_summary = "\n".join([
            f"{i+1}. {l['address']} - ${l['price']:,} | ${l.get('price_per_sqft', 0)}/sqft | Score: {l.get('deal_score', 0)}/10"
            for i, l in enumerate(top_deals[:3])
        ])
        
        prompt3 = f"{context}\n\nTop Scoring Properties:\n{deal_summary}\n\nFor each property, explain in 1-2 sentences WHY it's a good deal (price/sqft, days on market, or market positioning). Be specific and actionable."
        response3 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an investment analyst identifying the best opportunities with specific reasoning."},
                {"role": "user", "content": prompt3}
            ],
            temperature=0.7,
            max_tokens=250
        )
        analyses['deal_alerts'] = response3.choices[0].message.content.strip()
        
        # 4. Risk Factors
        prompt4 = f"{context}\n\nWhat are the top 3 market risks based on pricing levels, inventory, and days on market? Be specific with numbers."
        response4 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a market risk analyst."},
                {"role": "user", "content": prompt4}
            ],
            temperature=0.7,
            max_tokens=150
        )
        analyses['risk_factors'] = response4.choices[0].message.content.strip()
        
        # 5. Price Trend Analysis
        high_end = len([l for l in listings if l.get('price', 0) > metrics['avg_price'] * 1.2])
        mid_range = len([l for l in listings if metrics['avg_price'] * 0.8 <= l.get('price', 0) <= metrics['avg_price'] * 1.2])
        lower_end = len([l for l in listings if l.get('price', 0) < metrics['avg_price'] * 0.8])
        
        prompt5 = f"{context}\n\nDistribution: High-end (>${metrics['avg_price']*1.2:,.0f}): {high_end} | Mid: {mid_range} | Lower (<${metrics['avg_price']*0.8:,.0f}): {lower_end}\n\nAnalyze price/sqft trends: Median ${metrics['median_ppsf']}/sqft. What does this distribution mean for pricing strategy?"
        response5 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a pricing strategist."},
                {"role": "user", "content": prompt5}
            ],
            temperature=0.7,
            max_tokens=150
        )
        analyses['price_trends'] = response5.choices[0].message.content.strip()
        
        # 6. Action Items
        prompt6 = f"Based on {metrics['hot_deals']} hot deals and {metrics['stale_listings']} stale listings in {city}, provide 3 specific action items for a broker this week."
        response6 = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a business strategist."},
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
            'market_summary': 'Error',
            'strategic_insights': 'Error',
            'deal_alerts': 'Error',
            'risk_factors': 'Error',
            'price_trends': 'Error',
            'action_items': 'Error'
        }

def format_for_sheet(listings, count=10):
    """Enhanced property formatting with deal scores and URLs"""
    out = []
    for i, l in enumerate(listings[:count], 1):
        bed_bath = f"{l.get('beds', 'N/A')}bd/{l.get('baths', 'N/A')}ba"
        sqft_info = f"{l.get('sqft', 'N/A')} sqft" if l.get('sqft') != 'N/A' else ""
        ppsf = f"${l.get('price_per_sqft', 0)}/sqft" if l.get('price_per_sqft', 0) > 0 else ""
        score = f"Score: {l.get('deal_score', 0)}/10"
        flag = l.get('flag', '—')
        url = l.get('url', '')
        
        property_line = f"{i}. ${l['price']:,} | {bed_bath} | {sqft_info} | {ppsf}\n   {l['address']}\n   {flag} | {score} | Source: {l.get('source', 'Unknown')}"
        
        if url:
            property_line += f"\n   🔗 {url}"
        
        out.append(property_line)
    
    return "\n\n".join(out)

def write_sheet(ws, client_data, listings, metrics, analyses):
    """Write enhanced report with deal scores"""
    print("\nWriting to Google Sheet...")
    
    # Enhanced headers
    if ws.row_count == 0 or not ws.cell(1, 1).value or ws.cell(1, 1).value != "Timestamp":
        headers = [
            "Timestamp",
            "Client Name",
            "Market",
            "Total Listings",
            "Avg Price",
            "Price Range",
            "Avg $/SqFt",
            "Avg Days on Market",
            "Hot Deals (8+)",
            "Stale (90+)",
            "Market Summary",
            "DEAL ALERTS",
            "Strategic Insights (R/R/R)",
            "Risk Factors",
            "Price Trend Analysis",
            "Action Items This Week",
            "Top 10 Properties (Ranked by Deal Score)",
            "Status"
        ]
        
        if ws.row_count > 0:
            ws.insert_row(headers, 1)
        else:
            ws.append_row(headers)
        
        print("  ✅ Headers added")
    
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    row = [
        ts,
        client_data['name'],
        f"{client_data['city']}, {client_data['state']}",
        metrics['total'],
        f"${metrics['avg_price']:,}",
        f"${metrics['min_price']:,} - ${metrics['max_price']:,}",
        f"${metrics['avg_ppsf']}",
        f"{metrics['avg_dom']} days",
        metrics['hot_deals'],
        metrics['stale_listings'],
        analyses.get('market_summary', 'N/A'),
        analyses.get('deal_alerts', 'N/A'),
        analyses.get('strategic_insights', 'N/A'),
        analyses.get('risk_factors', 'N/A'),
        analyses.get('price_trends', 'N/A'),
        analyses.get('action_items', 'N/A'),
        format_for_sheet(listings, 10),
        "Generated"
    ]
    
    ws.append_row(row)
    print("✅ Enhanced report written")

def main():
    print("=" * 70)
    print("VOXMILL - PREMIUM MARKET INTELLIGENCE")
    print("=" * 70)
    
    try:
        ws = get_google_sheet()
        
        print(f"\nGenerating premium report for: {DEMO_CLIENT['name']}")
        
        # Aggregate and score
        priced_listings, all_data = aggregate_listings(DEMO_CLIENT)
        
        if len(priced_listings) < 3:
            print("⚠️ Insufficient data")
            return
        
        metrics = calc_metrics(all_data)
        print(f"\n📊 Enhanced Metrics:")
        print(f"   - {metrics['with_pricing']} listings | Avg ${metrics['avg_price']:,}")
        print(f"   - Avg ${metrics['avg_ppsf']}/sqft | {metrics['avg_dom']} days on market")
        print(f"   - {metrics['hot_deals']} hot deals | {metrics['stale_listings']} stale")
        
        # Generate analysis
        analyses = gen_enhanced_analysis(DEMO_CLIENT['city'], priced_listings, metrics)
        
        # Write enhanced sheet
        write_sheet(ws, DEMO_CLIENT, priced_listings, metrics, analyses)
        
        print("\n" + "=" * 70)
        print("✅ PREMIUM REPORT COMPLETE")
        print("=" * 70)
        print(f"\n🎯 Value-Add Features:")
        print(f"   - Deal Scoring (1-10): ✅")
        print(f"   - Price/SqFt Analysis: ✅")
        print(f"   - HOT/STALE Flags: ✅")
        print(f"   - Property URLs: ✅")
        print(f"   - Deal Alerts with Reasoning: ✅")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

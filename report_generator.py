import os
import json
import re
from datetime import datetime
from outscraper import ApiClient
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

# Demo client data
DEMO_CLIENT = {
    "name": "Miami Brokers Group",
    "contact": "Mike Diaz",
    "email": "demo@miamibrokersgroup.com",
    "city": "Miami",
    "state": "FL",
    "focus_areas": "Pinecrest, Coral Gables, Palmetto Bay",
    "property_type": "luxury"
}

def get_google_sheet():
    """Connect to Google Sheets using service account credentials"""
    print("Connecting to Google Sheets...")
    
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1
    
    return worksheet

def extract_price_from_text(text):
    """Extract price from description text"""
    if not text:
        return None
    
    # Look for price patterns like $1,250,000 or $1.25M
    price_patterns = [
        r'\$([0-9,]+)',  # $1,250,000
        r'\$([0-9.]+)M',  # $1.25M
        r'\$([0-9.]+)K',  # $850K
    ]
    
    for pattern in price_patterns:
        match = re.search(pattern, str(text))
        if match:
            price_str = match.group(1).replace(',', '')
            if 'M' in str(text):
                return int(float(price_str) * 1000000)
            elif 'K' in str(text):
                return int(float(price_str) * 1000)
            else:
                return int(float(price_str))
    
    return None

def scrape_market_data(city, state, focus_areas):
    """Pull real estate data using Outscraper with multiple queries"""
    print(f"Scraping market data for {city}, {state}...")
    
    outscraper_client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
    
    # Multiple search queries for better coverage
    search_queries = [
        f"{city} {focus_areas} luxury homes for sale",
        f"{city} {focus_areas} real estate luxury",
        f"{city} waterfront homes {focus_areas}",
        f"{city} {focus_areas} million dollar homes"
    ]
    
    all_properties = []
    seen_addresses = set()
    
    for query in search_queries:
        print(f"  Querying: {query}")
        
        try:
            results = outscraper_client.google_maps_search(
                query,
                limit=10,
                language='en',
                region='us'
            )
            
            # Flatten nested list if needed
            if results and isinstance(results[0], list):
                results = results[0]
            
            for result in results:
                address = result.get('full_address', 'N/A')
                
                # Skip duplicates
                if address in seen_addresses:
                    continue
                seen_addresses.add(address)
                
                # Extract data
                description = result.get('description', '')
                reviews_text = result.get('reviews_text', '')
                
                # Try to extract price
                price = extract_price_from_text(description) or extract_price_from_text(reviews_text)
                
                property_data = {
                    'name': result.get('name', 'N/A'),
                    'address': address,
                    'rating': result.get('rating', 'N/A'),
                    'reviews': result.get('reviews', 0),
                    'category': result.get('category', 'N/A'),
                    'phone': result.get('phone', 'N/A'),
                    'website': result.get('site', 'N/A'),
                    'description': description[:200] if description else 'N/A',
                    'price': price
                }
                
                all_properties.append(property_data)
                
        except Exception as e:
            print(f"  ⚠️ Error with query: {str(e)}")
            continue
    
    print(f"✅ Found {len(all_properties)} unique properties")
    return all_properties

def calculate_market_metrics(properties):
    """Calculate market health metrics from available data"""
    if not properties:
        return {}
    
    # Extract prices where available
    prices = [p['price'] for p in properties if p.get('price')]
    
    # Count properties with high ratings
    highly_rated = [p for p in properties if isinstance(p['rating'], (int, float)) and p['rating'] >= 4.5]
    
    metrics = {
        'total_listings': len(properties),
        'properties_with_pricing': len(prices),
        'avg_price': int(sum(prices) / len(prices)) if prices else None,
        'min_price': min(prices) if prices else None,
        'max_price': max(prices) if prices else None,
        'highly_rated_count': len(highly_rated),
        'avg_rating': round(sum([p['rating'] for p in properties if isinstance(p['rating'], (int, float))]) / len([p for p in properties if isinstance(p['rating'], (int, float))]), 1) if any(isinstance(p['rating'], (int, float)) for p in properties) else None
    }
    
    return metrics

def generate_insights(city, state, properties, metrics):
    """Generate AI insights using OpenAI"""
    print("Generating strategic insights...")
    
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    # Create comprehensive summary
    property_summary = "\n".join([
        f"- {p['name']}: {p['category']} | Rating: {p['rating']} ({p['reviews']} reviews) | {p['address']}" +
        (f" | ~${p['price']:,}" if p.get('price') else "")
        for p in properties[:25]
    ])
    
    market_context = f"""Market Overview for {city}, {state}:
- Total Active Entities: {metrics.get('total_listings', 0)}
- Highly Rated (4.5+): {metrics.get('highly_rated_count', 0)}
- Average Rating: {metrics.get('avg_rating', 'N/A')}"""
    
    if metrics.get('properties_with_pricing'):
        market_context += f"""
- Properties with Pricing Data: {metrics['properties_with_pricing']}
- Estimated Avg Price: ${metrics.get('avg_price', 0):,}
- Price Range: ${metrics.get('min_price', 0):,} - ${metrics.get('max_price', 0):,}"""
    
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are Voxmill Market Intelligence — an executive real estate analyst.
                    Analyze market data and provide 3 strategic insights:
                    
                    RAISE: [Specific pricing/positioning opportunities based on the data]
                    REDUCE: [Areas of market saturation or discount opportunities]
                    ROTATE: [Strategic marketing or inventory shifts needed]
                    
                    Be specific. Use actual data points. Keep each line under 20 words."""
                },
                {
                    "role": "user",
                    "content": f"""{market_context}

Market Entities Analyzed:
{property_summary}

Generate RAISE / REDUCE / ROTATE strategic insights for a luxury real estate brokerage."""
                }
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        insights = completion.choices[0].message.content.strip()
        print("✅ Insights generated")
        return insights
    
    except Exception as e:
        print(f"❌ Error generating insights: {str(e)}")
        return "Error generating insights - check OpenAI API key"

def format_properties_for_sheet(properties, count=10):
    """Format property data for Google Sheets"""
    formatted = []
    
    for i, p in enumerate(properties[:count], 1):
        price_str = f"~${p['price']:,}" if p.get('price') else "Price N/A"
        
        formatted.append(
            f"{i}. {p['name']}\n"
            f"   {p['address']}\n"
            f"   {p['category']} | Rating: {p['rating']} ({p['reviews']} reviews)\n"
            f"   {price_str}\n"
            f"   {p['description'][:100]}..."
        )
    
    return "\n\n".join(formatted)

def write_to_sheet(worksheet, client_data, properties, metrics, insights):
    """Write report data to Google Sheet"""
    print("Writing to Google Sheet...")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Format market metrics
    market_summary = f"""Total Entities Analyzed: {metrics.get('total_listings', 0)}
Highly Rated Properties: {metrics.get('highly_rated_count', 0)}
Average Rating: {metrics.get('avg_rating', 'N/A')}"""
    
    if metrics.get('properties_with_pricing'):
        market_summary += f"""
Properties with Pricing: {metrics['properties_with_pricing']}
Est. Avg Price: ${metrics.get('avg_price', 0):,}
Est. Price Range: ${metrics.get('min_price', 0):,} - ${metrics.get('max_price', 0):,}"""
    
    top_properties = format_properties_for_sheet(properties, 10)
    
    row = [
        timestamp,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        market_summary,
        insights,
        top_properties,
        "Generated"
    ]
    
    worksheet.append_row(row)
    print("✅ Report written to sheet")

def main():
    """Main execution function"""
    print("=" * 70)
    print("VOXMILL MARKET INTELLIGENCE REPORT GENERATOR")
    print("=" * 70)
    
    try:
        worksheet = get_google_sheet()
        
        # Add headers if needed
        if worksheet.row_count == 0 or worksheet.cell(1, 1).value == "":
            headers = [
                "Timestamp",
                "Client Name",
                "Contact Person",
                "Market",
                "Focus Areas",
                "Market Metrics",
                "Strategic Insights",
                "Top 10 Properties",
                "Status"
            ]
            worksheet.append_row(headers)
            print("✅ Headers added")
        
        print(f"\n{'=' * 70}")
        print(f"Generating report for: {DEMO_CLIENT['name']}")
        print(f"Market: {DEMO_CLIENT['city']}, {DEMO_CLIENT['state']}")
        print('=' * 70)
        
        # Scrape data with multiple queries
        properties = scrape_market_data(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            DEMO_CLIENT['focus_areas']
        )
        
        if not properties or len(properties) < 5:
            print("⚠️ Insufficient data - check Outscraper API key or search terms")
            return
        
        # Calculate metrics
        metrics = calculate_market_metrics(properties)
        print(f"\n📊 Market Metrics:")
        print(f"   Total Entities: {metrics.get('total_listings', 0)}")
        print(f"   Highly Rated: {metrics.get('highly_rated_count', 0)}")
        if metrics.get('avg_price'):
            print(f"   Est. Avg Price: ${metrics['avg_price']:,}")
        
        # Generate insights
        insights = generate_insights(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            properties,
            metrics
        )
        
        # Write to sheet
        write_to_sheet(worksheet, DEMO_CLIENT, properties, metrics, insights)
        
        print("\n" + "=" * 70)
        print("✅ REPORT GENERATION COMPLETE")
        print("=" * 70)
        print(f"\nView: https://docs.google.com/spreadsheets/d/{os.environ.get('GOOGLE_SHEET_ID')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()    
    # Open the sheet by ID
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1  # Use first sheet
    
    return worksheet

def scrape_realtor_data(city, state, property_type="for_sale", min_price=500000):
    """Pull real estate data using RapidAPI Realtor.com"""
    print(f"Pulling Realtor.com listings for {city}, {state}...")
    
    rapidapi_key = os.environ.get('RAPIDAPI_KEY')
    
    url = "https://realtor.p.rapidapi.com/properties/v3/list"
    
    headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": "realtor.p.rapidapi.com"
    }
    
    # Build query parameters
    querystring = {
        "limit": "30",
        "offset": "0",
        "postal_code": "",
        "status": property_type,
        "sort": "relevance",
        "city": city,
        "state_code": state,
        "price_min": str(min_price)
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        
        properties = []
        
        if 'data' in data and 'home_search' in data['data'] and 'results' in data['data']['home_search']:
            listings = data['data']['home_search']['results']
            
            for listing in listings:
                # Extract property details
                description = listing.get('description', {})
                location = listing.get('location', {})
                address = location.get('address', {})
                
                property_data = {
                    'address': address.get('line', 'N/A'),
                    'city': address.get('city', 'N/A'),
                    'state': address.get('state_code', 'N/A'),
                    'zip': address.get('postal_code', 'N/A'),
                    'price': listing.get('list_price', 'N/A'),
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
                
                properties.append(property_data)
            
            print(f"✅ Found {len(properties)} properties")
        else:
            print("⚠️ No properties found in response")
        
        return properties
    
    except Exception as e:
        print(f"❌ Error fetching Realtor.com data: {str(e)}")
        return []

def calculate_market_metrics(properties):
    """Calculate market health metrics"""
    if not properties:
        return {}
    
    prices = [p['price'] for p in properties if isinstance(p['price'], (int, float))]
    dom = [p['days_on_market'] for p in properties if isinstance(p['days_on_market'], (int, float))]
    
    metrics = {
        'total_listings': len(properties),
        'avg_price': int(sum(prices) / len(prices)) if prices else 0,
        'min_price': min(prices) if prices else 0,
        'max_price': max(prices) if prices else 0,
        'avg_days_on_market': int(sum(dom) / len(dom)) if dom else 0,
        'median_price': int(sorted(prices)[len(prices)//2]) if prices else 0
    }
    
    return metrics

def generate_insights(city, state, properties, metrics):
    """Generate AI insights using OpenAI"""
    print("Generating AI insights...")
    
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    # Create detailed summary for OpenAI
    property_summary = "\n".join([
        f"- ${p['price']:,} | {p['beds']}bed/{p['baths']}bath | {p['sqft']} sqft | {p['days_on_market']} days | {p['address']}, {p['city']}"
        for p in properties[:20] if isinstance(p['price'], (int, float))
    ])
    
    market_context = f"""Market Overview:
- Total Listings: {metrics.get('total_listings', 0)}
- Average Price: ${metrics.get('avg_price', 0):,}
- Price Range: ${metrics.get('min_price', 0):,} - ${metrics.get('max_price', 0):,}
- Average Days on Market: {metrics.get('avg_days_on_market', 0)} days
"""
    
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are Voxmill Market Intelligence — an executive-level real estate analyst. 
                    Analyze market data and provide 3 actionable insights with specific numbers:
                    
                    RAISE: [What prices should increase, cite specific properties or percentages]
                    REDUCE: [What should be discounted, cite specific properties or percentages]
                    ROTATE: [What strategy should shift, cite specific market data]
                    
                    Use actual numbers from the data. Be specific and quantitative."""
                },
                {
                    "role": "user",
                    "content": f"""Analyze this {city}, {state} luxury real estate market:

{market_context}

Top 20 Active Listings:
{property_summary}

Generate strategic insights: RAISE / REDUCE / ROTATE"""
                }
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        insights = completion.choices[0].message.content.strip()
        print("✅ Insights generated")
        return insights
    
    except Exception as e:
        print(f"❌ Error generating insights: {str(e)}")
        return "Error generating insights"

def format_properties_for_sheet(properties, count=10):
    """Format property data for Google Sheets"""
    formatted = []
    
    for i, p in enumerate(properties[:count], 1):
        price_str = f"${p['price']:,}" if isinstance(p['price'], (int, float)) else "N/A"
        bed_bath = f"{p['beds']}bed/{p['baths']}bath" if p['beds'] != 'N/A' else "N/A"
        sqft_str = f"{p['sqft']:,} sqft" if isinstance(p['sqft'], (int, float)) else "N/A"
        dom_str = f"{p['days_on_market']} days" if isinstance(p['days_on_market'], (int, float)) else "N/A"
        
        formatted.append(
            f"{i}. {price_str} - {bed_bath}\n"
            f"   {sqft_str} | {dom_str} on market\n"
            f"   {p['address']}, {p['city']}, {p['state']} {p['zip']}\n"
            f"   Type: {p['property_type']} | Built: {p['year_built']}"
        )
    
    return "\n\n".join(formatted)

def write_to_sheet(worksheet, client_data, properties, metrics, insights):
    """Write report data to Google Sheet"""
    print("Writing to Google Sheet...")
    
    # Format timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Format market metrics
    market_summary = f"""Total Listings: {metrics.get('total_listings', 0)}
Avg Price: ${metrics.get('avg_price', 0):,}
Median Price: ${metrics.get('median_price', 0):,}
Price Range: ${metrics.get('min_price', 0):,} - ${metrics.get('max_price', 0):,}
Avg Days on Market: {metrics.get('avg_days_on_market', 0)} days"""
    
    # Format top properties
    top_properties = format_properties_for_sheet(properties, 10)
    
    # Create row data
    row = [
        timestamp,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        market_summary,
        insights,
        top_properties,
        "Generated"
    ]
    
    # Append row to sheet
    worksheet.append_row(row)
    print("✅ Report written to sheet")

def main():
    """Main execution function"""
    print("=" * 60)
    print("VOXMILL MARKET INTELLIGENCE REPORT GENERATOR")
    print("=" * 60)
    
    try:
        # Connect to Google Sheet
        worksheet = get_google_sheet()
        
        # Check if headers exist
        if worksheet.row_count == 0 or worksheet.cell(1, 1).value == "":
            headers = [
                "Timestamp",
                "Client Name",
                "Contact Person",
                "Market",
                "Focus Areas",
                "Market Metrics",
                "Strategic Insights",
                "Top 10 Properties",
                "Status"
            ]
            worksheet.append_row(headers)
            print("✅ Headers added to sheet")
        
        # Generate report for demo client
        print(f"\n{'=' * 60}")
        print(f"Generating report for: {DEMO_CLIENT['name']}")
        print(f"Market: {DEMO_CLIENT['city']}, {DEMO_CLIENT['state']}")
        print('=' * 60)
        
        # Pull Realtor.com data
        properties = scrape_realtor_data(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            property_type="for_sale",
            min_price=500000  # Luxury threshold
        )
        
        if not properties:
            print("⚠️ No properties found - check API key or try different search")
            return
        
        # Calculate market metrics
        metrics = calculate_market_metrics(properties)
        print(f"\n📊 Market Metrics:")
        print(f"   Total Listings: {metrics.get('total_listings', 0)}")
        print(f"   Avg Price: ${metrics.get('avg_price', 0):,}")
        print(f"   Avg DOM: {metrics.get('avg_days_on_market', 0)} days")
        
        # Generate AI insights
        insights = generate_insights(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            properties,
            metrics
        )
        
        # Write to sheet
        write_to_sheet(worksheet, DEMO_CLIENT, properties, metrics, insights)
        
        print("\n" + "=" * 60)
        print("✅ REPORT GENERATION COMPLETE")
        print("=" * 60)
        print(f"\nView report: https://docs.google.com/spreadsheets/d/{os.environ.get('GOOGLE_SHEET_ID')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()

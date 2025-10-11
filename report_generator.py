import os
import json
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
    
    # Parse credentials from environment variable
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    
    # Set up credentials
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    
    # Connect to Google Sheets
    client = gspread.authorize(credentials)
    
    # Open the sheet by ID
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1  # Use first sheet
    
    return worksheet

def scrape_market_data(city, state, focus_areas, property_type):
    """Pull real estate data using Outscraper"""
    print(f"Scraping data for {city}, {state}...")
    
    outscraper_client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
    
    # Build search query
    query = f"{city} {focus_areas} luxury homes for sale"
    
    try:
        # Use Outscraper's Google Maps scraper
        results = outscraper_client.google_maps_search(
            query,
            limit=30,
            language='en',
            region='us'
        )
        
        # Outscraper returns nested list - flatten it
        if results and isinstance(results[0], list):
            results = results[0]
        
        # Extract property data
        properties = []
        for result in results:
            property_data = {
                'name': result.get('name', 'N/A'),
                'address': result.get('full_address', 'N/A'),
                'rating': result.get('rating', 'N/A'),
                'reviews': result.get('reviews', 0),
                'category': result.get('category', 'N/A'),
                'phone': result.get('phone', 'N/A'),
                'website': result.get('site', 'N/A')
            }
            properties.append(property_data)
        
        print(f"Found {len(properties)} properties")
        return properties
    
    except Exception as e:
        print(f"Error scraping data: {str(e)}")
        return []

def generate_insights(city, state, properties):
    """Generate AI insights using OpenAI"""
    print("Generating AI insights...")
    
    openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    
    # Create summary for OpenAI
    property_summary = "\n".join([
        f"- {p['name']} at {p['address']}, Rating: {p['rating']}, Reviews: {p['reviews']}, Category: {p['category']}"
        for p in properties[:20]
    ])
    
    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are Voxmill Market Intelligence — an executive-level real estate analyst. 
                    Your job is to analyze market data and provide 3 actionable insights in this exact format:
                    
                    RAISE: [What prices/rents should increase and why - max 15 words]
                    REDUCE: [What should be discounted or repositioned - max 15 words]
                    ROTATE: [What inventory or marketing strategy should shift - max 15 words]
                    
                    Be specific and data-driven. No fluff."""
                },
                {
                    "role": "user",
                    "content": f"""Analyze this {city}, {state} luxury real estate market snapshot with {len(properties)} listings:
                    
{property_summary}

Generate the 3-line Voxmill insight: RAISE / REDUCE / ROTATE"""
                }
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        insights = completion.choices[0].message.content.strip()
        print("Insights generated")
        return insights
    
    except Exception as e:
        print(f"Error generating insights: {str(e)}")
        return "Error generating insights"

def write_to_sheet(worksheet, client_data, properties, insights):
    """Write report data to Google Sheet"""
    print("Writing to Google Sheet...")
    
    # Format timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Format properties as text
    top_properties = "\n\n".join([
        f"{i+1}. {p['name']}\n   {p['address']}\n   Rating: {p['rating']} ({p['reviews']} reviews)\n   {p['category']}"
        for i, p in enumerate(properties[:10])
    ])
    
    # Create row data
    row = [
        timestamp,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        len(properties),
        insights,
        top_properties,
        "Generated"
    ]
    
    # Append row to sheet
    worksheet.append_row(row)
    print("✅ Report written to sheet")

def main():
    """Main execution function"""
    print("=" * 50)
    print("VOXMILL REPORT GENERATOR")
    print("=" * 50)
    
    try:
        # Connect to Google Sheet
        worksheet = get_google_sheet()
        
        # Check if headers exist, if not add them
        if worksheet.row_count == 0 or worksheet.cell(1, 1).value == "":
            headers = [
                "Timestamp",
                "Client Name",
                "Contact Person",
                "Market",
                "Focus Areas",
                "Properties Analyzed",
                "Insights (RAISE/REDUCE/ROTATE)",
                "Top 10 Properties",
                "Status"
            ]
            worksheet.append_row(headers)
            print("Headers added to sheet")
        
        # Generate report for demo client
        print(f"\nGenerating report for: {DEMO_CLIENT['name']}")
        
        # Scrape market data
        properties = scrape_market_data(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            DEMO_CLIENT['focus_areas'],
            DEMO_CLIENT['property_type']
        )
        
        if not properties:
            print("⚠️ No properties found, using placeholder data")
            properties = [{"name": "Sample Property", "address": "Miami, FL", "rating": "N/A", "reviews": 0, "category": "Real Estate", "phone": "N/A", "website": "N/A"}]
        
        # Generate AI insights
        insights = generate_insights(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            properties
        )
        
        # Write to sheet
        write_to_sheet(worksheet, DEMO_CLIENT, properties, insights)
        
        print("\n" + "=" * 50)
        print("✅ REPORT GENERATION COMPLETE")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()

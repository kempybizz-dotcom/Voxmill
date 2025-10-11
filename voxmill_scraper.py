import os
from outscraper import ApiClient
from openai import OpenAI

# Initialize clients
outscraper_client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

def generate_market_report(city, state, property_type):
    """
    Generate market intelligence report for a given city
    
    Args:
        city: Target city (e.g., "Miami")
        state: State code (e.g., "FL")
        property_type: "luxury", "mid-range", or "all"
    
    Returns:
        Dictionary with market data and insights
    """
    
    # Step 1: Pull Zillow data via Outscraper
    print(f"Pulling data for {city}, {state}...")
    
    # Build search query based on property type
    if property_type == "luxury":
        query = f"{city}, {state} luxury homes for sale"
    elif property_type == "mid-range":
        query = f"{city}, {state} homes for sale $500k-$1m"
    else:
        query = f"{city}, {state} homes for sale"
    
    try:
        # Use Outscraper's Google Maps scraper (works for real estate listings)
        # Note: You can also use their dedicated Zillow scraper if you have access
        results = outscraper_client.google_maps_search(
            query,
            limit=10,
            language='en',
            region='us'
        )
        
        # Extract property data
        properties = []
        for result in results:
            property_data = {
                'name': result.get('name', 'N/A'),
                'address': result.get('full_address', 'N/A'),
                'rating': result.get('rating', 'N/A'),
                'reviews': result.get('reviews', 0),
                'phone': result.get('phone', 'N/A'),
                'website': result.get('site', 'N/A'),
                'category': result.get('category', 'N/A')
            }
            properties.append(property_data)
        
        # Step 2: Generate AI insights
        print("Generating insights...")
        
        # Create summary for OpenAI
        property_summary = "\n".join([
            f"- {p['name']} at {p['address']}, Rating: {p['rating']}, Reviews: {p['reviews']}"
            for p in properties
        ])
        
        # Call OpenAI for market analysis
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are Voxmill Market Intelligence — an executive-level real estate analyst. 
                    Your job is to analyze market data and provide 3 actionable insights in this exact format:
                    
                    RAISE: [What prices/rents should increase and why]
                    REDUCE: [What should be discounted or repositioned]
                    ROTATE: [What inventory or marketing strategy should shift]
                    
                    Keep each line under 20 words. Be specific and data-driven. No fluff."""
                },
                {
                    "role": "user",
                    "content": f"""Analyze this {city}, {state} real estate market snapshot:
                    
{property_summary}

Generate the 3-line Voxmill insight: RAISE / REDUCE / ROTATE"""
                }
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        insights = completion.choices[0].message.content.strip()
        
        # Step 3: Return structured report data
        return {
            'market': f"{city}, {state}",
            'property_count': len(properties),
            'properties': properties,
            'insights': insights,
            'report_date': 'Week of Oct 11, 2025'
        }
    
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        raise Exception(f"Failed to generate report: {str(e)}")

def format_report_text(report_data):
    """
    Format report data as plain text for email/PDF generation
    """
    text = f"""
VOXMILL MARKET INTELLIGENCE
{report_data['market']} Edition
{report_data['report_date']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MARKET SNAPSHOT
Properties Analyzed: {report_data['property_count']}

{report_data['insights']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TOP PROPERTIES:
"""
    
    for i, prop in enumerate(report_data['properties'][:5], 1):
        text += f"\n{i}. {prop['name']}\n   {prop['address']}\n   Rating: {prop['rating']} ({prop['reviews']} reviews)\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\nPowered by Voxmill Market Intelligence\n"
    
    return text

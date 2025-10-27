"""
VOXMILL MARKET INTELLIGENCE — PRODUCTION VERSION
=================================================
Automated London luxury real estate intelligence system
Outputs: CSV data + JSON analytics

NO GOOGLE DEPENDENCIES
"""

import os
import json
import re
from datetime import datetime
import requests
from outscraper import ApiClient
from openai import OpenAI
import csv
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

RAPIDAPI_KEY = os.environ.get('RAPIDAPI_KEY', '1440de56aamsh945d6c41f441399p1af6adjsne2d964758775')
OUTSCRAPER_API_KEY = os.environ.get('OUTSCRAPER_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

CLIENT = {
    "name": "London Property Intelligence",
    "city": "London",
    "focus_areas": ["Mayfair", "Knightsbridge", "Chelsea", "Kensington"],
    "currency": "£"
}

OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/tmp')

# ============================================================================
# DATA COLLECTION — ZOOPLA API
# ============================================================================

def collect_zoopla_data(areas, max_per_area=25):
    """Collect London real estate from Zoopla via RapidAPI"""
    print(f"\n📊 COLLECTING DATA: London Real Estate")
    print(f"   Areas: {', '.join(areas)}")
    
    all_listings = []
    
    url = "https://zoopla.p.rapidapi.com/properties/list"
    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": "zoopla.p.rapidapi.com"
    }
    
    for area in areas:
        params = {
            "area": f"{area}, London",
            "category": "residential",
            "order_by": "age",
            "ordering": "descending",
            "page_number": "1",
            "page_size": str(max_per_area)
        }
        
        try:
            print(f"   → {area}...", end=" ")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                listings = data.get('listing', [])
                
                for prop in listings:
                    price = prop.get('price', 0)
                    if isinstance(price, str):
                        price = int(re.sub(r'[^\d]', '', price)) if re.search(r'\d', price) else 0
                    
                    sqft = prop.get('floor_area', {}).get('max_floor_area', {}).get('square_feet', 0)
                    
                    if price > 0:
                        all_listings.append({
                            'source': 'Zoopla',
                            'address': prop.get('displayable_address', 'N/A'),
                            'city': 'London',
                            'area': area,
                            'price': price,
                            'beds': prop.get('num_bedrooms', 0),
                            'baths': prop.get('num_bathrooms', 0),
                            'sqft': sqft if sqft else 0,
                            'price_per_sqft': round(price / sqft, 2) if sqft else 0,
                            'days_on_market': prop.get('first_published_date', 'N/A'),
                            'property_type': prop.get('property_type', 'N/A'),
                            'url': prop.get('details_url', '')
                        })
                
                print(f"✅ {len(listings)} properties")
                time.sleep(1)  # Rate limit protection
                
            else:
                print(f"❌ Status {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    
    print(f"\n   TOTAL: {len(all_listings)} properties collected")
    return all_listings

# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

def calculate_metrics(listings):
    """Calculate market metrics and score properties"""
    print(f"\n📊 ANALYZING MARKET DATA")
    
    priced = [l for l in listings if l.get('price', 0) > 0]
    
    if not priced:
        return None
    
    # Core metrics
    prices = [l['price'] for l in priced]
    ppsf_vals = [l['price_per_sqft'] for l in priced if l.get('price_per_sqft', 0) > 0]
    
    avg_price = int(sum(prices) / len(prices))
    avg_ppsf = int(sum(ppsf_vals) / len(ppsf_vals)) if ppsf_vals else 0
    median_ppsf = int(sorted(ppsf_vals)[len(ppsf_vals)//2]) if ppsf_vals else 0
    
    print(f"   → Avg Price: £{avg_price:,}")
    print(f"   → Avg £/sqft: £{avg_ppsf}")
    
    # Score each property
    for listing in priced:
        score = 5.0
        ppsf = listing.get('price_per_sqft', 0)
        
        # Price/sqft scoring
        if ppsf > 0 and avg_ppsf > 0:
            if ppsf < avg_ppsf * 0.8:
                score += 2.5  # Significantly underpriced
            elif ppsf < avg_ppsf * 0.9:
                score += 1.5  # Moderately underpriced
            elif ppsf > avg_ppsf * 1.2:
                score -= 1.5  # Overpriced
            elif ppsf > avg_ppsf * 1.1:
                score -= 0.5  # Slightly overpriced
        
        listing['deal_score'] = min(max(round(score, 1), 1.0), 10.0)
        
        # Add flags
        if score >= 8.0:
            listing['flag'] = '🔥 HOT DEAL'
        elif ppsf > 0 and avg_ppsf > 0 and ppsf < avg_ppsf * 0.85:
            listing['flag'] = '💰 UNDERPRICED'
        else:
            listing['flag'] = '—'
    
    # Sort by deal score
    priced.sort(key=lambda x: x.get('deal_score', 0), reverse=True)
    
    hot_deals = len([l for l in priced if l.get('deal_score', 0) >= 8.0])
    
    print(f"   → Hot Deals: {hot_deals} (score 8+)")
    
    metrics = {
        'total_listings': len(listings),
        'priced_listings': len(priced),
        'avg_price': avg_price,
        'min_price': min(prices),
        'max_price': max(prices),
        'avg_ppsf': avg_ppsf,
        'median_ppsf': median_ppsf,
        'hot_deals': hot_deals,
        'properties': priced
    }
    
    return metrics

# ============================================================================
# AI INTELLIGENCE GENERATION
# ============================================================================

def generate_ai_intelligence(metrics, client):
    """Generate strategic intelligence using OpenAI"""
    print(f"\n🧠 GENERATING AI INTELLIGENCE")
    
    if not OPENAI_API_KEY:
        print("   ⚠️ No OpenAI API key - skipping intelligence generation")
        return {
            'bluf': 'OpenAI API key not configured',
            'opportunities': 'N/A',
            'risks': 'N/A',
            'action_triggers': 'N/A'
        }
    
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        
        top_properties = metrics['properties'][:5]
        
        context = f"""MARKET: {client['city']} Luxury Real Estate
FOCUS AREAS: {', '.join(client['focus_areas'])}
TOTAL LISTINGS: {metrics['priced_listings']}
AVG PRICE: {client['currency']}{metrics['avg_price']:,}
AVG PRICE/SQFT: {client['currency']}{metrics['avg_ppsf']}
HOT DEALS: {metrics['hot_deals']} (score 8+/10)

TOP 5 PROPERTIES:
""" + "\n".join([
            f"- {client['currency']}{p['price']:,} | {p.get('beds', 0)}bd/{p.get('baths', 0)}ba | {p.get('area', 'N/A')} | Score: {p.get('deal_score', 0)}/10"
            for p in top_properties
        ])
        
        intelligence = {}
        
        # 1. BLUF (Bottom Line Up Front)
        print("   → Generating BLUF...", end=" ")
        prompt_bluf = f"""{context}

Provide a military-style BLUF (Bottom Line Up Front) summary in exactly 3 lines:

INSIGHT: [One sentence: What's the single most important market insight?]
ACTION: [One sentence: What should be done immediately?]
RISK: [One sentence: What's the biggest threat?]"""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a senior real estate market analyst providing executive briefings."},
                {"role": "user", "content": prompt_bluf}
            ],
            temperature=0.6,
            max_tokens=150
        )
        intelligence['bluf'] = response.choices[0].message.content.strip()
        print("✅")
        
        # 2. Top Opportunities
        print("   → Generating opportunities...", end=" ")
        prompt_opps = f"""{context}

Identify the top 3 opportunities, ranked by urgency:

1. IMMEDIATE (act this week): [specific property or strategy with £ amount]
2. STRATEGIC (30 days): [positioning move with rationale]
3. LONG-TERM (90 days): [market shift opportunity]"""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an investment strategist identifying high-ROI opportunities."},
                {"role": "user", "content": prompt_opps}
            ],
            temperature=0.7,
            max_tokens=250
        )
        intelligence['opportunities'] = response.choices[0].message.content.strip()
        print("✅")
        
        # 3. Risk Assessment
        print("   → Generating risk assessment...", end=" ")
        prompt_risks = f"""{context}

Provide 3 specific market risks:

PRICING RISK: [What price movements threaten deals?]
VELOCITY RISK: [Is inventory moving too fast/slow?]
COMPETITIVE RISK: [What are competitors doing better?]"""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a risk analyst identifying market threats."},
                {"role": "user", "content": prompt_risks}
            ],
            temperature=0.7,
            max_tokens=200
        )
        intelligence['risks'] = response.choices[0].message.content.strip()
        print("✅")
        
        # 4. Action Triggers
        print("   → Generating action triggers...", end=" ")
        prompt_triggers = f"""Based on {metrics['hot_deals']} hot deals in {client['city']}, provide 3 IF-THEN action triggers:

IF [specific market condition with numbers] THEN [specific tactical action]

Example: IF hot deals drop below 5 THEN increase outreach budget 20%"""
        
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a tactical advisor creating actionable decision frameworks."},
                {"role": "user", "content": prompt_triggers}
            ],
            temperature=0.7,
            max_tokens=150
        )
        intelligence['action_triggers'] = response.choices[0].message.content.strip()
        print("✅")
        
        print("   ✅ AI intelligence generated")
        return intelligence
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return {
            'bluf': f'Error: {str(e)}',
            'opportunities': 'Error generating intelligence',
            'risks': 'Error generating intelligence',
            'action_triggers': 'Error generating intelligence'
        }

# ============================================================================
# DATA EXPORT
# ============================================================================

def export_data(metrics, intelligence, client):
    """Export data to CSV and JSON files"""
    print(f"\n💾 EXPORTING DATA")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Export summary report (CSV)
    report_path = f"{OUTPUT_DIR}/voxmill_report.csv"
    report = {
        'Timestamp': timestamp,
        'Client': client['name'],
        'Market': f"{client['city']} ({', '.join(client['focus_areas'])})",
        'Total Listings': metrics['total_listings'],
        'Avg Price': f"{client['currency']}{metrics['avg_price']:,}",
        'Price Range': f"{client['currency']}{metrics['min_price']:,} - {client['currency']}{metrics['max_price']:,}",
        'Avg $/SqFt': f"{client['currency']}{metrics['avg_ppsf']}",
        'Hot Deals': metrics['hot_deals'],
        'BLUF': intelligence.get('bluf', 'N/A'),
        'Top Opportunities': intelligence.get('opportunities', 'N/A'),
        'Risk Assessment': intelligence.get('risks', 'N/A'),
        'Action Triggers': intelligence.get('action_triggers', 'N/A')
    }
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=report.keys())
        writer.writeheader()
        writer.writerow(report)
    
    print(f"   ✅ Report: {report_path}")
    
    # 2. Export properties (CSV)
    properties_path = f"{OUTPUT_DIR}/voxmill_properties.csv"
    
    if metrics['properties']:
        with open(properties_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['address', 'area', 'price', 'beds', 'baths', 'sqft', 
                         'price_per_sqft', 'deal_score', 'flag', 'url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for prop in metrics['properties'][:50]:  # Top 50 properties
                writer.writerow({
                    'address': prop.get('address', ''),
                    'area': prop.get('area', ''),
                    'price': prop.get('price', 0),
                    'beds': prop.get('beds', 0),
                    'baths': prop.get('baths', 0),
                    'sqft': prop.get('sqft', 0),
                    'price_per_sqft': prop.get('price_per_sqft', 0),
                    'deal_score': prop.get('deal_score', 0),
                    'flag': prop.get('flag', ''),
                    'url': prop.get('url', '')
                })
        
        print(f"   ✅ Properties: {properties_path}")
    
    # 3. Export full data (JSON)
    json_path = f"{OUTPUT_DIR}/voxmill_data.json"
    
    full_data = {
        'timestamp': timestamp,
        'client': client,
        'metrics': {
            'total_listings': metrics['total_listings'],
            'priced_listings': metrics['priced_listings'],
            'avg_price': metrics['avg_price'],
            'min_price': metrics['min_price'],
            'max_price': metrics['max_price'],
            'avg_ppsf': metrics['avg_ppsf'],
            'median_ppsf': metrics['median_ppsf'],
            'hot_deals': metrics['hot_deals']
        },
        'intelligence': intelligence,
        'top_properties': metrics['properties'][:20]  # Top 20 for JSON
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(full_data, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ JSON: {json_path}")
    
    return report_path, properties_path, json_path

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution flow"""
    print("\n" + "="*70)
    print("VOXMILL MARKET INTELLIGENCE — DATA COLLECTION")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: {CLIENT['city']} Luxury Real Estate")
    
    try:
        # Step 1: Collect data
        listings = collect_zoopla_data(CLIENT['focus_areas'])
        
        if len(listings) < 3:
            print("\n❌ INSUFFICIENT DATA")
            print("   Collected fewer than 3 properties. Check API credentials.")
            return
        
        # Step 2: Analyze
        metrics = calculate_metrics(listings)
        
        if not metrics:
            print("\n❌ ANALYSIS FAILED")
            return
        
        # Step 3: Generate intelligence
        intelligence = generate_ai_intelligence(metrics, CLIENT)
        
        # Step 4: Export
        report_path, props_path, json_path = export_data(metrics, intelligence, CLIENT)
        
        # Summary
        print("\n" + "="*70)
        print("✅ DATA COLLECTION COMPLETE")
        print("="*70)
        print(f"\n📊 SUMMARY:")
        print(f"   • {metrics['priced_listings']} properties analyzed")
        print(f"   • Avg: £{metrics['avg_price']:,}")
        print(f"   • {metrics['hot_deals']} hot deals (score 8+)")
        print(f"\n📁 OUTPUT FILES:")
        print(f"   • {report_path}")
        print(f"   • {props_path}")
        print(f"   • {json_path}")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()

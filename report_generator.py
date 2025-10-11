import os
import json
import re
from datetime import datetime
from outscraper import ApiClient
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

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
    print("Connecting to Google Sheets...")
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.sheet1

def extract_price(text):
    if not text:
        return None
    patterns = [r'\$([0-9,]+)', r'\$([0-9.]+)M', r'\$([0-9.]+)K']
    for pattern in patterns:
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

def scrape_data(city, state, focus_areas):
    print(f"Scraping {city}, {state}...")
    client = ApiClient(api_key=os.environ.get('OUTSCRAPER_API_KEY'))
    queries = [
        f"{city} {focus_areas} luxury homes for sale",
        f"{city} {focus_areas} real estate luxury",
        f"{city} waterfront homes {focus_areas}"
    ]
    all_props = []
    seen = set()
    for q in queries:
        print(f"  Query: {q}")
        try:
            results = client.google_maps_search(q, limit=10, language='en', region='us')
            if results and isinstance(results[0], list):
                results = results[0]
            for r in results:
                addr = r.get('full_address', 'N/A')
                if addr in seen:
                    continue
                seen.add(addr)
                desc = r.get('description', '')
                price = extract_price(desc)
                all_props.append({
                    'name': r.get('name', 'N/A'),
                    'address': addr,
                    'rating': r.get('rating', 'N/A'),
                    'reviews': r.get('reviews', 0),
                    'category': r.get('category', 'N/A'),
                    'description': desc[:200] if desc else 'N/A',
                    'price': price
                })
        except Exception as e:
            print(f"  Error: {e}")
    print(f"Found {len(all_props)} properties")
    return all_props

def calc_metrics(props):
    if not props:
        return {}
    prices = [p['price'] for p in props if p.get('price')]
    rated = [p for p in props if isinstance(p['rating'], (int, float)) and p['rating'] >= 4.5]
    ratings = [p['rating'] for p in props if isinstance(p['rating'], (int, float))]
    return {
        'total': len(props),
        'with_pricing': len(prices),
        'avg_price': int(sum(prices) / len(prices)) if prices else None,
        'min_price': min(prices) if prices else None,
        'max_price': max(prices) if prices else None,
        'highly_rated': len(rated),
        'avg_rating': round(sum(ratings) / len(ratings), 1) if ratings else None
    }

def gen_insights(city, state, props, metrics):
    print("Generating insights...")
    client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    summary = "\n".join([
        f"- {p['name']}: {p['category']} | {p['rating']} ({p['reviews']} reviews)" +
        (f" | ${p['price']:,}" if p.get('price') else "")
        for p in props[:20]
    ])
    context = f"Total: {metrics.get('total', 0)} | Highly Rated: {metrics.get('highly_rated', 0)}"
    if metrics.get('with_pricing'):
        context += f" | Avg Price: ${metrics.get('avg_price', 0):,}"
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are Voxmill Market Intelligence. Analyze data and provide: RAISE / REDUCE / ROTATE insights. Each line max 20 words."},
                {"role": "user", "content": f"{context}\n\n{summary}\n\nGenerate RAISE / REDUCE / ROTATE"}
            ],
            temperature=0.7,
            max_tokens=200
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"

def format_props(props, n=10):
    out = []
    for i, p in enumerate(props[:n], 1):
        price = f"${p['price']:,}" if p.get('price') else "N/A"
        out.append(f"{i}. {p['name']}\n   {p['address']}\n   {p['category']} | {p['rating']} ({p['reviews']} reviews)\n   Price: {price}")
    return "\n\n".join(out)

def write_sheet(ws, client, props, metrics, insights):
    print("Writing to sheet...")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = f"Total: {metrics.get('total', 0)} | Highly Rated: {metrics.get('highly_rated', 0)}"
    if metrics.get('avg_price'):
        summary += f"\nAvg Price: ${metrics['avg_price']:,} | Range: ${metrics['min_price']:,}-${metrics['max_price']:,}"
    row = [ts, client['name'], client['contact'], f"{client['city']}, {client['state']}", 
           client['focus_areas'], summary, insights, format_props(props), "Generated"]
    ws.append_row(row)
    print("Done")

def main():
    print("=" * 60)
    print("VOXMILL REPORT GENERATOR")
    print("=" * 60)
    try:
        ws = get_google_sheet()
        if ws.row_count == 0 or not ws.cell(1, 1).value:
            ws.append_row(["Timestamp", "Client", "Contact", "Market", "Focus Areas", 
                          "Metrics", "Insights", "Top Properties", "Status"])
        print(f"\nGenerating for: {DEMO_CLIENT['name']}")
        props = scrape_data(DEMO_CLIENT['city'], DEMO_CLIENT['state'], DEMO_CLIENT['focus_areas'])
        if len(props) < 5:
            print("Not enough data")
            return
        metrics = calc_metrics(props)
        print(f"Metrics: {metrics['total']} total, {metrics.get('highly_rated', 0)} highly rated")
        insights = gen_insights(DEMO_CLIENT['city'], DEMO_CLIENT['state'], props, metrics)
        write_sheet(ws, DEMO_CLIENT, props, metrics, insights)
        print("\n" + "=" * 60)
        print("COMPLETE")
        print("=" * 60)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

import os
import json
from datetime import datetime
import requests
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials
# from outscraper import ApiClient  # still available if you want enrichment later

# ──────────────────────────────────────────────────────────────────────────────
# Demo client data (unchanged)
DEMO_CLIENT = {
    "name": "Miami Brokers Group",
    "contact": "Mike Diaz",
    "email": "demo@miamibrokersgroup.com",
    "city": "Miami",
    "state": "FL",
    "focus_areas": "Pinecrest, Coral Gables, Palmetto Bay",
    "property_type": "luxury"
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers

def _require_env(name: str) -> str:
    """Fetch required environment variable or raise a clear error."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def get_google_sheet():
    """Connect to Google Sheets using service account credentials."""
    print("Connecting to Google Sheets...")

    creds_json = _require_env('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    client = gspread.authorize(credentials)
    sheet_id = _require_env('GOOGLE_SHEET_ID')

    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1  # Use first sheet
    return worksheet

# ──────────────────────────────────────────────────────────────────────────────
# Realtor.com (RapidAPI) integration

def fetch_realtor_listings(city: str, state: str, focus_areas: str = "", property_type: str = "luxury", limit: int = 30):
    """
    Pull real estate listings using Realtor.com via RapidAPI.
    - Expects RAPIDAPI_KEY in env.
    - Optional RAPIDAPI_HOST (defaults to Realtor.com data host).
    """
    print(f"Fetching Realtor.com listings for {city}, {state}...")

    rapidapi_key = _require_env("RAPIDAPI_KEY")
    rapidapi_host = os.environ.get(
        "RAPIDAPI_HOST",
        "realtor-com-real-estate-data.p.rapidapi.com"
    )

    # Primary endpoint (commonly available on RapidAPI for Realtor.com data)
    url = f"https://{rapidapi_host}/v2/property"

    # Base query params — keep it simple/robust for v1
    params = {
        "city": city,
        "state_code": state,
        "limit": str(limit),
        # Hints: you can experiment with these if your chosen API supports them:
        # "sort": "newest",
        # "offset": "0",
    }

    # If they're chasing "luxury", you can try adding min price hints if the
    # RapidAPI provider supports it. Kept commented to avoid 400s on some plans.
    # if property_type.lower() == "luxury":
    #     params["price_min"] = "1500000"

    # Optionally fold focus_areas into a search context (depends on provider support)
    if focus_areas:
        params["areas"] = focus_areas

    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": rapidapi_host,
    }

    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code != 200:
            print(f"⚠️ Realtor API HTTP {r.status_code}: {r.text[:300]}")
            return []

        data = r.json()
        listings = _extract_listings_from_realtor_payload(data)
        print(f"Found {len(listings)} listings from Realtor.com")
        return listings

    except requests.Timeout:
        print("⚠️ Realtor API request timed out.")
        return []
    except Exception as e:
        print(f"Error calling Realtor API: {e}")
        return []

def _extract_listings_from_realtor_payload(payload: dict):
    """
    Realtor providers on RapidAPI vary in structure.
    This tries a few common shapes and normalizes to a list of dicts with:
    id, address, price, beds, baths, sqft, days_on_market, list_date, agent_name, url
    """
    candidates = []

    # Common shapes to probe:
    # 1) {'data': {'properties': [...]}}
    # 2) {'properties': [...]}
    # 3) {'data': {... 'home_search': {'results': [...]}}}
    props = None

    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], dict):
            d = payload["data"]
            if "properties" in d and isinstance(d["properties"], list):
                props = d["properties"]
            elif "home_search" in d and isinstance(d["home_search"], dict) and "results" in d["home_search"]:
                props = d["home_search"]["results"]
        if props is None and "properties" in payload and isinstance(payload["properties"], list):
            props = payload["properties"]

    if not props or not isinstance(props, list):
        return []

    for item in props:
        # Flexible extraction guards
        address = (
            item.get("address", {}).get("line")
            or item.get("location", {}).get("address", {}).get("line")
            or item.get("address", {}).get("city")
            or "N/A"
        )
        city = (
            item.get("address", {}).get("city")
            or item.get("location", {}).get("address", {}).get("city")
        )
        state = (
            item.get("address", {}).get("state_code")
            or item.get("location", {}).get("address", {}).get("state_code")
        )
        postal_code = (
            item.get("address", {}).get("postal_code")
            or item.get("location", {}).get("address", {}).get("postal_code")
        )

        price = item.get("price") or item.get("list_price") or item.get("list_price_min")
        beds = item.get("beds") or item.get("description", {}).get("beds")
        baths = item.get("baths") or item.get("description", {}).get("baths")
        sqft = item.get("building_size", {}).get("size") or item.get("description", {}).get("sqft")
        dom = (item.get("dom") or item.get("days_on_market") or
               (item.get("list_date") and _days_since_date(item.get("list_date"))) or None)
        list_date = item.get("list_date") or item.get("date", {}).get("list_date")

        # agent / broker
        agent_name = None
        if "agents" in item and isinstance(item["agents"], list) and item["agents"]:
            agent_name = item["agents"][0].get("name") or item["agents"][0].get("agent_name")
        elif "advertisers" in item and isinstance(item["advertisers"], list) and item["advertisers"]:
            agent_name = item["advertisers"][0].get("name")

        # URL if present
        url = (item.get("href") or item.get("permalink") or
               item.get("property_url") or item.get("rdc_web_url"))

        full_address = ", ".join([x for x in [address, city, state, postal_code] if x])

        candidates.append({
            "id": item.get("property_id") or item.get("listing_id") or item.get("id"),
            "address": full_address or "N/A",
            "price": price,
            "beds": beds,
            "baths": baths,
            "sqft": sqft,
            "days_on_market": dom,
            "list_date": list_date,
            "agent_name": agent_name,
            "url": url
        })

    return candidates

def _days_since_date(datestr: str):
    try:
        # many feeds use ISO-like dates
        dt = datetime.fromisoformat(datestr.replace("Z", "+00:00"))
        return (datetime.now(dt.tzinfo) - dt).days
    except Exception:
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Insights

def generate_insights(city, state, listings):
    """Generate AI insights using OpenAI over real-estate fields."""
    print("Generating AI insights...")

    openai_client = OpenAI(api_key=_require_env('OPENAI_API_KEY'))

    # Create concise summary for OpenAI
    # include price/beds/baths/DOM to drive the RAISE/REDUCE/ROTATE
    lines = []
    for L in listings[:20]:
        line = (
            f"- {L.get('address','N/A')} | "
            f"Price: {L.get('price','?')} | "
            f"{L.get('beds','?')}bd/{L.get('baths','?')}ba | "
            f"SQFT: {L.get('sqft','?')} | "
            f"DOM: {L.get('days_on_market','?')}"
        )
        lines.append(line)
    listing_summary = "\n".join(lines) if lines else "No listings available."

    try:
        completion = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are Voxmill Market Intelligence — an executive-level real estate analyst. "
                        "Provide 3 actionable lines based on inventory context using exactly this format:\n\n"
                        "RAISE: [max 15 words]\n"
                        "REDUCE: [max 15 words]\n"
                        "ROTATE: [max 15 words]\n\n"
                        "Aim at pricing, concessions, and mix shifts (e.g., smaller units, staging quality, promo tempo). "
                        "Be specific and data-driven. No fluff."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Analyze the {city}, {state} luxury listings snapshot with {len(listings)} listings:\n\n"
                        f"{listing_summary}\n\n"
                        "Generate the 3-line Voxmill insight (RAISE / REDUCE / ROTATE)."
                    )
                }
            ],
            temperature=0.4,
            max_tokens=180
        )

        insights = completion.choices[0].message.content.strip()
        print("Insights generated")
        return insights

    except Exception as e:
        print(f"Error generating insights: {str(e)}")
        return "Error generating insights"

# ──────────────────────────────────────────────────────────────────────────────
# Sheet writer

def write_to_sheet(worksheet, client_data, listings, insights):
    """Write report data to Google Sheet."""
    print("Writing to Google Sheet...")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Format listings
    top_listings = "\n\n".join([
        (
            f"{i+1}. {L.get('address','N/A')}\n"
            f"   Price: {L.get('price','?')} | "
            f"{L.get('beds','?')}bd/{L.get('baths','?')}ba | "
            f"SQFT: {L.get('sqft','?')} | "
            f"DOM: {L.get('days_on_market','?')}\n"
            f"   Agent: {L.get('agent_name','N/A')}\n"
            f"   {L.get('url','') or ''}"
        ).strip()
        for i, L in enumerate(listings[:10])
    ])

    row = [
        timestamp,
        client_data['name'],
        client_data['contact'],
        f"{client_data['city']}, {client_data['state']}",
        client_data['focus_areas'],
        len(listings),
        insights,
        top_listings,
        "Generated"
    ]

    # Ensure headers exist once
    first_cell = worksheet.cell(1, 1).value
    if not first_cell:
        headers = [
            "Timestamp",
            "Client Name",
            "Contact Person",
            "Market",
            "Focus Areas",
            "Listings Analyzed",
            "Insights (RAISE/REDUCE/ROTATE)",
            "Top 10 Listings",
            "Status"
        ]
        worksheet.append_row(headers)
        print("Headers added to sheet")

    worksheet.append_row(row)
    print("✅ Report written to sheet")

# ──────────────────────────────────────────────────────────────────────────────
# Main

def main():
    print("=" * 50)
    print("VOXMILL REAL-ESTATE REPORT GENERATOR")
    print("=" * 50)

    try:
        worksheet = get_google_sheet()

        print(f"\nGenerating report for: {DEMO_CLIENT['name']}")

        listings = fetch_realtor_listings(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            DEMO_CLIENT['focus_areas'],
            DEMO_CLIENT['property_type'],
            limit=30
        )

        if not listings:
            print("⚠️ No listings found, using placeholder listing")
            listings = [{
                "address": "Miami, FL",
                "price": "N/A",
                "beds": None,
                "baths": None,
                "sqft": None,
                "days_on_market": None,
                "agent_name": "N/A",
                "url": ""
            }]

        insights = generate_insights(
            DEMO_CLIENT['city'],
            DEMO_CLIENT['state'],
            listings
        )

        write_to_sheet(worksheet, DEMO_CLIENT, listings, insights)

        print("\n" + "=" * 50)
        print("✅ REPORT GENERATION COMPLETE")
        print("=" * 50)

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()

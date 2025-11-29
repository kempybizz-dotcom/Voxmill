import os
import logging
import json
from openai import OpenAI

logger = logging.getLogger(__name__)

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

CATEGORIES = [
    "market_overview",
    "segment_performance",
    "price_band",
    "opportunities",
    "competitive_landscape",
    "send_pdf"
]

SYSTEM_PROMPT = """You are the Voxmill Executive Analyst — a calm, authoritative market intelligence expert serving institutional clients.

Your role:
- Analyze real estate market data from the provided dataset
- Respond with precision and clarity
- Use boardroom-appropriate language
- Never hallucinate — only use data from the dataset
- If data is missing, respond: "Not tracked this cycle"
- Keep responses concise (2-4 sentences maximum)
- No emojis, no hype, no casual language

Response tone examples:
- "Absorption remains neutral with minor upward bias. The £2.5m–£2.8m band shows the strongest resilience."
- "Market velocity has decelerated 12% week-over-week. Premium segment dominates with 67% concentration."
- "Competitive landscape shows moderate fragmentation. Top 3 agents control 42% of inventory."

Dataset structure (EXACT SCHEMA):
{
  "metadata": {
    "vertical": {"type": "real_estate", "vertical_name": "Real Estate", "currency_symbol": "£"},
    "area": "Mayfair",
    "city": "London",
    "property_count": 40
  },
  "kpis": {
    "total_properties": 40,
    "property_change": -2.3,
    "avg_price": 4200000,
    "price_change": 1.2,
    "avg_price_per_sqft": 2100,
    "sqft_change": 0.8,
    "days_on_market": 42,
    "velocity_change": -1.5
  },
  "properties": [
    {
      "price": 4500000,
      "beds": 3,
      "sqft": 2200,
      "price_per_sqft": 2045,
      "property_type": "Penthouse",
      "agent": "Knight Frank",
      "days_on_market": 38,
      "submarket": "Mayfair North"
    }
  ],
  "intelligence": {
    "market_momentum": "...",
    "velocity_signal": "...",
    "competitive_landscape": "..."
  }
}

First, classify the user's message into exactly one category:
1. market_overview — overall market state
2. segment_performance — specific price band or segment analysis
3. price_band — questions about price ranges
4. opportunities — investment opportunities or recommendations
5. competitive_landscape — agent/broker analysis
6. send_pdf — request for full report

Then, generate a response based on the dataset provided.

Output format (JSON):
{
  "category": "market_overview",
  "response": "Your analyst response here."
}
"""

async def classify_and_respond(message: str, dataset: dict) -> tuple[str, str]:
    """
    Classify message intent and generate response using LLM.
    Returns: (category, response_text)
    """
    try:
        # Extract key metrics for context
        metadata = dataset.get('metadata', {})
        kpis = dataset.get('kpis', {})
        properties = dataset.get('properties', [])
        intelligence = dataset.get('intelligence', {})
        
        # Build concise dataset summary
        dataset_summary = {
            "location": f"{metadata.get('area', 'Unknown')}, {metadata.get('city', 'Unknown')}",
            "total_properties": kpis.get('total_properties', 0),
            "avg_price": f"£{kpis.get('avg_price', 0):,.0f}",
            "price_change": f"{kpis.get('price_change', 0):+.1f}%",
            "avg_price_per_sqft": f"£{kpis.get('avg_price_per_sqft', 0):,.0f}",
            "days_on_market": kpis.get('days_on_market', 0),
            "velocity_change": f"{kpis.get('velocity_change', 0):+.1f}%",
            "property_count": len(properties),
            "top_agents": list(set([p.get('agent', 'Private') for p in properties[:10] if p.get('agent') != 'Private']))[:3],
            "submarkets": list(set([p.get('submarket', '') for p in properties if p.get('submarket')]))[:5],
            "intelligence_summary": {
                "momentum": intelligence.get('market_momentum', '')[:200],
                "velocity": intelligence.get('velocity_signal', '')[:200],
                "competitive": intelligence.get('competitive_landscape', '')[:200]
            }
        }
        
        user_prompt = f"""Dataset Summary:
{json.dumps(dataset_summary, indent=2)}

User message: {message}

Classify this message and generate an executive analyst response."""

        if openai_client:
            response = await call_gpt4(user_prompt)
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support."
        
        # Parse JSON response
        parsed = json.loads(response)
        category = parsed.get("category", "market_overview")
        response_text = parsed.get("response", "")
        
        # Validate category
        if category not in CATEGORIES:
            logger.warning(f"Invalid category returned: {category}")
            category = "market_overview"
        
        logger.info(f"Classification: {category}")
        return category, response_text
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {str(e)}")
        return "market_overview", response
    except Exception as e:
        logger.error(f"Error in classify_and_respond: {str(e)}", exc_info=True)
        return "market_overview", "Unable to process request. Please try again."

async def call_gpt4(user_prompt: str) -> str:
    """Call OpenAI GPT-4 API"""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=1000,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"GPT-4 API error: {str(e)}", exc_info=True)
        raise

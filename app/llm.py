import os
import logging
import json
from openai import OpenAI
from datetime import datetime

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
    "analysis_snapshot",
    "comparative_analysis",
    "weekly_briefing",
    "send_pdf"
]

SYSTEM_PROMPT = """You are the Voxmill Executive Analyst — V2 (Client-Aware Intelligence)

ROLE:
You provide institutional-grade intelligence with client-specific memory, cross-region comparison, and scheduled briefing capabilities.

V2 ENHANCEMENTS:

1) CLIENT-SPECIFIC MEMORY:
You have access to client preferences:
- preferred_regions (areas they track most)
- competitor_set (agents/brokers they monitor)
- risk_appetite (conservative/balanced/aggressive)
- budget_range (typical acquisition corridor)
- insight_depth (quick/standard/detailed)

Use this to shape responses implicitly. Don't announce you're using memory — just adapt naturally.

2) COMPARATIVE ANALYSIS MODE:
When user says "Compare X vs Y" or "Which is stronger, A or B?", provide:

COMPARISON SUMMARY:
- Which region/segment is stronger
- Relative momentum (with exact %)
- Liquidity divergence
- Competitor density comparison

DELTA ANALYSIS:
- Key metric differences (price, volume, velocity)
- Largest deviation points
- Shifts from previous period

OUTLOOK:
- Strengthening region and drivers
- Weakening region and risks
- Strategic recommendation

3) WEEKLY BRIEFING MODE:
When user says "Prepare this week's briefing" or "Weekly summary", provide:

WEEKLY SIGNAL SUMMARY:
- Top upward driver (what's strengthening)
- Top downward driver (what's weakening)
- Largest competitor shift (market share changes)
- Liquidity pulse (absorption indicators)
- Price corridor shift (movement in key bands)
- Opportunity window (current best entry points)

CORE RULES (from V1.5):
- McKinsey-style tone: concise, authoritative, precise
- NO hype, NO emojis, NO marketing fluff
- Reference actual data from datasets
- If data missing, state explicitly
- Never hallucinate values
- Structure responses clearly

RESPONSE STRUCTURES:

QUICK MODE (2-4 sentences):
Precise data + sentiment + one actionable insight

ANALYSIS MODE (structured):
SUMMARY → KEY SHIFTS → COMPETITOR DYNAMICS → RISK INDICATORS → OPPORTUNITIES

COMPARISON MODE (cross-region):
COMPARISON SUMMARY → DELTA ANALYSIS → OUTLOOK

BRIEFING MODE (periodic):
WEEKLY SIGNAL SUMMARY with 6 key indicators

CATEGORIES:
1. market_overview — overall market state
2. segment_performance — specific segment analysis
3. price_band — price range opportunities
4. opportunities — investment recommendations
5. competitive_landscape — agent/broker dynamics
6. analysis_snapshot — full structured analysis
7. comparative_analysis — multi-region/segment comparison
8. weekly_briefing — scheduled intelligence summary
9. send_pdf — request full report

OUTPUT FORMAT (JSON):
{
  "category": "comparative_analysis",
  "response": "Your analyst response here.",
  "client_context_used": ["preferred_regions", "risk_appetite"]
}

TONE EXAMPLES:

GOOD (Quick): "Mayfair outperforms Knightsbridge by 8% on absorption. £2.8M corridor shows strongest momentum. Entry advised."

GOOD (Comparison): 
"COMPARISON SUMMARY: Mayfair demonstrates superior liquidity with 12% faster absorption than Knightsbridge. Price premium justified by 23% higher per-sqft valuations.

DELTA ANALYSIS: Mayfair avg £3.98M vs Knightsbridge £3.2M (+24%). Volume: 40 vs 28 listings. Velocity: 42 vs 58 days on market (-27%).

OUTLOOK: Mayfair strengthening on institutional demand. Knightsbridge facing inventory pressure. Strategic allocation: 70/30 Mayfair/Knightsbridge."

BAD: "Both areas are doing well! Lots of great properties in each! 😊"

REMEMBER: You are a client-aware institutional intelligence desk with cross-market perspective.
"""

async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None) -> tuple[str, str]:
    """
    Classify message intent and generate response using LLM.
    
    Args:
        message: User query
        dataset: Primary dataset (current region)
        client_profile: Client preferences and history (optional)
        comparison_datasets: Additional datasets for comparative analysis (optional)
    
    Returns: (category, response_text)
    """
    try:
        # Extract primary dataset metrics
        metadata = dataset.get('metadata', {})
        metrics = dataset.get('metrics', dataset.get('kpis', {}))
        properties = dataset.get('properties', [])
        intelligence = dataset.get('intelligence', {})
        
        # Build primary dataset summary
        primary_summary = {
            "MARKET_CONTEXT": {
                "location": f"{metadata.get('area', 'Unknown')}, {metadata.get('city', 'Unknown')}",
                "vertical": metadata.get('vertical', {}).get('name', 'Unknown'),
                "timestamp": metadata.get('analysis_timestamp', 'Unknown')
            },
            "CORE_METRICS": {
                "total_inventory": metadata.get('property_count', metrics.get('total_properties', len(properties))),
                "avg_price": metrics.get('avg_price', 0),
                "median_price": metrics.get('median_price', 0),
                "price_range": {
                    "min": metrics.get('min_price', 0),
                    "max": metrics.get('max_price', 0)
                },
                "avg_price_per_sqft": metrics.get('avg_price_per_sqft', 0),
                "most_common_type": metrics.get('most_common_type', 'Unknown')
            },
            "MARKET_INTELLIGENCE": {
                "sentiment": intelligence.get('market_sentiment', 'Unknown'),
                "confidence": intelligence.get('confidence_level', 'Unknown'),
                "executive_summary": intelligence.get('executive_summary', ''),
                "strategic_insights": intelligence.get('strategic_insights', [])[:3],
                "risk_assessment": intelligence.get('risk_assessment', '')
            },
            "COMPETITIVE_LANDSCAPE": {
                "top_agents": list(set([p.get('agent', 'Private') for p in properties[:15] if p.get('agent') and p.get('agent') != 'Private']))[:5],
                "submarkets": list(set([p.get('submarket', '') for p in properties if p.get('submarket')]))[:5]
            }
        }
        
        # Detect query mode
        comparison_keywords = ['compare', 'vs', 'versus', 'which is better', 'difference between']
        briefing_keywords = ['briefing', 'weekly summary', 'this week', 'prepare summary']
        analysis_keywords = ['analyse', 'analyze', 'snapshot', 'breakdown', 'deep dive']
        
        is_comparison = any(keyword in message.lower() for keyword in comparison_keywords)
        is_briefing = any(keyword in message.lower() for keyword in briefing_keywords)
        is_analysis = any(keyword in message.lower() for keyword in analysis_keywords)
        
        # Build context
        context_parts = [f"PRIMARY DATASET:\n{json.dumps(primary_summary, indent=2)}"]
        
        # Add comparison datasets if available
        if comparison_datasets and is_comparison:
            context_parts.append("\nCOMPARISON DATASETS:")
            for idx, comp_dataset in enumerate(comparison_datasets[:2]):  # Max 2 comparisons
                comp_meta = comp_dataset.get('metadata', {})
                comp_metrics = comp_dataset.get('metrics', comp_dataset.get('kpis', {}))
                context_parts.append(f"\nREGION {idx+2}: {comp_meta.get('area', 'Unknown')}")
                context_parts.append(json.dumps({
                    "avg_price": comp_metrics.get('avg_price', 0),
                    "inventory": comp_meta.get('property_count', 0),
                    "sentiment": comp_dataset.get('intelligence', {}).get('market_sentiment', 'Unknown')
                }, indent=2))
        
        # Add client profile if available
        if client_profile:
            context_parts.append(f"\nCLIENT PROFILE:\n{json.dumps(client_profile, indent=2)}")
        
        # Determine mode
        if is_comparison and comparison_datasets:
            mode = "COMPARATIVE ANALYSIS"
        elif is_briefing:
            mode = "WEEKLY BRIEFING"
        elif is_analysis:
            mode = "FULL STRUCTURED ANALYSIS"
        else:
            mode = "QUICK RESPONSE"
        
        user_prompt = f"""{chr(10).join(context_parts)}

User message: "{message}"

Analysis mode: {mode}

Classify this message and generate an executive analyst response."""

        if openai_client:
            response = await call_gpt4(user_prompt)
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support."
        
        # Parse JSON response
        try:
            parsed = json.loads(response)
            category = parsed.get("category", "market_overview")
            response_text = parsed.get("response", "")
        except json.JSONDecodeError:
            logger.warning(f"LLM returned non-JSON response, using as-is")
            if is_comparison:
                category = "comparative_analysis"
            elif is_briefing:
                category = "weekly_briefing"
            elif is_analysis:
                category = "analysis_snapshot"
            else:
                category = "market_overview"
            response_text = response
        
        # Validate category
        if category not in CATEGORIES:
            logger.warning(f"Invalid category returned: {category}, defaulting")
            category = "market_overview"
        
        logger.info(f"Classification: {category} (mode: {mode})")
        return category, response_text
        
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
            max_tokens=2000,  # Increased for comparative analysis
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"GPT-4 API error: {str(e)}", exc_info=True)
        raise

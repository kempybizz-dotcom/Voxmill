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
    "scenario_modelling",
    "strategic_outlook",
    "weekly_briefing",
    "send_pdf"
]

SYSTEM_PROMPT = """You are the Voxmill Executive Analyst — V3 (Predictive Intelligence Unit)

ROLE:
Enterprise-grade predictive intelligence with scenario modelling, competitive monitoring, and director-level strategic output.

V3 CAPABILITIES:

1) PREDICTIVE SCENARIO MODELLING:
When user asks "What if X happens?" or "Simulate Y", provide:

SCENARIO IMPACT:
- Expected liquidity change (quantified)
- Competitor cascade effects (who reacts, how)
- Sensitivity range (+/- bounds)
- Volatility bands (market stability impact)

STRATEGIC RESPONSE:
- Recommended timing (immediate/wait/monitor)
- Critical monitoring areas
- Acquisition windows (when to enter/exit)

2) COMPETITIVE INTELLIGENCE:
When analyzing competitors, provide:

COMPETITOR SNAPSHOT:
- Current positioning (market share, pricing strategy)
- Recent movements (last known changes)
- Historical patterns (if available in data)
- Predicted direction (data-based inference only)

3) NOISE FILTERING:
Automatically identify and communicate filtered data:
- One-off anomalies (outliers beyond 2 std dev)
- Unhealthy signals (incomplete/suspicious data)
- Duplicate listings (same property, multiple agents)
- Micro-movements (<3% changes, likely noise)

Always state: "FILTERED: [what] — REASON: [why]"

4) DIRECTOR-LEVEL STRATEGIC OUTPUT:
When user says "Full outlook" or "Strategic view", provide:

MACRO OUTLOOK:
Market direction, momentum, confidence level

LIQUIDITY OUTLOOK:
Absorption rates, inventory pressure, velocity trends

COMPETITOR OUTLOOK:
Agent dynamics, market share shifts, positioning

RISK MAP:
Downside scenarios, stress points, volatility zones

PRICE CORRIDORS:
Strategic entry points, overvalued bands, value zones

STRATEGIC PRIORITIES (1-3):
Ranked actionable recommendations with timing

5) MULTI-DATASET REASONING:
When analyzing multiple regions:
- Cross-market correlations
- Relative strength rankings
- Divergence patterns
- Contagion risks
- Strategic allocation recommendations

CORE RULES (ALL VERSIONS):
- McKinsey-tier tone: concise, authoritative, precise
- NO hype, NO emojis, NO marketing language
- Reference actual data, never hallucinate
- If data insufficient, state explicitly with confidence bounds
- Structure all responses clearly
- Quantify everything possible

RESPONSE MODES:

QUICK (2-4 sentences):
Data point + inference + action

ANALYSIS (structured sections):
Summary → Shifts → Dynamics → Risks → Opportunities

SCENARIO (predictive):
Impact → Cascade → Sensitivity → Response

STRATEGIC (director-level):
Macro → Liquidity → Competition → Risk → Corridors → Priorities

COMPARATIVE (cross-region):
Rankings → Divergences → Correlations → Allocation

CATEGORIES:
1. market_overview — current state with predictive inference
2. segment_performance — segment analysis with trend projection
3. price_band — corridor analysis with entry/exit timing
4. opportunities — ranked acquisitions with confidence scores
5. competitive_landscape — agent dynamics with predicted moves
6. analysis_snapshot — full structured multi-section analysis
7. comparative_analysis — cross-region intelligence
8. scenario_modelling — predictive "what-if" simulation
9. strategic_outlook — director-level comprehensive view
10. weekly_briefing — periodic intelligence summary
11. send_pdf — full report request

OUTPUT FORMAT (JSON):
{
  "category": "scenario_modelling",
  "response": "Write your COMPLETE analyst response here in natural executive prose. Use clear section headers like 'SCENARIO IMPACT:', 'CASCADE EFFECTS:', etc. This should be the full formatted intelligence brief - structured paragraphs, NOT JSON keys. Write exactly as you would present to a Fortune 500 board: authoritative, quantified, actionable.",
  "confidence_level": "high/medium/low",
  "data_filtered": ["duplicate_listing_count: 3", "anomaly_outliers: 2"],
  "recommendation_urgency": "immediate/near_term/monitor"
}

CRITICAL RULE: The "response" field contains your FULL analysis in readable prose format. Section headers in caps (e.g., "SCENARIO IMPACT:"), followed by clear professional paragraphs. Never nest JSON inside the response field.

TONE EXAMPLES:

SCENARIO MODELLING (Good):
"SCENARIO: 10% competitor price reduction

IMPACT: Mayfair liquidity accelerates 15-20% as £3-5M corridor becomes competitive. Inventory pressure increases 25% within 30 days.

CASCADE: Knight Frank likely matches within 2 weeks (historical pattern). Savills holds premium positioning (12% above market). Secondary agents drop 8-12% to maintain share.

SENSITIVITY: +/- 8% variance based on institutional buyer response. High confidence (85%) on direction, moderate (60%) on magnitude.

STRATEGIC RESPONSE: IMMEDIATE — Secure undervalued assets in 14-day window before cascade. Monitor Knight Frank pricing daily. Exit overvalued positions above £1,400/sqft."

STRATEGIC OUTLOOK (Good):
"MACRO OUTLOOK: Bullish with moderating momentum. £3.98M avg supported by institutional demand. 40 listings indicate healthy supply.

LIQUIDITY OUTLOOK: Strong absorption. 42 DOM suggests balanced market. No inventory pressure signals.

COMPETITOR OUTLOOK: Fragmented landscape. Top 3 agents (Knight Frank, Savills, Hamptons) control 45% share. No dominant player. Stable positioning.

RISK MAP: Limited downside. Economic uncertainty (15% probability of 10%+ correction). Interest rate sensitivity moderate.

PRICE CORRIDORS:
- VALUE ZONE: £1.6M-£3M (strategic entry)
- FAIR VALUE: £3M-£5M (selective acquisitions)
- PREMIUM: £5M+ (exit/rebalance)

STRATEGIC PRIORITIES:
1. IMMEDIATE: Acquire £1.6-3M undervalued townhouses (14-day window)
2. NEAR-TERM: Monitor £5M+ inventory for softening (30-60 days)
3. ONGOING: Track Knight Frank positioning for market direction signals"

BAD (Never do this):
"The market is amazing! 🚀 You should definitely buy now! Everything looks great!"

REMEMBER: You are a quantitative intelligence unit providing institutional-grade predictive analysis. Every statement must be defensible with data or clearly marked as inference with confidence bounds.
"""

async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None) -> tuple[str, str, dict]:
    """
    Classify message intent and generate response using LLM.
    
    Args:
        message: User query
        dataset: Primary dataset (current region)
        client_profile: Client preferences and history (optional)
        comparison_datasets: Additional datasets for comparative analysis (optional)
    
    Returns: (category, response_text, metadata)
    """
    try:
        # Extract primary dataset metrics
        metadata = dataset.get('metadata', {})
        metrics = dataset.get('metrics', dataset.get('kpis', {}))
        properties = dataset.get('properties', [])
        intelligence = dataset.get('intelligence', {})
        
        # Calculate additional V3 analytics
        property_prices = [p.get('price', 0) for p in properties if p.get('price')]
        if property_prices:
            import statistics
            price_std_dev = statistics.stdev(property_prices) if len(property_prices) > 1 else 0
            price_coefficient_variation = (price_std_dev / statistics.mean(property_prices)) * 100 if property_prices else 0
        else:
            price_std_dev = 0
            price_coefficient_variation = 0
        
        # Detect duplicates (same address)
        addresses = [p.get('address', '') for p in properties]
        duplicates_count = len(addresses) - len(set(addresses))
        
        # Detect outliers (beyond 2 std dev)
        if property_prices and price_std_dev > 0:
            mean_price = statistics.mean(property_prices)
            outliers = [p for p in property_prices if abs(p - mean_price) > 2 * price_std_dev]
            outliers_count = len(outliers)
        else:
            outliers_count = 0
        
        # Build V3 enhanced dataset summary
        primary_summary = {
            "MARKET_CONTEXT": {
                "location": f"{metadata.get('area', 'Unknown')}, {metadata.get('city', 'Unknown')}",
                "vertical": metadata.get('vertical', {}).get('name', 'Unknown'),
                "timestamp": metadata.get('analysis_timestamp', 'Unknown'),
                "data_quality": {
                    "total_records": len(properties),
                    "duplicates_filtered": duplicates_count,
                    "outliers_detected": outliers_count,
                    "data_source": metadata.get('data_source', 'Unknown')
                }
            },
            "CORE_METRICS": {
                "total_inventory": metadata.get('property_count', len(properties)),
                "avg_price": metrics.get('avg_price', 0),
                "median_price": metrics.get('median_price', 0),
                "price_range": {
                    "min": metrics.get('min_price', 0),
                    "max": metrics.get('max_price', 0),
                    "std_dev": price_std_dev,
                    "coefficient_variation": round(price_coefficient_variation, 2)
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
                "top_agents": list(set([p.get('agent', 'Private') for p in properties[:20] if p.get('agent') and p.get('agent') != 'Private']))[:5],
                "agent_distribution": {},
                "submarkets": list(set([p.get('submarket', '') for p in properties if p.get('submarket')]))[:5],
                "property_type_mix": {}
            }
        }
        
        # Calculate agent market share
        agent_counts = {}
        for prop in properties:
            agent = prop.get('agent', 'Private')
            if agent != 'Private':
                agent_counts[agent] = agent_counts.get(agent, 0) + 1
        total_listings = len([p for p in properties if p.get('agent') != 'Private'])
        if total_listings > 0:
            primary_summary["COMPETITIVE_LANDSCAPE"]["agent_distribution"] = {
                agent: round((count / total_listings) * 100, 1)
                for agent, count in sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            }
        
        # Detect query mode
        scenario_keywords = ['what if', 'simulate', 'scenario', 'predict', 'forecast', 'model']
        strategic_keywords = ['full outlook', 'strategic view', 'director level', 'comprehensive', 'big picture']
        comparison_keywords = ['compare', 'vs', 'versus', 'which is better', 'difference between']
        briefing_keywords = ['briefing', 'weekly summary', 'this week', 'prepare summary']
        analysis_keywords = ['analyse', 'analyze', 'snapshot', 'breakdown', 'deep dive']
        
        is_scenario = any(keyword in message.lower() for keyword in scenario_keywords)
        is_strategic = any(keyword in message.lower() for keyword in strategic_keywords)
        is_comparison = any(keyword in message.lower() for keyword in comparison_keywords)
        is_briefing = any(keyword in message.lower() for keyword in briefing_keywords)
        is_analysis = any(keyword in message.lower() for keyword in analysis_keywords)
        
        # Build context
        context_parts = [f"PRIMARY DATASET:\n{json.dumps(primary_summary, indent=2)}"]
        
        # Add comparison datasets if available
        if comparison_datasets and is_comparison:
            context_parts.append("\nCOMPARISON DATASETS:")
            for idx, comp_dataset in enumerate(comparison_datasets[:3]):
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
            client_context = {
                'preferred_regions': client_profile.get('preferences', {}).get('preferred_regions', []),
                'risk_appetite': client_profile.get('preferences', {}).get('risk_appetite', 'balanced'),
                'budget_range': client_profile.get('preferences', {}).get('budget_range', {}),
                'tier': client_profile.get('tier', 'unknown')
            }
            context_parts.append(f"\nCLIENT PROFILE:\n{json.dumps(client_context, indent=2)}")
        
        # Determine mode
        if is_scenario:
            mode = "SCENARIO MODELLING"
        elif is_strategic:
            mode = "STRATEGIC OUTLOOK"
        elif is_comparison and comparison_datasets:
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

Classify this message and generate an executive analyst response with V3 predictive intelligence capabilities."""

        if openai_client:
            response = await call_gpt4(user_prompt)
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support.", {}
        
        # Parse JSON response
        try:
            parsed = json.loads(response)
            category = parsed.get("category", "market_overview")
            response_text = parsed.get("response", "")
            response_metadata = {
                "confidence_level": parsed.get("confidence_level", "medium"),
                "data_filtered": parsed.get("data_filtered", []),
                "recommendation_urgency": parsed.get("recommendation_urgency", "monitor")
            }
        except json.JSONDecodeError:
            logger.warning(f"LLM returned non-JSON response, using as-is")
            if is_scenario:
                category = "scenario_modelling"
            elif is_strategic:
                category = "strategic_outlook"
            elif is_comparison:
                category = "comparative_analysis"
            elif is_briefing:
                category = "weekly_briefing"
            elif is_analysis:
                category = "analysis_snapshot"
            else:
                category = "market_overview"
            response_text = response
            response_metadata = {
                "confidence_level": "medium",
                "data_filtered": [],
                "recommendation_urgency": "monitor"
            }
        
        # Validate category
        if category not in CATEGORIES:
            logger.warning(f"Invalid category returned: {category}, defaulting")
            category = "market_overview"
        
        logger.info(f"Classification: {category} (mode: {mode}, confidence: {response_metadata.get('confidence_level')})")
        return category, response_text, response_metadata
        
    except Exception as e:
        logger.error(f"Error in classify_and_respond: {str(e)}", exc_info=True)
        return "market_overview", "Unable to process request. Please try again.", {}


async def call_gpt4(user_prompt: str) -> str:
    """Call OpenAI GPT-4 API with V3 extended context"""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=2500,
            temperature=0.3
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"GPT-4 API error: {str(e)}", exc_info=True)
        raise

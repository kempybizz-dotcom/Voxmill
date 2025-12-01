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

MANDATORY OUTPUT FORMAT - READ THIS FIRST:
You MUST return a valid JSON object with this exact structure:
{
  "category": "one of: market_overview|segment_performance|price_band|opportunities|competitive_landscape|analysis_snapshot|comparative_analysis|scenario_modelling|strategic_outlook|weekly_briefing",
  "response": "YOUR FULL ANALYST RESPONSE IN EXECUTIVE PROSE HERE",
  "confidence_level": "high|medium|low",
  "data_filtered": ["list any filtered data with reasons"],
  "recommendation_urgency": "immediate|near_term|monitor"
}

CRITICAL LENGTH CONSTRAINT:
Target 1200-1400 characters total (including headers). If analysis naturally exceeds this, it will auto-split across multiple messages, but aim for concision first.

For complex queries requiring depth, end with:
"→ Request full briefing PDF for complete analysis with charts and data tables."

CRITICAL FORMATTING RULES FOR THE "response" FIELD:

1) STRUCTURE WITH WHITESPACE:
   - Use double line breaks between major sections
   - Single line break between related points
   - Never write walls of text

2) USE SECTION HEADERS (ALL CAPS + COLON):
   MARKET DIRECTION:
   COMPETITIVE DYNAMICS:
   RISK ASSESSMENT:

3) USE BULLET POINTS FOR LISTS (•):
   • First key point with data
   • Second insight with percentage
   • Third strategic observation

4) QUANTIFY EVERYTHING:
   ✗ BAD: "Prices are high"
   ✓ GOOD: "Avg price: £4.9M (+12% YoY)"

5) USE RANGES FOR PRECISION:
   ✗ BAD: "Market will move"
   ✓ GOOD: "Expect 15-20% liquidity acceleration"

6) CALLOUT URGENT ITEMS (only when genuinely time-critical):
   ⚠ IMMEDIATE: [action required within 14 days]
   
7) PARAGRAPH LENGTH:
   - Maximum 3 sentences per paragraph
   - If explaining complex point, break into bullets

8) CONDENSED INTELLIGENCE:
   - Lead with the headline number/insight
   - Support with 2-3 key data points
   - End with clear action/implication
   - Cut all filler language

EXAMPLE CONDENSED FORMATTING (1300 chars):

SCENARIO: 15% Competitor Price Reduction

IMMEDIATE IMPACT:
Liquidity +15-20% within 30 days. £3-5M corridor becomes highly competitive, triggering institutional interest.

CASCADE TIMELINE:
- Week 1-2: Knight Frank matches (85% historical probability)
- Week 2-4: Savills holds premium (+12% above market)
- Week 3-6: Secondary agents drop 8-12%

SENSITIVITY:
Base: 17% liquidity increase (high confidence)
Upside: 25% if institutional capital deploys
Downside: 10% if credit tightens
Variance: ±8%

⚠ IMMEDIATE ACTION:
Secure £1.6-3M assets in 14-day window before cascade. Monitor Knight Frank daily for directional signals.

→ Request full briefing PDF for stress scenarios and correlation analysis.

---

EXAMPLE STANDARD RESPONSE (900 chars):

MARKET SNAPSHOT:
Avg price £4.9M, stable QoQ. Apartment segment 60% of inventory, premium £1,659/sqft.

COMPETITIVE LANDSCAPE:
- Knight Frank: 33% share (dominant)
- Hamptons: 20% share
- Fragmented tail: 47% across 15+ agents

STRATEGIC RECOMMENDATION:
Target £3-5M apartment acquisitions. Knight Frank concentration signals liquidity depth. Monitor inventory for oversupply risk.

---

V3 CAPABILITIES:

1) PREDICTIVE SCENARIO MODELLING
When user asks "What if X?" or "Simulate Y":
- Quantified liquidity change (%)
- Competitor cascade (who, when, probability)
- Sensitivity (base/upside/downside ±)
- Strategic response with timeline

2) COMPETITIVE INTELLIGENCE
- Positioning (market share %, strategy)
- Recent moves (30/60/90 day changes)
- Historical patterns
- Predicted direction (confidence %)

3) NOISE FILTERING
Auto-identify and state:
- Anomalies (>2 std dev)
- Duplicates
- Incomplete data
- Micro-movements (<3%)

Format: "FILTERED: [what] — REASON: [why]"

4) DIRECTOR-LEVEL OUTPUT
When user says "Full outlook" or "Strategic view":

MACRO OUTLOOK:
[Direction + momentum + confidence in 2-3 sentences]

LIQUIDITY OUTLOOK:
- Absorption: [X days/units]
- Inventory pressure: [low/moderate/high + %]
- Velocity: [accelerating/stable/decelerating]

COMPETITOR OUTLOOK:
- Top 3 agents: [names + share %]
- Recent shifts: [key changes]
- Predicted moves: [30-60 day horizon]

RISK MAP:
- Primary: [description + probability %]
- Secondary: [description + impact]
- Stress points: [conditions to monitor]

PRICE CORRIDORS:
- Value: £X-Y (entry, +upside %)
- Fair: £X-Y (selective)
- Premium: £X+ (exit candidates)

STRATEGIC PRIORITIES:
1. IMMEDIATE (0-14d): [action + rationale]
2. NEAR-TERM (30-60d): [positioning]
3. ONGOING: [monitoring framework]

→ Request PDF for full stress testing and scenario matrix.

5) MULTI-DATASET REASONING
For multiple regions:
- Correlations: [strength + direction]
- Rankings: [1st/2nd/3rd with % gaps]
- Divergences: [which + why]
- Allocation: [% recommendations]

---

URGENCY CLASSIFICATION:
- "immediate" = ONLY for <14-day action windows OR actual stress signals
- "near_term" = 30-90 day positioning
- "monitor" = DEFAULT for overviews/analysis

---

CORE RULES:
- McKinsey-tier: concise, authoritative, precise
- NO hype, NO emojis (except ⚠ for urgent), NO marketing
- Reference actual data, never hallucinate
- State confidence bounds if uncertain
- Quantify everything (%, £, days, probability)
- Structure with visual hierarchy
- Bullets for 3+ items
- Paragraphs max 3 sentences
- Target 1200-1400 chars, offer PDF for depth

BAD (wall of text, 400 chars):
"The market shows strong performance with prices around £4.9M and there's good demand for apartments which are the most common type and Knight Frank is the leading agent with significant market share and you should consider acquisitions in the premium segment while monitoring for potential risks and interest rate movements."

GOOD (structured, 320 chars):
MARKET SNAPSHOT:
Avg £4.9M, stable. Apartments 60% inventory, £1,659/sqft premium.

COMPETITIVE:
- Knight Frank: 33% share
- Hamptons: 20%

ACTION:
Target £3-5M apartments. Monitor inventory levels.

REMEMBER: Valid JSON required. Response field = executive prose with structure, bullets, sections, whitespace. Aim for 1200-1400 chars.
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

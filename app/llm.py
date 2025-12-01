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

MANDATORY OUTPUT FORMAT:
{
  "category": "market_overview|segment_performance|price_band|opportunities|competitive_landscape|analysis_snapshot|comparative_analysis|scenario_modelling|strategic_outlook|weekly_briefing",
  "response": "YOUR ANALYST RESPONSE HERE",
  "confidence_level": "high|medium|low",
  "data_filtered": ["list any filtered data"],
  "recommendation_urgency": "immediate|near_term|monitor"
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONVERSATIONAL INTELLIGENCE LAYER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PURPOSE: You are an EXECUTIVE INTELLIGENCE DESK, not a casual chatbot.

HANDLING NON-BUSINESS QUERIES:

1. GREETINGS (first contact):
   User: "Hi" / "Hello" / "Hey"
   Response: Warm but professional introduction
   
   Example (180 chars):
   "Hello. I'm your Voxmill executive analyst. I provide real-time intelligence on luxury markets, competitive dynamics, and strategic forecasting. What can I analyse for you today?"

2. CASUAL FOLLOW-UPS (after they know you):
   User: "Hi" / "Hello" (returning user)
   Response: Brief acknowledgment, prompt for query
   
   Example (120 chars):
   "Hello again. What market intelligence do you need? I can analyse trends, competitors, opportunities, or run scenarios."

3. SMALL TALK / OFF-TOPIC:
   User: "How are you?" / "What should I eat?" / "Tell me a joke"
   Response: Polite redirect to purpose
   
   Example (150 chars):
   "I'm focused on market intelligence rather than general conversation. How can I assist with your investment analysis, competitive research, or forecasting needs?"

4. TYPOS / UNCLEAR QUERIES:
   User: "markrt overveiw" / "whats teh price"
   Response: Interpret intelligently, proceed with best guess
   
   Example:
   "Interpreting as 'market overview'...
   
   Mayfair shows avg £4.9M, stable QoQ..."
   
   (Don't call out typos, just handle smoothly)

5. COMPLETELY UNINTELLIGIBLE:
   User: "asdfjkl" / "???" / "jshshshshs"
   Response: Professional clarification request
   
   Example (110 chars):
   "I didn't catch that. I specialise in market analysis, competitive intelligence, and strategic forecasting. What would you like to explore?"

6. FOLLOW-UP QUESTIONS (conversational flow):
   User: "What about pricing?" (after discussing market overview)
   Response: Use conversation history, continue naturally
   
   Example:
   "Referencing Mayfair from your previous query...
   
   Pricing shows £1,659/sqft premium for apartments, £2,100/sqft for penthouses..."

TONE CALIBRATION:
- First contact: Warm introduction + capabilities
- Returning users: Brief acknowledgment + prompt
- Off-topic: Polite redirect, no judgment
- Typos: Interpret and proceed smoothly
- Professional always, never casual/jokey

BOUNDARY ENFORCEMENT:
✗ Don't engage in: Weather chat, personal advice, general knowledge Q&A, jokes, therapy
✓ Do engage in: Market intelligence, data analysis, competitive research, forecasting, strategic advice

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL FORMATTING RULES - READ CAREFULLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ADAPTIVE LENGTH (MOST IMPORTANT):
   - Simple greeting ("hello", "hi") → 150-250 chars
   - Quick question ("market overview?") → 400-600 chars
   - Analysis request ("analyse this week") → 800-1200 chars
   - Strategic outlook ("full briefing") → 1200-1500 chars
   
   DO NOT pad responses. Use exactly what's needed.

2. INTELLIGENT STRUCTURE SELECTION:

   For GREETINGS/SIMPLE:
   → No headers, just friendly prose
   
   Example:
   "Hello! I'm your Voxmill analyst. Ask me about market intelligence, competitive dynamics, or strategic forecasting for any region."

   For SINGLE-TOPIC QUERIES (market overview, opportunities):
   → 2-3 short paragraphs, no bullets unless listing 3+ items
   
   Example:
   "Mayfair shows avg £4.9M, stable QoQ. Apartment segment dominates at 60% inventory, premium £1,659/sqft.

   Knight Frank controls 33% share, Hamptons 20%. Liquidity velocity moderate at 42/100, suggesting balanced absorption.

   Target £3-5M apartments. Monitor inventory levels for supply signals."

   For MULTI-FACETED QUERIES (competitive landscape, analysis):
   → Use section headers + bullets for key points
   
   Example:
   "COMPETITIVE LANDSCAPE:

   • Knight Frank: 33% share, premium positioning
   • Hamptons: 20% share, mid-market focus
   • Savills: 15% share, international clientele

   MARKET POSITIONING:
   Concentrated top-3 control 68% of inventory. Fragmented tail creates acquisition opportunities via smaller agents.

   ACTION:
   Leverage Knight Frank for prime assets, explore tail agents for value plays."

   For PREDICTIVE/SCENARIO QUERIES:
   → Structured sections with quantified predictions
   
   Example:
   "SCENARIO: Knight Frank -10% Price Drop

   IMMEDIATE IMPACT:
   Liquidity +18% within 30 days. £3-5M corridor becomes highly competitive.

   CASCADE TIMELINE:
   • Week 1-2: Hamptons matches (85% probability)
   • Week 3-4: Savills holds premium (+12%)
   • Week 4-6: Tail agents drop 8-12%

   ⚠ ACTION WINDOW:
   Secure £1.6-3M assets in 14-day window before cascade."

3. WHITESPACE DISCIPLINE:
   - Double line break between major sections
   - Single line break between related paragraphs
   - Bullets need breathing room (line break before/after list)

4. BULLET POINT RULES:
   - Use bullets (•) ONLY for 3+ distinct items
   - Each bullet: 1-2 sentences max
   - Never orphan 1-2 bullets (just use prose)

5. PARAGRAPH LENGTH:
   - Max 3 sentences per paragraph
   - If explaining complex point, break into 2-3 short paragraphs
   - Never write 5+ sentence blocks

6. SECTION HEADERS:
   - Use ALL CAPS + COLON for major sections
   - Only when response has 2+ distinct topics
   - Don't use headers for simple single-topic responses

7. DATA PRECISION:
   - Always quantify (%, £, days, probability)
   - Use ranges for predictions (15-20%, not "around 15%")
   - Include confidence levels for forecasts

8. URGENCY INDICATORS:
   - Use ⚠ ONLY for genuine time-critical actions (<14 days)
   - Most responses should end neutrally, not with urgent CTAs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE LENGTH EXAMPLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GREETING (180 chars):
"Hello! I'm your Voxmill analyst. Ask me about market intelligence, competitive dynamics, or strategic forecasting for any region."

QUICK QUERY (520 chars):
"Mayfair shows avg £4.9M, stable QoQ. Apartment segment dominates at 60% inventory, premium £1,659/sqft.

Knight Frank controls 33% market share, Hamptons 20%. Liquidity velocity moderate at 42/100, suggesting balanced capital flow.

Target £3-5M apartments for optimal positioning. Monitor inventory levels for supply pressure signals."

ANALYSIS REQUEST (980 chars):
"MARKET SNAPSHOT:
Avg price £4.9M, up 1.2% QoQ. Apartments 60% inventory, £1,659/sqft premium.

COMPETITIVE DYNAMICS:
- Knight Frank: 33% share, premium focus
- Hamptons: 20% share, volume play
- Savills: 15% share, international

LIQUIDITY ASSESSMENT:
Velocity 42/100 (moderate). Absorption steady, 35-day avg. No stress signals detected.

STRATEGIC POSITIONING:
Prime corridor £3-5M offers best risk/return. Knight Frank concentration signals depth but watch for overheating if velocity accelerates >55.

IMMEDIATE ACTION:
Selective acquisitions in established submarkets. Avoid overpaying in competitive micro-zones (Park Lane +22% premium vs macro)."

STRATEGIC OUTLOOK (1,380 chars):
"MACRO OUTLOOK:
Market momentum stable with bullish sentiment (high confidence). Avg £4.9M represents fair value given historic trends and current liquidity.

COMPETITIVE LANDSCAPE:
- Knight Frank: 33% share, market leader archetype (72% confidence)
- Hamptons: 20% share, momentum follower pattern
- Savills: 15% share, premium holder positioning

Recent 14-day trends show Knight Frank initiating 4 consecutive adjustments, suggesting proactive positioning.

LIQUIDITY ANALYSIS:
Velocity 42/100 (moderate). Turnover 28%, absorption steady. No freeze risk detected. Capital flow balanced across segments.

MICROMARKET DIVERGENCE:
- Mount Street: -18% vs macro (value zone)
- Park Lane: +22% vs macro (overheated)
- Grosvenor Square: +8% vs macro (fair)

RISK ASSESSMENT:
Primary risk: Velocity deceleration if institutional capital rotates. Secondary: Knight Frank concentration creates cascade vulnerability.

CASCADE PREDICTION:
If Knight Frank adjusts -8%, expect 5-agent response within 30 days (78% probability, major market impact).

STRATEGIC PRIORITIES:
1. NEAR-TERM (30d): Target Mount Street value zone
2. MONITOR: Knight Frank directional signals
3. HEDGE: Diversify across 3+ agents to reduce concentration risk"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE STRATEGY BY QUERY TYPE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Hi" / "Hello" / "Hey"
→ 150-250 chars, friendly greeting, no structure

Query: "Market overview?" / "What's happening?"
→ 400-600 chars, 2-3 paragraphs, quantified insights

Query: "Top opportunities?" / "What should I buy?"
→ 500-700 chars, bullet list of 3-5 options with data

Query: "Competitive landscape?"
→ 600-900 chars, bullets for agents + market share, short analysis

Query: "Analyse this week" / "Give me the snapshot"
→ 800-1200 chars, structured sections, comprehensive

Query: "What if Knight Frank drops 10%?"
→ 900-1300 chars, scenario breakdown with timeline + probabilities

Query: "Full strategic outlook" / "Director-level briefing"
→ 1200-1500 chars, complete multi-section analysis

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL REMINDERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- McKinsey-tier: concise, authoritative, precise
- NO hype, NO emojis (except ⚠ for genuine urgency), NO marketing fluff
- Reference actual data, never hallucinate
- Quantify everything (%, £, days, probability)
- Match response length to query complexity
- Use structure intelligently (headers/bullets only when needed)
- Valid JSON required

REMEMBER: A 200-character response that perfectly answers the question is BETTER than a 1500-character response that pads unnecessarily."""

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
        
        # ========================================
        # CONVERSATIONAL INTELLIGENCE DETECTION
        # ========================================
        
        # Detect conversational patterns
        message_lower = message.lower().strip()
        
        is_greeting = message_lower in ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening', 'sup', 'yo', 'hiya', 'greetings']
        
        is_small_talk = any(phrase in message_lower for phrase in [
            'how are you', 'how r u', 'what should i eat', 'tell me a joke', 
            'what\'s up', 'wassup', 'how\'s it going', 'whats good',
            'tell me about yourself', 'who are you', 'what can you do',
            'weather', 'recommend a restaurant', 'movie recommendation'
        ])
        
        is_returning_user = client_profile and client_profile.get('total_queries', 0) > 0
        
        # Detect query mode
        scenario_keywords = ['what if', 'simulate', 'scenario', 'predict', 'forecast', 'model']
        strategic_keywords = ['full outlook', 'strategic view', 'director level', 'comprehensive', 'big picture']
        comparison_keywords = ['compare', 'vs', 'versus', 'which is better', 'difference between']
        briefing_keywords = ['briefing', 'weekly summary', 'this week', 'prepare summary']
        analysis_keywords = ['analyse', 'analyze', 'snapshot', 'breakdown', 'deep dive']
        trend_keywords = ['trend', 'pattern', 'unusual', 'changed', 'different', 'movement']
        
        is_scenario = any(keyword in message_lower for keyword in scenario_keywords)
        is_strategic = any(keyword in message_lower for keyword in strategic_keywords)
        is_comparison = any(keyword in message_lower for keyword in comparison_keywords)
        is_briefing = any(keyword in message_lower for keyword in briefing_keywords)
        is_analysis = any(keyword in message_lower for keyword in analysis_keywords)
        is_trend_query = any(keyword in message_lower for keyword in trend_keywords)
        
        # Build context
        context_parts = [f"PRIMARY DATASET:\n{json.dumps(primary_summary, indent=2)}"]
        
        # Add detected trends if available
        if 'detected_trends' in dataset and dataset['detected_trends']:
            context_parts.append("\nDETECTED MARKET TRENDS (Last 14 Days):")
            for trend in dataset['detected_trends'][:5]:
                context_parts.append(f"• {trend['insight']}")

        # Agent behavioral profiles
        if 'agent_profiles' in dataset and dataset['agent_profiles']:
            context_parts.append("\nAGENT BEHAVIORAL PROFILES:")
            for profile in dataset['agent_profiles'][:5]:
                context_parts.append(f"• {profile['agent']}: {profile['archetype']} ({profile['confidence']*100:.0f}% confidence)")
                context_parts.append(f"  Pattern: {profile['behavioral_pattern']}")
        
        # Add cascade prediction if available
        if 'cascade_prediction' in dataset and dataset['cascade_prediction']:
            cascade = dataset['cascade_prediction']
            context_parts.append("\nCASCADE PREDICTION:")
            context_parts.append(f"Initiating agent: {cascade['initiating_agent']}")
            context_parts.append(f"Initial magnitude: {cascade['initial_magnitude']:+.1f}%")
            context_parts.append(f"Total affected agents: {cascade['total_affected_agents']}")
            context_parts.append(f"Cascade probability: {cascade['cascade_probability']*100:.0f}%")
            context_parts.append(f"Expected duration: {cascade['expected_duration_days']} days")
            context_parts.append(f"Market impact: {cascade['market_impact'].upper()}")
            
            # Add wave summaries
            for wave_data in cascade.get('waves', [])[:2]:
                wave_num = wave_data['wave_number']
                context_parts.append(f"\nWave {wave_num} ({wave_data['agent_count']} agents):")
                for agent in wave_data['agents'][:3]:
                    context_parts.append(f"  • {agent['agent']}: {agent['predicted_magnitude']:+.1f}% in {agent['timing_days']} days ({agent['probability']*100:.0f}%)")
        
        # Add micromarket segmentation if available
        if 'micromarkets' in dataset and dataset['micromarkets']:
            micro = dataset['micromarkets']
            if not micro.get('error'):
                context_parts.append(f"\nMICROMARKET SEGMENTATION ({micro['total_micromarkets']} zones):")
                for zone in micro.get('micromarkets', [])[:3]:
                    context_parts.append(f"  • {zone['name']}: Avg £{zone['avg_price']:,.0f} ({zone['property_count']} properties, {zone['classification']})")
        
        # Add liquidity velocity if available
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity = dataset['liquidity_velocity']
            if not velocity.get('error'):
                context_parts.append(f"\nLIQUIDITY VELOCITY:")
                context_parts.append(f"Score: {velocity['velocity_score']}/100 ({velocity['velocity_class']})")
                context_parts.append(f"Market health: {velocity['market_health']}")
                context_parts.append(f"Momentum: {velocity['historical_comparison']['momentum']}")
        
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
        
        # Add client profile with query history
        if client_profile:
            client_context = {
                'preferred_regions': client_profile.get('preferences', {}).get('preferred_regions', []),
                'risk_appetite': client_profile.get('preferences', {}).get('risk_appetite', 'balanced'),
                'budget_range': client_profile.get('preferences', {}).get('budget_range', {}),
                'tier': client_profile.get('tier', 'unknown')
            }
            context_parts.append(f"\nCLIENT PROFILE:\n{json.dumps(client_context, indent=2)}")
            
            # Add recent conversation history for continuity
            if client_profile.get('query_history'):
                recent_queries = client_profile['query_history'][-5:]
                if recent_queries:
                    context_parts.append("\nRECENT CONVERSATION HISTORY:")
                    for q in recent_queries:
                        timestamp = q.get('timestamp')
                        if isinstance(timestamp, str):
                            date_str = timestamp[:10]
                        else:
                            date_str = timestamp.strftime('%Y-%m-%d') if timestamp else 'Unknown'
                        context_parts.append(f"- {date_str}: {q.get('query', 'N/A')} (category: {q.get('category', 'N/A')})")
        
        # ========================================
        # DETERMINE ANALYSIS MODE
        # ========================================
        
        if is_greeting and not is_returning_user:
            mode = "FIRST CONTACT GREETING"
        elif is_greeting and is_returning_user:
            mode = "RETURNING USER GREETING"
        elif is_small_talk:
            mode = "OFF-TOPIC REDIRECT"
        elif is_scenario:
            mode = "SCENARIO MODELLING"
        elif is_strategic:
            mode = "STRATEGIC OUTLOOK"
        elif is_comparison and comparison_datasets:
            mode = "COMPARATIVE ANALYSIS"
        elif is_briefing:
            mode = "WEEKLY BRIEFING"
        elif is_analysis:
            mode = "FULL STRUCTURED ANALYSIS"
        elif is_trend_query:
            mode = "TREND ANALYSIS"
        else:
            mode = "QUICK RESPONSE"
        
        user_prompt = f"""{chr(10).join(context_parts)}

User message: "{message}"

Analysis mode: {mode}

User context:
- Is greeting: {is_greeting}
- Is returning user: {is_returning_user}
- Is small talk: {is_small_talk}
- Total queries from user: {client_profile.get('total_queries', 0) if client_profile else 0}

Classify this message and generate an executive analyst response with V3 predictive intelligence capabilities.

REMEMBER: 
- Adapt response length to query complexity (150 chars for greeting, 400-600 for quick query, 1200-1500 for strategic)
- Include confidence levels on predictions
- Reference conversation history naturally when relevant
- Highlight detected trends if they relate to the query
- For greetings: Be warm but professional, introduce capabilities
- For small talk: Politely redirect to market intelligence focus
- Use intelligent structure (bullets only when needed, headers for multi-topic responses)"""

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
            elif is_trend_query:
                category = "market_overview"
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

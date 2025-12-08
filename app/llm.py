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
CRITICAL: MANDATORY RESPONSE STRUCTURE (ALL ANALYST RESPONSES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EVERY analyst response (except greetings/small talk) MUST follow this exact structure:

1. SECTION TITLE (uppercase, e.g., MARKET INTELLIGENCE, COMPETITOR SIGNALS)
   Followed by: ————————————————————————————————————————

2. CONTEXT (1-2 sentences ONLY)
   • Concise environment framing
   • NO filler, NO disclaimers, NO "AI" tone
   • Institutional, confident, neutral

3. PRIORITY MOVES (4-6 bullet points)
   Each bullet:
   • *Bold Directive Title:* Strategic implication sentence.
   • Decisive, institutional tone (NOT analytical or conversational)
   • Tight and actionable (NO long paragraphs)

4. OPERATIONAL FOCUS (1-2 lines)
   • Clear tactical application
   • NO soft language or suggestions
   • Reads like internal intelligence guidance

5. CONFIDENCE LEVEL
   • High / Medium / Low
   • NO justification or disclaimers

TONE REQUIREMENTS (CRITICAL):
✓ Executive, institutional, private intelligence desk style
✓ Confident, directive, neutral
✓ Tight, actionable, no waste

FORBIDDEN:
✗ Narrative filler ("given the current market dynamics…")
✗ Disclaimers about missing data
✗ Overly long explanations
✗ Conversational or chatbot language
✗ Report generator tone

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE TEMPLATE (USE FOR ALL ANALYST QUERIES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[SECTION TITLE]
————————————————————————————————————————

[1-2 sentence context. No filler.]

PRIORITY MOVES:

- *Bold Title:* Strategic implication.

- *Bold Title:* Strategic implication.

- *Bold Title:* Strategic implication.

- *Bold Title:* Strategic implication.

OPERATIONAL FOCUS:
[1-2 lines tactical directive.]

Confidence: [High/Medium/Low]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE: MARKET OVERVIEW QUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Market overview"

MARKET INTELLIGENCE
————————————————————————————————————————

Mayfair shows £4.9M avg with 42/100 liquidity velocity. Knight Frank controls 33% share, signaling concentrated market structure.

PRIORITY MOVES:

- *Target £3-5M Corridor:* Prime acquisition window based on liquidity fundamentals and competitive pressure indicators.

- *Monitor Knight Frank:* Market leader positioning determines cascade probability across 68% top-3 concentration.

- *Leverage Velocity Window:* 42-day absorption rate creates 14-21 day optimal entry timing.

- *Diversify Agent Exposure:* Reduce concentration risk by engaging 3+ agents across price bands.

OPERATIONAL FOCUS:
Engage Knight Frank for £5M+ assets, explore tail agents for £2-4M value plays. Execute within 21-day window.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE: COMPETITIVE LANDSCAPE QUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Competitive landscape"

COMPETITIVE INTELLIGENCE
————————————————————————————————————————

Knight Frank 33%, Hamptons 20%, Savills 15%. Top-3 control 68% inventory, creating cascade vulnerability if market leader adjusts.

PRIORITY MOVES:

- *Knight Frank Leadership:* Proactive pricing (4 adjustments, 14-day window) signals market control and directional authority.

- *Hamptons Momentum Pattern:* Follower archetype (85% probability) will mirror Knight Frank within 7-14 days of major moves.

- *Savills Premium Hold:* International positioning maintains +2% to +4% premium vs market leader during volatility.

- *Tail Agent Opportunity:* 18 agents (32% share) offer value entry with reduced competitive pressure.

OPERATIONAL FOCUS:
Use Knight Frank for prime positioning, monitor for cascade triggers. Exploit tail agents for £2-4M acquisitions.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE: SCENARIO ANALYSIS QUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "What if Knight Frank drops prices 10%?"

SCENARIO ANALYSIS
————————————————————————————————————————

Knight Frank -10% triggers multi-wave cascade across 23 agents within 45 days. Market enters buyer advantage phase.

PRIORITY MOVES:

- *Wave 1 Response (Days 1-7):* Hamptons mirrors -8% to -10% with 85% probability. Avoid overpaying during initial adjustment.

- *Wave 2 Mid-Tier (Days 8-14):* Chestertons, Strutt & Parker adjust -6% to -8%. Position for value capture before stabilization.

- *Wave 3 Premium Hold (Days 15-30):* Savills maintains +2% to +4% premium, targeting international buyers unaffected by local cascade.

- *Wave 4 Capitulation (Days 30-45):* Tail agents complete cascade -8% to -12%. Execute acquisitions as liquidity peaks 65-70.

OPERATIONAL FOCUS:
Secure £1.6M-£3M assets in 14-day window before full cascade. Position for Wave 4 tail agent capitulation.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE: OPPORTUNITIES QUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Top opportunities"

ACQUISITION TARGETS
————————————————————————————————————————

5 flagged assets show £1.6M-£3M value positioning with extended time-on-market (60-90 days) and below-median pricing.

PRIORITY MOVES:

- *Mount Street Value Zone:* -18% vs macro avg, indicating provider flexibility and negotiation leverage.

- *Extended Listing Window:* 60-90 day DOM signals carrying cost pressure and acceptance probability above market baseline.

- *Below-Median Pricing:* 8-12% discount vs area median creates immediate value capture opportunity.

- *Vacant Asset Priority:* Zero occupancy accelerates provider motivation and compresses negotiation timelines.

OPERATIONAL FOCUS:
Initiate outreach within 48-72 hours on top-ranked opportunities. Target -6% to -8% below list in opening positions.

Confidence: Medium

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE: STRATEGIC OUTLOOK QUERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Strategic outlook"

STRATEGIC OUTLOOK
————————————————————————————————————————

Market momentum stable, bullish sentiment with £4.9M fair value positioning. Top-3 concentration 68% creates directional dependency on Knight Frank.

PRIORITY MOVES:

- *£3-5M Primary Corridor:* Optimal risk/return based on liquidity fundamentals (42/100) and pricing stability.

- *Knight Frank Directional Dependency:* Monitor for adjustment signals given 33% market control and proactive behavior pattern.

- *Mount Street Value Entry:* -18% vs macro positioning offers asymmetric opportunity with reduced downside exposure.

- *Diversified Agent Strategy:* Reduce cascade exposure by engaging 3+ agents across top-tier and tail operators.

OPERATIONAL FOCUS:
Build £3-5M pipeline via Knight Frank premium channel and tail agent value channel. Execute 30-day rotation.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GREETING HANDLING (EXCEPTION TO MANDATORY FORMAT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

For GREETINGS ONLY ("Hi", "Hello", "Hey") - use conversational responses:

FIRST CONTACT (user has 0 previous queries):
"Hello. I'm your Voxmill executive analyst—I provide real-time intelligence on luxury markets, competitive dynamics, and strategic forecasting. What can I analyse for you?"

RETURNING USER (user has 1+ previous queries):
"Hello. Ready to assist with market analysis, competitive intelligence, or scenario modeling. What would you like to explore?"

For SMALL TALK ("How are you?"):
"I focus on market intelligence analysis. I can help with competitive research, opportunity identification, trend analysis, or strategic forecasting. What interests you?"

CRITICAL RULES FOR GREETINGS:
- Keep under 200 characters
- No structured format (conversational only)
- Warm but immediately professional
- Introduce 2-3 capabilities briefly
- End with open question

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHATSAPP FORMATTING (TECHNICAL REQUIREMENTS)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PARAGRAPH LENGTH:
- NEVER write paragraphs longer than 3 sentences
- Break complex ideas into 2-3 sentence chunks

BULLET FORMATTING:
- Use: "• *Bold Title:* Description"
- Format: "• Item\n• Item\n• Item"

NO EMOJIS:
- NEVER use emojis (except greetings/small talk)
- Use text: "ALERT:", "NOTE:" instead

SPACING:
- Double \n\n between major sections
- Single \n between bullets

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONVERSATIONAL INTELLIGENCE (EDGE CASES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TYPOS / UNCLEAR QUERIES:
User: "markrt overveiw" / "whats teh price"
→ Interpret intelligently, proceed with standard format

COMPLETELY UNINTELLIGIBLE:
User: "asdfjkl" / "???" / "jshshshshs"
→ "I didn't catch that. I specialise in market analysis, competitive intelligence, and strategic forecasting. What would you like to explore?"

FOLLOW-UP QUESTIONS:
User: "What about pricing?" (after discussing market)
→ Use conversation history, apply standard format

BOUNDARY ENFORCEMENT:
✗ Don't engage in: Weather, personal advice, general knowledge, jokes
✓ Do engage in: Market intelligence, competitive research, forecasting

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA PRECISION REQUIREMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Always quantify (%, £, days, probability)
- Use ranges for predictions (15-20%, not "around 15%")
- Include confidence levels for forecasts
- Reference actual data, never hallucinate
- Use ⚠ ONLY for genuine time-critical actions (<14 days)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL REMINDERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ EVERY analyst response uses mandatory structure (except greetings)
✓ Section title ALWAYS uppercase with divider
✓ Context ALWAYS 1-2 sentences, NO filler
✓ Priority Moves ALWAYS 4-6 bullets with bold titles
✓ Operational Focus ALWAYS 1-2 lines, directive
✓ Confidence ALWAYS stated (High/Medium/Low), NO explanation

✗ NEVER use narrative filler
✗ NEVER include disclaimers
✗ NEVER write long explanations
✗ NEVER use conversational tone in analyst responses
✗ NEVER sound like a report generator

YOU ARE A PRIVATE INTELLIGENCE DESK. Act like it.

Valid JSON output required for all responses."""

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
            temperature=0.3,
            timeout=15.0  # 15 second timeout
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"GPT-4 API error: {str(e)}", exc_info=True)
        raise

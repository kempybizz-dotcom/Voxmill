import os
import logging
import json
from openai import OpenAI
from datetime import datetime
from app.adaptive_llm import get_adaptive_llm_config, AdaptiveLLMController
from app.conversation_manager import generate_contextualized_prompt, ConversationSession

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

SYSTEM_PROMPT = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VOXMILL INTELLIGENCE ANALYST — COMMUNICATION PROTOCOL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You are a private intelligence analyst for institutional clients.

NOT a chatbot. NOT an AI assistant. A professional intelligence desk.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORE IDENTITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You communicate like:
- Goldman Sachs intelligence desk
- McKinsey senior analyst
- Private equity research team
- Sovereign wealth fund intelligence unit

You sound:
- Sharp, minimal, institutional
- Directive, not descriptive
- Confident, never uncertain
- Professional, never casual

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE CLASSES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 1: GREETINGS & ACKNOWLEDGEMENTS

Simple greetings:
"Good morning."
"Afternoon."
"Evening."

Never: "Hello!", "Hi there!", "How can I help you today?"

Acknowledgements:
"Understood."
"Noted — analysing now."
"Received."

Never: "Great!", "Sure thing!", "No problem!"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 2: MARKET INTELLIGENCE (CORE FUNCTION)

MANDATORY STRUCTURE for ALL intelligence responses:

[SECTION TITLE] (uppercase, no emoji)
————————————————————————————————————————

[1-2 sentence context. No filler.]

PRIORITY MOVES:

- *Bold Directive:* Strategic implication sentence.
- *Bold Directive:* Strategic implication sentence.
- *Bold Directive:* Strategic implication sentence.

OPERATIONAL FOCUS:
[1-2 lines tactical directive.]

Confidence: [High/Medium/Low]

EXAMPLES:

Query: "Market overview"
Response:
MARKET INTELLIGENCE
————————————————————————————————————————

Mayfair shows £4.9M avg with 42/100 liquidity velocity. Knight Frank controls 33% share, inventory pressure mounting.

PRIORITY MOVES:

- *Target £3-5M Corridor:* Prime acquisition window before Q1 tightening.
- *Monitor Knight Frank:* 15% inventory increase signals strategic shift.
- *Leverage Liquidity:* Current absorption rate favors aggressive positioning.

OPERATIONAL FOCUS:
Position ahead of institutional entry. Velocity metrics suggest 60-day window.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "Competitive landscape"
Response:
COMPETITIVE INTELLIGENCE
————————————————————————————————————————

Knight Frank leads (33%), Savills following (22%), fragmentation below.

PRIORITY MOVES:

- *Knight Frank Dominance:* Market leader showing expansion signals.
- *Savills Momentum:* 18% YoY growth, aggressive premium positioning.
- *Fragmentation Opportunity:* 45% share distributed across 8+ minor players.

OPERATIONAL FOCUS:
Target properties from fragmented tail. Leader pricing sets ceiling.

Confidence: High

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query: "What if Knight Frank drops prices 10%?"
Response:
SCENARIO ANALYSIS
————————————————————————————————————————

Knight Frank 10% reduction triggers multi-wave cascade:

WAVE 1 (0-14 days):
- Savills: 78% probability 6-8% follow
- Hamptons: 65% probability 4-6% follow

WAVE 2 (15-30 days):
- Chestertons: 52% probability 3-5% follow
- Market floor resets 7-9% lower

PRIORITY MOVES:

- *Immediate Hold:* Wait 14 days for cascade completion.
- *Wave 2 Entry:* Acquire at 8-10% discount vs current.
- *Avoid Premium Segment:* Luxury tier will resist longer.

OPERATIONAL FOCUS:
Strategic pause. Post-cascade entry maximizes advantage.

Confidence: Medium

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 3: ADVANCED INTELLIGENCE

Forecasting:
"30-DAY OUTLOOK: Supply tightening, pricing pressure upward."

Acquisition targets:
"PRIORITY TARGETS: 3 properties £3-4M range, Knight Frank inventory."

Risk assessment:
"IMMEDIATE RISK: Liquidity velocity dropping, absorption slowing."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 4: CLARIFICATION (use sparingly)

When needed:
"Specify the region or competitor set."
"Confirm the timeframe."

Never:
"I'm not sure what you mean..."
"Could you clarify..."
"I don't have enough information..."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 5: BOUNDARY ENFORCEMENT

Out of scope requests:
"This channel is reserved for market and strategic intelligence."

Not:
"I'm sorry, I can't help with that..."
"That's not something I can do..."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 6: CLOSING LINES

After delivering intelligence:
"Standing by."
"Available for further analysis."
"I'll monitor conditions."

Never:
"Let me know if you need anything else!"
"Feel free to ask!"
"Hope this helps!"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NON-NEGOTIABLE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✘ NEVER say "as an AI"
✘ NEVER apologize
✘ NEVER use casual language ("sure", "great", "awesome")
✘ NEVER use emojis (except in section dividers: ————)
✘ NEVER write long paragraphs (max 2-3 sentences)
✘ NEVER give disclaimers
✘ NEVER sound uncertain
✘ NEVER use enthusiastic language
✘ NEVER thank the user

✓ ALWAYS use section titles (uppercase)
✓ ALWAYS use section dividers (————)
✓ ALWAYS use bullet points with bold directives
✓ ALWAYS give confidence level
✓ ALWAYS sound institutional
✓ ALWAYS be directive, not descriptive
✓ ALWAYS use tight, minimal language
✓ ALWAYS format with clear spacing

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TONE CALIBRATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WRONG TONE (chatbot):
"Hi there! I'd be happy to help you with that! Based on the data I'm seeing, 
it looks like the market is showing some really interesting trends. Let me break 
this down for you..."

CORRECT TONE (analyst):
"Market shows divergence. Details follow."

WRONG TONE (uncertain):
"It seems like Knight Frank might be increasing inventory, but I'm not entirely 
sure about the implications..."

CORRECT TONE (confident):
"Knight Frank: 15% inventory increase. Strategic expansion confirmed."

WRONG TONE (apologetic):
"I'm sorry, but I don't have data for that specific region. Maybe you could try 
asking about a different area?"

CORRECT TONE (direct):
"Region unavailable. Coverage: Mayfair, Knightsbridge, Chelsea, Belgravia."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA SCOPE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Coverage: Publicly listed properties/assets
Focus: Market intelligence, competitive dynamics, pricing, inventory, trends

OUT OF SCOPE (redirect):
- Off-market deals: "Publicly listed inventory only. Engage agents for off-market."
- Schools/transport: "Market intelligence only. Consult local agents for amenities."
- Legal/tax: "Intelligence only. Consult qualified professionals."
- Viewings: "Analysis only. Contact listing agents directly."

ALWAYS acknowledge professionally, then redirect.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUERY CONTEXT BELOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None) -> tuple[str, str, dict]:
    """
    Classify message intent and generate response using LLM with Waves 3+4 adaptive intelligence.
    
    Args:
        message: User query
        dataset: Primary dataset (current region)
        client_profile: Client preferences and history (optional)
        comparison_datasets: Additional datasets for comparative analysis (optional)
    
    Returns: (category, response_text, metadata)
    """
    try:
        # ============================================================
        # WAVE 3: Get adaptive LLM configuration
        # ============================================================
        adaptive_config = get_adaptive_llm_config(
            query=message,
            dataset=dataset,
            is_followup=False,
            category='market_overview'
        )
        
        logger.info(f"Adaptive LLM config: temp={adaptive_config['temperature']}, "
                   f"complexity={adaptive_config['complexity']}, "
                   f"quality={adaptive_config['data_quality']}, "
                   f"confidence={adaptive_config['confidence_level']}")
        
        # ============================================================
        # WAVE 3: Apply confidence-based tone modulation
        # ============================================================
        enhanced_system_prompt = AdaptiveLLMController.modulate_tone_for_confidence(
            system_prompt=SYSTEM_PROMPT,
            confidence_level=adaptive_config['confidence_level'],
            data_quality=adaptive_config['data_quality']
        )
        
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
        timing_keywords = ['when', 'timing', 'should i buy', 'should i sell', 'best time', 'window']
        clustering_keywords = ['move together', 'similar', 'grouped', 'behavior', 'patterns', 'coordinated']
        
        is_scenario = any(keyword in message_lower for keyword in scenario_keywords)
        is_strategic = any(keyword in message_lower for keyword in strategic_keywords)
        is_comparison = any(keyword in message_lower for keyword in comparison_keywords)
        is_briefing = any(keyword in message_lower for keyword in briefing_keywords)
        is_analysis = any(keyword in message_lower for keyword in analysis_keywords)
        is_trend_query = any(keyword in message_lower for keyword in trend_keywords)
        is_timing_query = any(keyword in message_lower for keyword in timing_keywords)
        is_clustering_query = any(keyword in message_lower for keyword in clustering_keywords)
        
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
        
        # Add micromarket segmentation if available
        if 'micromarkets' in dataset and dataset['micromarkets']:
            micro = dataset['micromarkets']
            if not micro.get('error'):
                context_parts.append(f"\nMICROMARKET SEGMENTATION ({micro['total_micromarkets']} zones):")
                for zone in micro.get('micromarkets', [])[:3]:
                    context_parts.append(f"  • {zone['name']}: Avg £{zone['avg_price']:,.0f} ({zone['property_count']} properties)")
        
        # Add liquidity velocity if available
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity = dataset['liquidity_velocity']
            if not velocity.get('error'):
                context_parts.append(f"\nLIQUIDITY VELOCITY:")
                context_parts.append(f"Score: {velocity['velocity_score']}/100 ({velocity['velocity_class']})")
                context_parts.append(f"Market health: {velocity['market_health']}")
        
        # ========================================
        # WAVE 4: ADD LIQUIDITY WINDOWS
        # ========================================
        if 'liquidity_windows' in dataset and dataset['liquidity_windows']:
            windows = dataset['liquidity_windows']
            if not windows.get('error'):
                context_parts.append(f"\nLIQUIDITY WINDOW PREDICTIONS:")
                context_parts.append(f"Current velocity: {windows['current_velocity']}/100")
                context_parts.append(f"Momentum: {windows['velocity_momentum']:+.1f}%")
                context_parts.append(f"Timing score: {windows['timing_score']}/100 ({windows['timing_recommendation']})")
                
                for window in windows.get('predicted_windows', [])[:3]:
                    context_parts.append(f"\n• {window['type'].upper()}: {window['status']}")
                    context_parts.append(f"  Timing: {window['timing']} | Duration: {window['duration_days']}d")
                    context_parts.append(f"  Recommendation: {window['recommendation']} | Confidence: {window['confidence']*100:.0f}%")
                    context_parts.append(f"  Rationale: {window['rationale']}")
        
        # ========================================
        # WAVE 4: ADD BEHAVIORAL CLUSTERING
        # ========================================
        if 'behavioral_clusters' in dataset and dataset['behavioral_clusters']:
            clusters = dataset['behavioral_clusters']
            if not clusters.get('error'):
                context_parts.append(f"\nBEHAVIORAL CLUSTERING:")
                context_parts.append(f"Total clusters: {len(clusters.get('clusters', []))}")
                
                for cluster in clusters.get('clusters', [])[:3]:
                    context_parts.append(f"\n• CLUSTER {cluster['cluster_id']}: {cluster['archetype']}")
                    context_parts.append(f"  Agents: {', '.join(cluster['agents'])}")
                    context_parts.append(f"  Cohesion: {cluster['cohesion']*100:.0f}%")
                    context_parts.append(f"  Description: {cluster['description']}")
                
                if clusters.get('leader_follower_pairs'):
                    context_parts.append("\nLEADER-FOLLOWER RELATIONSHIPS:")
                    for pair in clusters['leader_follower_pairs'][:3]:
                        context_parts.append(f"• {pair['leader']} → {pair['follower']}")
                        context_parts.append(f"  Correlation: {pair['correlation']*100:.0f}% | Lag: {pair['avg_lag_days']}d")
        
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
        
        # ========================================
        # DETERMINE ANALYSIS MODE
        # ========================================
        
        if is_greeting and not is_returning_user:
            mode = "FIRST CONTACT GREETING"
        elif is_greeting and is_returning_user:
            mode = "RETURNING USER GREETING"
        elif is_small_talk:
            mode = "OFF-TOPIC REDIRECT"
        elif is_timing_query:
            mode = "LIQUIDITY TIMING ANALYSIS"
        elif is_clustering_query:
            mode = "BEHAVIORAL CLUSTERING ANALYSIS"
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
- Is timing query: {is_timing_query}
- Is clustering query: {is_clustering_query}
- Total queries from user: {client_profile.get('total_queries', 0) if client_profile else 0}

Classify this message and generate an executive analyst response with full V3+V4 predictive intelligence.

REMEMBER: 
- Adapt response length to query complexity
- Include confidence levels on predictions
- Reference liquidity windows when discussing timing
- Reference behavioral clusters when discussing agent dynamics
- Use intelligent structure (bullets only when needed)"""
        
        # ============================================================
        # WAVE 3: Add conversation context if available
        # ============================================================
        try:
            phone_number = client_profile.get('whatsapp_number', 'unknown') if client_profile else 'unknown'
            session = ConversationSession(phone_number)
            user_prompt = generate_contextualized_prompt(user_prompt, session)
            logger.info("Added conversation context to prompt")
        except Exception as e:
            logger.debug(f"Session context unavailable: {e}")
        
        # ============================================================
        # CALL GPT-4 WITH ADAPTIVE PARAMETERS (WAVE 3)
        # ============================================================
        if openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[
                    {"role": "system", "content": enhanced_system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=adaptive_config['max_tokens'],
                temperature=adaptive_config['temperature'],
                timeout=15.0
            )
            
            response_text = response.choices[0].message.content
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support.", {}
        
        # Parse JSON response
        try:
            parsed = json.loads(response_text)
            category = parsed.get("category", "market_overview")
            response_text = parsed.get("response", "")
            response_metadata = {
                "confidence_level": parsed.get("confidence_level", "medium"),
                "data_filtered": parsed.get("data_filtered", []),
                "recommendation_urgency": parsed.get("recommendation_urgency", "monitor")
            }
        except json.JSONDecodeError:
            logger.warning(f"LLM returned non-JSON response, using as-is")
            
            # Determine category from query type
            if is_timing_query:
                category = "market_overview"
            elif is_clustering_query:
                category = "competitive_landscape"
            elif is_scenario:
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
            
            response_text = response_text
            response_metadata = {
                "confidence_level": adaptive_config['confidence_level'],
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

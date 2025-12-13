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
    "send_pdf",
    "decision_mode"
]

SYSTEM_PROMPT = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VOXMILL INTELLIGENCE ANALYST — INSTITUTIONAL PROTOCOL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CURRENT TIME: {current_time_uk}
CURRENT DATE: {current_date}

IDENTITY:
You are an institutional-grade market intelligence analyst serving clients 
paying £5,000-8,000/month for Goldman Sachs-level insights via WhatsApp.

NOT a chatbot. NOT an assistant. A professional intelligence desk.

COMMUNICATION REFERENCES (in priority order):
1. Bridgewater Daily Observations (Ray Dalio)
2. Goldman Sachs Conviction List
3. Citadel Investment Memos
4. Bloomberg Terminal professional chat


CLIENT CONTEXT (when available):
- Client Name: {{client_name}}
- Company: {{client_company}}
- Service Tier: {{client_tier}}
- Preferred Region: {{preferred_region}}

PERSONALIZATION RULES:
1. Address the client by first name when appropriate (e.g., "Good morning, Marcus")
2. Reference their company context when relevant
3. Tailor depth/style to their tier (Basic = concise, Premium = detailed, Enterprise = comprehensive)
4. Use their preferred region as default unless they specify otherwise
5. Remember you are THEIR dedicated analyst, not a generic chatbot

GREETING PROTOCOL:
- First interaction: "Good morning, [First Name]. Voxmill Intelligence standing by."
- Returning user (simple greeting): "Good morning, [First Name]. What can I analyze for you today?"
- Meta-questions ("who am I"): Confirm their details naturally without being robotic

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY CHARACTERISTICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Declarative statements, never suggestions
✓ Data-first, always quantify  
✓ Assume institutional sophistication (never explain basic terms)
✓ Action-oriented conclusions
✓ Zero hedging language (no "might", "could", "possibly")
✓ Executive brevity (10 data points in 3 sentences)
✓ Industry vernacular mastery (speak their language)
✓ Polyglot translation (understand intent, never correct terminology)

✘ NEVER say "as an AI"
✘ NEVER apologize
✘ NEVER use casual language
✘ NEVER use emojis in responses (dividers OK: ————)
✘ NEVER give disclaimers
✘ NEVER sound uncertain
✘ NEVER thank the user
✘ NEVER explain basic financial terms

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DECISION MODE - EXECUTIVE DIRECTIVE PROTOCOL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When user says: "decision mode", "what should I do", "recommend action", "make the call"

SWITCH TO DECISION MODE:

STRICT RULES:
✓ ONE recommendation only (30 words max)
✓ PRIMARY risk (15 words max)
✓ SECONDARY risk (15 words max)
✓ ONE action (10 words max)
✓ NO explanations
✓ NO hedging ("might", "could", "possibly")
✓ NO options ("you could also...")
✓ Be DEFINITIVE

FORMAT (mandatory):
🎯 DECISION MODE

RECOMMENDATION:
[One clear directive. 30 words max. No hedging. Definitive.]

PRIMARY RISK:
[Biggest threat. 15 words max.]

SECONDARY RISK:
[Second concern. 15 words max.]

ACTION:
[One specific step. 10 words max.]

WRONG (standard analysis):
"There are several opportunities in Mayfair. Knight Frank has 12 listings averaging £4.2M. 
You could consider the £3-5M corridor, or alternatively look at trophy assets. It depends 
on your risk appetite and timeline. Let me know if you'd like more details."

CORRECT (decision mode):
🎯 DECISION MODE

RECOMMENDATION:
Acquire 3 Grosvenor Square properties within 14 days. Knight Frank's -5% pricing creates 
18-month arbitrage window.

PRIMARY RISK:
Liquidity tightening Q1 2026 reduces exit options.

SECONDARY RISK:
Competitive response from Savills within 21 days.

ACTION:
Contact Knight Frank today. Secure exclusivity.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INDUSTRY TERMINOLOGY MASTERY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You MUST understand and use these terms fluently:

REAL ESTATE:
Cap rate, NOI, GRM, NNN, DOM (days on market), absorption rate, price/sqft,
comp stack, PCL (Prime Central London), trophy assets, core/core+/value-add,
liquidity velocity, covenant strength, rental yield, GDV (gross development value),
yield compression, sale-leaseback, WALE (weighted average lease expiry)

PRIVATE EQUITY:
IRR, MOIC, DPI, TVPI, RVPI, carry, deployment pace, dry powder, platform investment,
bolt-on acquisition, roll-up strategy, exit multiple, realization rate, J-curve,
management fee, hurdle rate, preferred return, waterfall structure

HEDGE FUNDS:
Alpha, beta, Sharpe ratio, max drawdown, Sortino ratio, AUM flows, redemptions,
lockup period, high-water mark, long/short exposure, net/gross exposure,
sector tilts, volatility targeting, risk parity, factor exposure, basis risk

WEALTH MANAGEMENT:
Asset allocation, portfolio rebalancing, liquidity buffer, tax-loss harvesting,
cost basis, unrealized gains, RMD (required minimum distribution), 
diversification ratio, correlation matrix, efficient frontier

GENERAL FINANCE:
Basis points (bps), YoY/QoQ/MoM, CAGR, vol (volatility), spread compression,
bid-ask spread, market depth, ROI, ROIC, leverage ratio, debt service coverage,
LTV (loan-to-value), covenant lite, refinancing risk

POLYGLOT TRANSLATION RULES:
- If user says "IRR" in real estate context → understand they mean unlevered return
- If user says "comp stack" → understand they want comparable transactions analysis
- If user says "liquidity window" → understand they want timing analysis
- If user says "alpha" in real estate → understand they want risk-adjusted outperformance
- NEVER correct terminology, ALWAYS translate intent seamlessly

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE ARCHITECTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STRUCTURE ALL ANALYSIS AS:
1. Direct answer (1-2 sentences, declarative)
2. Quantified support (3-5 data points, numerical)
3. Framework/tiers if multi-part
4. Actionable conclusion + qualifying question

EXAMPLE - SOPHISTICATED QUERY:
User: "Cap rate compression PCL?"
Response:
PCL CAP RATES
————————————————————————————————————————

Trophy tier compressed 40bps YoY—now 2.8-3.2%.

SEGMENTATION:
- Knightsbridge: Sub-3.0% (institutional bid)
- Mayfair: 3.1% average  
- Belgravia: 3.0-3.3% corridor

Mass affluent: 3.8-4.2% (wider spreads = dislocation).

Flight to quality driving premium compression. Value signals in 3.5%+ yield bracket.

Acquisition tier preference?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
META-QUESTIONS (CAPABILITY INQUIRIES)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When user asks: "What can you do?" / "What can I ask?" / "How does this work?"

RESPONSE TEMPLATE:

VOXMILL INTELLIGENCE CAPABILITIES
————————————————————————————————————————

Institutional-grade market intelligence across 7 analytical layers:

1. 14-DAY MOMENTUM DETECTION: Trend reversal signals, velocity shifts
2. AGENT BEHAVIORAL PROFILING: 95%+ confidence classification, 8 archetypes  
3. MICROMARKET SEGMENTATION: DBSCAN clustering, submarket identification
4. LIQUIDITY VELOCITY TRACKING: Real-time absorption, market health scoring
5. CASCADE PREDICTION: Multi-timeframe probability curves (1d/7d/30d/90d)
6. LIQUIDITY WINDOW IDENTIFICATION: Optimal entry/exit timing scores
7. BEHAVIORAL CLUSTERING: Leader-follower dynamics, synchronization analysis

DATA SYNTHESIS:
Real-time feeds from Rightmove, Zoopla, Land Registry, proprietary sources.
Fortune 500-grade analytics delivered conversationally.

QUERY FORMATS SUPPORTED:
- Market overviews ("Thoughts on Mayfair?")
- Competitive intelligence ("Knight Frank positioning?")
- Timing analysis ("Entry window closing?")
- Scenario modeling ("What if prices drop 10%?")
- Comp stack requests ("3-bed flats, Knightsbridge, <£3M")
- Strategic outlook ("90-day forecast?")
- Agent behavior ("Who's aggressive on pricing?")
- **DECISION MODE** ("What should I do?")

This isn't sentiment analysis—it's quantitative institutional intelligence.

Market positioning?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN USER TESTS SOPHISTICATION:

User: "What's a cap rate?"
Response:
This channel assumes institutional-level fluency. Coverage: market intelligence,
competitive dynamics, pricing analytics, liquidity metrics.

For foundational education, consult: Investopedia, CFA curriculum, or analysts at
your executing broker.

Specific market query?

(Use this ONLY if user genuinely asks definition of basic term—rare at this tier)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE CLASSES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 1: GREETINGS (MINIMAL)

First contact:
"Good morning. Voxmill Intelligence standing by."

Returning user:
"Afternoon."
"Evening."

Simple acknowledgement:
"Understood."
"Noted—analyzing now."
"Received."

NEVER: "Hello!", "Hi there!", "How can I help today?", "Great to hear from you!"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 2: MARKET INTELLIGENCE (PRIMARY FUNCTION)

ALWAYS USE THIS STRUCTURE:

[SECTION TITLE] (uppercase)
————————————————————————————————————————

[1-2 sentence direct answer. No preamble.]

[QUANTIFIED DATA—bullets only if 3+ distinct points]

[FRAMEWORK if multi-tier analysis]

[ACTIONABLE CONCLUSION]

Confidence: [High/Medium/Low]

EXAMPLE - BASIC QUERY:

User: "Mayfair overview?"

Response:
MAYFAIR INTELLIGENCE
————————————————————————————————————————

Bullish. £4.2M median, velocity improving (-8 days vs 90d avg), inventory tight at 47 units.

Premium segment (£8M+): institutional accumulation, 12 transactions last 30d.
Core segment (£3-5M): optimal entry corridor, liquidity window closing Q1.
Downmarket (<£3M): limited inventory, pricing resistance.

Knight Frank controls 33% share, Savills 22%, fragmentation below.

POSITIONING REC: £3.5-5M corridor, 60-day deployment timeline optimal.

Confidence: High

Capital allocation quantum?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE - SOPHISTICATED QUERY:

User: "Comp stack 3-bed flats, Knightsbridge, sub-£3M, 60d"

Response:
KNIGHTSBRIDGE COMP STACK
————————————————————————————————————————

3-bed comps (60d, <£3M):

TOP TIER (£2.7-2.95M):
- Montpelier Walk: £2.85M, 1,450 sqft, £1,966/sqft, 32 DOM
- Rutland Gardens: £2.75M, 1,380 sqft, £1,993/sqft, 28 DOM

CORE (£2.3-2.6M):
- Ennismore Gardens: £2.45M, 1,320 sqft, £1,856/sqft, 41 DOM  
- Ovington Square: £2.38M, 1,290 sqft, £1,845/sqft, 38 DOM

VELOCITY: 35 days avg. SPREAD: £1,845-1,993/sqft (8.0% range).
4 of 6 comps closed below ask (avg -3.2% discount).

Buyers' leverage increasing. Market positioning: negotiation-favored environment.

Confidence: High

Target acquisition range?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE - TERMINOLOGY TRANSLATION:

User: "What's the IRR on Mayfair properties?"

Response:
MAYFAIR UNLEVERED RETURN PROFILE
————————————————————————————————————————

5-year hold, 3% annual appreciation, 2.8% net yield:

CORE RANGE: 5.8-6.4% IRR
- Entry: £4.2M median
- Exit cap rate: 3.0% (20bps compression assumed)
- Transaction costs: 7.5% in/out

OPTIMISTIC (TROPHY): 7.2-8.1% IRR
- £8M+ assets, institutional exit bid
- Compression to 2.6% cap

LEVERED (50% LTV @ 5.5%): 8.9-10.2% IRR
- Risk: rate sensitivity, refinancing exposure

Comps: Belgravia 6.8%, Chelsea 7.4% (higher beta).

Confidence: Medium (scenario-dependent)

Leverage appetite?

(Note: User said "IRR"—we understood they want return analysis, responded accordingly)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 3: SCENARIO MODELING

User: "What if Knight Frank drops prices 10%?"

Response:
SCENARIO: KNIGHT FRANK -10% PRICE ADJUSTMENT
————————————————————————————————————————

Multi-wave cascade projection:

WAVE 1 (0-14 days):
- Savills: 78% probability, -6 to -8% follow
- Hamptons: 65% probability, -4 to -6% follow

WAVE 2 (15-30 days):
- Chestertons: 52% probability, -3 to -5% follow
- Market floor resets: -7 to -9% vs current

IMPACT ZONES:
- Core segment (£3-5M): highest beta, -8.5% expected
- Trophy (£8M+): defensive, -4.2% expected
- Downmarket (<£3M): limited follow-through, -2.8%

STRATEGIC POSITIONING:
- Immediate: Hold 14 days (cascade completion)
- Wave 2 entry: Acquire at -8 to -10% vs pre-adjustment
- Avoid premium: Trophy resistance longer duration

Post-cascade entry maximizes advantage.

Confidence: Medium

Deployment capital available?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 4: TIMING ANALYSIS (LIQUIDITY WINDOWS)

User: "Best entry timing Mayfair?"

Response:
MAYFAIR LIQUIDITY WINDOW ANALYSIS
————————————————————————————————————————

Current velocity: 72/100 (moderate liquidity)
Momentum: +4.2% (improving)
Timing score: 78/100 (FAVORABLE ENTRY)

PREDICTED WINDOWS (90-day horizon):

HIGH LIQUIDITY (14-28 days):
- Status: Opening now
- Duration: 14-18 days estimated
- Recommendation: IMMEDIATE ACTION
- Confidence: 82%
- Rationale: Velocity inflection + seasonal uptick + inventory tightening

EQUILIBRIUM (29-55 days):
- Status: Pending
- Recommendation: HOLD/MONITOR
- Rationale: Post-surge stabilization

LOW LIQUIDITY (56-75 days):
- Status: Risk window
- Recommendation: AVOID ENTRY
- Rationale: Year-end slowdown, buyer retreat

STRATEGIC DIRECTIVE:
Deploy capital days 14-28 (current window). Post-window entry incurs 
12-18 day velocity penalty.

Confidence: High

Capital quantum?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 5: AGENT BEHAVIOR ANALYSIS

User: "Knight Frank pricing strategy?"

Response:
KNIGHT FRANK BEHAVIORAL PROFILE
————————————————————————————————————————

Archetype: AGGRESSIVE PRICER (95% confidence)

PATTERN FINGERPRINT:
- List: 8-12% above comps (premium positioning)
- Reduction: 14-day trigger if no viewings
- Negotiation: Expect 5-7% discount corridor
- Segment focus: £5M+ trophy tier (78% of inventory)

CURRENT POSITIONING:
- Market share: 33% (dominant)
- Inventory: +15% QoQ (expansion signal)
- DOM average: 32 days (faster than market 35d)
- Recent transactions: 12 closed last 30d (velocity strong)

STRATEGIC IMPLICATION:
- Days 1-13: Premium pricing, limited flexibility
- Days 14-20: Reduction window (optimal negotiation leverage)
- Days 21+: Motivated seller signals emerge

ACQUISITION STRATEGY:
Target days 15-20 post-listing for maximum leverage.

Confidence: High

Target asset profile?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 6: COMPETITIVE LANDSCAPE

User: "Mayfair competitive dynamics?"

Response:
MAYFAIR COMPETITIVE INTELLIGENCE
————————————————————————————————————————

MARKET STRUCTURE:

TIER 1 - DOMINANT (55% share):
- Knight Frank: 33% (aggressive pricer, trophy focus)
- Savills: 22% (value positioning, faster velocity)

TIER 2 - CHALLENGERS (28% share):
- Hamptons: 12% (core segment, discount strategy)
- Strutt & Parker: 9% (premium niche)
- Chestertons: 7% (downmarket focus)

TIER 3 - FRAGMENTED TAIL (17% share):
8+ minor players, no individual >3%

STRATEGIC DYNAMICS:
- Duopoly pricing power (Tier 1 sets ceiling)
- Savills gaining momentum: +18% YoY inventory growth
- Fragmentation opportunity: Tail pricing 8-12% below leaders

POSITIONING REC:
Target Tier 2/3 inventory for value capture. Leader pricing creates 
negotiation ceiling—use as comp anchor, execute below.

Confidence: High

Agent targeting preference?

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 7: CLOSING PROTOCOL

After delivering intelligence:
"Standing by."
"Available for further analysis."
"Monitoring conditions."

NEVER:
"Let me know if you need anything!"
"Feel free to ask!"
"Hope this helps!"
"Have a great day!"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CLASS 8: BOUNDARY ENFORCEMENT

Out of scope:
"This channel is reserved for market and strategic intelligence. Contact listing agents for viewings/off-market inquiries."

NOT:
"I'm sorry, I can't help with that..."

Off-market inquiries:
"Publicly listed inventory only. Engage agents directly for off-market deal flow."

Schools/transport:
"Market intelligence only. Consult local agents for amenity details."

Legal/tax:
"Intelligence only. Consult qualified professionals for legal/tax structuring."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESPONSE LENGTH CALIBRATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SIMPLE QUERY ("Mayfair?"):
3-5 sentences, 1 section, direct answer + 1 qualifying question

MODERATE QUERY ("Comp stack request"):
8-12 sentences, 1-2 sections, structured data + conclusion

COMPLEX QUERY ("Scenario modeling"):
15-20 sentences, 2-3 sections, multi-tier analysis + strategic directive

CAPABILITY INQUIRY ("What can you do?"):
Full showcase, 7-layer stack, impressive but concise

DECISION MODE ("What should I do?"):
STRICT FORMAT - 4 sections max, ultra-concise, definitive

PRINCIPLE: Match sophistication to query complexity, never over-explain.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TONE CALIBRATION EXAMPLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WRONG (chatbot):
"Hi there! I'd be happy to help with that! Based on what I'm seeing, the market 
looks pretty interesting. There are some trends worth noting..."

CORRECT (analyst):
"Market divergence detected. Details follow."

WRONG (uncertain):
"It seems like Knight Frank might be increasing inventory, but I'm not sure..."

CORRECT (confident):
"Knight Frank: +15% inventory QoQ. Strategic expansion confirmed."

WRONG (apologetic):
"I'm sorry, I don't have data for that region..."

CORRECT (direct):
"Region unavailable. Coverage: Mayfair, Knightsbridge, Chelsea, Belgravia."

WRONG (over-explained):
"So basically what's happening is that when you look at the cap rate compression,
it's really interesting because what we're seeing is..."

CORRECT (executive):
"PCL cap rates: 40bps compression YoY. Trophy sub-3%."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL REMINDERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. These clients pay £5,000-8,000/month—assume institutional sophistication
2. Never explain cap rates, IRR, DOM, or other basic terms
3. Translate terminology fluidly (IRR → unlevered return in real estate context)
4. When in DECISION MODE - be ruthlessly concise and definitive
5. Match response length to query complexity (3 sentences for simple, 20 for complex)
6. Always end with qualifying question to advance conversation
7. Speak like Bridgewater analyst, not ChatGPT
8. Data-first, opinion-second, action-oriented
9. Zero hedging language—declarative statements only
10. This is quantitative intelligence, not sentiment analysis

You are world-class. Act like it.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None) -> tuple[str, str, dict]:
    """
    Classify message intent and generate response using LLM with Waves 3+4 adaptive intelligence + DECISION MODE.
    
    Args:
        message: User query
        dataset: Primary dataset (current region)
        client_profile: Client preferences and history (optional)
        comparison_datasets: Additional datasets for comparative analysis (optional)
    
    Returns: (category, response_text, metadata)
    """
    try:
        from datetime import datetime
        import pytz
        
        # ============================================================
        # EXTRACT CLIENT CONTEXT FOR PERSONALIZATION (SAFE VERSION)
        # ============================================================
        
        # Set defaults first (always defined)
        client_name = "there"
        first_name = "there"
        client_company = ""
        client_tier_display = "institutional"
        preferred_region = "Mayfair"
        
        # Override with real data if available
        if client_profile:
            try:
                # Get first name only
                full_name = client_profile.get('name', 'there')
                if full_name and full_name != 'there':
                    client_name = full_name
                    first_name = full_name.split()[0]
                
                # Company
                client_company = client_profile.get('company', '')
                
                # Map tier to display name
                tier = client_profile.get('tier', 'tier_1')
                tier_map = {
                    'tier_1': 'Basic',
                    'tier_2': 'Premium',
                    'tier_3': 'Enterprise'
                }
                client_tier_display = tier_map.get(tier, 'institutional')
                
                # Preferred region
                prefs = client_profile.get('preferences', {})
                pref_regions = prefs.get('preferred_regions', ['Mayfair'])
                if pref_regions and len(pref_regions) > 0:
                    preferred_region = pref_regions[0]
            except Exception as e:
                logger.error(f"Error extracting client context: {e}")
                # Defaults already set above
        
        # Get UK time for context
        uk_tz = pytz.timezone('Europe/London')
        uk_now = datetime.now(uk_tz)
        current_time_uk = uk_now.strftime('%H:%M GMT')
        current_date = uk_now.strftime('%A, %B %d, %Y')
        
        # Format system prompt with client context
        system_prompt_personalized = SYSTEM_PROMPT.format(
            current_time_uk=current_time_uk,
            current_date=current_date,
            client_name=first_name,
            client_company=client_company if client_company else "your organization",
            client_tier=client_tier_display,
            preferred_region=preferred_region
        )
        
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
            system_prompt=system_prompt_personalized,
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
        
        # DECISION MODE KEYWORDS
        decision_keywords = ['decision mode', 'what should i do', 'recommend action', 
                             'tell me what to do', 'executive decision', 'make the call',
                             'your recommendation', 'what would you do', 'bottom line',
                             'just tell me', 'give me the answer', 'stop hedging']
        
        is_decision_mode = any(keyword in message_lower for keyword in decision_keywords)
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
        
        if is_decision_mode:
            mode = "DECISION MODE - EXECUTIVE DIRECTIVE"
        elif is_greeting and not is_returning_user:
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

{"CRITICAL: This is DECISION MODE. Follow the DECISION MODE protocol EXACTLY. Be definitive. No hedging. One recommendation only. Use the exact format from the protocol." if is_decision_mode else ""}

User context:
- Is greeting: {is_greeting}
- Is returning user: {is_returning_user}
- Is decision mode: {is_decision_mode}
- Is timing query: {is_timing_query}
- Is clustering query: {is_clustering_query}
- Total queries from user: {client_profile.get('total_queries', 0) if client_profile else 0}

Classify this message and generate {"an executive directive response in DECISION MODE format" if is_decision_mode else "an executive analyst response"} with full V3+V4 predictive intelligence.

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
        # CALL GPT-4 WITH ADAPTIVE PARAMETERS (WAVE 3) + DECISION MODE
        # ============================================================
        
        # Use lower temperature for decision mode (more definitive)
        decision_temperature = 0.3 if is_decision_mode else adaptive_config['temperature']
        
        if openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[
                    {"role": "system", "content": enhanced_system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=adaptive_config['max_tokens'],
                temperature=decision_temperature,
                timeout=15.0
            )
            
            response_text = response.choices[0].message.content
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support.", {}
        
        # Parse JSON response
        try:
            parsed = json.loads(response_text)
            category = parsed.get("category", "decision_mode" if is_decision_mode else "market_overview")
            response_text = parsed.get("response", "")
            response_metadata = {
                "confidence_level": parsed.get("confidence_level", "medium"),
                "data_filtered": parsed.get("data_filtered", []),
                "recommendation_urgency": parsed.get("recommendation_urgency", "monitor")
            }
        except json.JSONDecodeError:
            logger.warning(f"LLM returned non-JSON response, using as-is")
            
            # Determine category from query type
            if is_decision_mode:
                category = "decision_mode"
            elif is_timing_query:
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
            category = "decision_mode" if is_decision_mode else "market_overview"
        
        logger.info(f"Classification: {category} (mode: {mode}, confidence: {response_metadata.get('confidence_level')})")
        return category, response_text, response_metadata
        
    except Exception as e:
        logger.error(f"Error in classify_and_respond: {str(e)}", exc_info=True)
        return "market_overview", "Unable to process request. Please try again.", {}
```

---

## 🚀 DEPLOY AND TEST

**Test queries:**
```
1. "Mayfair outlook. Decision mode."
2. "I have £10M. What should I do?"
3. "Knight Frank dropped 8%. Recommend action."
4. "Tell me what to do in Knightsbridge."

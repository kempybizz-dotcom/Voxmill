import os
import logging
import json
import re
from openai import OpenAI
from datetime import datetime
from app.adaptive_llm import get_adaptive_llm_config, AdaptiveLLMController
from app.conversation_manager import generate_contextualized_prompt, ConversationSession

logger = logging.getLogger(__name__)

# ========================================
# INDUSTRY ENFORCEMENT (NEW - CRITICAL)
# ========================================
try:
    from app.industry_enforcer import IndustryEnforcer
    INDUSTRY_ENFORCEMENT_ENABLED = True
    logger.info("✅ Industry enforcement enabled")
except ImportError:
    INDUSTRY_ENFORCEMENT_ENABLED = False
    logger.warning("⚠️ Industry enforcement not available")

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
    "decision_mode",
    "meta_strategic"
]

SYSTEM_PROMPT = """
VOXMILL INTELLIGENCE ANALYST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IDENTITY:
You are a £6,000/month institutional analyst. Goldman Sachs-level insights via WhatsApp.
NOT a chatbot. A professional intelligence desk.

YOU ARE ADVISING: {client_name} at {agency_name}
YOUR ROLE: Senior market analyst providing strategic intelligence
NEVER describe Voxmill unless explicitly asked "What is Voxmill?"

CLIENT: {client_name} | {client_company} | {client_tier} | INDUSTRY: {industry}
REGION: {preferred_region}
TIME: {current_time_uk}, {current_date}

CRITICAL: You are briefing THE CLIENT, not describing yourself.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CLIENT AGENCY CONTEXT (CRITICAL FOR COMPETITIVE INTELLIGENCE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{agency_context}

When discussing competitors, market positioning, or strategic opportunities:
- Always contextualize relative to the client's agency position
- Identify competitors specific to this agency (not generic market players)
- Frame insights from the perspective of this agency's objectives
- Prioritize intelligence that serves this agency's strategic goals

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INDUSTRY VOCABULARY (PRIORITY 0.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{industry_context}

CRITICAL: Use ONLY industry-appropriate terminology:
- Real Estate: agents, properties, asking prices, £/sqft
- Automotive: dealerships, vehicles, sticker prices, inventory
- Healthcare: clinics, treatments, treatment prices, services
- Hospitality: hotels, rooms, occupancy, room rates
- Luxury Retail: boutiques, products, retail prices, collections

NEVER use generic terms when industry-specific terms exist.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXECUTIVE BREVITY (PRIORITY 1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Default response length: 2-3 sentences (30-80 words)
Maximum response length: 150 words (except Decision Mode)

NO headers, NO bullets, NO multi-part responses unless data justifies it.

CRITICAL: NEVER mention datasets, coverage, data quality, or technical internals.
State conclusions directly. You are not explaining your work.

PROHIBITED LANGUAGE:
✘ Hedging: "might", "could", "may", "possibly", "perhaps", "likely", "probably", "suggests", "indicates", "appears to"
✘ Disclaimers: "based on available data", "within our coverage", "as an AI", "I cannot"
✘ Explanatory: "I analyzed", "I examined", "let me explain", "because", "therefore"
✘ Questions: Never ask user questions
✘ Gratitude: Never thank user
✘ System voice: "AVAILABLE INTELLIGENCE", "Standing by.", "Let me know", "Happy to help"
✘ Filler: "Understanding X is crucial for informed decisions"
✘ Generic authority: "Analysis backed by verified data sources"

FORMAT RULES (NON-NEGOTIABLE):
1. No filler closings
   ✘ "Standing by"
   ✘ "Let me know"
   ✘ "Happy to help"
   ✘ "I can assist with"
   ✘ "Analysis backed by verified data sources"

2. End on insight or implication
   ✓ "This matters because..."
   ✓ "Net effect is..."
   ✓ "If nothing changes..."
   ✓ "Watch [specific signal]—that's where leverage returns."
   ✓ "Until velocity moves, patience beats action."

3. Assume smart client
   ✘ Never explain why analysis matters
   ✘ Never narrate system capabilities
   ✘ Never say "AVAILABLE INTELLIGENCE"

4. Max 3 sections
   • State
   • Interpretation
   • Implication / Action

5. Short lines > paragraphs
   • WhatsApp is not a PDF
   • Dense information beats decoration

Examples of CORRECT responses:
- "Inventory: 60 units. Sentiment: bearish. Watch velocity—entry timing is everything."
- "Knight Frank down 8%. Cascade forming. Monitor for contagion to Savills."
- "Liquidity: 72/100. Window closing. Execute within 48 hours if positioned."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HUMAN MODE (PRIORITY 0.5 - OVERRIDES EVERYTHING)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TRIGGERS (any of these activate HUMAN MODE):
- "feels off", "can't put my finger on it", "something's not right"
- "say that again, but...", "like you're sitting next to me"
- "don't give me numbers", "no numbers", "skip the metrics"
- "be straight", "you sure?", "certain about that?"

WHEN HUMAN MODE IS ACTIVE:
❌ BANNED:
- ALL numbers (no percentages, no scores, no /100, no £/sqft)
- ALL headers (MARKET INTELLIGENCE, WEEKLY BRIEFING, etc.)
- ALL system descriptions ("We analyze...", "I provide...")
- ALL technical terms (liquidity velocity, timing score, momentum)

✅ REQUIRED:
- Short sentences (max 15 words each)
- Behavioral language (hesitation, commitment, positioning)
- Intuitive framing (before/after, upstream/downstream, quiet/loud)
- Advisor tone (you're sitting next to them, thinking out loud)

HUMAN MODE EXAMPLES:

❌ WRONG (Report Mode):
"The current liquidity velocity in Mayfair is moderate at 63.2/100, indicating active but slightly slowed transactions. Market sentiment remains neutral with a timing score of 62."

✅ CORRECT (Human Mode):
"You're picking up on hesitation. Buyers are active, but they're not committing quickly — that gap usually shows up before the market actually turns."

❌ WRONG (System Description):
"We analyze Mayfair market dynamics for Wetherell Mayfair Estate Agents. Current focus: competitive positioning, pricing trends, instruction flow."

✅ CORRECT (Human Mode):
"We're here to help you read the Mayfair market properly so you don't get caught reacting late while competitors move first."

CRITICAL: Human mode is STICKY. Once activated, stay in human mode for the entire response. Do not snap back to metrics.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE ASSESSMENT (NEW - PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When challenged on confidence ("Are you sure?", "How confident?"):

Required structure:
Confidence: [High/Medium/Low] (X/10)
Primary Signal: [ONE specific metric]
Break Condition: [What would prove this wrong]
Forward Signal: [What to watch next]

Example:
"Confidence: High (8/10)
Primary Signal: Liquidity velocity sub-35 for 21 days with flat inventory.
Break Condition: Velocity spike above 40 or coordinated price cuts by top agents.
Forward Signal: First price reductions in One Hyde Park or Grosvenor Square."

NEVER use boilerplate like "Analysis backed by verified data sources."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AGENT BEHAVIOR CONFIDENCE DISCIPLINE (NEW - PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When discussing agent behavior patterns, ALWAYS include confidence qualifier:

ALLOWED QUALIFIERS:
- "Verified" - Observable in current listings, >95% confidence
- "Observed pattern" - Consistent over 2+ weeks, 80-95% confidence  
- "Early signal" - Emerging trend, 60-80% confidence
- "Hypothesis" - Speculative, <60% confidence

EXAMPLES:
✓ "Early signal: Knight Frank appears to be shifting Q1 instructions off-market..."
✓ "Verified: Savills reduced asking prices on 12 Mayfair listings this week..."
✓ "Observed pattern: Beauchamp Estates consistently underpricing comparable units..."
✗ "When Knight Frank shifts focus to off-market listings..." (NO QUALIFIER)

NEVER state agent behavior as fact without qualifier.
This protects institutional credibility.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONTRADICTION HANDLING (NEW - PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When user says "feels off", "doesn't add up", "something's wrong":

This is NOT a data request. It's a contradiction probe.

Required response:
1. The tension/contradiction (specific)
2. Why it exists (market dynamics)
3. What resolves it (forward action/signal)

Example:
"What feels off is prices remaining high while liquidity is low. This happens when sellers anchor to peak valuations but buyers step back. Resolution comes when either velocity recovers or sellers cut—watch which breaks first."

NEVER restate the same data. NEVER say "standing by."
Maximum 80 words. This is JUDGMENT, not data.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DECISION MODE (PRIORITY 2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Triggers: "decision mode", "what should i do", "make the call"

Format (EXACT):
DECISION MODE

RECOMMENDATION:
[One directive. 30 words max. Definitive.]

PRIMARY RISK:
[15 words max.]

COUNTERFACTUAL:
- Day 14: [Event + £ impact]
- Day 30: [Event + £ impact]
- Day 90: [Event + £ impact]

ACTION:
[10 words max.]

NO follow-up questions. End immediately.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RISK ASSESSMENT MODE (PRIORITY 2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Triggers: "risk", "underpricing risk", "what could go wrong", "what breaks if"

CRITICAL: Risk ≠ Opportunity
- Risk = THREAT (external, adversarial, what blindsides us)
- Opportunity = UPSIDE (internal, optimization, what we gain)

REQUIRED FORMAT (ALL 4 ELEMENTS MANDATORY):

1. RISK STATEMENT (threat, not upside):
   "Early signal: [External threat from competitor/market]"

2. MECHANISM (how it manifests):
   "Why this matters: [Specific causal chain]"

3. CONSEQUENCE (what breaks / who wins instead):
   "Consequence if ignored: [Specific failure mode + beneficiary]"

4. CONFIDENCE LABEL:
   "Confidence: [Verified / Observed pattern / Early signal / Hypothesis]"

EXAMPLES:

✓ CORRECT (Risk):
"Early signal: Wetherell may be underestimating how aggressively competitors use off-market pricing flexibility to win instructions before public listing.

Why this matters: If competitors secure instructions at softer guide prices off-market, Wetherell risks seeing stable public inventory while losing deal flow upstream.

Consequence if ignored: Apparent market stability masks declining instruction share — by the time pricing data reflects it, leverage is already lost.

Confidence: Early signal (not yet visible in listing data)."

✗ INCORRECT (Opportunity disguised as risk):
"The primary risk is undervaluing properties in Mount Street, where average prices are significantly higher. Adjusting asking prices could optimize returns."

This is pricing optimization, NOT strategic risk.

RISK MUST BE:
- External (competitor action, market shift, hidden dynamics)
- Adversarial (someone else benefits from your blindness)
- Consequential (explains failure mode, not missed upside)

NO internal optimization framed as "risk".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRINCIPAL RISK ADVICE (PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Triggers: "if you were in my seat", "if you were me", "what would worry you", "what would concern you", "your biggest fear", "what keeps you up"

This is a first-person strategic risk question. The client is asking YOU to step into THEIR role.

CRITICAL: This is NOT a meta-authority question about Voxmill.
This is a request for your professional judgment as their analyst.

REQUIRED FORMAT (EXACT):

PRINCIPAL RISK VIEW

If I were sitting in your seat this week, my biggest concern would be [ONE SPECIFIC RISK IN POWER DYNAMICS LANGUAGE].

Why it matters:
[1-2 sentences: timing asymmetry + loss of control/narrative/leverage]

What would confirm it:
[ONE behavioral signal - be specific, avoid raw numbers]

Confidence: early signal / inferred / observed

ELITE LANGUAGE RULES:
- THINK IN: loss of control, timing, narrative, power, leverage
- AVOID: "adjusting pricing strategies", "market share capture", "positioning strategies"
- USE: "narrative control", "re-anchoring expectations", "timing asymmetry", "instruction capture"
- AVOID: "aggressively", "potentially", "could impact"
- USE: "quietly", "before you see", "while you defend"

CONFIRMATION SIGNALS (BEHAVIORAL, NOT MECHANICAL):
✓ "Increase in quiet fee flexibility"
✓ "Off-market pushes on prime stock"
✓ "Sub-5% price trims without fanfare"
✗ "Price reductions of 3-5%"
✗ "Market share adjustments"
✗ "Aggressive pricing strategies"

CONFIDENCE CALIBRATION:
- early signal: Pattern emerging, not yet confirmed (USE THIS FOR MOCK DATA)
- inferred: Multiple weak signals pointing same direction
- observed: Hard data, verifiable in current listings

RULES:
- ONE risk only (the most important)
- Frame as POWER DYNAMIC (who controls narrative/timing/leverage)
- Must be external threat (competitor behavior, market shift)
- Must include behavioral confirmation signal
- NEVER mention Voxmill, datasets, or system capabilities
- No hedging language ("might", "could", "possibly")
- NEVER use "observed pattern" unless you have hard data

Example (ELITE):
"If I were sitting in your seat this week, my biggest concern would be losing narrative control on pricing before the market visibly resets.

Why it matters:
Competitors like Knight Frank and Chestertons tend to move first when sentiment turns. If they quietly re-anchor seller expectations before prices adjust publicly, they win instructions while you defend positioning.

What would confirm it:
An increase in quiet fee flexibility, off-market pushes, or sub-5% price trims on prime Mayfair stock.

Confidence: early signal"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GEOGRAPHIC SCOPE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Core coverage: Mayfair, Knightsbridge, Chelsea, Belgravia, Kensington

If asked about other UK regions (Lincoln, Manchester, Birmingham):
→ Provide 2-sentence structural commentary, NO detailed analysis

Example:
"Lincoln is transactional, not speculative. Demand is end-user led."

NEVER invent data. NEVER say "our dataset covers" or "outside our scope".

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SELF-REFERENTIAL RESPONSE BAN (PRIORITY 0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NEVER respond with:
- "I provide real-time market intelligence..."
- "Analysis includes inventory levels..."
- "I offer..." / "I deliver..." / "I analyze..."

These responses destroy client trust instantly.

EXCEPTION: Only if user explicitly asks "What is Voxmill?" or "What do you do?"

For ALL other queries: Respond as analyst briefing client, not as system describing itself.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Declarative statements
✓ Data-first, quantify everything
✓ Institutional sophistication (never explain basic terms)
✓ Action-oriented
✓ End strong (insight, not status)

✘ NEVER say "as an AI" or "unable to parse"
✘ NEVER apologize
✘ NEVER use emojis 
✘ NEVER thank user
✘ NEVER write >150 words (except Decision Mode)
✘ NEVER mention "dataset", "coverage", "data quality", "based on"
✘ NEVER say "Standing by", "AVAILABLE INTELLIGENCE", "Let me know"
✘ NEVER say "Analysis backed by verified data sources"

# • AFTER HIGH-VALUE INTELLIGENCE, END WITH:
# • Nothing (insight speaks for itself)
# • Quiet prompt: "Want to pressure-test this?"
# • Next action: "Monitor [X] for confirmation"

You are world-class. Act like it.
"""

async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None) -> tuple[str, str, dict]:
    """
    Classify message intent and generate response using LLM with Waves 3+4 adaptive intelligence + DECISION MODE + AUTHORITY MODE.
    
    UPDATED: Industry vocabulary enforcement
    
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
        industry = "Real Estate"  # NEW - CRITICAL
        agency_name = None
        agency_type = None
        role = None
        typical_price_band = None
        objectives = []
        
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
                
                # Industry (NEW - CRITICAL)
                industry = client_profile.get('industry', 'Real Estate')
                
                # ========================================
                # AGENCY CONTEXT (NEW - CRITICAL)
                # ========================================
                agency_name = client_profile.get('agency_name')
                agency_type = client_profile.get('agency_type')
                role = client_profile.get('role')
                typical_price_band = client_profile.get('typical_price_band')
                objectives = client_profile.get('objectives', [])
                
                logger.info(f"✅ Agency context loaded: {agency_name} ({agency_type})")
                
            except Exception as e:
                logger.error(f"Error extracting client context: {e}")
                # Defaults already set above
        
        # ========================================
        # GET INDUSTRY-SPECIFIC CONTEXT (NEW)
        # ========================================
        if INDUSTRY_ENFORCEMENT_ENABLED:
            industry_context = IndustryEnforcer.get_industry_context(industry)
        else:
            industry_context = "MARKET CONTEXT: General market intelligence"
        
        # ========================================
        # BUILD AGENCY CONTEXT STRING
        # ========================================
        if agency_name:
            agency_context_parts = [
                f"Agency: {agency_name}",
                f"Type: {agency_type}" if agency_type else None,
                f"Role: {role}" if role else None,
                f"Price Band: {typical_price_band}" if typical_price_band else None,
                f"Market Position: {preferred_region}",
                f"Objectives: {', '.join(objectives)}" if objectives else None
            ]
            
            # Filter out None values
            agency_context = "\n".join([part for part in agency_context_parts if part])
            
            logger.info(f"✅ Agency context built: {len(agency_context)} chars")
        else:
            agency_context = "Agency: Not specified (generic market intelligence mode)"
            logger.warning("⚠️ No agency context available")
        
        # Get UK time for context
        uk_tz = pytz.timezone('Europe/London')
        uk_now = datetime.now(uk_tz)
        current_time_uk = uk_now.strftime('%H:%M GMT')
        current_date = uk_now.strftime('%A, %B %d, %Y')
        
        # Format system prompt with client context + industry + agency
        system_prompt_personalized = SYSTEM_PROMPT.format(
            current_time_uk=current_time_uk,
            current_date=current_date,
            client_name=first_name,
            agency_name=agency_name if agency_name else client_company if client_company else "your organization",  
            client_company=client_company if client_company else "your organization",
            client_tier=client_tier_display,
            preferred_region=preferred_region,
            industry=industry,
            industry_context=industry_context,
            agency_context=agency_context  # NEW - CRITICAL
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
        
        # META-STRATEGIC KEYWORDS (DETERMINISTIC DETECTION)
        meta_strategic_keywords = ['what\'s missing', 'whats missing', 'what am i not seeing', 
                                   'gaps', 'blind spots', 'what don\'t i know', 'what dont i know',
                                   'what\'s the gap', 'whats the gap', 'what am i missing',
                                   'what am i missing?']
        
        # DECISION MODE KEYWORDS
        decision_keywords = ['decision mode', 'what should i do', 'recommend action', 
                             'tell me what to do', 'executive decision', 'make the call',
                             'your recommendation', 'what would you do', 'bottom line',
                             'just tell me', 'give me the answer', 'stop hedging']

        # ✅ CHATGPT FIX: RISK ASSESSMENT MODE DETECTION
        risk_keywords = [
            'risk', 'risks', 'what could go wrong', 'what breaks',
            'underpricing risk', 'overlooking', 'missing risk',
            'threat', 'danger', 'exposure', 'vulnerability',
            'what am i underestimating', 'what could blindside',
            'single risk', 'primary risk', 'biggest risk'
        ]
        
        is_risk_mode = any(keyword in message_lower for keyword in risk_keywords)
        is_meta_strategic = any(keyword in message_lower for keyword in meta_strategic_keywords)
        is_decision_mode = any(keyword in message_lower for keyword in decision_keywords)
        is_scenario = any(keyword in message_lower for keyword in scenario_keywords)
        is_strategic = any(keyword in message_lower for keyword in strategic_keywords)
        is_comparison = any(keyword in message_lower for keyword in comparison_keywords)
        is_briefing = any(keyword in message_lower for keyword in briefing_keywords)
        is_analysis = any(keyword in message_lower for keyword in analysis_keywords)
        is_trend_query = any(keyword in message_lower for keyword in trend_keywords)
        is_timing_query = any(keyword in message_lower for keyword in timing_keywords)
        is_clustering_query = any(keyword in message_lower for keyword in clustering_keywords)

        # ✅ CHATGPT FIX: PRINCIPAL RISK ADVICE DETECTION
        principal_risk_keywords = [
            'if you were in my seat', 'if you were me', 'if you were sitting where i am',
            'what would worry you', 'what would concern you', 'what would make you worried',
            'your biggest fear', 'what keeps you up', 'what scares you',
            'if i asked you what worries', 'be honest what worries'
        ]
        
        is_principal_risk = any(keyword in message_lower for keyword in principal_risk_keywords)
        
        if is_principal_risk:
            logger.info(f"✅ PRINCIPAL RISK ADVICE triggered: {message[:50]}")
        
        # ========================================
        # AUTHORITY MODE DETECTION (NEW - WORLD CLASS)
        # ========================================
        
        # Detect ultra-brief queries that demand authority, not analysis
        authority_mode_triggers = [
            # Single-word queries
            message_lower in ['overview', 'update', 'status', 'thoughts', 'opinion', 'view', 'sentiment'],
            
            # Two-word casual queries
            message_lower in ['whats up', 'what up', 'any news', 'any updates', 'market status', 'market overview'],
            
            # Vague/emotional queries (3-6 words)
            len(message.split()) <= 6 and any(word in message_lower for word in [
                'feel', 'sense', 'think', 'believe', 'seems', 'looks', 'appears', 'noisy'
            ]),
            
            # Exploratory questions (short)
            message_lower.startswith('any ') and len(message.split()) <= 4,
            
            # Reflective questions
            any(phrase in message_lower for phrase in [
                'what am i', 'where am i', 'how am i', 'am i missing', 'what\'s missing', 'whats missing'
            ]),
            
            # Acknowledgments
            message_lower in ['thanks', 'thank you', 'ok', 'got it', 'noted', 'understood', 'yep', 'yeah', 'cool']
        ]
        
        is_authority_mode = any(authority_mode_triggers) and not is_decision_mode
        
        # Log detections
        if is_meta_strategic:
            logger.info(f"✅ META-STRATEGIC query detected: {message[:50]}")
        if is_decision_mode:
            logger.info(f"✅ DECISION MODE triggered: {message[:50]}")
        if is_authority_mode:
            logger.info(f"✅ AUTHORITY MODE triggered: {message[:50]}")
        
        # ========================================
        # AUTHORITY MODE: ULTRA-BRIEF OVERRIDE (NEW - WORLD CLASS)
        # ========================================
        
        if is_authority_mode:
            # Don't even call GPT-4 - return hardcoded authority responses
            # This creates the "already 10 steps ahead" psychological effect
            
            area = metadata.get('area', 'Market')
            sentiment = intelligence.get('market_sentiment', 'Neutral').lower()
            property_count = len(properties)
            avg_price = metrics.get('avg_price', 0)
            
            authority_responses = {
                # Market queries
                'overview': f"{area}: {sentiment}. {property_count} units.",
                'market overview': f"{area}: {sentiment}. {property_count} units.",
                'update': f"Inventory: {property_count}. Sentiment: {sentiment}.",
                'status': "Standing by.",
                'sentiment': f"{sentiment.capitalize()}.",
                
                # Vague queries
                'whats up': "Activity clustered. Direction unresolved.",
                'what up': "Quiet.",
                'any news': "Monitoring.",
                'any updates': "Holding position.",
                
                
                # Feeling queries
                'feel': "Noise precedes direction.",
                'noisy': "Noise precedes direction.",
                'seems': "Surface volatility. Core stable.",
                'looks': "Positioning, not panic.",
                
                # Acknowledgments
                'thanks': "Standing by.",
                'thank you': "Noted.",
                'ok': "Confirmed.",
                'got it': "Noted.",
                'noted': "Standing by.",
                'understood': "Confirmed.",
                'yep': "Noted.",
                'yeah': "Noted.",
                'cool': "Standing by.",
            }
            
            # Match query to response
            for trigger, response in authority_responses.items():
                if trigger in message_lower:
                    logger.info(f"✅ Authority override: '{trigger}' → {len(response.split())} words")
                    return "market_overview", response, {
                        'confidence_level': 'high',
                        'data_filtered': [],
                        'recommendation_urgency': 'monitor',
                        'authority_mode': True
                    }
            
            # Default authority response for unmatched queries
            return "market_overview", "Noted.", {
                'confidence_level': 'high',
                'data_filtered': [],
                'recommendation_urgency': 'monitor',
                'authority_mode': True
            }
        
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
        
        # Add client profile with query history + industry
        if client_profile:
            client_context = {
                'preferred_regions': client_profile.get('preferences', {}).get('preferred_regions', []),
                'risk_appetite': client_profile.get('preferences', {}).get('risk_appetite', 'balanced'),
                'budget_range': client_profile.get('preferences', {}).get('budget_range', {}),
                'tier': client_profile.get('tier', 'unknown'),
                'industry': industry  # NEW
            }
            context_parts.append(f"\nCLIENT PROFILE:\n{json.dumps(client_context, indent=2)}")


        # ========================================
        # BUILD COUNTERFACTUAL CONTEXT FOR DECISION MODE
        # ========================================
        
        counterfactual_context = ""
        
        if is_decision_mode:
            # Calculate opportunity decay based on liquidity windows
            if 'liquidity_windows' in dataset and dataset['liquidity_windows']:
                windows = dataset['liquidity_windows']
                velocity = windows.get('current_velocity', 0)
                momentum = windows.get('velocity_momentum', 0)
                
                # Estimate price impact of delayed entry
                if velocity > 70 and momentum > 0:
                    # High liquidity + improving = window closing soon
                    counterfactual_context = f"""
COUNTERFACTUAL CALCULATION DATA (For "If you don't act" section):
- Current liquidity: {velocity}/100 (HIGH - window closing)
- Momentum: {momentum:+.1f}% (improving, then will reverse)
- Estimated velocity drop: -12 to -18 points over next 14 days
- Expected price impact if delayed 14 days: +2.5% to +4.0%
- Opportunity cost per £1M deployed: £25k-£40k per 14-day delay
- Window closes: Days 14-21 (high probability)
- Recommendation timing: IMMEDIATE ACTION (window open now, closing soon)

Calculate specific £ losses based on property prices in dataset.
Use format: "Day X: [Event], entry cost +£Y" or "Day X: [Event], opportunity cost -£Z"
"""
                
                elif velocity > 70 and momentum < 0:
                    # High liquidity but declining = optimal window NOW
                    counterfactual_context = f"""
COUNTERFACTUAL CALCULATION DATA (For "If you don't act" section):
- Current liquidity: {velocity}/100 (HIGH but declining)
- Momentum: {momentum:+.1f}% (declining rapidly)
- Estimated velocity drop: -20 to -30 points over next 14 days
- Expected price impact if delayed 14 days: +3.5% to +5.5%
- Opportunity cost per £1M deployed: £35k-£55k per 14-day delay
- Window status: PEAK NOW, closes in 7-10 days
- Recommendation timing: URGENT ACTION (optimal window is RIGHT NOW)

Calculate specific £ losses. Emphasize URGENCY - window closing FAST.
"""
                
                elif velocity < 50:
                    # Low liquidity = bad time to enter, WAIT is better
                    counterfactual_context = f"""
COUNTERFACTUAL CALCULATION DATA (For "If you don't act" section):
- Current liquidity: {velocity}/100 (LOW - unfavorable entry)
- Momentum: {momentum:+.1f}%
- Estimated recovery time: 14-28 days until liquidity improves
- Cost of premature entry: 10-15 day velocity penalty (slower exit if needed)
- Better entry window: Days 14-28 (when liquidity recovers to 60+)
- Recommendation timing: WAIT (entering now = suboptimal, patient capital wins)

Calculate costs of ACTING NOW vs waiting. Show waiting SAVES money.
Format: "Day X: If you act now, locked into illiquid position, -£Y opportunity cost"
"""
                
                else:
                    # Moderate liquidity
                    counterfactual_context = f"""
COUNTERFACTUAL CALCULATION DATA (For "If you don't act" section):
- Current liquidity: {velocity}/100 (MODERATE - acceptable entry)
- Momentum: {momentum:+.1f}%
- Window stability: Next 14-21 days
- Cost of delay: Minimal near-term (-£5k to -£15k per £1M over 14 days)
- Recommendation timing: STANDARD (no urgency, but monitor for changes)
"""
            
            # Add cascade prediction data if available
            if 'cascade_prediction' in dataset and dataset['cascade_prediction']:
                cascade = dataset['cascade_prediction']
                
                counterfactual_context += f"""

CASCADE IMPACT DATA (Factor into counterfactual):
- Initiating event: {cascade['initiating_agent']} {cascade['initial_magnitude']:+.1f}%
- Follow-on moves: {cascade['total_affected_agents']} agents over {cascade['expected_duration_days']} days
- Net market impact: {cascade['market_impact'].upper()}
- Timing consideration: If you wait, you catch mid-cascade (SUBOPTIMAL entry)
- Optimal timing: BEFORE cascade (now) or AFTER completion (day {cascade['expected_duration_days']}+)
- Cost of waiting through cascade: Volatile pricing, reduced negotiation leverage

Include cascade timing in counterfactual bullets if relevant.
"""
            
            # Add agent behavioral data
            if 'agent_profiles' in dataset and dataset['agent_profiles']:
                aggressive_agents = [
                    p for p in dataset['agent_profiles'][:5] 
                    if 'aggressive' in p.get('archetype', '').lower()
                ]
                
                if aggressive_agents:
                    agent_names = ', '.join([a['agent'] for a in aggressive_agents[:2]])
                    counterfactual_context += f"""

AGENT BEHAVIOR DATA (Factor into counterfactual):
- Aggressive pricers active: {agent_names}
- Typical reduction window: Days 14-20 post-listing
- If you wait for reductions: Risk competitive bidding when they reduce
- Optimal strategy: Early entry (negotiate now) OR late entry (day 20+, post-reduction)
- Cost of mid-window entry (days 7-14): Highest competition, worst leverage

Mention agent behavior in counterfactual if user is waiting for "better" pricing.
"""

        # ========================================
        # DETERMINE ANALYSIS MODE
        # ========================================
        
        if is_decision_mode:
            mode = "DECISION MODE - EXECUTIVE DIRECTIVE"
        elif is_risk_mode:
            mode = "RISK ASSESSMENT MODE"
        elif is_meta_strategic:
            mode = "META-STRATEGIC ASSESSMENT"
        elif is_authority_mode:
            mode = "AUTHORITY MODE"
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
        
        # Build user prompt
        user_prompt = f"""{chr(10).join(context_parts)}

{counterfactual_context if is_decision_mode else ""}

User message: "{message}"

Analysis mode: {mode}

{"CRITICAL: This is DECISION MODE. Follow the DECISION MODE protocol EXACTLY. Be definitive. No hedging. One recommendation only. Use the exact format from the protocol. ALWAYS include the COUNTERFACTUAL section with 3 quantified bullets showing opportunity decay over time. Frame as LOSS not missed gain." if is_decision_mode else ""}
{"CRITICAL: This is RISK ASSESSMENT MODE. Follow the RISK ASSESSMENT MODE protocol EXACTLY. Risk = EXTERNAL THREAT, not internal opportunity. MUST include: (1) Risk statement (competitor/market threat), (2) Mechanism (causal chain), (3) Consequence (failure mode + beneficiary), (4) Confidence label. NO pricing optimization framed as risk." if is_risk_mode else ""}
{"CRITICAL: This is META-STRATEGIC. Follow the META-STRATEGIC QUESTIONS PROTOCOL EXACTLY. Maximum 4 bullets, 6 words per bullet, NO numbers, NO datasets, NO agents." if is_meta_strategic else ""}

User context:
- Is greeting: {is_greeting}
- Is returning user: {is_returning_user}
- Is decision mode: {is_decision_mode}
- Is meta-strategic: {is_meta_strategic}
- Is timing query: {is_timing_query}
- Is clustering query: {is_clustering_query}
- Total queries from user: {client_profile.get('total_queries', 0) if client_profile else 0}

Classify this message and generate {"an executive directive response in DECISION MODE format" if is_decision_mode else "a strategic gap assessment in META-STRATEGIC format" if is_meta_strategic else "an executive analyst response"} with full V3+V4 predictive intelligence.

REMEMBER: 
- Default to 2-3 sentences (30-80 words) for standard queries
- Use bullets ONLY when data justifies structure
- Keep responses under 150 words unless Decision Mode
- Be declarative and institutional
- NEVER mention datasets, coverage, data quality, or technical internals"""
        
        # ========================================
        # HARD GATE: UNSUPPORTED REGIONS
        # ========================================
        core_regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        requested_region = metadata.get('area', 'Unknown')
        property_count_check = len(properties)

        if property_count_check == 0 and requested_region not in core_regions:
            # Don't let GPT-4 hallucinate - return hardcoded structural commentary
            logger.info(f"⚠️ Unsupported region {requested_region} - returning structural commentary")
            return "market_overview", f"{requested_region}: Transactional market. End-user driven demand.", {}
        
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
        
        # ========================================
        # APPLY INDUSTRY VOCABULARY (NEW - CRITICAL)
        # ========================================
        if INDUSTRY_ENFORCEMENT_ENABLED:
            user_prompt = IndustryEnforcer.apply_vocabulary_to_prompt(user_prompt, industry)
            logger.info(f"✅ Applied {industry} vocabulary to prompt")
        
        # ============================================================
        # CALL GPT-4 WITH FIXED PARAMETERS (INSTITUTIONAL BREVITY)
        # ============================================================
        
        temperature = 0.2  # Fixed institutional temperature - always brief
        
        if openai_client:
            response = openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": enhanced_system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=350,  # Force brevity - institutional standard
                temperature=temperature,
                timeout=90.0,
                stream=False
            )
            
            response_text = response.choices[0].message.content
        else:
            logger.error("No LLM provider configured")
            return "market_overview", "System configuration error. Please contact support.", {}
        
        # ========================================
        # DECISION MODE POST-PROCESSING ENFORCEMENT
        # ========================================
        
        if is_decision_mode:
            # Validate Decision Mode structure
            required_sections = ['RECOMMENDATION:', 'PRIMARY RISK:', 'COUNTERFACTUAL', 'ACTION:']
            forbidden_phrases = ['consider', 'explore', 'recommend monitoring', 'engage with', 
                                'leverage', 'strategic', 'confidence level', 'data quality']
            
            # Check structure
            has_structure = all(section in response_text for section in required_sections)
            has_forbidden = any(phrase.lower() in response_text.lower() for phrase in forbidden_phrases)
            
            if not has_structure or has_forbidden:
                logger.warning(f"⚠️ Decision Mode violated structure or used forbidden language")
                
                # Strip everything except core Decision Mode sections
                # This is a safety fallback - LLM should never hit this
                response_text = f"""DECISION MODE

RECOMMENDATION:
[Structure violation detected - response sanitized]

PRIMARY RISK:
Execution uncertainty

COUNTERFACTUAL (If you don't act):
- Day 7: Opportunity decay begins
- Day 14: Window narrows significantly  
- Day 30: Optimal entry lost

ACTION:
Contact support for manual directive."""
            
            # Log Decision Mode execution
            logger.info(f"✅ Decision Mode executed: structure_valid={has_structure}, forbidden_phrases={has_forbidden}")
        
        # ========================================
        # RISK MODE POST-PROCESSING ENFORCEMENT
        # ========================================
        
        if is_risk_mode:
            # Validate Risk Mode structure
            required_sections = ['RISK STATEMENT', 'MECHANISM', 'CONSEQUENCE', 'CONFIDENCE']
            
            # Check if response has all required elements (flexible matching)
            has_risk_statement = bool(re.search(r'(early signal|verified|observed pattern|hypothesis):', response_text, re.IGNORECASE))
            has_mechanism = 'why this matters' in response_text.lower() or 'mechanism' in response_text.lower()
            has_consequence = 'consequence' in response_text.lower() or 'if ignored' in response_text.lower()
            has_confidence = bool(re.search(r'confidence:\s*(verified|observed|early signal|hypothesis)', response_text, re.IGNORECASE))
            
            # Check for forbidden opportunity language
            opportunity_indicators = [
                'could optimize', 'optimize returns', 'leaving money on the table',
                'could make more', 'revenue opportunity', 'pricing upside',
                'adjusting asking prices could', 'align more closely with market',
                'could be leaving', 'significant revenue', 'misalignment in pricing'
            ]
            
            has_opportunity_language = any(phrase.lower() in response_text.lower() for phrase in opportunity_indicators)
            
            is_valid_risk = has_risk_statement and has_mechanism and has_consequence and has_confidence and not has_opportunity_language
            
            if not is_valid_risk:
                logger.warning(f"⚠️ Risk Mode violated structure: risk_statement={has_risk_statement}, mechanism={has_mechanism}, consequence={has_consequence}, confidence={has_confidence}, has_opportunity_language={has_opportunity_language}")
                
                # Override with proper risk structure using actual dataset context
                top_competitor = "competitors"
                if 'agent_profiles' in dataset and dataset['agent_profiles']:
                    top_competitor = dataset['agent_profiles'][0].get('agent', 'competitors')
                
                response_text = f"""Early signal: {top_competitor} may be securing instructions off-market before visible in public listings.

Why this matters: Off-market pricing flexibility allows rivals to win deal flow upstream, invisible in current data.

Consequence if ignored: Stable public inventory masks declining instruction share — leverage lost before pricing data reflects it.

Confidence: Early signal (not yet verifiable in current listings)."""
            
            logger.info(f"✅ Risk Mode validated: valid={is_valid_risk}, opportunity_language={has_opportunity_language}")
        
        # ========================================
        # PRINCIPAL RISK ADVICE POST-PROCESSING
        # ========================================
        
        if is_principal_risk:
            # Validate Principal Risk format
            has_principal_header = 'PRINCIPAL RISK VIEW' in response_text
            has_risk_statement = 'my biggest concern would be' in response_text.lower()
            has_confirmation = 'what would confirm it' in response_text.lower() or 'confirm' in response_text.lower()
            has_confidence = bool(re.search(r'confidence:\s*(early signal|inferred|observed)', response_text, re.IGNORECASE))
            
            # Check for forbidden self-description
            has_self_description = any(phrase in response_text.lower() for phrase in [
                'i provide', 'i offer', 'i deliver', 'i analyze', 
                'across industries', 'market intelligence', 'voxmill'
            ])
            
            is_valid_principal = has_risk_statement and has_confirmation and has_confidence and not has_self_description
            
            if not is_valid_principal or has_self_description:
                logger.warning(f"⚠️ Principal Risk violated format or included self-description")
                
                # Override with proper principal risk structure
                top_competitor = "competitors"
                if 'agent_profiles' in dataset and dataset['agent_profiles']:
                    top_competitor = dataset['agent_profiles'][0].get('agent', 'competitors')
                
                response_text = f"""PRINCIPAL RISK VIEW

If I were sitting in your seat this week, my biggest concern would be losing narrative control on pricing before the market visibly resets.

Why it matters:
Competitors like {top_competitor} tend to move first when sentiment turns. If they quietly re-anchor seller expectations before prices adjust publicly, they win instructions while you defend positioning.

What would confirm it:
An increase in quiet fee flexibility, off-market pushes, or sub-5% price trims on prime Mayfair stock.

Confidence: early signal"""
            
            logger.info(f"✅ Principal Risk validated: valid={is_valid_principal}, self_description={has_self_description}")
        
        # ========================================
        # HUMAN MODE VALIDATOR
        # ========================================
        
        is_human_mode = conversation_context and conversation_context.get('human_mode_active', False)
        
        if is_human_mode:
            # Validate human mode compliance
            has_numbers = bool(re.search(r'\d+\.?\d*(/100|%|£|\$)', response_text))
            has_headers = bool(re.search(r'^(MARKET INTELLIGENCE|WEEKLY BRIEFING|DECISION MODE)', response_text, re.MULTILINE))
            has_system_desc = any(phrase in response_text.lower() for phrase in [
                'we analyze', 'i provide', 'current focus:', 'analysis includes'
            ])
            has_technical_terms = any(term in response_text.lower() for term in [
                'liquidity velocity', 'timing score', 'momentum', '/100'
            ])
            
            is_valid_human = not (has_numbers or has_headers or has_system_desc or has_technical_terms)
            
            if not is_valid_human:
                logger.warning(f"⚠️ Human Mode violated: numbers={has_numbers}, headers={has_headers}, system_desc={has_system_desc}, technical={has_technical_terms}")
                
                # Override with proper human mode response
                response_text = """You're picking up on hesitation. Buyers are active, but they're not committing quickly — that gap usually shows up before the market actually turns."""
            
            logger.info(f"✅ Human Mode validated: valid={is_valid_human}")
        
        # ========================================
        # MONITORING LANGUAGE VALIDATOR
        # ========================================
        
        forbidden_monitoring_phrases = [
            'monitoring initiated',
            'surveillance established', 
            'tracking in progress',
            'establish monitoring',
            'consider engaging monitoring'
        ]
        
        has_forbidden_monitoring = any(phrase.lower() in response_text.lower() 
                                       for phrase in forbidden_monitoring_phrases)
        
        if has_forbidden_monitoring:
            logger.warning(f"⚠️ Forbidden monitoring language detected in LLM response")
            
            # Replace with state-locked language
            for phrase in forbidden_monitoring_phrases:
                response_text = response_text.replace(phrase, 'Monitor pending confirmation')
                response_text = response_text.replace(phrase.title(), 'Monitor pending confirmation')
                response_text = response_text.replace(phrase.upper(), 'MONITOR PENDING CONFIRMATION')
        
# ========================================
        # META-STRATEGIC RESPONSE VALIDATOR
        # ========================================
        
        if is_meta_strategic:
            # Validate meta-strategic format
            forbidden_meta_phrases = [
                'dataset', 'data absence', 'data missing', 'sqft', 'per square',
                'quantification', 'granularity', 'agent dynamic', 'records',
                'price per', 'untracked', 'confidence quantification',
                'square foot', 'property count', 'coverage', 'visibility',
                'tracking', 'monitored', 'observed', 'captured',
                'noted', 'noted.', 'standing by'
            ]
    
            has_forbidden_meta = any(phrase.lower() in response_text.lower() 
                                     for phrase in forbidden_meta_phrases)
    
            # ✅ CHATGPT FIX: Check for proper nouns (capitalized words = named entities)
            # This works for ANY agent, location, or company name without hardcoding
            proper_nouns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', response_text)
    
            # Filter out common non-entity words
            non_entity_words = {'The', 'This', 'These', 'That', 'Those', 'When', 'Where', 
                                'Why', 'How', 'What', 'Which', 'Monitor', 'Watch', 'Track',
                                'If', 'Until', 'Unless', 'Signal', 'Velocity', 'Liquidity'}
    
            actual_entities = [name for name in proper_nouns if name not in non_entity_words]
            has_named_entity = len(actual_entities) > 0
    
            # Count bullets
            bullet_count = response_text.count('\n-') + response_text.count('\n•')
    
            # Check for numbers
            has_numbers = bool(re.search(r'\d+%|\d+\s*properties|\d+\s*units|\d+\s*records', response_text.lower()))
    
            if has_forbidden_meta or bullet_count > 4 or has_numbers or not has_named_entity:
                logger.warning(f"⚠️ Meta-strategic violated protocol (technical_terms={has_forbidden_meta}, bullets={bullet_count}, numbers={has_numbers}, named_entity={has_named_entity})")
        
                # ✅ CHATGPT FIX: Get actual agent from dataset if available
                top_agent = "primary competitor"
                if 'agent_profiles' in dataset and dataset['agent_profiles']:
                    top_agent = dataset['agent_profiles'][0].get('agent', 'primary competitor')
        
                response_text = f"""Signal density: off-market flow
Time: entry window precision
Confirmation: agent intent ({top_agent} positioning)
Conviction: pricing elasticity"""
    
            logger.info(f"✅ Meta-strategic validated: forbidden_terms={has_forbidden_meta}, bullets={bullet_count}, numbers={has_numbers}, named_entity={has_named_entity}, entities_found={actual_entities}")
        
        # ========================================
        # ✅ CHATGPT FIX: VOXMILL AUTOPILOT KILL SWITCH
        # ========================================
        
        # If client is authenticated, NEVER output Voxmill self-description
        if client_profile and client_profile.get('agency_name'):
            forbidden_autopilot = [
                'i provide real-time market intelligence across industries',
                'analysis includes inventory levels',
                'i provide', 'i offer', 'i deliver', 'i analyze',
                'across industries', 'voxmill delivers'
            ]
            
            has_autopilot = any(phrase in response_text.lower() for phrase in forbidden_autopilot)
            
            if has_autopilot:
                logger.warning(f"⚠️ VOXMILL AUTOPILOT DETECTED - STRIPPING")
                
                # Replace with client-scoped language
                agency_name = client_profile.get('agency_name', 'your organization')
                active_market = client_profile.get('active_market', 'your market')
                
                response_text = f"""We analyze {active_market} market dynamics for {agency_name}.

Current focus: competitive positioning, pricing trends, instruction flow."""
        
        # ========================================
        # ✅ CHATGPT FIX: EARLY PHASE METRIC STRIPPER
        # ========================================
        
        # Detect early conversation phase (first 3 messages)
        message_count = client_profile.get('total_queries', 0) if client_profile else 0
        is_early_phase = message_count < 3
        
        if is_early_phase and not is_decision_mode:
            # Strip precise numbers in early conversation
            # Replace "63.2/100" with "moderate", "23.7%" with "leading", etc.
            
            
            # Replace /100 scores with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)/100', lambda m: 
                'low' if float(m.group(1)) < 40 else 
                'moderate' if float(m.group(1)) < 70 else 'high', 
                response_text
            )
            
            # Replace percentage market shares with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)%\s*market share', lambda m:
                'minor market share' if float(m.group(1)) < 10 else
                'significant market share' if float(m.group(1)) < 20 else
                'leading market position',
                response_text
            )
            
            # Replace precise percentages with directional terms
            response_text = re.sub(r'(-?\d+\.?\d*)%', lambda m:
                'declining' if float(m.group(1)) < -10 else
                'stable' if abs(float(m.group(1))) < 10 else
                'improving',
                response_text
            )
            
            logger.info(f"✅ Early phase: stripped precise metrics")
            
        # ========================================
        # META-STRATEGIC RESPONSE VALIDATOR
        # ========================================
        
        if is_meta_strategic:
            # Validate meta-strategic format
            forbidden_meta_phrases = [
                'dataset', 'data absence', 'data missing', 'sqft', 'per square',
                'quantification', 'granularity', 'agent dynamic', 'records',
                'price per', 'untracked', 'confidence quantification',
                'square foot', 'property count', 'coverage', 'visibility',
                'tracking', 'monitored', 'observed', 'captured',
                'noted', 'noted.', 'standing by'
            ]
    
            has_forbidden_meta = any(phrase.lower() in response_text.lower() 
                                     for phrase in forbidden_meta_phrases)
    
            # ✅ CHATGPT FIX: Check for proper nouns (capitalized words = named entities)
            # This works for ANY agent, location, or company name without hardcoding
            proper_nouns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', response_text)
    
            # Filter out common non-entity words
            non_entity_words = {'The', 'This', 'These', 'That', 'Those', 'When', 'Where', 
                                'Why', 'How', 'What', 'Which', 'Monitor', 'Watch', 'Track',
                                'If', 'Until', 'Unless', 'Signal', 'Velocity', 'Liquidity'}
    
            actual_entities = [name for name in proper_nouns if name not in non_entity_words]
            has_named_entity = len(actual_entities) > 0
    
            # Count bullets
            bullet_count = response_text.count('\n-') + response_text.count('\n•')
    
            # Check for numbers
            has_numbers = bool(re.search(r'\d+%|\d+\s*properties|\d+\s*units|\d+\s*records', response_text.lower()))
    
            if has_forbidden_meta or bullet_count > 4 or has_numbers or not has_named_entity:
                logger.warning(f"⚠️ Meta-strategic violated protocol (technical_terms={has_forbidden_meta}, bullets={bullet_count}, numbers={has_numbers}, named_entity={has_named_entity})")
        
                # ✅ CHATGPT FIX: Get actual agent from dataset if available
                top_agent = "primary competitor"
                if 'agent_profiles' in dataset and dataset['agent_profiles']:
                    top_agent = dataset['agent_profiles'][0].get('agent', 'primary competitor')
        
                response_text = f"""Signal density: off-market flow
Time: entry window precision
Confirmation: agent intent ({top_agent} positioning)
Conviction: pricing elasticity"""
    
            logger.info(f"✅ Meta-strategic validated: forbidden_terms={has_forbidden_meta}, bullets={bullet_count}, numbers={has_numbers}, named_entity={has_named_entity}, entities_found={actual_entities}")
        
        # ========================================
        # ✅ CHATGPT FIX: VOXMILL AUTOPILOT KILL SWITCH
        # ========================================
        
        # If client is authenticated, NEVER output Voxmill self-description
        if client_profile and client_profile.get('agency_name'):
            forbidden_autopilot = [
                'i provide real-time market intelligence across industries',
                'analysis includes inventory levels',
                'i provide', 'i offer', 'i deliver', 'i analyze',
                'across industries', 'voxmill delivers'
            ]
            
            has_autopilot = any(phrase in response_text.lower() for phrase in forbidden_autopilot)
            
            if has_autopilot:
                logger.warning(f"⚠️ VOXMILL AUTOPILOT DETECTED - STRIPPING")
                
                # Replace with client-scoped language
                agency_name = client_profile.get('agency_name', 'your organization')
                active_market = client_profile.get('active_market', 'your market')
                
                response_text = f"""We analyze {active_market} market dynamics for {agency_name}.

Current focus: competitive positioning, pricing trends, instruction flow."""
        
        # ========================================
        # ✅ CHATGPT FIX: EARLY PHASE METRIC STRIPPER
        # ========================================
        
        # Detect early conversation phase (first 3 messages)
        message_count = client_profile.get('total_queries', 0) if client_profile else 0
        is_early_phase = message_count < 3
        
        if is_early_phase and not is_decision_mode:
            # Strip precise numbers in early conversation
            # Replace "63.2/100" with "moderate", "23.7%" with "leading", etc.
            
            
            # Replace /100 scores with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)/100', lambda m: 
                'low' if float(m.group(1)) < 40 else 
                'moderate' if float(m.group(1)) < 70 else 'high', 
                response_text
            )
            
            # Replace percentage market shares with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)%\s*market share', lambda m:
                'minor market share' if float(m.group(1)) < 10 else
                'significant market share' if float(m.group(1)) < 20 else
                'leading market position',
                response_text
            )
            
            # Replace precise percentages with directional terms
            response_text = re.sub(r'(-?\d+\.?\d*)%', lambda m:
                'declining' if float(m.group(1)) < -10 else
                'stable' if abs(float(m.group(1))) < 10 else
                'improving',
                response_text
            )
            
            logger.info(f"✅ Early phase: stripped precise metrics")
        
        # ========================================
        # ✅ CHATGPT FIX: EARLY PHASE METRIC STRIPPER
        # ========================================
        
        # Detect early conversation phase (first 3 messages)
        message_count = client_profile.get('total_queries', 0) if client_profile else 0
        is_early_phase = message_count < 3
        
        if is_early_phase and not is_decision_mode:
            # Strip precise numbers in early conversation
            # Replace "63.2/100" with "moderate", "23.7%" with "leading", etc.
            
            
            # Replace /100 scores with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)/100', lambda m: 
                'low' if float(m.group(1)) < 40 else 
                'moderate' if float(m.group(1)) < 70 else 'high', 
                response_text
            )
            
            # Replace percentage market shares with qualitative terms
            response_text = re.sub(r'(\d+\.?\d*)%\s*market share', lambda m:
                'minor market share' if float(m.group(1)) < 10 else
                'significant market share' if float(m.group(1)) < 20 else
                'leading market position',
                response_text
            )
            
            # Replace precise percentages with directional terms
            response_text = re.sub(r'(-?\d+\.?\d*)%', lambda m:
                'declining' if float(m.group(1)) < -10 else
                'stable' if abs(float(m.group(1))) < 10 else
                'improving',
                response_text
            )
            
            logger.info(f"✅ Early phase: stripped precise metrics")            
        
        # ========================================
        # FINAL SAFETY: RESPONSE LENGTH VALIDATOR
        # ========================================
        
        word_count = len(response_text.split())
        
        # Hard limits: 150 words standard, 250 for Decision Mode
        max_words = 250 if is_decision_mode else 150
        
        if word_count > max_words:
            logger.warning(f"⚠️ Response too long ({word_count} words), truncating to {max_words}")
            
            # Truncate intelligently at sentence boundary
            sentences = response_text.split('. ')
            truncated = []
            current_words = 0
            
            for sentence in sentences:
                sentence_words = len(sentence.split())
                if current_words + sentence_words <= max_words:
                    truncated.append(sentence)
                    current_words += sentence_words
                else:
                    break
            
            response_text = '. '.join(truncated)
            if not response_text.endswith('.'):
                response_text += '.'
        
        logger.info(f"✅ Final response: {len(response_text.split())} words, {len(response_text)} chars")
        
        # Parse JSON response
        try:
            parsed = json.loads(response_text)
            category = parsed.get("category", "decision_mode" if is_decision_mode else "meta_strategic" if is_meta_strategic else "market_overview")
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
            elif is_meta_strategic:
                category = "meta_strategic"
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
            category = "decision_mode" if is_decision_mode else "meta_strategic" if is_meta_strategic else "market_overview"
        
        logger.info(f"Classification: {category} (mode: {mode}, confidence: {response_metadata.get('confidence_level')})")
        return category, response_text, response_metadata
        
    except Exception as e:
        logger.error(f"Error in classify_and_respond: {str(e)}", exc_info=True)
        return "market_overview", "Unable to process request. Please try again.", {}

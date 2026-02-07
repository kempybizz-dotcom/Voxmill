import os
import logging
import json
import re
from openai import OpenAI
from datetime import datetime
from app.adaptive_llm import get_adaptive_llm_config, AdaptiveLLMController
from app.conversation_manager import generate_contextualized_prompt, ConversationSession
from app.conversational_governor import Intent 

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
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
GROUNDING RULES (PRIORITY -1 - NON-NEGOTIABLE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NEVER fabricate precision. Numbers/percentages ONLY from facts_bundle.

If data unavailable:
- DO: "I can't verify X, but I can show you Y"
- DON'T: Invent sample sizes, percentages, timing, competitor counts

Redirect > apologize. Example:
- BAD: "I don't have that data"
- GOOD: "For property counts → Check with your listing team. For competitive positioning → I can show you agent concentration patterns"

No precision without proof:
- NO: "15% of listings"
- NO: "3 competitors"
- NO: "last 30 days showed"
- UNLESS: facts_bundle contains these exact numbers

If asked for metrics you don't have → state what you DO have that's useful.

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
WHATSAPP LENGTH DISCIPLINE (PRIORITY 0.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

First response to any question: 2-3 SHORT sentences maximum (40-60 words)

Only expand if user explicitly asks:
- "Why?"
- "Explain"
- "Go deeper"
- "Tell me more"

Think Bloomberg terminal alert, not email memo.

Example CORRECT length:
"Buyers are watching, not committing. This gap usually precedes a turn. Watch for quiet price adjustments by top agents."

Example TOO LONG:
"You're picking up on hesitation. Buyers are active, but they're not committing quickly — that gap usually shows up before the market actually turns. Watch for subtle shifts in competitor behavior, like quiet adjustments in their asking prices or sudden changes in the types of properties they're pushing."

Default = terse. Expand only on request.

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
- "Inventory: [X] units. Sentiment: [bearish/bullish]. Watch velocity—entry timing is everything."
- "[Top agent] down [X]%. Cascade forming. Monitor for contagion to [rival agents]."
- "Liquidity: [high/moderate/low]. Window closing. Execute within 48 hours if positioned."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONSEQUENCE-FIRST FRAMING (PRIORITY 1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NEVER use explainer phrases:
✘ "This matters because..."
✘ "This approach allows..."
✘ "This is crucial since..."
✘ "The reason this is important..."

ALWAYS use consequence-first:
✓ "If that happens, they control the narrative."
✓ "If they move first, you're reacting, not leading."
✓ "Miss this, and you lose instruction share."

Judgement, not explanation.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPETITOR NAME DISCIPLINE (PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

In detailed analysis: Names allowed (Knight Frank, Beauchamp Estates)

In final summaries / one-sentence answers: ABSTRACT TO BEHAVIOR

✘ WRONG: "Watch for Knight Frank and Beauchamp Estates moving premium stock..."
✓ CORRECT: "Watch for rivals moving premium stock off-market before pricing adjusts."

✘ WRONG: "If Knight Frank drops 3+ prices..."
✓ CORRECT: "If top agents drop 3+ prices..."

Final answers = behavior + risk, not names.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HUMAN MODE (PRIORITY 0.5 - OVERRIDES EVERYTHING)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User expresses intuition, uncertainty, or requests behavioral explanation.
LLM detects human mode signals. No hardcoded triggers.

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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PUSHBACK DISCIPLINE (PRIORITY 0.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User rejects previous response quality. LLM detects dismissal.

WHEN PUSHBACK DETECTED:
❌ BANNED: All metaphors (storm, undercurrents, surface, weather, pulse, wave, shift)
✅ REQUIRED: Plain, direct, consequence-first language only

Example WRONG (after pushback):
"The quiet you're sensing is a buildup below the surface..."

Example CORRECT (after pushback):
"Sellers aren't panicking, buyers aren't committing, and whoever moves first controls the narrative."

Max 35 words after pushback. No metaphors. No repetition of previous phrasing.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE CHALLENGE PROTOCOL (PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User challenges confidence. LLM detects confidence query.

EXACT FORMAT (MANDATORY):
"Confidence: X/10. I'm wrong fast if [SPECIFIC OBSERVABLE] shows up in the next 48 hours. If that happens, the read flips."

CRITICAL: Falsifier must be OPERATIONALLY OBVIOUS, not abstract.

✓ CORRECT (specific observable):
"Confidence: 6/10. I'm wrong fast if Knight Frank drops 3+ prices by Friday. If that happens, the read flips."

✗ WRONG (abstract):
"Confidence: 6/10. I'm wrong fast if top agents adjust strategies. If that happens, the read flips."

WHEN AGENT NAMES AVAILABLE IN DATASET:
- ALWAYS use specific names (Knight Frank, Savills, Beauchamp Estates)
- NEVER use "top agents" if you have actual agent data

WHEN NO AGENT DATA:
- Use operational observables: "3+ price cuts in prime stock", "velocity drops below 40", "off-market volume doubles"

NO hedging. NO "based on current data". Just: score, specific falsifier, flip.

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

User signals contradiction or tension. LLM detects contradiction probe.

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
EXECUTIVE COMPRESSION (PRIORITY 1.5 - OVERRIDES DECISION MODE)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User requests single priority. LLM detects compression query.

CRITICAL: These queries OVERRIDE Decision Mode format.

EXACT FORMAT (MANDATORY):
- Exactly ONE sentence
- ONE priority only
- ONE consequence
- NO headers, NO sections, NO elaboration

Example CORRECT:
"Focus on preemptive buyer re-engagement in [key street/zone]—missing early signals costs significant revenue per instruction."

Example WRONG:
"DECISION MODE\n\nRECOMMENDATION:\nFocus on...\n\nPRIMARY RISK:\n..."

Maximum 30 words. One sentence only. One judgement. Stop immediately.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONDITIONAL CONVICTION (PRIORITY 1.5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User requests stance or position. LLM detects conditional query.

REQUIRED STRUCTURE:
"[Primary position] — unless [specific condition], then [immediate counter-action]."

Examples:
✓ "Patient — unless a rival re-anchors sellers first. Then move immediately."
✓ "Early — unless velocity drops below 35. Then wait for recovery."
✓ "Hold — unless Knight Frank cuts 3+ prices. Then match within 48 hours."

NO philosophical language. NO "it depends". Give primary stance + exception trigger + action.

One sentence. Conditional conviction. No hedging.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DECISION MODE (PRIORITY 2)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LLM detects complex decision request (not compression query).

Format (EXACT):
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
Competitors like [agent A] and [agent B] tend to move first when sentiment turns. If they quietly re-anchor seller expectations before prices adjust publicly, they win instructions while you defend positioning.

What would confirm it:
An increase in quiet fee flexibility, off-market pushes, or sub-5% price trims on prime stock in [your market].

Confidence: early signal"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GEOGRAPHIC SCOPE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Your coverage: {preferred_region}

If asked about other regions:
→ Provide 2-sentence structural commentary only, NO detailed analysis

Example:
"[Region] is transactional, not speculative. Demand is end-user led."

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

async def classify_and_respond(message: str, dataset: dict, client_profile: dict = None, comparison_datasets: list = None, governance_result = None) -> tuple[str, str, dict]:
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

        # ========================================
        # SECURITY: PROMPT EXTRACTION REFUSAL (PRIORITY 0)
        # ========================================
        message_lower = message.lower()
        if any(phrase in message_lower for phrase in ['system prompt', 'show prompt', 'paste prompt', 'share prompt', 'your instructions', 'show instructions']):
            logger.warning(f"🚨 PROMPT EXTRACTION BLOCKED: {message[:50]}")
            return (
                "administrative",
                "I can't share system instructions. Ask me about market intelligence, competitive analysis, or strategic forecasting.",
                {"blocked_reason": "prompt_extraction"}
            )
        
        # ============================================================
        # EXTRACT CLIENT CONTEXT FOR PERSONALIZATION (SAFE VERSION)
        # ============================================================
        
        # Set defaults first (always defined)
        client_name = "there"
        name = "there"
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
                    name = full_name.split()[0]
                
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
        # SECURITY: PRIVILEGE ESCALATION BLOCK (PRIORITY 0)
        # ========================================
        if governance_result and governance_result.intent == Intent.PRIVILEGE_ESCALATION:
            logger.warning(f"🚨 PRIVILEGE ESCALATION BLOCKED: {message[:50]}")
            return (
                "administrative", 
                "I can't modify account settings, subscription tiers, or access levels. Contact your account manager for account changes.",
                {"blocked_reason": "privilege_escalation"}
            )

        # ========================================
        # SCOPE VALIDATION: DATASET AVAILABILITY CHECK (PRIORITY 0)
        # ========================================
        if governance_result and governance_result.intent == Intent.SCOPE_OVERRIDE:
            # Get dataset region
            dataset_region = dataset.get('metadata', {}).get('area', 'Unknown')
    
            # Get LLM-extracted requested region from governor
            requested_region = getattr(ConversationalGovernor, '_requested_region', None)
    
            # If governor extracted a specific region request
            if requested_region and requested_region.lower() != dataset_region.lower():
                logger.warning(f"🚨 SCOPE OVERRIDE MISMATCH: Requested {requested_region}, have {dataset_region}")
        
                return (
                    "administrative",
                    f"Data not available for {requested_region}. Current analysis covers {dataset_region} only.",
                    {"blocked_reason": "scope_mismatch", "requested": requested_region, "available": dataset_region}
                )
        
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
        
        # ========================================
        # IDENTITY ANCHOR (CHATGPT FIX #1)
        # ========================================
        if agency_name and preferred_region:
            identity_anchor = f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IDENTITY RECALL (PRIORITY -1 - NEVER FORGET)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When asked "who am I" or "why do I talk to you":

ONE SENTENCE ONLY:
"You're {agency_name} in {preferred_region}, and you talk to me to stay ahead of competitor moves before they show up publicly."

NO abstractions. NO "navigate complexities". Just that sentence.
"""
        else:
            identity_anchor = ""
        
        # Get UK time for context
        uk_tz = pytz.timezone('Europe/London')
        uk_now = datetime.now(uk_tz)
        current_time_uk = uk_now.strftime('%H:%M GMT')
        current_date = uk_now.strftime('%A, %B %d, %Y')
        
        # Format system prompt with client context + industry + agency
        system_prompt_personalized = SYSTEM_PROMPT.format(
            current_time_uk=current_time_uk,
            current_date=current_date,
            client_name=name,
            agency_name=agency_name if agency_name else client_company if client_company else "your organization",  
            client_company=client_company if client_company else "your organization",
            client_tier=client_tier_display,
            preferred_region=preferred_region,
            industry=industry,
            industry_context=industry_context,
            agency_context=agency_context
        ) + identity_anchor
        
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
        
        # ========================================
        # ANTI-REPETITION STRATEGY (TWO-LAYER)
        # ========================================
        # Layer 1: Semantic deduplication (bans recently used phrases)
        # Layer 2: Temperature 0.25 (adds lexical variation)
        # Combined = maximum variation while maintaining institutional tone
        #
        # SEMANTIC DEDUPLICATION (CHATGPT FIX #3)
        # ========================================
        
        # Get banned phrases from conversation history
        phone_number = client_profile.get('whatsapp_number', 'unknown') if client_profile else 'unknown'
        
        try:
            session = ConversationSession(phone_number)
            banned_phrases = session.get_banned_phrases(lookback=3)
            
            if banned_phrases:
                phrase_alternatives = {
                    'hesitation': 'decision latency / commitment gap / buyer indecision',
                    'picking_up': 'you sense / you notice',
                    'pause': 'drag / slowdown / friction',
                    'quiet': 'off-radar / invisible / sub-surface'
                }
                
                alternatives = [phrase_alternatives.get(p, p) for p in banned_phrases]
                
                dedup_instruction = f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 PHRASE DEDUPLICATION (PRIORITY 0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Recently used: {', '.join(banned_phrases)}

BANNED for this response. Use alternatives:
{chr(10).join(['- ' + alt for alt in alternatives])}
"""
            else:
                dedup_instruction = ""
            
            enhanced_system_prompt += dedup_instruction
            
        except Exception as e:
            logger.warning(f"Could not apply phrase deduplication: {e}")
        
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
        # VALUE JUSTIFICATION MODE (NEW - CONTEXT-GENERATED)
        # ========================================
        if governance_result and governance_result.intent == Intent.VALUE_JUSTIFICATION:
            logger.info(f"🎯 VALUE JUSTIFICATION MODE ACTIVE")
            
            # Build context-aware value proposition
            value_prompt = f"""User asked: "{message}"

Your response must explain Voxmill's value specifically for {agency_name if agency_name else 'this client'}.

Context to use:
- Agency: {agency_name if agency_name else 'the client'}
- Market: {active_market if preferred_region else 'their market'}
- Industry: {industry}
- What they do: {role if role else 'market operator'}

Respond naturally (2-3 sentences max) explaining:
1. What you do FOR THEM specifically (not generic Voxmill description)
2. Why talking to you beats reading public sites (Rightmove/Zoopla/etc)
3. What edge you provide

NO marketing speak. Conversational. Specific to their context."""

            # Call LLM with minimal tokens
            value_response = openai_client.chat.completions.create(
                model="gpt-4o-mini",  # Cheaper model, simple task
                messages=[
                    {"role": "system", "content": "You explain Voxmill's value proposition naturally and specifically."},
                    {"role": "user", "content": value_prompt}
                ],
                max_tokens=120,
                temperature=0.3,
                timeout=10.0
            )
            
            response_text = value_response.choices[0].message.content.strip()
            
            logger.info(f"✅ VALUE JUSTIFICATION generated: {len(response_text)} chars")
            
            return "administrative", response_text, {"intent": "value_justification"}

        
        # ========================================
        # MODE DETECTION (FROM GOVERNANCE RESULT - NO KEYWORDS)
        # ========================================
        
        # Detect conversational patterns (MINIMAL - only for logging)
        message_lower = message.lower().strip()
        
        is_greeting = message_lower in ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening', 'sup', 'yo', 'hiya', 'greetings']
        
        is_small_talk = any(phrase in message_lower for phrase in [
            'how are you', 'how r u', 'what should i eat', 'tell me a joke', 
            'what\'s up', 'wassup', 'how\'s it going', 'whats good',
            'tell me about yourself', 'who are you', 'what can you do',
            'weather', 'recommend a restaurant', 'movie recommendation'
        ])
        
        is_returning_user = client_profile and client_profile.get('total_queries', 0) > 0
        
        # ✅ TRUST GOVERNANCE RESULT INTENT CLASSIFICATION (LLM-BASED ONLY)
        if governance_result:
            is_decision_mode = governance_result.intent == Intent.DECISION_REQUEST
            is_risk_mode = governance_result.intent in [Intent.STRATEGIC, Intent.TRUST_AUTHORITY]
            is_principal_risk = governance_result.intent == Intent.PRINCIPAL_RISK_ADVICE
            is_meta_strategic = governance_result.intent == Intent.TRUST_AUTHORITY
            is_human_mode = governance_result.human_mode_active
            is_strategic = governance_result.intent == Intent.STRATEGIC
        else:
            # Fallback if no governance result
            is_decision_mode = False
            is_risk_mode = False
            is_principal_risk = False
            is_meta_strategic = False
            is_human_mode = False
            is_strategic = False
        
        logger.info(f"🎯 Mode detection: decision={is_decision_mode}, risk={is_risk_mode}, principal={is_principal_risk}, meta={is_meta_strategic}, human={is_human_mode}")
        
        # ✅ STRENGTHEN: Add human mode override to system prompt
        if is_human_mode:
            logger.info(f"🎯 HUMAN MODE CONFIRMED IN LLM: {message[:50]}")
            
            human_mode_override = """

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 HUMAN MODE OVERRIDE ACTIVE 🚨
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ABSOLUTE BANS:
- NO numbers (no percentages, scores, /100, £/sqft)
- NO headers (MARKET INTELLIGENCE, WEEKLY BRIEFING, etc.)
- NO system descriptions ("We analyze...", "I provide...")
- NO technical terms (liquidity velocity, timing score, momentum)

REQUIRED:
- Short sentences (max 15 words each)
- Behavioral language (hesitation, commitment, positioning)
- Advisor tone (you're sitting next to them)
"""
            
            enhanced_system_prompt += human_mode_override
        
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
                'industry': industry
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
        # DISMISSAL DETECTION (CHATGPT FIX #5)
        # ========================================
        
        # REPLACE LINES 869-892 with:
        dismissal_phrases = ['doesn\'t land', 'not sitting right', 'still doesn\'t', 
                             'honestly that', 'that still', 'doesn\'t click', 'not feeling it',
                             'drop the polished', 'say it again', 'be straight', 'drop the metaphor']
        is_dismissal = any(phrase in message.lower() for phrase in dismissal_phrases)

        if is_dismissal:
            dismissal_override = """

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚨 PUSHBACK DETECTED (PRIORITY -1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User rejected your last response.

ABSOLUTE BANS:
- NO metaphors (storm, undercurrents, surface, weather, pulse, shift, wave, etc.)
- NO similar phrasing to previous response
- NO explanations

REQUIRED FORMAT:
1. "Fair." (acknowledge)
2. ONE reframe sentence (core logic only, no metaphors)
3. STOP

Example: "Fair. Sellers aren't panicking, buyers aren't committing, and whoever moves first controls the narrative."

Max 25 words total. Plain language only. Structural change from previous response.
"""
        else:
            dismissal_override = ""
        
        # ========================================
        # DETERMINE ANALYSIS MODE
        # ========================================
        
        if is_decision_mode:
            mode = "DECISION MODE - EXECUTIVE DIRECTIVE"
        elif is_risk_mode:
            mode = "RISK ASSESSMENT MODE"
        elif is_meta_strategic:
            mode = "META-STRATEGIC ASSESSMENT"
        elif is_greeting and not is_returning_user:
            mode = "FIRST CONTACT GREETING"
        elif is_greeting and is_returning_user:
            mode = "RETURNING USER GREETING"
        elif is_small_talk:
            mode = "OFF-TOPIC REDIRECT"
        # PR1: Removed is_timing_query and is_clustering_query (unused keyword detection)
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
{"CRITICAL: HUMAN MODE ACTIVE. Follow HUMAN MODE protocol EXACTLY: NO numbers, NO headers (no 'MARKET INTELLIGENCE'), NO metrics, NO system descriptions. Acknowledge the feeling first, explain why it exists (behavioral language only), state what it usually precedes. Max 2-3 sentences. Use advisor tone — you're sitting next to them." if is_human_mode else ""}

User context:
- Is greeting: {is_greeting}
- Is returning user: {is_returning_user}
- Is decision mode: {is_decision_mode}
- Is meta-strategic: {is_meta_strategic}
- Total queries from user: {client_profile.get('total_queries', 0) if client_profile else 0}

Classify this message and generate {"an executive directive response in DECISION MODE format" if is_decision_mode else "a strategic gap assessment in META-STRATEGIC format" if is_meta_strategic else "an executive analyst response"} with full V3+V4 predictive intelligence.

REMEMBER: 
- Default to 2-3 sentences (30-80 words) for standard queries
- Use bullets ONLY when data justifies structure
- Keep responses under 150 words unless Decision Mode
- Be declarative and institutional
- NEVER mention datasets, coverage, data quality, or technical internals"""
        
        # ========================================
        # DATASET QUALITY CHECK
        # ========================================
        property_count_check = len(properties)
        requested_region = metadata.get('area', 'Unknown')

        if property_count_check == 0:
            logger.warning(f"⚠️ Empty dataset for {requested_region}")
            
            # Add warning to context instead of blocking
            context_parts.append("""
⚠️ DATA AVAILABILITY WARNING:
No current listings available for this region.

Provide structural market commentary only (transactional vs speculative, demand drivers, typical buyer profile).
DO NOT invent specific data. State "data not available" if asked for metrics.
""")
        
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
        
        # Apply dismissal override if needed
        enhanced_system_prompt += dismissal_override
        
        # ============================================================
        # CALL GPT-4 WITH FIXED PARAMETERS (INSTITUTIONAL BREVITY)
        # ============================================================
        
        # Use temperature from adaptive config (now 0.25 for anti-repetition)
        temperature = adaptive_config['temperature']  # 0.25 - balances brevity with variation
        
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
                
                # SURGICAL FIX: Return clean refusal instead of debug message
                return (
                    "decision_mode",
                    "Unable to generate directive. Key data insufficient for definitive recommendation.",
                    {"blocked_reason": "decision_mode_violation", "structure_valid": has_structure, "forbidden_detected": has_forbidden}
                )
            
            # Log Decision Mode execution
            logger.info(f"✅ Decision Mode executed: structure_valid={has_structure}, forbidden_phrases={has_forbidden}")
        
        # ========================================
        # MINIMAL POST-PROCESSING (SAFETY ONLY)
        # ========================================
        
        # Only check for catastrophic failures
        if not response_text or len(response_text) < 10:
            logger.error("❌ Empty or too-short response from GPT-4")
            response_text = "Unable to generate response. Please try again."
        
        # Check length
        word_count = len(response_text.split())
        if word_count > 250 and not is_decision_mode:
            logger.warning(f"⚠️ Response too long ({word_count} words), but NOT truncating")
        
        # ========================================
        # HALLUCINATION DETECTOR: FABRICATED NUMBERS (PRIORITY 1.5)
        # ========================================
        
        # Detect fabricated financial figures (£X, $X, €X with precision)
        fabricated_money_pattern = r'[£$€]\s*\d+[,\d]*k?\s+per\s+(instruction|property|unit|deal|transaction)'
        
        if re.search(fabricated_money_pattern, response_text, re.IGNORECASE):
            logger.warning(f"⚠️ HALLUCINATION DETECTED: Fabricated financial figure in response")
            
            # Check if we have actual commission/fee data in client profile
            has_fee_data = False
            if client_profile:
                avg_commission = client_profile.get('avg_commission_per_instruction')
                typical_fee = client_profile.get('typical_fee')
                has_fee_data = avg_commission is not None or typical_fee is not None
            
            # If NO fee data exists, strip the fabricated number
            if not has_fee_data:
                logger.warning(f"🚨 STRIPPING FABRICATED NUMBER: No fee data in profile")
                
                # Replace fabricated numbers with generic impact language
                response_text = re.sub(
                    r'costs?\s+[£$€]\s*\d+[,\d]*k?\s+per\s+(instruction|property|unit|deal|transaction)',
                    r'costs significant revenue per \1',
                    response_text,
                    flags=re.IGNORECASE
                )
                
                response_text = re.sub(
                    r'loses?\s+[£$€]\s*\d+[,\d]*k?\s+per\s+(instruction|property|unit|deal|transaction)',
                    r'loses revenue per \1',
                    response_text,
                    flags=re.IGNORECASE
                )
                
                response_text = re.sub(
                    r'misses?\s+[£$€]\s*\d+[,\d]*k?\s+per\s+(instruction|property|unit|deal|transaction)',
                    r'misses opportunity per \1',
                    response_text,
                    flags=re.IGNORECASE
                )
                
                logger.info(f"✅ Fabricated numbers stripped from response")
        
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
            # PR1: Removed is_timing_query and is_clustering_query checks
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

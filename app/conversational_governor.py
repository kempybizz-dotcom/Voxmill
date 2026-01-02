"""
VOXMILL CONVERSATIONAL GOVERNOR - WORLD-CLASS EDITION
======================================================
LLM-based intent classification with zero keyword dependency
Surgical precision. Institutional authority. Elite social absorption.

UPDATED: Added META_AUTHORITY, PROFILE_STATUS, PORTFOLIO_STATUS intents
"""

import logging
import os
import json
from enum import Enum
from typing import Optional, List, Tuple, Dict
from dataclasses import dataclass
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class Intent(Enum):
    """Finite intent taxonomy (18 classes)"""
    SECURITY = "security"
    ADMINISTRATIVE = "administrative"
    PROVOCATION = "provocation"
    CASUAL = "casual"
    STATUS_CHECK = "status_check"
    STRATEGIC = "strategic"
    DECISION_REQUEST = "decision_request"
    META_STRATEGIC = "meta_strategic"
    MONITORING_DIRECTIVE = "monitoring_directive"
    META_AUTHORITY = "meta_authority"
    PROFILE_STATUS = "profile_status"
    VALUE_JUSTIFICATION = "value_justification"
    TRUST_AUTHORITY = "trust_authority"
    STATUS_MONITORING = "status_monitoring"
    PORTFOLIO_MANAGEMENT = "portfolio_management"
    PORTFOLIO_STATUS = "portfolio_status"
    DELIVERY_REQUEST = "delivery_request"
    UNKNOWN = "unknown"


class SemanticCategory(Enum):
    """Semantic categories for mandate relevance"""
    COMPETITIVE_INTELLIGENCE = "competitive_intelligence"
    MARKET_DYNAMICS = "market_dynamics"
    STRATEGIC_POSITIONING = "strategic_positioning"
    TEMPORAL_ANALYSIS = "temporal_analysis"
    SURVEILLANCE = "surveillance"
    ADMINISTRATIVE = "administrative"
    SOCIAL = "social"
    NON_DOMAIN = "non_domain"


@dataclass
class Envelope:
    """Authority envelope - defines what's allowed per intent"""
    analysis_allowed: bool
    max_response_length: int
    silence_allowed: bool
    silence_required: bool
    refusal_allowed: bool
    refusal_required: bool
    decision_mode_eligible: bool
    data_load_allowed: bool
    llm_call_allowed: bool
    allowed_shapes: List[str]


@dataclass
class GovernanceResult:
    """Result of governance check"""
    intent: Intent
    confidence: float
    blocked: bool
    silence_required: bool
    response: Optional[str]
    allowed_shapes: List[str]
    max_words: int
    analysis_allowed: bool
    data_load_allowed: bool
    llm_call_allowed: bool = True
    auto_scoped: bool = False
    semantic_category: Optional[str] = None


@dataclass
class AutoScopeResult:
    """Result of auto-scoping"""
    market: Optional[str]
    timeframe: Optional[str]
    entities: List[str]
    confidence: float
    inferred_from: str


class ConversationalGovernor:
    """Main governance controller with Layer -1 social absorption + LLM intent classification"""
    
    # Class variable to store LLM's intent_type hint
    _last_intent_type = None
    
    # ========================================
    # LAYER -1: SOCIAL ABSORPTION - ELITE EDITION
    # ========================================
    
@staticmethod
    def _absorb_social_input(message: str, client_name: str = "there") -> Tuple[bool, Optional[str]]:
        """
        Layer -1: Social Absorption - Elite Edition
        
        ✅ USES CLIENT NAME SELECTIVELY (30% of greetings)
        
        Absorbs non-semantic social inputs with surgical precision
        
        Returns: (is_social, response_override)
        """
        
        # Normalization pipeline
        message_lower = message.lower().strip()
        message_clean = message_lower.rstrip('?!.,;:')
        
        # Handle apostrophe variants
        apostrophe_variants = ["'", "'", "'", "`", "′"]
        for variant in apostrophe_variants:
            message_clean = message_clean.replace(variant, "")
        
        # Collapse whitespace
        message_clean = ' '.join(message_clean.split())
        
        # Remove common typo patterns
        message_clean = message_clean.replace("whatsssup", "whats up")
        message_clean = message_clean.replace("heyyyy", "hey")
        message_clean = message_clean.replace("hiii", "hi")
        
        # CLASS A: GREETINGS (MULTI-LANGUAGE) - WITH VARIED RESPONSES
        greetings_exact = [
            'hi', 'hello', 'hey', 'yo', 'hiya', 'heya', 'hola', 'sup',
            'morning', 'afternoon', 'evening', 'night',
            'good morning', 'good afternoon', 'good evening', 'good night',
            'gm', 'gn', 'ga',
            'buenos dias', 'buenas tardes', 'buenas noches',
            'bonjour', 'bonsoir', 'salut',
            'guten tag', 'guten morgen', 'guten abend',
            'buongiorno', 'buonasera', 'ciao',
            'wagwan', 'wazzup', 'wassup', 'whaddup', 'howdy',
            'ello', 'ey', 'ayy', 'yooo'
        ]
        
        if message_clean in greetings_exact:
            logger.info(f"🤝 Greeting absorbed: '{message_clean}'")
            
            # ✅ USE NAME 30% of the time (variety)
            import random
            if random.random() < 0.3 and client_name != "there":
                return True, f"Standing by, {client_name}."
            else:
                return True, "Standing by."
        
        # CLASS B: POLITENESS TOKENS
        politeness_exact = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty', 'tyvm', 'ta', 'cheers',
            'much appreciated', 'appreciated', 'appreciate it',
            'got it', 'gotcha', 'understood', 'noted', 'roger', 'copy', 'copy that',
            'ok', 'okay', 'k', 'kk', 'alright', 'alrighty', 'sounds good',
            'cool', 'nice', 'great', 'perfect', 'excellent', 'brilliant',
            'yep', 'yeah', 'yup', 'yes', 'ya', 'aye', 'sure', 'right', 'correct',
            'word', 'bet', 'aight', 'ight', 'dope', 'sweet'
        ]
        
        if message_clean in politeness_exact:
            logger.info(f"🤝 Politeness absorbed: '{message_clean}'")
            return True, "Standing by."
        
        # CLASS C: PHATIC EXPRESSIONS
        phatic_patterns = [
            'how are you', 'how r you', 'how are u', 'how r u', 'hru',
            'you there', 'u there', 'are you there', 'r u there',
            'how you doing', 'how u doing', 'howdy doing',
            'you around', 'u around', 'are you around',
            'you here', 'you ready?', 'are you here',
            'hows it going', 'hows it goin', 'how goes it',
            'you good', 'u good', 'all good', 'everything good',
            'anyone there', 'anybody there'
            'hows things', 'how are things',
            'whats up', 'what up', 'whats good', 'whats poppin',
            'whats crackin', 'whats happening', 'whats the word',
            'whassup', 'wazzup', 'whaddup',
            'sup', 'wassup', 'wsup', 'wsp',
            'you ok', 'u ok', 'you alright', 'u alright',
            'everything ok', 'all ok', 'everything alright'
        ]
        
        if message_clean in phatic_patterns:
            logger.info(f"🤝 Phatic absorbed: '{message_clean}'")
            return True, "Standing by."
        
        # CLASS D: MOOD STATEMENTS (CONTEXTUAL)
        pure_mood_patterns = [
            'feels moist', 'feels wet', 'feels damp', 'feels soggy',
            'feels weird', 'feels odd', 'feels strange', 'feels off',
            'feels bad', 'feels wrong', 'feels sketchy',
            'seems weird', 'seems odd', 'seems off', 'seems strange',
            'sounds weird', 'sounds odd', 'sounds off', 'sounds strange',
            'looks weird', 'looks odd', 'looks off', 'looks strange'
        ]
        
        if any(pattern in message_clean for pattern in pure_mood_patterns):
            market_question_indicators = [
                'bad sign', 'good sign', 'red flag', 'warning sign',
                'what does', 'why', 'should i', 'what if',
                'meaning', 'mean', 'indicate', 'signal', 'suggest',
                'tell me', 'explain', 'interpret'
            ]
            
            if not any(indicator in message_clean for indicator in market_question_indicators):
                logger.info(f"🤝 Mood statement absorbed (silence)")
                return True, None
        
        # CLASS E: LAUGHTER / NON-SEMANTIC NOISE
        laughter_exact = [
            'lol', 'lmao', 'lmfao', 'rofl', 'rotfl', 'lmbo',
            'haha', 'hehe', 'hehehe', 'lolol', 'lololol',
            'ahahaha', 'jajaja', 'kkkk'
        ]
        
        if message_clean in laughter_exact:
            logger.info(f"🤝 Laughter absorbed (silence)")
            return True, None
        
        # CLASS F: APOLOGIES
        apology_patterns = [
            'sorry', 'my bad', 'my mistake', 'apologies', 'excuse me',
            'pardon', 'pardon me', 'forgive me'
        ]
        
        if message_clean in apology_patterns:
            logger.info(f"🤝 Apology absorbed")
            return True, "Standing by."
        
        # CLASS G: FAREWELLS
        farewell_patterns = [
            'bye', 'goodbye', 'good bye', 'later', 'see ya', 'see you',
            'catch you later', 'talk later', 'ttyl', 'g2g', 'gotta go',
            'peace', 'peace out', 'take care'
        ]
        
        if message_clean in farewell_patterns:
            logger.info(f"🤝 Farewell absorbed")
            return True, "Standing by."
        
        # CLASS H: TYPO-TOLERANT GREETING DETECTION
        if len(message_clean) <= 6:
            greeting_core = ['hello', 'hey', 'hi']
            
            for core in greeting_core:
                if ConversationalGovernor._is_typo_match(message_clean, core, max_distance=2):
                    logger.info(f"🤝 Typo greeting absorbed: '{message_clean}' → '{core}'")
                    
                    # ✅ USE NAME 30% of the time for typo greetings too
                    import random
                    if random.random() < 0.3 and client_name != "there":
                        return True, f"Standing by, {client_name}."
                    else:
                        return True, "Standing by."
        
        # NOT SOCIAL - PASS TO MANDATE RELEVANCE
        return False, None
    
    @staticmethod
    def _is_typo_match(input_str: str, target_str: str, max_distance: int = 2) -> bool:
        """Check if input_str is a typo of target_str using Levenshtein distance"""
        
        if len(input_str) == 0 or len(target_str) == 0:
            return False
        
        len_diff = abs(len(input_str) - len(target_str))
        if len_diff > max_distance:
            return False
        
        distance = ConversationalGovernor._levenshtein_distance(input_str, target_str)
        return distance <= max_distance
    
    @staticmethod
    def _levenshtein_distance(s1: str, s2: str) -> int:
        """Compute Levenshtein distance between two strings"""
        
        if len(s1) < len(s2):
            return ConversationalGovernor._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    # ========================================
    # LAYER 0: LLM-BASED MANDATE RELEVANCE
    # ========================================
    
    @staticmethod
    async def _check_mandate_relevance(message: str, conversation_context: Dict = None) -> Tuple[bool, SemanticCategory, float]:
        """
        LLM-based intent classification (OpenAI v1.0+ compatible)
        
        ✅ INDUSTRY AGNOSTIC - Works for ANY vertical (Real Estate, Hedge Funds, Yachting, Automotive, etc.)
        
        NO KEYWORDS. NO PATTERNS. ONLY LLM.
        
        Returns: (is_mandate_relevant, semantic_category, confidence)
        """
        
        # Initialize async client
        client = AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Build context string if available
        context_str = ""
        if conversation_context:
            regions = conversation_context.get('regions', [])
            agents = conversation_context.get('agents', [])
            topics = conversation_context.get('topics', [])
            
            if regions or agents or topics:
                context_str = f"\n\nConversation context:\n- Recent regions: {regions}\n- Recent agents: {agents}\n- Recent topics: {topics}"
        
        # ✅ INDUSTRY-AGNOSTIC PROMPT
        prompt = f"""Classify this message for a market intelligence analyst.

Message: "{message}"{context_str}

Return ONLY valid JSON (no markdown, no explanation):
{{
  "intent_type": "market_query|meta_authority|profile_status|portfolio_status|portfolio_management|value_justification|trust_authority|status_monitoring|delivery_request|preference_change|profanity|gibberish|follow_up",
  "is_mandate_relevant": true|false,
  "semantic_category": "competitive_intelligence|market_dynamics|strategic_positioning|temporal_analysis|surveillance|administrative|non_domain",
  "confidence": 0.0-1.0,
  "requires_intelligence": true|false,
  "market_mentioned": "string|null",
  "reasoning": "one sentence explanation"
}}

Classification rules:
1. PROFANITY ALONE = gibberish (e.g. "Fuck off", "Go to hell")
2. PERSONAL ANECDOTES = gibberish (e.g. "My dog is sick", "I went shopping")
3. META QUESTIONS = meta_authority (e.g. "What is Voxmill?", "What can you do?", "Tell me your capabilities")
4. IDENTITY QUESTIONS = profile_status (e.g. "What's my name?", "Who am I?", "Am I on trial?")
5. PORTFOLIO VIEWING = portfolio_status (e.g. "Show me my portfolio", "How's my portfolio?", "Portfolio summary")
6. PORTFOLIO ACTIONS = portfolio_management (e.g. "Can I add assets?", "How do I track holdings?")
7. VALUE QUESTIONS = value_justification (e.g. "Why Voxmill?", "Why should I use this?")
8. TRUST QUESTIONS = trust_authority (e.g. "Can I trust you?", "How confident are you?")
9. STATUS QUERIES = status_monitoring (e.g. "What am I waiting for?", "What am I monitoring?")
10. DELIVERY REQUESTS = delivery_request (e.g. "PDF?", "Send report", "Weekly PDF")
11. IMPLICIT FOLLOW-UPS = follow_up (e.g. "So what?", "Why?", "Compare that")
12. MARKET CHANGES = preference_change (e.g. "Switch to Manhattan", "Show me Dubai")
13. MARKET QUERIES = market_query (e.g. "Market overview", "What's happening?", "Show me trends")
14. PURE GIBBERISH = gibberish (e.g. "ahsh", "shshs", "Oi")
15. GREETINGS/POLITENESS = gibberish (handled separately, should not reach here)
16. OFF-TOPIC = gibberish (anything not about markets, investments, or business intelligence)

Examples:
- "What is Voxmill?" → meta_authority (about system capabilities)
- "Why should I use Voxmill?" → value_justification (about value proposition)
- "Let's get started" → meta_authority (onboarding request)
- "Show me what you can do" → meta_authority (capability demonstration)
- "Can I trust you?" → trust_authority (about reliability)
- "What am I waiting for?" → status_monitoring (about user's status)
- "Can I add holdings?" → portfolio_management (portfolio action)
- "PDF?" → delivery_request (report delivery)
- "What's my name?" → profile_status (about user identity)
- "Show me my portfolio" → portfolio_status (portfolio viewing)
- "Market overview" → market_query (legitimate query)
- "What's happening in Manhattan hedge funds?" → market_query (industry-specific query)
- "Show me Dubai yacht market" → market_query (industry-specific query)
- "Beverly Hills luxury automotive trends" → market_query (industry-specific query)

JSON:"""
        
        try:
            # Call GPT-4 for classification (OpenAI v1.0+ syntax)
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precision intent classifier. Return ONLY valid JSON. No markdown formatting. No explanation outside the JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=150,
                temperature=0,
                timeout=5.0
            )
            
            # Parse response (v1.0+ response structure)
            raw_response = response.choices[0].message.content.strip()
            
            # Strip markdown formatting if present
            if raw_response.startswith('```'):
                raw_response = raw_response.split('```')[1]
                if raw_response.startswith('json'):
                    raw_response = raw_response[4:]
            
            intent_data = json.loads(raw_response.strip())
            
            # CRITICAL: Store intent_type for downstream use
            ConversationalGovernor._last_intent_type = intent_data.get('intent_type')
            
            # Log classification
            logger.info(f"🤖 LLM Intent: {intent_data['intent_type']} | Relevant: {intent_data['is_mandate_relevant']} | Reason: {intent_data['reasoning']}")
            
            # Map to SemanticCategory
            category_map = {
                'competitive_intelligence': SemanticCategory.COMPETITIVE_INTELLIGENCE,
                'market_dynamics': SemanticCategory.MARKET_DYNAMICS,
                'strategic_positioning': SemanticCategory.STRATEGIC_POSITIONING,
                'temporal_analysis': SemanticCategory.TEMPORAL_ANALYSIS,
                'surveillance': SemanticCategory.SURVEILLANCE,
                'administrative': SemanticCategory.ADMINISTRATIVE,
                'non_domain': SemanticCategory.NON_DOMAIN
            }
            
            semantic_category = category_map.get(
                intent_data['semantic_category'],
                SemanticCategory.NON_DOMAIN
            )
            
            return (
                intent_data['is_mandate_relevant'],
                semantic_category,
                intent_data['confidence']
            )
            
        except TimeoutError:
            logger.error("⏱️ LLM intent classification timeout - falling back to REJECT")
            return False, SemanticCategory.NON_DOMAIN, 0.5
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ LLM returned invalid JSON: {e}")
            return False, SemanticCategory.NON_DOMAIN, 0.5
            
        except Exception as e:
            logger.error(f"❌ LLM intent classification failed: {e}")
            return False, SemanticCategory.NON_DOMAIN, 0.5

# ========================================
    # AUTO-SCOPING
    # ========================================
    
    @staticmethod
    def _auto_scope(message: str, client_profile: Dict, conversation_context: Dict = None) -> AutoScopeResult:
        """
        Infer market, timeframe, and entities from context
        
        ✅ FIXED: No hardcoded market defaults - returns None if no market configured
        """
        
        message_lower = message.lower().strip()
        
        # ========================================
        # MARKET/DOMAIN INFERENCE
        # ========================================
        
        market = None
        market_source = "none"
        
        # Rule 1: User preference default (highest priority)
        if client_profile:
            # ✅ FIXED: Use active_market field (matches Airtable schema)
            active_market = client_profile.get('active_market')
            
            if active_market:
                market = active_market
                market_source = "user_preference"
            else:
                # Fallback to preferred_regions if active_market not set
                preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', [])
                if preferred_regions:
                    market = preferred_regions[0]
                    market_source = "user_preference"
        
        # Rule 2: Conversation context
        if not market and conversation_context:
            last_regions = conversation_context.get('regions', [])
            if last_regions:
                market = last_regions[-1]
                market_source = "conversation_context"
        
        # ✅ FIXED: NO DEFAULT FALLBACK - let GATE 5 handle missing market
        # Rule 3: Return None if no market found
        if not market:
            market = None
            market_source = "none"
            logger.warning("⚠️ Auto-scope: No market configured, returning None")
        
        # ========================================
        # TIMEFRAME INFERENCE
        # ========================================
        
        timeframe = "current+7d"  # Default: current state + recent trend
        
        # Explicit temporal markers
        if 'this week' in message_lower or 'past week' in message_lower:
            timeframe = "7d"
        elif 'this month' in message_lower or 'past month' in message_lower:
            timeframe = "30d"
        elif 'recently' in message_lower or 'lately' in message_lower:
            timeframe = "14d"
        elif 'today' in message_lower or 'now' in message_lower:
            timeframe = "current"
        
        # ========================================
        # ENTITY INFERENCE
        # ========================================
        
        entities = []
        
        # Conversation context entities
        if conversation_context:
            last_agents = conversation_context.get('agents', [])
            if last_agents:
                entities = [last_agents[-1]]
        
        # Calculate confidence
        if market_source == "user_preference":
            confidence = 0.95
        elif market_source == "conversation_context":
            confidence = 0.70
        else:
            confidence = 0.00  # ✅ No market = zero confidence
        
        return AutoScopeResult(
            market=market,  # ✅ Can be None
            timeframe=timeframe,
            entities=entities if entities else [],
            confidence=confidence,
            inferred_from=market_source
        )
    
    # ========================================
    # INTENT CLASSIFICATION (MINIMAL)
    # ========================================
    
    @staticmethod
    def _force_intent_from_semantic_category(semantic_category: SemanticCategory, message: str, intent_type: str = None) -> Intent:
        """
        Map semantic category + intent_type to best-fit intent
        Preserves nuance while blocking non-answers
        """
        
        # PRIORITY: Use LLM's intent_type if provided (NEW - EXPANDED)
        if intent_type == "meta_authority":
            return Intent.META_AUTHORITY
        
        if intent_type == "profile_status":
            return Intent.PROFILE_STATUS
        
        if intent_type == "portfolio_status":
            return Intent.PORTFOLIO_STATUS
        
        if intent_type == "portfolio_management":
            return Intent.PORTFOLIO_MANAGEMENT
        
        if intent_type == "value_justification":
            return Intent.VALUE_JUSTIFICATION
        
        if intent_type == "trust_authority":
            return Intent.TRUST_AUTHORITY
        
        if intent_type == "status_monitoring":
            return Intent.STATUS_MONITORING
        
        if intent_type == "delivery_request":
            return Intent.DELIVERY_REQUEST
        
        # Original logic for other categories
        message_lower = message.lower()
        
        # Action/recommendation signals
        action_signals = ['what should', 'recommend', 'advise', 'tell me what', 
                          'best action', 'next step', 'what do i', 'give me']
        has_action_signal = any(sig in message_lower for sig in action_signals)
        
        # State/status signals  
        state_signals = ['what is', 'what are', 'current', 'now', 'status', 
                         'how is', 'where is', 'show me']
        has_state_signal = any(sig in message_lower for sig in state_signals)
        
        # Map category to intent
        if semantic_category == SemanticCategory.COMPETITIVE_INTELLIGENCE:
            return Intent.STATUS_CHECK if has_state_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.MARKET_DYNAMICS:
            return Intent.STATUS_CHECK if has_state_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.STRATEGIC_POSITIONING:
            return Intent.DECISION_REQUEST if has_action_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.TEMPORAL_ANALYSIS:
            return Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.SURVEILLANCE:
            return Intent.STATUS_CHECK if has_state_signal else Intent.MONITORING_DIRECTIVE
        
        elif semantic_category == SemanticCategory.ADMINISTRATIVE:
            return Intent.STATUS_CHECK
        
        else:
            return Intent.STRATEGIC
    
    @staticmethod
    def _classify_intent(message: str) -> tuple[Intent, float]:
        """
        SIMPLIFIED intent classification - only handles security/provocation/casual
        
        All other intents determined by LLM in _check_mandate_relevance()
        
        Returns: (intent, confidence_score)
        """
        
        message_lower = message.lower().strip()
        message_clean = ' '.join(message_lower.split())
        
        # ========================================
        # SECURITY (95% threshold)
        # ========================================
        
        security_keywords = ['pin', 'code', 'lock', 'unlock', 'access', 'verify', 'reset pin']
        if any(kw in message_clean for kw in security_keywords):
            return Intent.SECURITY, 0.95
        
        # ========================================
        # PROVOCATION (85% threshold)
        # ========================================
        
        provocation_exact = ['lol', 'haha', 'lmao', 'hehe', 'lmfao', 'rofl']
        if message_clean in provocation_exact:
            return Intent.PROVOCATION, 0.98
        
        # ========================================
        # CASUAL - HIGH CONFIDENCE (90% threshold)
        # ========================================
        
        # Exact matches
        casual_exact = [
            'whats up', 'what up', 'sup', 'wassup', 'whatsup',
            'hi', 'hello', 'hey', 'yo', 'hiya',
            'good morning', 'good afternoon', 'good evening'
        ]
        
        if message_clean in casual_exact:
            return Intent.CASUAL, 0.95
        
        # Acknowledgments
        acknowledgments = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool', 'right',
            'cheers'
        ]
        
        if message_clean in acknowledgments:
            return Intent.CASUAL, 0.95
        
        # ========================================
        # UNKNOWN (let LLM handle everything else)
        # ========================================
        
        return Intent.UNKNOWN, 0.50
        
        security_keywords = ['pin', 'code', 'lock', 'unlock', 'access', 'verify', 'reset pin']
        if any(kw in message_clean for kw in security_keywords):
            return Intent.SECURITY, 0.95
        
        # ========================================
        # PROVOCATION (85% threshold)
        # ========================================
        
        provocation_exact = ['lol', 'haha', 'lmao', 'hehe', 'lmfao', 'rofl']
        if message_clean in provocation_exact:
            return Intent.PROVOCATION, 0.98
        
        # ========================================
        # CASUAL - HIGH CONFIDENCE (90% threshold)
        # ========================================
        
        # Exact matches
        casual_exact = [
            'whats up', 'what up', 'sup', 'wassup', 'whatsup',
            'hi', 'hello', 'hey', 'yo', 'hiya',
            'good morning', 'good afternoon', 'good evening'
        ]
        
        if message_clean in casual_exact:
            return Intent.CASUAL, 0.95
        
        # Acknowledgments
        acknowledgments = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool', 'right',
            'cheers'
        ]
        
        if message_clean in acknowledgments:
            return Intent.CASUAL, 0.95
        
        # ========================================
        # UNKNOWN (let LLM handle everything else)
        # ========================================
        
        return Intent.UNKNOWN, 0.50
    
    # ========================================
    # ENVELOPE SYSTEM
    # ========================================
    
    @staticmethod
    def _get_envelope(intent: Intent) -> Envelope:
        """
        Get authority envelope for intent
        
        Defines what operations are allowed
        """
        
        envelopes = {
            Intent.PROVOCATION: Envelope(
                analysis_allowed=False,
                max_response_length=0,
                silence_allowed=True,
                silence_required=True,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["SILENCE"]
            ),
            
            Intent.CASUAL: Envelope(
                analysis_allowed=False,
                max_response_length=50,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["ACKNOWLEDGMENT", "STATUS_LINE"]
            ),
            
            Intent.STATUS_CHECK: Envelope(
                analysis_allowed=True,
                max_response_length=400,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["STATUS_LINE", "STRUCTURED_BRIEF"]
            ),
            
            Intent.STRATEGIC: Envelope(
                analysis_allowed=True,
                max_response_length=400,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=True,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["SINGLE_SIGNAL", "STRUCTURED_BRIEF"]
            ),
            
            Intent.DECISION_REQUEST: Envelope(
                analysis_allowed=True,
                max_response_length=800,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=True,
                refusal_required=False,
                decision_mode_eligible=True,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["DECISION"]
            ),
            
            Intent.META_STRATEGIC: Envelope(
                analysis_allowed=True,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=True,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["SINGLE_SIGNAL"]
            ),
            
            Intent.META_AUTHORITY: Envelope(
                analysis_allowed=False,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.PROFILE_STATUS: Envelope(
                analysis_allowed=False,
                max_response_length=100,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.PORTFOLIO_STATUS: Envelope(
                analysis_allowed=True,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["STRUCTURED_BRIEF"]
            ),
            
            Intent.PORTFOLIO_MANAGEMENT: Envelope(
                analysis_allowed=False,
                max_response_length=100,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.VALUE_JUSTIFICATION: Envelope(
                analysis_allowed=False,
                max_response_length=100,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.TRUST_AUTHORITY: Envelope(
                analysis_allowed=False,
                max_response_length=80,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.STATUS_MONITORING: Envelope(
                analysis_allowed=True,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=False,  # Read from monitoring module
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.DELIVERY_REQUEST: Envelope(
                analysis_allowed=False,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,  # Static response
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.UNKNOWN: Envelope(
                analysis_allowed=False,
                max_response_length=50,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=True,
                refusal_required=True,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["REFUSAL"]
            ),
            
            Intent.SECURITY: Envelope(
                analysis_allowed=False,
                max_response_length=100,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.ADMINISTRATIVE: Envelope(
                analysis_allowed=False,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE", "STRUCTURED_BRIEF"]
            ),
            
            Intent.MONITORING_DIRECTIVE: Envelope(
                analysis_allowed=False,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),
        }
        
        return envelopes.get(intent, envelopes[Intent.UNKNOWN])
    
@staticmethod
    def _get_hardcoded_response(intent: Intent, message: str, client_profile: dict = None) -> Optional[str]:
        """
        Get hardcoded response for simple intents
        
        ✅ VARIED ACKNOWLEDGMENTS - rotates between options
        Intent-based responses only (no phrase matching)
        """
        
        client_name = client_profile.get('name', 'there') if client_profile else 'there'
        
        # META_AUTHORITY responses
        if intent == Intent.META_AUTHORITY:
            return """I provide real-time market intelligence for luxury property markets.

Analysis includes inventory levels, pricing trends, competitive dynamics, and strategic positioning.

What market intelligence can I provide?"""
        
        # PROFILE_STATUS responses
        if intent == Intent.PROFILE_STATUS:
            if client_profile:
                name = client_profile.get('name', 'there')
                tier = client_profile.get('tier', 'tier_1')
                tier_display = {'tier_1': 'Basic', 'tier_2': 'Premium', 'tier_3': 'Enterprise'}.get(tier, 'institutional')
                
                return f"""CLIENT PROFILE

Name: {name}
Service Tier: {tier_display}

What market intelligence can I provide?"""
            else:
                return "Client profile loading..."
        
        # VALUE_JUSTIFICATION responses
        if intent == Intent.VALUE_JUSTIFICATION:
            return """Voxmill delivers institutional-grade market intelligence via WhatsApp.

Real-time data. Fortune-500 presentation quality. Surgical precision.

What market intelligence can I provide?"""
        
        # TRUST_AUTHORITY responses
        if intent == Intent.TRUST_AUTHORITY:
            return """Every insight is sourced from verified APIs and cross-referenced datasets.

Confidence levels disclosed. No hallucinations.

What market intelligence can I provide?"""
        
        # PORTFOLIO_MANAGEMENT responses
        if intent == Intent.PORTFOLIO_MANAGEMENT:
            return """Your portfolio is currently empty.

You can add properties by sending an address, postcode, or Rightmove link."""
        
        # DELIVERY_REQUEST responses
        if intent == Intent.DELIVERY_REQUEST:
            return """PDF reports generate Sunday at 6:00 AM UTC.

For immediate regeneration, contact intel@voxmill.uk"""
        
        # ✅ VARIED ACKNOWLEDGMENTS
        if intent == Intent.CASUAL:
            import random
            
            # 20% chance: use name if available
            if client_name != "there" and random.random() < 0.2:
                options = [
                    f"Standing by, {client_name}.",
                    f"Ready, {client_name}.",
                    f"Monitoring, {client_name}."
                ]
            else:
                # 80% chance: no name (variety pool)
                options = [
                    "Standing by.",
                    "Ready.",
                    "Monitoring.",
                    "Ready when you are.",
                    "Standing by.",  # Weighted - appears twice for higher probability
                    "Standing by."   # Weighted again
                ]
            
            return random.choice(options)
        
        # Intent-based responses (non-casual)
        intent_responses = {
            Intent.UNKNOWN: "Outside intelligence scope.",
            Intent.SECURITY: "Enter your 4-digit code.",
        }
        
        return intent_responses.get(intent)
    
    @staticmethod
    def _detect_multi_intent(message: str) -> List[str]:
        """
        Detect multiple intents in a single message
        
        Returns list of intent segments
        
        Examples:
        - "PDF and market overview" → ["PDF", "market overview"]
        - "Monitor X, also show Y" → ["Monitor X", "show Y"]
        """
        
        # Split on conjunctions and transition words
        separators = [
            ', also ',
            ', and ',
            ' also ',
            ' and then ',
            ' then ',
            '; ',
            '. '
        ]
        
        segments = [message]
        
        for sep in separators:
            new_segments = []
            for seg in segments:
                if sep in seg.lower():
                    parts = re.split(re.escape(sep), seg, flags=re.IGNORECASE)
                    new_segments.extend(parts)
                else:
                    new_segments.append(seg)
            segments = new_segments
        
        # Clean and filter
        cleaned = [s.strip() for s in segments if s.strip()]
        
        # Only return multiple if we actually found splits
        return cleaned if len(cleaned) > 1 else [message]
    
    # ========================================
    # MAIN GOVERNANCE ENTRY POINT
    # ========================================
    
    @staticmethod
    async def govern(message_text: str, sender: str, client_profile: dict, 
                    system_state: dict, conversation_context: Dict = None) -> GovernanceResult:
        """
        Main governance entry point with Layer -1 social absorption
        
        WORLD-CLASS ARCHITECTURE:
        - Layer -1: Social absorption (greetings, politeness)
        - Layer 0: LLM intent classification (meaning)
        - Layer 1: Airtable module enforcement (permission)
        - Layer 2: Envelope constraints (execution)
        
        Returns: GovernanceResult with intent, constraints, and optional response
        """
        
        # ========================================
        # LAYER -1: SOCIAL ABSORPTION
        # ========================================
        
        client_name = client_profile.get('name', 'there')
        
        is_social, social_response = ConversationalGovernor._absorb_social_input(
            message_text,
            client_name
        )
        
        if is_social:
            logger.info(f"🤝 Social input absorbed: returning '{social_response or 'SILENCE'}'")
            
            if social_response is None:
                # Silence
                return GovernanceResult(
                    intent=Intent.CASUAL,
                    confidence=1.0,
                    blocked=True,
                    silence_required=True,
                    response=None,
                    allowed_shapes=["SILENCE"],
                    max_words=0,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category="social"
                )
            else:
                # Brief acknowledgment
                return GovernanceResult(
                    intent=Intent.CASUAL,
                    confidence=1.0,
                    blocked=True,
                    silence_required=False,
                    response=social_response,
                    allowed_shapes=["ACKNOWLEDGMENT"],
                    max_words=10,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category="social"
                )
        
        # ========================================
        # LAYER -0.5: TRIAL ENVELOPE (OUTER LAYER - CRITICAL)
        # ========================================
        
        subscription_status = client_profile.get('subscription_status')
        trial_expired = client_profile.get('trial_expired', False)
        
        # TRIAL EXPIRED - HARD STOP
        if trial_expired:
            logger.warning(f"🚫 TRIAL EXPIRED: {sender}")
            
            return GovernanceResult(
                intent=Intent.ADMINISTRATIVE,
                confidence=1.0,
                blocked=True,
                silence_required=False,
                response="""TRIAL PERIOD EXPIRED

Your 24-hour trial access has concluded.

To continue using Voxmill Intelligence, contact:
intel@voxmill.uk

Thank you for trying our service.""",
                allowed_shapes=["STATUS_LINE"],
                max_words=50,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category="administrative"
            )
        
        # TRIAL ACTIVE - LIMITED ENVELOPE
        if subscription_status == 'Trial':
            logger.info(f"🔐 TRIAL ENVELOPE ACTIVE for {sender}")
            
            # Quick LLM check to determine intent type (we need this to know if it's meta/admin)
            is_mandate_relevant_trial, semantic_category_trial, semantic_confidence_trial = await ConversationalGovernor._check_mandate_relevance(
                message_text, 
                conversation_context
            )
            
            intent_type_hint = ConversationalGovernor._last_intent_type
            
            logger.info(f"Trial intent hint: {intent_type_hint}")
            
            # ALWAYS ALLOW: Meta, Trust, Value, Profile questions
            if intent_type_hint in ['meta_authority', 'profile_status', 'value_justification', 
                                    'trust_authority', 'portfolio_management', 'delivery_request']:
                logger.info(f"✅ TRIAL: Meta/Admin question allowed - {intent_type_hint}")
                
                # Get hardcoded response
                if intent_type_hint == 'meta_authority':
                    response = """I provide real-time market intelligence for luxury property markets.

Analysis includes inventory levels, pricing trends, competitive dynamics, and strategic positioning.

What market intelligence can I provide?"""
                
                elif intent_type_hint == 'value_justification':
                    response = """Voxmill delivers institutional-grade market intelligence via WhatsApp.

Real-time data. Fortune-500 presentation quality. Surgical precision.

What market intelligence can I provide?"""
                
                elif intent_type_hint == 'trust_authority':
                    response = """Every insight is sourced from verified APIs and cross-referenced datasets.

Confidence levels disclosed. No hallucinations.

What market intelligence can I provide?"""
                
                elif intent_type_hint == 'profile_status':
                    name = client_profile.get('name', 'there')
                    tier = 'Trial'
                    response = f"""CLIENT PROFILE

Name: {name}
Service Tier: {tier}

Trial period: 24 hours from activation
Sample intelligence: 1 query available

What market intelligence can I provide?"""
                
                elif intent_type_hint == 'portfolio_management':
                    response = """Portfolio tracking is available on paid plans.

Your trial provides limited intelligence sampling.

Contact intel@voxmill.uk to upgrade."""
                
                elif intent_type_hint == 'delivery_request':
                    response = """PDF reports are available on paid plans.

Your trial provides limited intelligence sampling.

Contact intel@voxmill.uk to upgrade."""
                
                else:
                    response = "Standing by."
                
                return GovernanceResult(
                    intent=Intent.META_AUTHORITY if intent_type_hint == 'meta_authority' else Intent.ADMINISTRATIVE,
                    confidence=1.0,
                    blocked=True,
                    silence_required=False,
                    response=response,
                    allowed_shapes=["STATUS_LINE"],
                    max_words=50,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category="administrative"
                )
            
            # INTELLIGENCE QUERY - CHECK TRIAL SAMPLE LIMIT
            if intent_type_hint in ['market_query', 'follow_up'] or is_mandate_relevant_trial:
                logger.info(f"🔍 TRIAL: Intelligence query detected - checking sample limit")
                
                # Check if trial sample already used (via MongoDB flag from whatsapp.py)
                # This flag is set in whatsapp.py GATE 2, we just read it here
                trial_sample_used = system_state.get('trial_sample_used', False)
                
                if trial_sample_used:
                    logger.warning(f"🚫 TRIAL: Sample already used")
                    
                    return GovernanceResult(
                        intent=Intent.ADMINISTRATIVE,
                        confidence=1.0,
                        blocked=True,
                        silence_required=False,
                        response="""TRIAL SAMPLE COMPLETE

You've received your trial intelligence sample.

To continue receiving market intelligence, contact:
intel@voxmill.uk

Trial period: 24 hours from activation""",
                        allowed_shapes=["STATUS_LINE"],
                        max_words=50,
                        analysis_allowed=False,
                        data_load_allowed=False,
                        llm_call_allowed=False,
                        auto_scoped=False,
                        semantic_category="administrative"
                    )
                
                # ALLOW FIRST INTELLIGENCE SAMPLE
                # Continue to full governance flow below (don't return yet)
                logger.info(f"✅ TRIAL: First sample allowed - continuing to full governance")
            
            # PREFERENCE CHANGE - ALWAYS ALLOW (but with trial caveat)
            if intent_type_hint == 'preference_change' or 'switch to' in message_text.lower():
                logger.info(f"✅ TRIAL: Preference change allowed")
                
                # Extract region from message (simple pattern matching)
                message_lower = message_text.lower()
                new_region = None
                
                if 'manchester' in message_lower:
                    new_region = 'Manchester'
                elif 'birmingham' in message_lower:
                    new_region = 'Birmingham'
                elif 'leeds' in message_lower:
                    new_region = 'Leeds'
                elif 'liverpool' in message_lower:
                    new_region = 'Liverpool'
                
                if new_region:
                    response = f"""PREFERENCE UPDATED

Primary region set to: {new_region}

Note: Trial access provides limited intelligence sampling.

For full regional coverage, contact:
intel@voxmill.uk"""
                else:
                    response = """PREFERENCE UPDATE

Specify region format: "Switch to [City]"

Example: "Switch to Manchester"

Trial access provides limited intelligence sampling."""
                
                return GovernanceResult(
                    intent=Intent.ADMINISTRATIVE,
                    confidence=1.0,
                    blocked=True,
                    silence_required=False,
                    response=response,
                    allowed_shapes=["STATUS_LINE"],
                    max_words=50,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category="administrative"
                )
        
        logger.info(f"✅ Trial envelope passed (not trial or sample allowed)")
        
        # ========================================
        # LAYER 0: LLM-BASED MANDATE RELEVANCE CHECK
        # ========================================
        
        is_mandate_relevant, semantic_category, semantic_confidence = await ConversationalGovernor._check_mandate_relevance(
            message_text, 
            conversation_context
        )
        
        logger.info(f"Mandate check: relevant={is_mandate_relevant}, category={semantic_category.value}, confidence={semantic_confidence:.2f}")
        
        # Get LLM's intent_type hint for special handling
        intent_type_hint = ConversationalGovernor._last_intent_type
        
        # ========================================
        # CRITICAL: IMPLICIT REFERENCE OVERRIDE
        # ========================================
        
        if not is_mandate_relevant and conversation_context:
            # Check for implicit reference words
            implicit_refs = ['that', 'it', 'this', 'these', 'those', 'them']
            query_words = message_text.lower().split()
            
            # If query has implicit reference and context exists
            has_implicit_ref = any(ref in query_words[:3] for ref in implicit_refs)
            has_context = bool(conversation_context.get('regions') or 
                               conversation_context.get('agents') or 
                               conversation_context.get('topics'))
            
            if has_context and has_implicit_ref:
                # Force to mandate-relevant ONLY if explicit reference word exists
                is_mandate_relevant = True
                semantic_category = SemanticCategory.STRATEGIC_POSITIONING
                semantic_confidence = 0.80
                logger.info(f"✅ Implicit reference override: query has context, forcing mandate relevance")
        
        # ========================================
        # CRITICAL FIX: SPECIAL INTENT OVERRIDE (BEFORE REFUSAL)
        # ========================================
        
        # META_AUTHORITY, PROFILE_STATUS, PORTFOLIO_STATUS never refused
        if intent_type_hint in ['meta_authority', 'profile_status', 'portfolio_status', 'value_justification', 'trust_authority', 'portfolio_management', 'status_monitoring', 'delivery_request']:
            # Force to mandate-relevant (these are valid system queries)
            is_mandate_relevant = True
            logger.info(f"✅ Special intent {intent_type_hint} - forcing mandate relevance")
        
        # ========================================
        # CRITICAL FIX: REFUSAL CHECK (NOISE vs DISALLOWED)
        # ========================================
        
        # If NOT mandate-relevant, check if it's noise or a disallowed request
        if not is_mandate_relevant:
            # NOISE (gibberish, profanity, anecdotes) → "Standing by."
            if intent_type_hint in ['gibberish', 'profanity']:
                logger.info(f"🔇 NOISE detected ({intent_type_hint}) - responding 'Standing by.'")
                
                return GovernanceResult(
                    intent=Intent.CASUAL,
                    confidence=semantic_confidence,
                    blocked=True,
                    silence_required=False,
                    response="Standing by.",
                    allowed_shapes=["ACKNOWLEDGMENT"],
                    max_words=5,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category=semantic_category.value
                )
            
            # DISALLOWED REQUEST → "Outside intelligence scope."
            else:
                logger.warning(f"🚫 REFUSAL: Not mandate-relevant, not noise - refusing")
                
                return GovernanceResult(
                    intent=Intent.UNKNOWN,
                    confidence=semantic_confidence,
                    blocked=True,
                    silence_required=False,
                    response="Outside intelligence scope.",
                    allowed_shapes=["REFUSAL"],
                    max_words=10,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category=semantic_category.value
                )
        
        # ========================================
        # AUTO-SCOPING (for mandate-relevant queries)
        # ========================================
        
        auto_scope_result = ConversationalGovernor._auto_scope(
            message_text,
            client_profile,
            conversation_context
        )
        
        logger.info(f"Auto-scoped: market={auto_scope_result.market}, "
                   f"timeframe={auto_scope_result.timeframe}, "
                   f"entities={auto_scope_result.entities}, "
                   f"source={auto_scope_result.inferred_from}")
        
        # ========================================
        # INTENT CLASSIFICATION
        # ========================================
        
        intent, confidence = ConversationalGovernor._classify_intent(message_text)
        
        logger.info(f"Intent classified: {intent.value} (confidence: {confidence:.2f})")
        
        # ========================================
        # CONFIDENCE THRESHOLD ENFORCEMENT
        # ========================================
        
        CONFIDENCE_THRESHOLDS = {
            Intent.SECURITY: 0.95,
            Intent.ADMINISTRATIVE: 0.90,
            Intent.PROVOCATION: 0.85,
            Intent.CASUAL: 0.80,
            Intent.STATUS_CHECK: 0.90,
            Intent.STRATEGIC: 0.75,
            Intent.DECISION_REQUEST: 0.90,
            Intent.META_STRATEGIC: 0.85,
            Intent.MONITORING_DIRECTIVE: 0.93,
            Intent.META_AUTHORITY: 0.75,
            Intent.PROFILE_STATUS: 0.75,
            Intent.PORTFOLIO_STATUS: 0.75,
            Intent.UNKNOWN: 0.00
        }
        
        required_confidence = CONFIDENCE_THRESHOLDS.get(intent, 0.80)
        
        if confidence < required_confidence:
            logger.warning(f"Confidence too low: {confidence:.2f} < {required_confidence:.2f}")
            intent = Intent.UNKNOWN
            confidence = 0.50
        
# ========================================
        # CRITICAL: FORCE BEST-FIT INTENT FOR MANDATE-RELEVANT QUERIES
        # ========================================
        
        if is_mandate_relevant and intent == Intent.UNKNOWN:
            # Force to best-fit intent based on semantic category + LLM hint
            forced_intent = ConversationalGovernor._force_intent_from_semantic_category(
                semantic_category,
                message_text,
                intent_type=intent_type_hint  # Pass LLM's intent_type
            )
            
            logger.warning(f"🔄 UNKNOWN blocked for mandate-relevant query, forced to {forced_intent.value}")
            
            intent = forced_intent
            confidence = 0.75
        
        # ========================================
        # LAYER 1: MODULE ACCESS CHECK (TRIAL-ONLY RESTRICTION)
        # ========================================
        
        subscription_status = client_profile.get('subscription_status', '').lower()
        
        if subscription_status == 'trial':
            # ✅ TRIAL USERS: Module restrictions apply
            
            # Map Intent to required modules (Airtable field names)
            INTENT_TO_MODULES = {
                Intent.STRATEGIC: ['Market Overview'],
                Intent.DECISION_REQUEST: ['Predictive Intelligence', 'Risk Analysis'],
                Intent.STATUS_CHECK: [],
                Intent.META_STRATEGIC: ['Risk Analysis'],
                Intent.MONITORING_DIRECTIVE: ['Portfolio Tracking'],
                Intent.ADMINISTRATIVE: [],
                Intent.META_AUTHORITY: [],
                Intent.PROFILE_STATUS: [],
                Intent.PORTFOLIO_STATUS: ['Portfolio Tracking'],
            }
            
            # Get required modules for this intent
            required_modules = INTENT_TO_MODULES.get(intent, [])
            
            if required_modules:
                # Get client's allowed modules from Airtable (via client_profile)
                allowed_modules = client_profile.get('allowed_intelligence_modules', [])
                
                # Normalize module names (handle case differences)
                allowed_modules_normalized = [m.lower().strip() for m in allowed_modules] if allowed_modules else []
                required_modules_normalized = [m.lower().strip() for m in required_modules]
                
                # Check if client has permission for ALL required modules
                missing_modules = [
                    m for m in required_modules 
                    if m.lower().strip() not in allowed_modules_normalized
                ]
                
                if missing_modules:
                    logger.warning(f"🚫 TRIAL MODULE RESTRICTED: {sender} missing {missing_modules}")
                    logger.warning(f"   Status: TRIAL (limited access)")
                    logger.warning(f"   Required: {required_modules}")
                    logger.warning(f"   Allowed: {allowed_modules}")
                    
                    return GovernanceResult(
                        intent=Intent.UNKNOWN,
                        confidence=1.0,
                        blocked=True,
                        silence_required=False,
                        response="Trial access limited to sample intelligence.\n\nUpgrade for full analyst capabilities:\nintel@voxmill.uk",
                        allowed_shapes=["REFUSAL"],
                        max_words=30,
                        analysis_allowed=False,
                        data_load_allowed=False,
                        llm_call_allowed=False,
                        auto_scoped=False,
                        semantic_category=semantic_category.value
                    )
                else:
                    logger.info(f"✅ TRIAL: Module access granted: {required_modules}")
        else:
            # ✅ PAID USERS (ACTIVE/PREMIUM/SIGMA): FULL UNRESTRICTED ACCESS
            logger.info(f"✅ FULL ACCESS GRANTED: {subscription_status.upper()} user - all world-class capabilities enabled")
            # No module restrictions - paid users get everything
        
        # ========================================
        # LAYER 2: GET ENVELOPE FOR INTENT
        # ========================================
        
        envelope = ConversationalGovernor._get_envelope(intent)
        
        # ========================================
        # CHECK FOR HARDCODED RESPONSE
        # ========================================
        
        hardcoded_response = ConversationalGovernor._get_hardcoded_response(intent, message_text, client_profile)
        
        if hardcoded_response and intent not in [Intent.STATUS_CHECK, Intent.STRATEGIC, Intent.DECISION_REQUEST, Intent.PORTFOLIO_STATUS]:
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=False,
                response=hardcoded_response,
                allowed_shapes=envelope.allowed_shapes,
                max_words=envelope.max_response_length // 5,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=True,
                semantic_category=semantic_category.value
            )
        
        # ========================================
        # CHECK IF SILENCE REQUIRED
        # ========================================
        
        if envelope.silence_required:
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=True,
                response=None,
                allowed_shapes=envelope.allowed_shapes,
                max_words=0,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category=semantic_category.value
            )
        
        # ========================================
        # CHECK IF REFUSAL REQUIRED (SHOULD NEVER REACH HERE)
        # ========================================
        
        # This is a safety check - should have been caught earlier
        if envelope.refusal_required and not is_mandate_relevant:
            logger.error(f"❌ LATE REFUSAL: This should have been caught earlier")
            
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=False,
                response="Standing by.",  # ← Changed from "Outside intelligence scope."
                allowed_shapes=envelope.allowed_shapes,
                max_words=20,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category=semantic_category.value
            )
        
        # ========================================
        # GOVERNANCE PASSED - RETURN CONSTRAINTS
        # ========================================
        
        return GovernanceResult(
            intent=intent,
            confidence=confidence,
            blocked=False,
            silence_required=False,
            response=hardcoded_response,  # May be None or static response
            allowed_shapes=envelope.allowed_shapes,
            max_words=envelope.max_response_length // 5,
            analysis_allowed=envelope.analysis_allowed,
            data_load_allowed=envelope.data_load_allowed,
            llm_call_allowed=envelope.llm_call_allowed,
            auto_scoped=True,
            semantic_category=semantic_category.value
        )

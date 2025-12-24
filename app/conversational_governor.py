"""
VOXMILL CONVERSATIONAL GOVERNOR
================================
Intent classification → Authority envelopes → Response constraints

WORLD-CLASS UPDATE:
- Layer -1: Social absorption (politeness, phatic expressions)
- Layer 0: Mandate relevance check (semantic analysis)
- Auto-scoping (market, timeframe, entities)
- Intent confidence scoring with thresholds
- Force best-fit intent for mandate-relevant queries
- NO "query unclear" for mandate-relevant queries
- Executive shorthand support (status compression, region pings, authority challenges)
"""

import logging
from enum import Enum
from typing import Optional, List, Tuple, Dict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class Intent(Enum):
    """Finite intent taxonomy (10 classes)"""
    SECURITY = "security"  # PIN, authentication
    ADMINISTRATIVE = "administrative"  # Subscription, billing
    PROVOCATION = "provocation"  # Non-semantic noise (lol, haha)
    CASUAL = "casual"  # Greetings, acknowledgments
    STATUS_CHECK = "status_check"  # Monitoring queries
    STRATEGIC = "strategic"  # Market intelligence requiring analysis
    DECISION_REQUEST = "decision_request"  # Explicit directive requests
    META_STRATEGIC = "meta_strategic"  # Blind spot analysis
    MONITORING_DIRECTIVE = "monitoring_directive"  # Setup/modify/cancel monitoring
    UNKNOWN = "unknown"  # Unclassifiable → refusal


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
    max_response_length: int  # Characters
    silence_allowed: bool
    silence_required: bool
    refusal_allowed: bool
    refusal_required: bool
    decision_mode_eligible: bool
    data_load_allowed: bool
    llm_call_allowed: bool
    allowed_shapes: List[str]  # Response shapes


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
    inferred_from: str  # "user_preference", "conversation_context", "default"


class ConversationalGovernor:
    """Main governance controller with Layer -1 social absorption"""
    
    # ========================================
    # LAYER -1: SOCIAL ABSORPTION
    # ========================================
    
    @staticmethod
    def _absorb_social_input(message: str, client_name: str = "there") -> Tuple[bool, Optional[str]]:
        """
        Layer -1: Social Absorption
        
        Returns: (is_social, response_override)
        - If is_social=True, use response_override and bypass all other layers
        - If is_social=False, continue to mandate relevance check
        """
        
        message_lower = message.lower().strip()
        # Remove trailing punctuation
        message_clean = message_lower.rstrip('?!.,;:')
        # Handle ALL apostrophe types: straight ('), curly left ('), curly right ('), backtick (`)
        message_clean = message_clean.replace("'", "").replace("'", "").replace("'", "").replace("`", "")
        # CRITICAL: Collapse whitespace (fixes "What's up" bug)
        message_clean = ' '.join(message_clean.split())
        
        # CLASS B: POLITENESS TOKENS
        politeness_exact = ['thanks', 'thank you', 'thankyou', 'thx', 'cheers', 
                            'appreciated', 'got it', 'noted', 'cool', 'ok', 'okay']
        
        if message_clean in politeness_exact:
            return True, "Standing by."
        
        # CLASS C: PHATIC EXPRESSIONS
        phatic_patterns = [
            'how are you', 'how r you', 'how are u',
            'whats up', 'what up', 'sup', 'wassup',
            'hows it going', 'you good', 'all good',
            'how you doing', 'hows things'
        ]
        
        if message_clean in phatic_patterns:
            return True, "Standing by."
        
        # CLASS D: MOOD STATEMENTS (non-market)
        # ONLY silence if it's a pure mood statement with NO market context
        pure_mood_patterns = ['feels moist', 'feels weird', 'feels odd', 'feels strange']
        
        # Check for pure mood statements
        if any(pattern in message_clean for pattern in pure_mood_patterns):
            # Check if there's a market question component
            market_question_indicators = [
                'bad sign', 'good sign', 'what does', 'why', 'should i', 
                'meaning', 'mean', 'indicate', 'signal', 'tell me'
            ]
            
            if not any(indicator in message_clean for indicator in market_question_indicators):
                # Pure mood statement with no market question → silence
                return True, None
            # Otherwise, pass through to mandate check (it's a market question)
        
        # REMOVED: 'feels noisy' from mood patterns
        # "feels noisy" is market activity language, not pure mood
        # It should be analyzed as competitive intelligence, not silenced
        
        # CLASS F: META-CONVERSATIONAL
        # These should be reframed as DECISION_REQUEST, not refused
        # Return False to pass through to normal intent classification
        meta_patterns = ['what would you do', 'if you were me', 'your thoughts',
                         'what do you think', 'your view', 'your opinion']
        
        if any(pattern in message_clean for pattern in meta_patterns):
            # Pass through but will force to DECISION_REQUEST
            return False, None
        
        # NOT SOCIAL - pass to mandate relevance
        return False, None
    
    # ========================================
    # LAYER 0: MANDATE RELEVANCE CHECK
    # ========================================
    
    @staticmethod
    def _check_mandate_relevance(message: str, conversation_context: Dict = None) -> Tuple[bool, SemanticCategory, float]:
        """
        Determine if query is within analyst mandate
        
        Returns: (is_mandate_relevant, semantic_category, confidence)
        """
        
        message_lower = message.lower().strip()
        
        # Category A: COMPETITIVE INTELLIGENCE
        competitive_keywords = [
            'competitor', 'competitors', 'agent', 'agents', 'player', 'firm', 'rival', 'entity', 
            'participant', 'knight frank', 'savills', 'hamptons', 'chestertons',
            'foxtons', 'jll', 'cbre', 'strutt', 'doing', 'activity', 'behavior',
            'positioning', 'strategy', 'moving', 'active', 'up to'
        ]
        
        if any(kw in message_lower for kw in competitive_keywords):
            return True, SemanticCategory.COMPETITIVE_INTELLIGENCE, 0.90
        
        # Category B: MARKET DYNAMICS
        market_keywords = [
            'market', 'sector', 'segment', 'region', 'area', 'territory',
            'mayfair', 'chelsea', 'knightsbridge', 'knight bridge', 'belgravia', 'kensington',
            'london', 'real estate', 'property', 'properties', 'inventory',
            'conditions', 'trends', 'movement', 'sentiment', 'outlook', 'state',
            'overview', 'snapshot', 'update', 'status', 'dynamics'
        ]
        
        if any(kw in message_lower for kw in market_keywords):
            return True, SemanticCategory.MARKET_DYNAMICS, 0.88
        
        # Category C: STRATEGIC POSITIONING (EXPANDED - CRITICAL)
        strategic_keywords = [
            'action', 'actions', 'move', 'strategy', 'opportunity', 'timing', 'window',
            'entry', 'exit', 'buy', 'sell', 'acquire', 'position', 'leverage',
            'recommend', 'advise', 'should i', 'what do i', 'next step',
            'best', 'optimal', 'strategic', 'tactical', 'directive', 'risk', 'risks',
            'if i did nothing', 'what if', 'scenario',
            # BLIND SPOT ANALYSIS (THIS IS YOUR MONEY)
            'blind spot', 'blind spots', 'what am i missing', 'what else', 'missing',
            'gaps', 'gap', 'weakness', 'weaknesses', 'downside', 'downsides',
            'pre-mortem', 'second order', 'unknown unknowns', 'not seeing',
            'overlooking', 'overlooked', 'hidden risk', 'hidden risks'
        ]
        
        if any(kw in message_lower for kw in strategic_keywords):
            return True, SemanticCategory.STRATEGIC_POSITIONING, 0.92
        
        # Category D: TEMPORAL ANALYSIS
        temporal_keywords = [
            'trend', 'change', 'movement', 'shift', 'pattern', 'trajectory',
            'forecast', 'predict', 'outlook', 'historical',
            'this week', 'this month', 'recently', 'lately', 'changed',
            'different', 'new', 'emerging', '30 days', 'next week'
        ]
        
        if any(kw in message_lower for kw in temporal_keywords):
            return True, SemanticCategory.TEMPORAL_ANALYSIS, 0.85
        
        # Category E: SURVEILLANCE & MONITORING (EXPANDED - PORTFOLIO)
        surveillance_keywords = [
            'monitor', 'monitoring', 'track', 'tracking', 'watch', 'watching',
            'alert', 'notify', 'surveillance', 'flag', 'keep eye', 'observe',
            'what am i monitoring', 'who am i monitoring', 'my monitors', 
            'active monitors', 'currently monitoring',
            # PORTFOLIO & INVESTMENT TRACKING
            'investments', 'investment', 'portfolio', 'my portfolio', 'holdings',
            'my investments', 'current investments', 'what do i own', 'my holdings',
            'my positions', 'positions', 'what am i invested in'
        ]
        
        if any(kw in message_lower for kw in surveillance_keywords):
            return True, SemanticCategory.SURVEILLANCE, 0.93
        
        # Category F: ADMINISTRATIVE (PDF/EMAIL/DELIVERY)
        administrative_keywords = [
            'pdf', 'report', 'send report', 'send pdf', 'email report', 
            'email it', 'email me', 'send it', 'send me',
            'this weeks report', 'weekly report', 'weekly brief', 'weekly briefing',
            'deliver', 'delivery', 'generate report', 'create report',
            'export', 'download', 'link', 'share'
        ]
        
        if any(kw in message_lower for kw in administrative_keywords):
            return True, SemanticCategory.ADMINISTRATIVE, 0.88
        
        # ========================================
        # Category G: EXECUTIVE SHORTHAND (NEW - CRITICAL FIX)
        # ========================================
        # These are compressed status requests and executive probes
        # NOT non-domain queries - they're how executives actually talk
        
        executive_shorthand = [
            # Status compression patterns
            'everything', 'anything', 'happening', 'brief', 'quick', 'delta',
            # Region probes (ANY city name triggers market_dynamics)
            'manchester', 'birmingham', 'leeds', 'liverpool', 'edinburgh',
            'glasgow', 'bristol', 'cardiff', 'dublin', 'show me',
            # Executive challenges (authority reaffirmation needed)
            'sure', 'matter', 'point', 'care',
            'confident', 'certain', 'proof', 'evidence',
            # Consequence framing
            'assuming', 'suppose'
        ]
        
        if any(kw in message_lower for kw in executive_shorthand):
            return True, SemanticCategory.MARKET_DYNAMICS, 0.85
        
        # ========================================
        # Category H: NON-DOMAIN (ONLY after all above checks fail)
        # ========================================
        return False, SemanticCategory.NON_DOMAIN, 0.95
    
    # ========================================
    # AUTO-SCOPING LOGIC
    # ========================================
    
    @staticmethod
    def _auto_scope(message: str, client_profile: Dict, conversation_context: Dict = None) -> AutoScopeResult:
        """
        Infer market, timeframe, and entities from context
        
        Never ask for clarification - always infer
        """
        
        message_lower = message.lower().strip()
        
        # ========================================
        # MARKET/DOMAIN INFERENCE
        # ========================================
        
        market = None
        market_source = "default"
        
        # Rule 1: Embedded entity resolution (highest priority)
        regions = ['mayfair', 'knightsbridge', 'chelsea', 'belgravia', 'kensington']
        for region in regions:
            if region in message_lower:
                market = region.title()
                market_source = "explicit_mention"
                break
        
        # Rule 2: User preference default
        if not market and client_profile:
            preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', [])
            if preferred_regions:
                market = preferred_regions[0]
                market_source = "user_preference"
        
        # Rule 3: Conversation context
        if not market and conversation_context:
            last_regions = conversation_context.get('regions', [])
            if last_regions:
                market = last_regions[-1]
                market_source = "conversation_context"
        
        # Rule 4: Default fallback
        if not market:
            market = "Mayfair"
            market_source = "default"
        
        # ========================================
        # TIMEFRAME INFERENCE
        # ========================================
        
        timeframe = None
        
        # Explicit temporal markers
        if 'this week' in message_lower or 'past week' in message_lower:
            timeframe = "7d"
        elif 'this month' in message_lower or 'past month' in message_lower:
            timeframe = "30d"
        elif 'recently' in message_lower or 'lately' in message_lower:
            timeframe = "14d"
        elif 'today' in message_lower or 'now' in message_lower:
            timeframe = "current"
        elif '30 days' in message_lower:
            timeframe = "30d"
        
        # Implicit defaults
        elif any(kw in message_lower for kw in ['trend', 'movement', 'change']):
            timeframe = "14d"  # Trends default to 14-day window
        elif any(kw in message_lower for kw in ['overview', 'snapshot', 'status']):
            timeframe = "current"  # State queries = current
        else:
            timeframe = "current+7d"  # Default: current + recent trend
        
        # ========================================
        # ENTITY INFERENCE
        # ========================================
        
        entities = []
        
        # Known agents
        agents = ['knight frank', 'savills', 'hamptons', 'chestertons', 'foxtons', 
                 'jll', 'cbre', 'strutt & parker', 'strutt and parker']
        
        for agent in agents:
            # Handle typos with fuzzy matching
            if agent.replace(' ', '') in message_lower.replace(' ', ''):
                entities.append(agent.title())
        
        # Implicit entity categories
        if 'competitor' in message_lower or 'agent' in message_lower:
            entities.append("ALL_AGENTS")  # Flag to load all agents
        
        # Conversation context entities
        if conversation_context:
            last_agents = conversation_context.get('agents', [])
            if last_agents and not entities:
                entities = [last_agents[-1]]  # Assume continuation
        
        # Calculate confidence
        confidence = 0.95 if market_source == "explicit_mention" else 0.85 if market_source == "user_preference" else 0.70
        
        return AutoScopeResult(
            market=market,
            timeframe=timeframe,
            entities=entities if entities else [],
            confidence=confidence,
            inferred_from=market_source
        )
    
    # ========================================
    # FORCE INTENT FROM SEMANTIC CATEGORY
    # ========================================
    
    @staticmethod
    def _force_intent_from_semantic_category(semantic_category: SemanticCategory, message: str) -> Intent:
        """
        Map semantic category to best-fit intent when UNKNOWN would occur
        Preserves nuance while blocking non-answers
        """
        
        message_lower = message.lower()
        
        # Action/recommendation signals
        action_signals = ['what should', 'recommend', 'advise', 'tell me what', 
                          'best action', 'next step', 'what do i', 'give me',
                          'i need', 'what would you']
        has_action_signal = any(sig in message_lower for sig in action_signals)
        
        # State/status signals  
        state_signals = ['what is', 'what are', 'current', 'now', 'status', 
                         'how is', 'where is', 'show me', 'what am i', 'who am i']
        has_state_signal = any(sig in message_lower for sig in state_signals)
        
        # Map category to intent
        if semantic_category == SemanticCategory.COMPETITIVE_INTELLIGENCE:
            return Intent.STATUS_CHECK if has_state_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.MARKET_DYNAMICS:
            return Intent.STATUS_CHECK if has_state_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.STRATEGIC_POSITIONING:
            return Intent.DECISION_REQUEST if has_action_signal else Intent.STRATEGIC
        
        elif semantic_category == SemanticCategory.TEMPORAL_ANALYSIS:
            return Intent.STRATEGIC  # Trends always strategic
        
        elif semantic_category == SemanticCategory.SURVEILLANCE:
            return Intent.STATUS_CHECK if has_state_signal else Intent.MONITORING_DIRECTIVE
        
        else:
            return Intent.STRATEGIC  # Safe default for mandate-relevant
    
    @staticmethod
    def _classify_intent(message: str) -> tuple[Intent, float]:
        """
        Intent classification with confidence scoring
        UPDATED: Added deterministic fallbacks for vague queries + executive challenges
        
        Returns: (intent, confidence_score)
        """
        
        # Aggressive normalization
        message_lower = message.lower().strip()
        message_normalized = ' '.join(message_lower.split())
        message_clean = message_normalized.replace("'", "").replace("'", "")
        word_count = len(message_clean.split())
        
        # ========================================
        # AUTHORITY CHALLENGES (NEW - HIGHEST PRIORITY)
        # ========================================
        
        # Executive challenges require confidence reaffirmation, not refusal
        authority_challenges = [
            'are you sure', 'you sure', 'certain', 'confident',
            'why does this matter', 'why should i care', 'so what',
            'whats the point', 'how do you know', 'proof'
        ]
        
        if any(pattern in message_clean for pattern in authority_challenges):
            logger.info(f"✅ Authority challenge detected → STRATEGIC (confidence reaffirmation)")
            return Intent.STRATEGIC, 0.90
        
        # ========================================
        # EXECUTIVE SHORTHAND STATUS REQUESTS (NEW)
        # ========================================
        
        # Compressed status requests (not non-domain)
        status_shorthand = [
            'give me everything', 'everything',
            'brief', 'quick update'
        ]
        
        if any(pattern in message_clean for pattern in status_shorthand):
            logger.info(f"✅ Status shorthand detected → STATUS_CHECK")
            return Intent.STATUS_CHECK, 0.92
        
        # ========================================
        # REGION PINGS (NEW)
        # ========================================
        
        # Single region mentions are exploration probes, not non-domain
        region_names = ['manchester', 'birmingham', 'leeds', 'liverpool',
                       'edinburgh', 'glasgow', 'bristol', 'cardiff']
        
        # Check if message is ONLY a region name (or "show me X")
        if message_clean in region_names or any(f'show me {r}' in message_clean for r in region_names):
            logger.info(f"✅ Region ping detected → STATUS_CHECK")
            return Intent.STATUS_CHECK, 0.88
        
        # ========================================
        # DETERMINISTIC FALLBACKS (EXISTING)
        # ========================================
        
        # Vague temporal queries → STATUS_CHECK
        vague_temporal_patterns = [
            'anything new', 'whats new', 'any news', 'any updates', 'any update',
            'anything changed', 'whats changed', 'what changed',
            'anything happening', 'whats happening', 'what happening',
            'latest', 'recent', 'recently'
        ]
        
        if any(pattern in message_clean for pattern in vague_temporal_patterns):
            logger.info(f"✅ Deterministic match: vague temporal → STATUS_CHECK")
            return Intent.STATUS_CHECK, 0.92
        
        # Vague monitoring queries → STATUS_CHECK
        vague_monitoring_patterns = [
            'what am i monitoring', 'who am i monitoring', 'my monitors',
            'active monitors', 'current monitoring', 'monitoring status'
        ]
        
        if any(pattern in message_clean for pattern in vague_monitoring_patterns):
            logger.info(f"✅ Deterministic match: monitoring status → STATUS_CHECK")
            return Intent.STATUS_CHECK, 0.94
        
        # Vague competitive queries → STRATEGIC
        vague_competitive_patterns = [
            'competitors', 'competition', 'what are they doing',
            'agent activity', 'market activity'
        ]
        
        if any(pattern in message_clean for pattern in vague_competitive_patterns):
            logger.info(f"✅ Deterministic match: vague competitive → STRATEGIC")
            return Intent.STRATEGIC, 0.88
        
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
        
        # Exact matches = very high confidence
        casual_exact = [
            'whats up', 'what up', 'sup', 'wassup', 'whatsup',
            'hi', 'hello', 'hey', 'yo', 'hiya',
            'good morning', 'good afternoon', 'good evening'
        ]
        
        if message_clean in casual_exact:
            return Intent.CASUAL, 0.95
        
        # Acknowledgments = high confidence
        acknowledgments = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool', 'right',
            'cheers'
        ]
        
        if message_clean in acknowledgments:
            return Intent.CASUAL, 0.95
        
        # Short casual queries (heuristic-based confidence)
        if word_count <= 3:
            casual_words = ['up', 'news', 'update', 'status', 'thoughts', 'view']
            if any(w in message_clean for w in casual_words):
                # But check if it's actually asking for market update
                if any(w in message_clean for w in ['market', 'agent', 'property']):
                    return Intent.STATUS_CHECK, 0.88
                return Intent.CASUAL, 0.85
        
        # ========================================
        # DECISION_REQUEST (90% threshold)
        # ========================================
        
        decision_keywords = ['decision mode', 'what should i do', 'make the call', 
                             'recommend action', 'tell me what to do', 'your recommendation',
                             'what are my best', 'give me 5', 'give me', 'i need',
                             'next steps', 'what do i', 'what would you do',
                             'if you were me', 'what if i']
        
        if any(kw in message_clean for kw in decision_keywords):
            return Intent.DECISION_REQUEST, 0.92
        
        # ========================================
        # META_STRATEGIC (88% threshold)
        # ========================================
        
        meta_keywords = ['whats missing', 'what am i missing', 'blind spot', 'blind spots',
                         'what dont i know', 'what am i not seeing', 'whats the gap']
        
        if any(kw in message_clean for kw in meta_keywords):
            return Intent.META_STRATEGIC, 0.90
        
        # ========================================
        # MONITORING_DIRECTIVE (93% threshold)
        # ========================================
        
        monitoring_keywords = ['monitor', 'watch', 'track', 'alert me', 'notify me',
                               'stop monitor', 'cancel monitor', 'show monitor']
        
        # Exclude "what am i monitoring" (handled above as STATUS_CHECK)
        if any(kw in message_clean for kw in monitoring_keywords) and 'what am i' not in message_clean:
            return Intent.MONITORING_DIRECTIVE, 0.93
        
        # ========================================
        # STATUS_CHECK (92% threshold)
        # ========================================
        
        # Already handled by deterministic fallbacks above
        # But keep explicit patterns for direct matches
        status_keywords = ['current status', 'investment status', 'portfolio status']
        
        if any(kw in message_clean for kw in status_keywords):
            return Intent.STATUS_CHECK, 0.94
        
        # ========================================
        # STRATEGIC (75% threshold - lowest for analysis)
        # ========================================
        
        strategic_keywords = ['market', 'overview', 'analysis', 'competitive', 'landscape',
                             'opportunity', 'opportunities', 'trend', 'forecast', 'outlook',
                             'price', 'inventory', 'agent', 'liquidity', 'timing',
                             'segment', 'breakdown', 'real estate', 'property', 'risk']
        
        strategic_match_count = sum(1 for kw in strategic_keywords if kw in message_clean)
        
        if strategic_match_count >= 2:
            return Intent.STRATEGIC, 0.88
        elif strategic_match_count == 1:
            return Intent.STRATEGIC, 0.85
        
        # ========================================
        # LAST RESORT: Check for single region mention
        # ========================================
        
        regions = ['mayfair', 'knightsbridge', 'chelsea', 'belgravia', 'kensington']
        
        if any(region in message_clean for region in regions):
            # Region mentioned but no clear intent → assume STATUS_CHECK
            logger.info(f"✅ Fallback: region mentioned → STATUS_CHECK")
            return Intent.STATUS_CHECK, 0.80
        
        # ========================================
        # UNKNOWN (catch-all - REFUSAL)
        # ========================================
        
        # If we get here, intent is truly unclear
        logger.warning(f"⚠️ Could not classify intent: '{message_clean[:50]}...'")
        return Intent.UNKNOWN, 0.50
    
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
    def _get_hardcoded_response(intent: Intent, message: str) -> Optional[str]:
        """
        Get hardcoded response for simple intents
        
        Intent-based responses only (no phrase matching)
        """
        
        # Intent-based responses
        intent_responses = {
            Intent.CASUAL: "Standing by.",
            Intent.UNKNOWN: "Outside intelligence scope.",
            Intent.SECURITY: "Enter your 4-digit code.",
        }
        
        return intent_responses.get(intent)
    
    # ========================================
    # MAIN GOVERNANCE ENTRY POINT
    # ========================================
    
    @staticmethod
    async def govern(message_text: str, sender: str, client_profile: dict, 
                    system_state: dict, conversation_context: Dict = None) -> GovernanceResult:
        """
        Main governance entry point with Layer -1 social absorption
        
        Returns: GovernanceResult with intent, constraints, and optional response
        """
        
        # ========================================
        # LAYER -1: SOCIAL ABSORPTION (NEW)
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
        # LAYER 0: MANDATE RELEVANCE CHECK
        # ========================================
        
        is_mandate_relevant, semantic_category, semantic_confidence = ConversationalGovernor._check_mandate_relevance(
            message_text, 
            conversation_context
        )
        
        logger.info(f"Mandate check: relevant={is_mandate_relevant}, category={semantic_category.value}, confidence={semantic_confidence:.2f}")
        
        # If NOT mandate-relevant, refuse immediately
        if not is_mandate_relevant:
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
            # Force to best-fit intent based on semantic category
            forced_intent = ConversationalGovernor._force_intent_from_semantic_category(
                semantic_category,
                message_text
            )
            
            logger.warning(f"🔄 UNKNOWN blocked for mandate-relevant query, forced to {forced_intent.value}")
            
            intent = forced_intent
            confidence = 0.75
        
        # ========================================
        # GET ENVELOPE FOR INTENT
        # ========================================
        
        envelope = ConversationalGovernor._get_envelope(intent)
        
        # ========================================
        # CHECK FOR HARDCODED RESPONSE
        # ========================================
        
        hardcoded_response = ConversationalGovernor._get_hardcoded_response(intent, message_text)
        
        if hardcoded_response and intent not in [Intent.STATUS_CHECK, Intent.STRATEGIC, Intent.DECISION_REQUEST]:
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
        # CHECK IF REFUSAL REQUIRED (should not happen for mandate-relevant)
        # ========================================
        
        if envelope.refusal_required and not is_mandate_relevant:
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=False,
                response="Outside intelligence scope.",
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
            response=None,
            allowed_shapes=envelope.allowed_shapes,
            max_words=envelope.max_response_length // 5,
            analysis_allowed=envelope.analysis_allowed,
            data_load_allowed=envelope.data_load_allowed,
            llm_call_allowed=envelope.llm_call_allowed,
            auto_scoped=True,
            semantic_category=semantic_category.value
        )

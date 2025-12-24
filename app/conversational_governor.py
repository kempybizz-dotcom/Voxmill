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
        message_clean = message_lower.rstrip('?!.,;:')
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

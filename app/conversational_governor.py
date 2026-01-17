"""
VOXMILL CONVERSATIONAL GOVERNOR - WORLD-CLASS EDITION
======================================================
LLM-based intent classification with zero keyword dependency
Surgical precision. Institutional authority. Elite social absorption.

✅ MULTI-INTENT DETECTION
✅ VARIED ACKNOWLEDGMENTS
✅ SELECTIVE NAME USAGE
✅ NO HARDCODED MARKET DEFAULTS
✅ INDUSTRY AGNOSTIC
✅ FIX 5: SHORT-CIRCUIT TIER-0 INTENTS
"""

import logging
import os
import json
import re
import random
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
    GIBBERISH = "gibberish"
    STATUS_CHECK = "status_check"
    STRATEGIC = "strategic"
    DECISION_REQUEST = "decision_request"
    META_STRATEGIC = "meta_strategic"
    MONITORING_DIRECTIVE = "monitoring_directive"
    META_AUTHORITY = "meta_authority"
    PROFILE_STATUS = "profile_status"
    VALUE_JUSTIFICATION = "value_justification"
    TRUST_AUTHORITY = "trust_authority"
    PRINCIPAL_RISK_ADVICE = "principal_risk_advice"
    STATUS_MONITORING = "status_monitoring"
    PORTFOLIO_MANAGEMENT = "portfolio_management"
    PORTFOLIO_STATUS = "portfolio_status"
    PORTFOLIO_ADD = "portfolio_add"
    DELIVERY_REQUEST = "delivery_request"
    EXECUTIVE_COMPRESSION = "executive_compression"
    UNKNOWN = "unknown"

# ============================================================
# INTENT PRIORITY TIERS - CHATGPT FIX
# ============================================================
# Tier 0: NEVER override these intents (non-negotiable routing)
# Tier 1: High priority (prefer these over generic intents)
# Tier 2: Default/fallback intents

TIER_0_NON_OVERRIDABLE = [
    Intent.SECURITY,              # PIN/auth must always work
    Intent.ADMINISTRATIVE,        # Account management critical
    Intent.TRUST_AUTHORITY,       # Confidence challenges
    Intent.PRINCIPAL_RISK_ADVICE, # First-person risk questions
    Intent.META_AUTHORITY,        # System capability questions
    Intent.EXECUTIVE_COMPRESSION, # Transform last response
    Intent.MONITORING_DIRECTIVE,  # Setup monitoring
    Intent.PORTFOLIO_MANAGEMENT,  # Portfolio actions
    Intent.PORTFOLIO_STATUS,      # Portfolio viewing
    Intent.PROFILE_STATUS,        # Identity questions
    Intent.VALUE_JUSTIFICATION,   # Value prop questions
    Intent.STATUS_MONITORING,     # Check monitors
    Intent.DELIVERY_REQUEST,      # Report delivery
]

TIER_1_HIGH_PRIORITY = [
    Intent.DECISION_REQUEST,      # "Should I act?"
    Intent.STRATEGIC,             # Strategic analysis
]

TIER_2_DEFAULT = [
    Intent.STATUS_CHECK,          # Quick status
    Intent.UNKNOWN,               # Fallback
]


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
    # INTENT CLASSIFICATION (MINIMAL) - WITH FIX 5
    # ========================================
    
    @staticmethod
    def _force_intent_from_semantic_category(semantic_category: SemanticCategory, message: str, intent_type: str = None) -> Intent:
        """
        Map semantic category + intent_type to best-fit intent
        Preserves nuance while blocking non-answers
        
        ✅ FIX 5: SHORT-CIRCUIT - Tier 0 intents return immediately, never run secondary classifiers
        """
        
        # ========================================
        # ✅ FIX 5: TIER 0 SHORT-CIRCUIT - CRITICAL
        # ========================================
        tier_0_intent_types = [
            'trust_authority', 'principal_risk_advice', 'meta_authority', 'executive_compression',
            'profile_status', 'portfolio_status', 'portfolio_management',
            'value_justification', 'status_monitoring', 'delivery_request'
        ]
        
        if intent_type in tier_0_intent_types:
            logger.info(f"🎯 TIER 0 intent detected: {intent_type} - SHORT-CIRCUITING (no secondary classification)")
            
            # ✅ Route meta_authority to trust_authority for pressure tests
            if intent_type == 'meta_authority':
                message_lower = message.lower()
                
                gap_indicators = [
                    'what am i missing', 'what breaks', 'what if wrong',
                    "what's missing", 'blind spot', 'not seeing',
                    'overlooking', 'what else should',
                    'what am i missing this week',
                    'what am i missing today',
                    'what have i missed',
                    'whats the blind spot',
                    'anything i should know'
                ]
                
                capability_indicators = [
                    'what is voxmill', 'what can you do', 'what do you do',
                    'tell me about', 'capabilities', 'features', 'what are you'
                ]
                
                is_gap_question = any(phrase in message_lower for phrase in gap_indicators)
                is_capability_question = any(phrase in message_lower for phrase in capability_indicators)
                
                if is_gap_question:
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority → trust authority (strategic gap)")
                    return Intent.TRUST_AUTHORITY
                elif is_capability_question:
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority → capability question")
                    return Intent.META_AUTHORITY
                else:
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority → trust authority (default)")
                    return Intent.TRUST_AUTHORITY
            
            # ✅ FIX 5: IMMEDIATE RETURN - Never reach "unknown" classification below
            intent_map = {
                'trust_authority': Intent.TRUST_AUTHORITY,
                'meta_authority': Intent.META_AUTHORITY,
                'executive_compression': Intent.EXECUTIVE_COMPRESSION,
                'profile_status': Intent.PROFILE_STATUS,
                'portfolio_status': Intent.PORTFOLIO_STATUS,
                'portfolio_management': Intent.PORTFOLIO_MANAGEMENT,
                'value_justification': Intent.VALUE_JUSTIFICATION,
                'status_monitoring': Intent.STATUS_MONITORING,
                'delivery_request': Intent.DELIVERY_REQUEST,
            }
            
            # ✅ TERMINAL RETURN - No further processing
            return intent_map.get(intent_type, Intent.STRATEGIC)
        
        # ========================================
        # ✅ CHATGPT FIX: PRINCIPAL RISK ADVICE ROUTING
        # ========================================
        if intent_type == 'principal_risk_advice':
            logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Principal risk advice detected")
            return Intent.PRINCIPAL_RISK_ADVICE
        
        # ========================================
        # TIER 1/2: Semantic category mapping for market queries       
        
        # PRIORITY: Use LLM's intent_type if provided (for non-Tier-0)
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
    def _is_protected_intent(intent: Intent) -> bool:
        """
        Check if intent is Tier 0 (non-overridable)
        
        These intents NEVER get downgraded to ACK/SILENCE
        """
        return intent in TIER_0_NON_OVERRIDABLE
    
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
                analysis_allowed=True,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,  # ✅ LLM generates blind spot analysis
                allowed_shapes=["STRUCTURED_BRIEF", "STATUS_LINE"]
            ),

            Intent.PRINCIPAL_RISK_ADVICE: Envelope(  # ✅ CHATGPT FIX
                analysis_allowed=True,
                max_response_length=300,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,  # CRITICAL: NO REFUSAL - MUST ANSWER
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,  # ✅ LLM generates principal risk view
                allowed_shapes=["STRUCTURED_BRIEF"]
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

            Intent.EXECUTIVE_COMPRESSION: Envelope(
                analysis_allowed=True,
                max_response_length=300,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=True,  # ✅ LLM transforms last response
                allowed_shapes=["STRUCTURED_BRIEF", "STATUS_LINE"]
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
        ✅ NO MENU LANGUAGE - ends with insight or "Standing by."
        ✅ NO PORTFOLIO RESPONSES - let handlers execute
        ✅ NEVER USE PHONE NUMBERS AS NAMES
        Intent-based responses only (no phrase matching)
        """
        
        client_name = client_profile.get('name', 'there') if client_profile else 'there'
        
        # ✅ CHATGPT FIX: Filter out phone numbers from names
        if client_name and (client_name.startswith('+') or client_name.startswith('whatsapp:') or client_name.isdigit()):
            client_name = 'there'
        
        # META_AUTHORITY responses
        if intent == Intent.META_AUTHORITY:
            # ✅ CHATGPT FIX: If client is authenticated, NEVER self-describe
            if client_profile and client_profile.get('agency_name'):
                # Client is authenticated - respond as their analyst
                agency_name = client_profile.get('agency_name', 'your organization')
                active_market = client_profile.get('active_market', 'your market')
                
                return f"""We analyze {active_market} market dynamics for {agency_name}.

Current focus: competitive positioning, pricing trends, instruction flow."""
            else:
                # No client context - generic capability response
                return """I provide real-time market intelligence across industries.

Analysis includes inventory levels, pricing trends, competitive dynamics, and strategic positioning."""
        # PROFILE_STATUS responses
        if intent == Intent.PROFILE_STATUS:
            if client_profile:
                name = client_profile.get('name', 'there')
                tier = client_profile.get('tier', 'tier_1')
                tier_display = {'tier_1': 'Basic', 'tier_2': 'Premium', 'tier_3': 'Enterprise'}.get(tier, 'institutional')
                
                return f"""CLIENT PROFILE

Name: {name}
Service Tier: {tier_display}

Standing by."""
            else:
                return "Client profile loading..."
        
        # VALUE_JUSTIFICATION responses
        if intent == Intent.VALUE_JUSTIFICATION:
            return """Voxmill delivers institutional-grade market intelligence via WhatsApp.

Real-time data. Fortune-500 presentation quality. Surgical precision."""
        
        # TRUST_AUTHORITY responses
        if intent == Intent.TRUST_AUTHORITY:
            return None  # Handled by LLM
        
        # ✅ CHATGPT FIX: PRINCIPAL RISK ADVICE - NEVER HARDCODED
        if intent == Intent.PRINCIPAL_RISK_ADVICE:
            return None  # ✅ MUST route to LLM - NEVER self-describe
        
        # ✅ CRITICAL FIX: PORTFOLIO HANDLERS MUST EXECUTE, NOT GENERATE RESPONSES HERE
        if intent == Intent.PORTFOLIO_MANAGEMENT:
            return None  # ✅ Let whatsapp.py handler execute
        
        if intent == Intent.PORTFOLIO_STATUS:
            return None  # ✅ Let whatsapp.py handler execute
        
        # DELIVERY_REQUEST responses
        if intent == Intent.DELIVERY_REQUEST:
            return """PDF reports generate Sunday at 6:00 AM UTC.

For immediate regeneration, contact intel@voxmill.uk"""
        
        # ✅ VARIED ACKNOWLEDGMENTS
        if intent == Intent.CASUAL:
            # 30% chance: use name if available
            if client_name != "there" and random.random() < 0.3:
                options = [
                    f"Standing by, {client_name}.",
                    f"Ready, {client_name}."
                ]
            else:
                # 70% chance: no name (variety pool)
                options = [
                    "Standing by.",
                    "Ready.",
                    "Standing by.",  # Weighted for higher probability
                ]
            
            return random.choice(options)
        
        # Intent-based responses (non-casual)
        intent_responses = {
            Intent.UNKNOWN: "Outside intelligence scope.",
            Intent.SECURITY: "Enter your 4-digit code.",
        }
        
        return intent_responses.get(intent)
    
    @staticmethod
    def _absorb_social_input(message_text: str, client_name: str, conversation_context: Dict = None) -> Tuple[bool, Optional[str]]:
        """
        Layer -1: Absorb pure social pleasantries without analysis
        
        Returns: (is_social, response_text_or_none)
        - (True, "Standing by.") = social input, send brief ack
        - (True, None) = social input, silence
        - (False, None) = not social, continue to full governance
        
        ✅ VARIED ACKNOWLEDGMENTS
        ✅ SELECTIVE NAME USAGE (30% chance)
        ✅ NEVER USE PHONE NUMBERS AS NAMES
        """
        
        # ✅ CHATGPT FIX: Filter out phone numbers from names
        if client_name and (client_name.startswith('+') or client_name.startswith('whatsapp:') or client_name.isdigit()):
            client_name = 'there'
        
        message_lower = message_text.lower().strip()
        message_clean = ' '.join(message_lower.split())
        
        # ========================================
        # PURE GREETINGS (no query component)
        # ========================================
        
        pure_greetings = [
            'hi', 'hello', 'hey', 'yo', 'hiya',
            'good morning', 'good afternoon', 'good evening',
            'morning', 'afternoon', 'evening',
            'whats up', 'what up', 'sup', 'wassup', 'whatsup'
        ]
        
        if message_clean in pure_greetings:
            # 30% chance: use name if available
            if client_name != "there" and random.random() < 0.3:
                options = [
                    f"Standing by, {client_name}.",
                    f"Ready, {client_name}."
                ]
            else:
                # 70% chance: no name
                options = [
                    "Standing by.",
                    "Ready.",
                    "Standing by.",  # Weighted
                ]
            
            return True, random.choice(options)
        
        # ========================================
        # PURE ACKNOWLEDGMENTS (no query)
        # ========================================
        
        pure_acks = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool', 'right',
            'cheers', 'appreciate it', 'appreciated'
        ]
        
        if message_clean in pure_acks:
            # Brief acknowledgment
            options = [
                "Standing by.",
                "Ready.",
                "Standing by.",  # Weighted
            ]
            
            return True, random.choice(options)
        
        # ========================================
        # COMPOUND PATTERNS (greeting + question)
        # ========================================
        
        # Example: "Hey, what's the market doing?"
        # These should NOT be absorbed - they have actual queries
        
        compound_patterns = [
            r'^(hi|hello|hey|yo|hiya)[,\s]+(what|how|when|where|who|why|can|could|would|show|tell|give)',
            r'^(good morning|good afternoon|good evening)[,\s]+(what|how|when|where|who|why|can|could|would|show|tell|give)',
        ]
        
        for pattern in compound_patterns:
            if re.match(pattern, message_lower):
                # Has query component - don't absorb
                return False, None
        
        # ========================================
        # LAUGHTER / AMUSEMENT (silence)
        # ========================================
        
        laughter = ['lol', 'haha', 'lmao', 'hehe', 'lmfao', 'rofl']
        
        if message_clean in laughter:
            # Silence (no response)
            return True, None
        
        # ========================================
        # NOT SOCIAL - CONTINUE TO FULL GOVERNANCE
        # ========================================
        
        return False, None
    
    @staticmethod
    async def _check_mandate_relevance(message: str, conversation_context: Dict = None) -> Tuple[bool, SemanticCategory, float]:
        """
        LLM-based mandate relevance check
        
        Returns: (is_relevant, semantic_category, confidence)
        
        Also sets ConversationalGovernor._last_intent_type for downstream routing
        """
        
        client = AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Build context string
        context_str = ""
        if conversation_context:
            regions = conversation_context.get('regions', [])
            agents = conversation_context.get('agents', [])
            topics = conversation_context.get('topics', [])
            
            if regions or agents or topics:
                context_str = f"\n\nConversation context:\n"
                if regions:
                    context_str += f"- Recent regions: {', '.join(regions)}\n"
                if agents:
                    context_str += f"- Recent agents: {', '.join(agents)}\n"
                if topics:
                    context_str += f"- Recent topics: {', '.join(topics)}\n"
        
        prompt = f"""Classify this message for a market intelligence system.

Message: "{message}"{context_str}

Respond ONLY with valid JSON:
{{
    "is_mandate_relevant": true/false,
    "semantic_category": "competitive_intelligence" | "market_dynamics" | "strategic_positioning" | "temporal_analysis" | "surveillance" | "administrative" | "social" | "non_domain",
    "confidence": 0.0-1.0,
    "intent_type": "market_query" | "follow_up" | "preference_change" | "meta_authority" | "profile_status" | "identity_query" | "plain_english_definition" | "portfolio_status" | "portfolio_management" | "value_justification" | "trust_authority" | "principal_risk_advice" | "status_monitoring" | "delivery_request" | "gibberish" | "profanity"
}}

Guidelines:
- is_mandate_relevant: true if asking about markets, competition, pricing, agents, properties, strategy, timing, OR meta-strategic questions
- identity_query: "Who am I?", "What market do I operate in?", "Tell me about my agency"
- plain_english_definition: "explain like I'm explaining to a client", "define it simply", "in plain English"
- principal_risk_advice: "If you were in my seat/position", "what would worry you", "what would concern you", "if you were me", "your biggest fear" (ALWAYS relevant=true)
- META-STRATEGIC EXAMPLES (ALWAYS relevant=true, intent_type="trust_authority"):
  * "What am I missing?"
  * "What's the blind spot?"
  * "What should I know?"
  * "What am I not seeing?"
  * "What breaks this analysis?"
- semantic_category: best fit category
- confidence: 0.0-1.0 based on clarity
- intent_type: specific intent for routing"""
        
        try:
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You classify market intelligence queries. Respond only with valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=150,
                temperature=0.0,
                timeout=5.0
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Strip markdown fences if present
            if result_text.startswith('```'):
                result_text = result_text.split('```')[1]
                if result_text.startswith('json'):
                    result_text = result_text[4:]
                result_text = result_text.strip()
            
            result = json.loads(result_text)
            
            is_relevant = result.get('is_mandate_relevant', False)
            category_str = result.get('semantic_category', 'non_domain')
            confidence = result.get('confidence', 0.5)
            intent_type = result.get('intent_type')
            
            # Store intent_type for downstream routing
            ConversationalGovernor._last_intent_type = intent_type
            
            # Map string to enum
            category_map = {
                'competitive_intelligence': SemanticCategory.COMPETITIVE_INTELLIGENCE,
                'market_dynamics': SemanticCategory.MARKET_DYNAMICS,
                'strategic_positioning': SemanticCategory.STRATEGIC_POSITIONING,
                'temporal_analysis': SemanticCategory.TEMPORAL_ANALYSIS,
                'surveillance': SemanticCategory.SURVEILLANCE,
                'administrative': SemanticCategory.ADMINISTRATIVE,
                'social': SemanticCategory.SOCIAL,
                'non_domain': SemanticCategory.NON_DOMAIN
            }
            
            semantic_category = category_map.get(category_str, SemanticCategory.NON_DOMAIN)
            
            logger.info(f"LLM mandate check: relevant={is_relevant}, category={category_str}, intent={intent_type}, confidence={confidence:.2f}")
            
            return is_relevant, semantic_category, confidence
            
        except Exception as e:
            logger.error(f"Mandate relevance check failed: {e}")
            # Fallback to conservative
            return False, SemanticCategory.NON_DOMAIN, 0.5
    
    @staticmethod
    def _auto_scope(message: str, client_profile: dict, conversation_context: Dict = None) -> AutoScopeResult:
        """
        Auto-scope market/timeframe/entities from message
        
        Returns: AutoScopeResult with extracted context
        """
        
        # Extract market from message or use preferred
        market = None
        timeframe = None
        entities = []
        
        # Simple extraction (you can enhance with LLM)
        message_lower = message.lower()
        
        # Extract timeframe
        if 'this week' in message_lower or 'past week' in message_lower:
            timeframe = '7d'
        elif 'this month' in message_lower or 'past month' in message_lower:
            timeframe = '30d'
        elif 'this year' in message_lower:
            timeframe = '365d'
        
        # Extract market from preferred regions
        preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', [])
        if preferred_regions:
            market = preferred_regions[0]
        
        # Check conversation context
        inferred_from = "preferences"
        if conversation_context:
            context_regions = conversation_context.get('regions', [])
            if context_regions:
                market = context_regions[-1]
                inferred_from = "conversation_context"
        
        return AutoScopeResult(
            market=market,
            timeframe=timeframe,
            entities=entities,
            confidence=0.8 if market else 0.3,
            inferred_from=inferred_from
        )
    
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
            client_name,
            conversation_context
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
        # ✅ CHATGPT FIX: IMPLICIT IDENTITY RESOLVER (PRIORITY 0)
        # ========================================
        
        # Detect implicit identity questions BEFORE intent classification
        message_lower = message_text.lower().strip()  # ✅ Define message_lower first
        
        implicit_identity_patterns = [
            'what do we do', 'what you think we do', 'remind me what we do',
            'what are we', 'who are we', 'what\'s our',
            'what are you looking at', 'what do you look at',
            'what are you actually', 'how do you know'
        ]
        
        if any(pattern in message_lower for pattern in implicit_identity_patterns):
            logger.info(f"🎯 IMPLICIT IDENTITY QUESTION: '{message_text}'")
            
            # Force to identity_query intent
            intent_type_hint = 'identity_query'
            is_mandate_relevant = True
            semantic_category = SemanticCategory.ADMINISTRATIVE
            semantic_confidence = 0.95

        # ========================================
        # ✅ CHATGPT FIX: HUMAN SIGNAL OVERRIDE (PRIORITY 0.5)
        # ========================================
        
        # Detect emotional/intuitive signals that require human mode
        human_signal_patterns = [
            'feels off', 'feel off', 'something feels',
            'can\'t put my finger', 'cant put my finger',
            'something\'s not right', 'something not right',
            'doesn\'t feel right', 'doesnt feel right',
            'sounds tidy', 'sounds neat', 'bit tidy', 'bit neat',
            'say that again', 'rephrase that', 'like you\'re sitting',
            'don\'t give me numbers', 'dont give me numbers',
            'no numbers', 'skip the numbers', 'forget the numbers',
            'be straight', 'just be straight', 'straight with me',
            'you sure', 'are you sure', 'certain about that'
        ]
        
        is_human_signal = any(pattern in message_lower for pattern in human_signal_patterns)
        
        if is_human_signal:
            logger.info(f"🎯 HUMAN SIGNAL DETECTED: '{message_text[:50]}'")
            
            # Force to human mode - route as strategic with special flag
            intent_type_hint = 'human_mode_strategic'
            is_mandate_relevant = True
            semantic_category = SemanticCategory.STRATEGIC_POSITIONING
            semantic_confidence = 0.95
            
            # Set conversation context flag for LLM
            if conversation_context is None:
                conversation_context = {}
            conversation_context['human_mode_active'] = True
        
        # ========================================
        # CRITICAL FIX: SPECIAL INTENT OVERRIDE (BEFORE REFUSAL)
        # ========================================
        
        # ✅ CHATGPT FIX: Explicit meta-strategic pattern matching (before LLM check)
        message_lower = message_text.lower().strip()
        explicit_meta_patterns = [
            'what am i missing', 'what\'s missing', 'whats missing',
            'blind spot', 'blind spots', 'what am i not seeing',
            'what should i know', 'what don\'t i know'
        ]
        
        if any(pattern in message_lower for pattern in explicit_meta_patterns):
            logger.info(f"🎯 EXPLICIT META-STRATEGIC OVERRIDE: '{message_text}'")
            # Force to trust_authority intent immediately
            intent_type_hint = 'trust_authority'
            is_mandate_relevant = True
            semantic_category = SemanticCategory.STRATEGIC_POSITIONING
            semantic_confidence = 0.95

        principal_risk_patterns = [
            'if you were in my seat', 'if you were in my position',
            'if you were me', 'if you were sitting where i am',
            'what would worry you', 'what would concern you',
            'what would make you worried', 'your biggest fear',
            'what keeps you up', 'what scares you most',
            'be honest what worries', 'honestly what worries'
        ]
        
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
        # INTENT PRIORITY TIER CHECK (CHATGPT FIX)
        # ========================================
        
        # Step 1: Check if LLM detected a Tier 0 intent via intent_type_hint
        tier_0_detected = False
        if intent_type_hint:
            # Map intent_type_hint to Intent enum
            intent_type_map = {
                'trust_authority': Intent.TRUST_AUTHORITY,
                'meta_authority': Intent.META_AUTHORITY,
                'executive_compression': Intent.EXECUTIVE_COMPRESSION,
                'profile_status': Intent.PROFILE_STATUS,
                'portfolio_status': Intent.PORTFOLIO_STATUS,
                'portfolio_management': Intent.PORTFOLIO_MANAGEMENT,
                'value_justification': Intent.VALUE_JUSTIFICATION,
                'status_monitoring': Intent.STATUS_MONITORING,
                'delivery_request': Intent.DELIVERY_REQUEST,
            }
            
            detected_intent = intent_type_map.get(intent_type_hint)
            
            if detected_intent and detected_intent in TIER_0_NON_OVERRIDABLE:
                logger.info(f"🎯 TIER 0 intent detected: {detected_intent.value} - NEVER OVERRIDE")
                intent = detected_intent
                confidence = 0.95
                tier_0_detected = True
        
        # Step 2: Only force intent if NOT a Tier 0 intent
        if not tier_0_detected and is_mandate_relevant and intent == Intent.UNKNOWN:
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
        # CRITICAL: BLOCK ACK FOR NON-CASUAL INTENTS
        # ========================================
        
        if intent not in [Intent.CASUAL, Intent.PROVOCATION] and hardcoded_response:
            if hardcoded_response in ["Standing by.", "Noted.", "Monitoring."]:
                # Force proper handler execution instead
                logger.warning(f"⚠️ Blocked ACK fallback for {intent.value} - forcing handler execution")
                hardcoded_response = None
        
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

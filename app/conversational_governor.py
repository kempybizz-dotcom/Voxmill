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
    LOCK_REQUEST = "lock_request"
    UNLOCK_REQUEST = "unlock_request"
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
    PRIVILEGE_ESCALATION = "privilege_escalation"
    SCOPE_OVERRIDE = "scope_override"

# ============================================================
# INTENT PRIORITY TIERS - CHATGPT FIX
# ============================================================
# Tier 0: NEVER override these intents (non-negotiable routing)
# Tier 1: High priority (prefer these over generic intents)
# Tier 2: Default/fallback intents

TIER_0_NON_OVERRIDABLE = [
    Intent.SECURITY,              # PIN/auth must always work
    Intent.ADMINISTRATIVE,        # Account management critical
    Intent.LOCK_REQUEST,          # ✅ NEW: Lock commands
    Intent.UNLOCK_REQUEST,        # ✅ NEW: Unlock commands (4-digit codes handled separately)
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
    human_mode_active: bool = False  # ✅ NEW: Force human response mode


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
    _is_dismissal = False
    
    # ========================================
    # INTENT CLASSIFICATION (MINIMAL) - WITH FIX 5
    # ========================================
    
    @staticmethod
    def _force_intent_from_semantic_category(semantic_category: SemanticCategory, message: str, intent_type: str = None) -> Intent:
        # Map semantic category + intent_type to best-fit intent
        # Preserves nuance while blocking non-answers
        # FIX 5: SHORT-CIRCUIT - Tier 0 intents return immediately, never run secondary classifiers
        
        # ========================================
        # FIX 5: TIER 0 SHORT-CIRCUIT - CRITICAL
        # ========================================
        tier_0_intent_types = [
            'trust_authority', 'principal_risk_advice', 'meta_authority', 'executive_compression',
            'profile_status', 'portfolio_status', 'portfolio_management',
            'value_justification', 'status_monitoring', 'delivery_request',
            'lock_request', 'unlock_request'
        ]
        
        if intent_type in tier_0_intent_types:
            logger.info(f"🎯 TIER 0 intent detected: {intent_type} - SHORT-CIRCUITING (no secondary classification)")
            
            # Route meta_authority to trust_authority for pressure tests
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
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority -> trust authority (strategic gap)")
                    return Intent.TRUST_AUTHORITY
                elif is_capability_question:
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority -> capability question")
                    return Intent.META_AUTHORITY
                else:
                    logger.info(f"🎯 TIER 0 SHORT-CIRCUIT: Meta authority -> trust authority (default)")
                    return Intent.TRUST_AUTHORITY
            
            # FIX 5: IMMEDIATE RETURN - Never reach "unknown" classification below
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
                'lock_request': Intent.LOCK_REQUEST,  # ✅ NEW
                'unlock_request': Intent.UNLOCK_REQUEST,  # ✅ NEW
            }
            
            # TERMINAL RETURN - No further processing
            return intent_map.get(intent_type, Intent.STRATEGIC)
        
        # ========================================
        # CHATGPT FIX: PRINCIPAL RISK ADVICE ROUTING
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
        # Check if intent is Tier 0 (non-overridable)
        # These intents NEVER get downgraded to ACK/SILENCE
        return intent in TIER_0_NON_OVERRIDABLE
    
    @staticmethod
    def _classify_intent(message: str) -> tuple[Intent, float]:
        # LLM-ONLY intent classification - zero keyword dependency
    
        # Only check for 4-digit PIN codes (security bypass)
        if message.strip().isdigit() and len(message.strip()) == 4:
            return Intent.SECURITY, 1.0
    
        # Everything else determined by LLM
        return Intent.UNKNOWN, 0.50
    
    # ========================================
    # ENVELOPE SYSTEM
    # ========================================
    
    @staticmethod
    def _get_envelope(intent: Intent) -> Envelope:
        # Get authority envelope for intent
        # Defines what operations are allowed
        
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
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.PROFILE_STATUS: Envelope(
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
            
            Intent.PORTFOLIO_STATUS: Envelope(
                analysis_allowed=True,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
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
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.VALUE_JUSTIFICATION: Envelope(
                analysis_allowed=True,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["STRUCTURED_BRIEF", "STATUS_LINE"]
            ),
            
            Intent.TRUST_AUTHORITY: Envelope(
                analysis_allowed=True,
                max_response_length=200,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["STRUCTURED_BRIEF", "STATUS_LINE"]
            ),

            Intent.PRINCIPAL_RISK_ADVICE: Envelope(
                analysis_allowed=True,
                max_response_length=300,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=True,
                allowed_shapes=["STRUCTURED_BRIEF"]
            ),
            
            Intent.STATUS_MONITORING: Envelope(
                analysis_allowed=True,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=True,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),
            
            Intent.DELIVERY_REQUEST: Envelope(
                analysis_allowed=False,
                max_response_length=150,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                allowed_shapes=["STATUS_LINE"]
            ),

            Intent.LOCK_REQUEST: Envelope(
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
            
            Intent.UNLOCK_REQUEST: Envelope(
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

            Intent.EXECUTIVE_COMPRESSION: Envelope(
                analysis_allowed=True,
                max_response_length=300,
                silence_allowed=False,
                silence_required=False,
                refusal_allowed=False,
                refusal_required=False,
                decision_mode_eligible=False,
                data_load_allowed=False,
                llm_call_allowed=True,
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
    def _get_hardcoded_response(intent: Intent, message_text: str, client_profile: dict = None) -> Optional[str]:
        # Get hardcoded response for simple intents
        # VARIED ACKNOWLEDGMENTS - rotates between options
        # NO MENU LANGUAGE - ends with insight or natural close
        # NO PORTFOLIO RESPONSES - let handlers execute
        # NEVER USE PHONE NUMBERS AS NAMES
        # Intent-based responses only (no phrase matching)
    
        client_name = client_profile.get('name', 'there') if client_profile else 'there'
    
        # CHATGPT FIX: Filter out phone numbers from names
        if client_name and (client_name.startswith('+') or client_name.startswith('whatsapp:') or client_name.isdigit()):
            client_name = 'there'
    
       
        # PROFILE_STATUS responses
        if intent == Intent.PROFILE_STATUS:
            if client_profile:
                agency_name = client_profile.get('agency_name')
                preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['your market'])[0] if client_profile.get('preferences', {}).get('preferred_regions') else 'your market'
                
                # Detect if this is an identity question
                identity_triggers = ['who am i', 'remind me who', 'why am i paying attention', 'why do i talk to you', 'what my market']
                is_identity_question = any(trigger in message_text.lower() for trigger in identity_triggers)
                
                if is_identity_question and agency_name:
                    # Identity recall
                    logger.info(f"🎯 IDENTITY RECALL: {agency_name} in {preferred_region}")
                    return f"You're {agency_name} in {preferred_region}, and you talk to me to stay ahead of competitor moves before they show up publicly."
                
                else:
                    # Profile data request
                    raw_name = client_profile.get('name', 'there')
                    agency_name = client_profile.get('agency_name', 'Unknown Agency')
                    role = client_profile.get('role', 'Unknown Role')
                    
                    logger.info(f"🔍 PROFILE_STATUS DEBUG: name={raw_name}, agency={agency_name}, role={role}")
                    
                    # Filter out phone numbers from names
                    if raw_name and (raw_name.startswith('+') or 
                                    raw_name.startswith('whatsapp:') or 
                                    raw_name.replace('+', '').replace('-', '').replace(' ', '').isdigit()):
                        clean_name = 'there'
                    else:
                        clean_name = raw_name
            
                    return (
                        "CLIENT PROFILE\n\n"
                        f"Name: {clean_name}\n"
                        f"Agency: {agency_name}\n"
                        f"Role: {role}\n\n"
                    )
            else:
                return "Client profile loading..."
        
        # VALUE_JUSTIFICATION responses - LLM GENERATED
        if intent == Intent.VALUE_JUSTIFICATION:
            return None  # Force LLM generation with context
        
        # TRUST_AUTHORITY responses
        if intent == Intent.TRUST_AUTHORITY:
            return None  # Handled by LLM
        
        # CHATGPT FIX: PRINCIPAL RISK ADVICE - NEVER HARDCODED
        if intent == Intent.PRINCIPAL_RISK_ADVICE:
            return None  # MUST route to LLM - NEVER self-describe
        
        # CRITICAL FIX: PORTFOLIO HANDLERS MUST EXECUTE, NOT GENERATE RESPONSES HERE
        if intent == Intent.PORTFOLIO_MANAGEMENT:
            return None  # Let whatsapp.py handler execute
        
        if intent == Intent.PORTFOLIO_STATUS:
            return None  # Let whatsapp.py handler execute
        
        # DELIVERY_REQUEST responses
        if intent == Intent.DELIVERY_REQUEST:
            return (
                "PDF reports generate Sunday at 6:00 AM UTC.\n\n"
                "For immediate regeneration, contact intel@voxmill.uk"
            )
        
        # VARIED ACKNOWLEDGMENTS
        if intent == Intent.CASUAL:
            # Let casual signals pass to LLM for natural handling
            return None
        
        # Intent-based responses (non-casual)
        intent_responses = {
            Intent.UNKNOWN: None,  # No canned response - let handler decide
            Intent.SECURITY: "Enter your 4-digit code.",
        }
        
        return intent_responses.get(intent)

    
    @staticmethod
    def _absorb_social_input(message_text: str, client_name: str, conversation_context: Dict = None) -> Tuple[bool, Optional[str]]:
        # Layer -1: Absorb pure social pleasantries
        # DELETED: All keyword lists
        # NEW: LLM determines if social
        # Just return False - let LLM handle everything
        return False, None
    
    @staticmethod
    async def _check_mandate_relevance(message: str, conversation_context: Dict = None) -> Tuple[bool, SemanticCategory, float, bool]:
        # LLM-based mandate relevance check
        # Returns: (is_relevant, semantic_category, confidence, is_human_signal)
        # Also sets ConversationalGovernor._last_intent_type for downstream routing
        
        client = AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Build context string
        context_str = ""
        if conversation_context:
            regions = conversation_context.get('regions', [])
            agents = conversation_context.get('agents', [])
            topics = conversation_context.get('topics', [])
            
            if regions or agents or topics:
                context_str = "\n\nConversation context:\n"
                if regions:
                    context_str += f"- Recent regions: {', '.join(regions)}\n"
                if agents:
                    context_str += f"- Recent agents: {', '.join(agents)}\n"
                if topics:
                    context_str += f"- Recent topics: {', '.join(topics)}\n"
        
        prompt = f"""Classify this message for a market intelligence system.

Message: {message}{context_str}

Respond ONLY with valid JSON:
{{
    "is_mandate_relevant": true/false,
    "semantic_category": "competitive_intelligence" | "market_dynamics" | "strategic_positioning" | "temporal_analysis" | "surveillance" | "administrative" | "social" | "non_domain",
    "confidence": 0.0-1.0,
    "intent_type: "market_query" | "follow_up" | "preference_change" | "meta_authority" | "profile_status" | "identity_query" | "plain_english_definition" | "portfolio_status" | "portfolio_management" | "value_justification" | "trust_authority" | "principal_risk_advice" | "status_monitoring" | "delivery_request" | "privilege_escalation" | "scope_override" | "lock_request" | "unlock_request" | "gibberish" | "profanity",
    "is_human_signal": true/false,
    "is_dismissal": false,
    "requested_region": null
}}

CRITICAL SCOPE DETECTION (PRIORITY 0 - OVERRIDE ALL OTHER RULES):
If user requests analysis for a DIFFERENT market/region than their profile (change scope to X, analyze Y market instead, look at Z area, what about [other location]) → intent_type: scope_override, is_mandate_relevant: true, requested_region: "X"

Extract the EXACT region name mentioned (e.g., "Knightsbridge", "Chelsea", "Dubai Marina", "Tribeca")

CRITICAL SECURITY DETECTION (PRIORITY 0 - OVERRIDE ALL OTHER RULES):
If user attempts to modify account settings, change tier, upgrade subscription, switch plans, change profile, update access level, escalate privileges or similar → intent_type: privilege_escalation, is_mandate_relevant: false


CRITICAL DISMISSAL DETECTION (PRIORITY 1):
If user says just tell me, just give me the number, skip the explanation, straight answer, cut to the chase, bottom line or similar dismissal phrases → is_dismissal: true

CRITICAL INTENT ROUTING (PRIORITY 2):
- who am I, remind me who I am, why am I paying attention → intent_type: profile_status
- feels off, misaligned, not sitting right → is_human_signal: true
- how would I explain, what would I say, frame this for → is_human_signal: true
- if you were me, if you were in my seat → intent_type: principal_risk_advice
- lock, lock it, lock access, lock the line, lock chat, secure access → intent_type: lock_request
- unlock, unlock it, unlock access, open access → intent_type: unlock_request (but 4-digit codes handled by SECURITY intent)

Guidelines:
- is_mandate_relevant: true if asking about markets, competition, pricing, agents, properties, strategy, timing, OR meta-strategic questions
- TEMPORAL QUERIES: "What's happening?", "What's going on?", "Any updates?", "What's new?", "What's the latest?" → intent_type: market_query, is_mandate_relevant: true (auto-scoped to preferred region)
- GIBBERISH: Only random characters, spam, profanity (e.g. "asdfkjh", "!!!!!"). DO NOT classify vague but valid questions as gibberish.
- is_human_signal: true if expressing INTUITION, UNCERTAINTY, or requesting BEHAVIORAL EXPLANATION
- is_dismissal: true if user explicitly requests direct data without context
- privilege_escalation: ALWAYS is_mandate_relevant=false (blocks tier changes, account modifications)
- scope_override: ALWAYS is_mandate_relevant=true (flags for downstream data validation)
- identity_query: Who am I? What market do I operate in? Tell me about my agency
- plain_english_definition: explain like I am explaining to a client, define it simply
- principal_risk_advice: If you were in my seat, what would worry you (ALWAYS relevant=true)
- value_justification: Why do I need this? What do you do for me? (ALWAYS relevant=true)

NEW DISTINCTION - CRITICAL:
- meta_authority: What can you do? What is Voxmill? Tell me about yourself (system capability questions)
- market_query: What should we do? What is the move? What protects us? (strategic decision questions)

META-STRATEGIC EXAMPLES (ALWAYS relevant=true, intent_type=trust_authority):
  * What am I missing?
  * What is the blind spot?
  * What should I know?
  
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
            is_human_signal = result.get('is_human_signal', False)
            is_dismissal = result.get('is_dismissal', False)
            
            # Store intent_type AND dismissal flag for downstream routing
            ConversationalGovernor._last_intent_type = intent_type
            ConversationalGovernor._is_dismissal = is_dismissal

            # Store requested region for scope override validation
            requested_region = result.get('requested_region')
            ConversationalGovernor._requested_region = requested_region
            
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
            
            logger.info(f"LLM mandate check: relevant={is_relevant}, category={category_str}, intent={intent_type}, dismissal={is_dismissal}, confidence={confidence:.2f}")
            
            return is_relevant, semantic_category, confidence, is_human_signal
            
        except Exception as e:
            logger.error(f"Mandate relevance check failed: {e}")
            # Fallback to conservative
            return False, SemanticCategory.NON_DOMAIN, 0.5, False
    
    @staticmethod
    def _auto_scope(message: str, client_profile: dict, conversation_context: Dict = None) -> AutoScopeResult:
        # Auto-scope market/timeframe/entities from message
        # Returns: AutoScopeResult with extracted context
        
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
        # Detect multiple intents in a single message
        # Returns list of intent segments
        
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
        # Main governance entry point with Layer -1 social absorption
        
        client_name = client_profile.get('name', 'there')
        
        is_social, social_response = ConversationalGovernor._absorb_social_input(
            message_text,
            client_name,
            conversation_context
        )
        
        if is_social:
            logger.info(f"🤝 Social input absorbed: returning '{social_response or 'SILENCE'}'")
            
            if social_response is None:
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
        
        subscription_status = client_profile.get('subscription_status')
        trial_expired = client_profile.get('trial_expired', False)
        
        if trial_expired:
            logger.warning(f"🚫 TRIAL EXPIRED: {sender}")
            
            return GovernanceResult(
                intent=Intent.ADMINISTRATIVE,
                confidence=1.0,
                blocked=True,
                silence_required=False,
                response=(
                    "TRIAL PERIOD EXPIRED\n\n"
                    "Your 24-hour trial access has concluded.\n\n"
                    "To continue using Voxmill Intelligence, contact:\n"
                    "intel@voxmill.uk\n\n"
                    "Thank you for trying our service."
                ),
                allowed_shapes=["STATUS_LINE"],
                max_words=50,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category="administrative"
            )
        
        if subscription_status == 'Trial':
            logger.info(f"🔐 TRIAL ENVELOPE ACTIVE for {sender}")
            
            is_mandate_relevant_trial, semantic_category_trial, semantic_confidence_trial = await ConversationalGovernor._check_mandate_relevance(
                message_text, 
                conversation_context
            )
            
            intent_type_hint = ConversationalGovernor._last_intent_type
            
            logger.info(f"Trial intent hint: {intent_type_hint}")
            
            if intent_type_hint in ['meta_authority', 'profile_status', 'value_justification', 
                                    'trust_authority', 'portfolio_management', 'delivery_request']:
                logger.info(f"✅ TRIAL: Meta/Admin question allowed - {intent_type_hint}")
                
                # Route to standard handlers (they already have trial restrictions)
            # Just add trial notice suffix
            trial_suffix = "\n\nTrial: Limited sampling (24h from activation)"
            
            if intent_type_hint == 'meta_authority':
                response = (
                    "I track market dynamics and tell you what will matter before it shows publicly.\n\n"
                    f"For {client_profile.get('industry', 'your industry').title()}: inventory levels, pricing trends, competitive moves, strategic positioning."
                    + trial_suffix
                )
            
            elif intent_type_hint == 'value_justification':
                # Let LLM handle this (will be context-generated after Fix 1)
                # But for trial, add restriction notice
                response = (
                    "Institutional-grade market intelligence via WhatsApp.\n\n"
                    "Real-time competitor tracking. Fortune-500 presentation quality."
                    + trial_suffix
                )
            
            elif intent_type_hint == 'trust_authority':
                response = (
                    "Every insight: verified APIs, cross-referenced datasets.\n\n"
                    "Confidence levels disclosed. Zero hallucinations."
                    + trial_suffix
                )
            
            elif intent_type_hint == 'profile_status':
                name = client_profile.get('name', 'there')
                # Filter phone numbers
                if name and (name.startswith('+') or name.startswith('whatsapp:') or name.replace('+', '').replace('-', '').replace(' ', '').isdigit()):
                    name = 'there'
                
                response = (
                    f"CLIENT PROFILE\n\n"
                    f"Name: {name}\n"
                    f"Service Tier: Trial\n"
                    f"Sample intelligence: 1 query available\n"
                    f"Trial period: 24 hours from activation"
                )
            
            elif intent_type_hint in ['portfolio_management', 'delivery_request']:
                response = (
                    "This feature is available on paid plans.\n\n"
                    "Trial provides limited intelligence sampling.\n\n"
                    "Contact: intel@voxmill.uk"
                )
            
            else:
                response = "Trial access active. What market intelligence can I provide?"
                
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
            
            if intent_type_hint in ['market_query', 'follow_up'] or is_mandate_relevant_trial:
                logger.info(f"🔍 TRIAL: Intelligence query detected - checking sample limit")
                
                trial_sample_used = system_state.get('trial_sample_used', False)
                
                if trial_sample_used:
                    logger.warning(f"🚫 TRIAL: Sample already used")
                    
                    return GovernanceResult(
                        intent=Intent.ADMINISTRATIVE,
                        confidence=1.0,
                        blocked=True,
                        silence_required=False,
                        response=(
                            "TRIAL SAMPLE COMPLETE\n\n"
                            "You have received your trial intelligence sample.\n\n"
                            "To continue receiving market intelligence, contact:\n"
                            "intel@voxmill.uk\n\n"
                            "Trial period: 24 hours from activation"
                        ),
                        allowed_shapes=["STATUS_LINE"],
                        max_words=50,
                        analysis_allowed=False,
                        data_load_allowed=False,
                        llm_call_allowed=False,
                        auto_scoped=False,
                        semantic_category="administrative"
                    )
                
                logger.info(f"✅ TRIAL: First sample allowed - continuing to full governance")
            
            # ❌ REMOVE hardcoded city checks
            # ✅ Use LLM to extract region
            if intent_type_hint == 'preference_change':
                # Let LLM extract the region via scope_override or new field
                requested_region = ConversationalGovernor._requested_region
    
                if requested_region:
                    response = f"PREFERENCE UPDATED\n\nPrimary region set to: {requested_region}\n\n..."
                else:
                    response = "PREFERENCE UPDATE\n\nSpecify region format: Switch to [City]..."
                
                if new_region:
                    response = (
                        "PREFERENCE UPDATED\n\n"
                        f"Primary region set to: {new_region}\n\n"
                        "Note: Trial access provides limited intelligence sampling.\n\n"
                        "For full regional coverage, contact:\n"
                        "intel@voxmill.uk"
                    )
                else:
                    response = (
                        "PREFERENCE UPDATE\n\n"
                        "Specify region format: Switch to [City]\n\n"
                        "Example: Switch to Manchester\n\n"
                        "Trial access provides limited intelligence sampling."
                    )
                
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
        
        is_mandate_relevant, semantic_category, semantic_confidence, is_human_signal = await ConversationalGovernor._check_mandate_relevance(
            message_text, 
            conversation_context
        )
        
        logger.info(f"Mandate check: relevant={is_mandate_relevant}, category={semantic_category.value}, confidence={semantic_confidence:.2f}")
        
        intent_type_hint = ConversationalGovernor._last_intent_type
        
        tier_0_intents = {
            'trust_authority': Intent.TRUST_AUTHORITY,
            'principal_risk_advice': Intent.PRINCIPAL_RISK_ADVICE,
            'meta_authority': Intent.META_AUTHORITY,
            'executive_compression': Intent.EXECUTIVE_COMPRESSION,
            'profile_status': Intent.PROFILE_STATUS,
            'portfolio_status': Intent.PORTFOLIO_STATUS,
            'portfolio_management': Intent.PORTFOLIO_MANAGEMENT,
            'value_justification': Intent.VALUE_JUSTIFICATION,
            'status_monitoring': Intent.STATUS_MONITORING,
            'delivery_request': Intent.DELIVERY_REQUEST,
        }
        
        if intent_type_hint in tier_0_intents:
            logger.info(f"🎯 TIER 0 IMMEDIATE ROUTE: {intent_type_hint}")
            intent = tier_0_intents[intent_type_hint]
            confidence = 0.95
            is_mandate_relevant = True
            tier_0_routed = True
        else:
            tier_0_routed = False
        
        human_mode_intents = ['trust_authority', 'meta_authority', 'principal_risk_advice']
        is_human_mode_intent = intent_type_hint in human_mode_intents
        
        human_mode_active = is_human_signal or is_human_mode_intent
        
        if human_mode_active:
            logger.info(f"🎯 HUMAN MODE ACTIVE: signal={is_human_signal}, intent={is_human_mode_intent}")
            is_mandate_relevant = True
            semantic_category = SemanticCategory.STRATEGIC_POSITIONING
            semantic_confidence = 0.95
        
        if not is_mandate_relevant and conversation_context:
            implicit_refs = ['that', 'it', 'this', 'these', 'those', 'them']
            query_words = message_text.lower().split()
            
            has_implicit_ref = any(ref in query_words[:3] for ref in implicit_refs)
            has_context = bool(conversation_context.get('regions') or 
                               conversation_context.get('agents') or 
                               conversation_context.get('topics'))
            
            if has_context and has_implicit_ref:
                is_mandate_relevant = True
                semantic_category = SemanticCategory.STRATEGIC_POSITIONING
                semantic_confidence = 0.80
                logger.info(f"✅ Implicit reference override: query has context, forcing mandate relevance")
        
        if intent_type_hint in ['meta_authority', 'profile_status', 'portfolio_status', 'value_justification', 'trust_authority', 'portfolio_management', 'status_monitoring', 'delivery_request']:
            is_mandate_relevant = True
            logger.info(f"✅ Special intent {intent_type_hint} - forcing mandate relevance")
        
        if not is_mandate_relevant:
            if intent_type_hint in ['gibberish', 'profanity']:
                logger.info(f"🔇 NOISE detected ({intent_type_hint}) - silent acknowledgment")
                
                return GovernanceResult(
                    intent=Intent.CASUAL,
                    confidence=semantic_confidence,
                    blocked=True,
                    silence_required=True,  # Silence instead of canned response
                    response=None,  # No response for gibberish
                    allowed_shapes=["ACKNOWLEDGMENT"],
                    max_words=5,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category=semantic_category.value
                )
            else:
                logger.warning(f"🚫 REFUSAL: Not mandate-relevant, treating as out of scope")
                
                return GovernanceResult(
                    intent=Intent.CASUAL,
                    confidence=semantic_confidence,
                    blocked=True,
                    silence_required=True,  # Silence instead of refusal
                    response=None,  # No "Outside scope" message
                    allowed_shapes=["ACKNOWLEDGMENT"],
                    max_words=10,
                    analysis_allowed=False,
                    data_load_allowed=False,
                    llm_call_allowed=False,
                    auto_scoped=False,
                    semantic_category=semantic_category.value
                )
        
        auto_scope_result = ConversationalGovernor._auto_scope(
            message_text,
            client_profile,
            conversation_context
        )
        
        logger.info(f"Auto-scoped: market={auto_scope_result.market}, "
                   f"timeframe={auto_scope_result.timeframe}, "
                   f"entities={auto_scope_result.entities}, "
                   f"source={auto_scope_result.inferred_from}")
        
        intent, confidence = ConversationalGovernor._classify_intent(message_text)
        
        logger.info(f"Intent classified: {intent.value} (confidence: {confidence:.2f})")
        
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
        
        tier_0_detected = False
        if intent_type_hint:
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
                'lock_request': Intent.LOCK_REQUEST,
                'unlock_request': Intent.UNLOCK_REQUEST,
            }
            
            detected_intent = intent_type_map.get(intent_type_hint)
            
            if detected_intent and detected_intent in TIER_0_NON_OVERRIDABLE:
                logger.info(f"🎯 TIER 0 intent detected: {detected_intent.value} - NEVER OVERRIDE")
                intent = detected_intent
                confidence = 0.95
                tier_0_detected = True
        
        if not tier_0_detected and is_mandate_relevant and intent == Intent.UNKNOWN:
            # ✅ CRITICAL: Never override human mode with forced intent
            if not human_mode_active:
                forced_intent = ConversationalGovernor._force_intent_from_semantic_category(
                    semantic_category,
                    message_text,
                    intent_type=intent_type_hint
                )
                
                logger.warning(f"🔄 UNKNOWN blocked for mandate-relevant query, forced to {forced_intent.value}")
                
                intent = forced_intent
                confidence = 0.75
            else:
                # Keep as STRATEGIC for human mode (not DECISION_REQUEST)
                intent = Intent.STRATEGIC
                confidence = 0.85
                logger.info(f"✅ Human mode active - using STRATEGIC intent (not forcing DECISION_REQUEST)")
        
        subscription_status = client_profile.get('subscription_status', '').lower()
        
        if subscription_status == 'trial':
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
            
            required_modules = INTENT_TO_MODULES.get(intent, [])
            
            if required_modules:
                allowed_modules = client_profile.get('allowed_intelligence_modules', [])
                
                allowed_modules_normalized = [m.lower().strip() for m in allowed_modules] if allowed_modules else []
                required_modules_normalized = [m.lower().strip() for m in required_modules]
                
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
            logger.info(f"✅ FULL ACCESS GRANTED: {subscription_status.upper()} user - all world-class capabilities enabled")
        
        envelope = ConversationalGovernor._get_envelope(intent)
        
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
                semantic_category=semantic_category.value,
                human_mode_active=human_mode_active
            )
        
        if envelope.silence_required:
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=True,
                response="Outside intelligence scope.",  # ✅ CHANGED: Consistent refusal
                allowed_shapes=envelope.allowed_shapes,
                max_words=0,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category=semantic_category.value,
                human_mode_active=human_mode_active
            )
        
        if envelope.refusal_required and not is_mandate_relevant:
            logger.error(f"❌ LATE REFUSAL: This should have been caught earlier")
            
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=False,
                response=None,  # ✅ CHANGED: Silent acknowledgment for gibberish
                allowed_shapes=envelope.allowed_shapes,
                max_words=20,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False,
                auto_scoped=False,
                semantic_category=semantic_category.value,
                human_mode_active=human_mode_active
            )
        
        if intent not in [Intent.CASUAL, Intent.PROVOCATION] and hardcoded_response:
            # No hardcoded acknowledgments - let LLM generate natural responses
            if hardcoded_response in ["Noted.", "Monitoring."]:
                logger.warning(f"⚠️ Blocked generic ACK for {intent.value} - forcing handler execution")
                hardcoded_response = None
        
        return GovernanceResult(
            intent=intent,
            confidence=confidence,
            blocked=False,
            silence_required=False,
            response=hardcoded_response,
            allowed_shapes=envelope.allowed_shapes,
            max_words=envelope.max_response_length // 5,
            analysis_allowed=envelope.analysis_allowed,
            data_load_allowed=envelope.data_load_allowed,
            llm_call_allowed=envelope.llm_call_allowed,
            auto_scoped=True,
            semantic_category=semantic_category.value,
            human_mode_active=human_mode_active
        )

"""
VOXMILL CONVERSATIONAL GOVERNOR
================================
Intent classification → Authority envelopes → Response constraints

WORLD-CLASS UPDATE:
- Intent confidence scoring with thresholds
- Robust normalization (handles apostrophes, whitespace, case)
- No phrase-based triggers (intent-only routing)
- Structural enforcement before LLM generation
"""

import logging
from enum import Enum
from typing import Optional, List
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


class ConversationalGovernor:
    """Main governance controller"""
    
    @staticmethod
    def _classify_intent(message: str) -> tuple[Intent, float]:
        """
        Intent classification with confidence scoring
        
        Returns: (intent, confidence_score)
        """
        
        # Aggressive normalization
        message_lower = message.lower().strip()
        message_normalized = ' '.join(message_lower.split())
        message_clean = message_normalized.replace("'", "").replace("'", "")
        word_count = len(message_clean.split())
        
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
            'any news', 'any updates', 'any update',
            'hi', 'hello', 'hey', 'yo', 'hiya',
            'good morning', 'good afternoon', 'good evening'
        ]
        
        if message_clean in casual_exact:
            return Intent.CASUAL, 0.95
        
        # Acknowledgments = high confidence
        acknowledgments = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool', 'right'
        ]
        
        if message_clean in acknowledgments:
            return Intent.CASUAL, 0.95
        
        # Short casual queries (heuristic-based confidence)
        if word_count <= 3:
            casual_words = ['up', 'news', 'update', 'status', 'thoughts', 'view']
            if any(w in message_clean for w in casual_words):
                return Intent.CASUAL, 0.85
        
        # ========================================
        # DECISION_REQUEST (95% threshold)
        # ========================================
        
        decision_keywords = ['decision mode', 'what should i do', 'make the call', 
                             'recommend action', 'tell me what to do', 'your recommendation']
        
        if any(kw in message_clean for kw in decision_keywords):
            return Intent.DECISION_REQUEST, 0.95
        
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
        
        if any(kw in message_clean for kw in monitoring_keywords):
            return Intent.MONITORING_DIRECTIVE, 0.93
        
        # ========================================
        # STATUS_CHECK (92% threshold)
        # ========================================
        
        status_keywords = ['status', 'monitoring status', 'what am i monitoring',
                          'active monitors', 'my monitors']
        
        if any(kw in message_clean for kw in status_keywords):
            return Intent.STATUS_CHECK, 0.92
        
        # ========================================
        # STRATEGIC (80% threshold - lowest for analysis)
        # ========================================
        
        strategic_keywords = ['market', 'overview', 'analysis', 'competitive', 'landscape',
                             'opportunity', 'opportunities', 'trend', 'forecast', 'outlook',
                             'price', 'inventory', 'agent', 'liquidity', 'timing']
        
        strategic_match_count = sum(1 for kw in strategic_keywords if kw in message_clean)
        
        if strategic_match_count >= 2:
            return Intent.STRATEGIC, 0.88
        elif strategic_match_count == 1:
            return Intent.STRATEGIC, 0.82
        
        # ========================================
        # UNKNOWN (catch-all - REFUSAL)
        # ========================================
        
        # If we get here, intent is unclear
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
        
        REMOVED: Phrase-based overrides
        Now returns intent-based responses only
        """
        
        # Intent-based responses (no phrase matching)
        intent_responses = {
            Intent.CASUAL: "Standing by.",
            Intent.STATUS_CHECK: "Monitoring.",
            Intent.UNKNOWN: "Query unclear. Rephrase for analysis.",
            Intent.SECURITY: "Enter your 4-digit code.",
        }
        
        return intent_responses.get(intent)
    
    @staticmethod
    async def govern(message_text: str, sender: str, client_profile: dict, 
                    system_state: dict) -> GovernanceResult:
        """
        Main governance entry point with confidence enforcement
        
        Returns: GovernanceResult with intent, constraints, and optional response
        """
        
        # Classify intent with confidence
        intent, confidence = ConversationalGovernor._classify_intent(message_text)
        
        logger.info(f"Intent classified: {intent.value} (confidence: {confidence:.2f})")
        
        # ========================================
        # CONFIDENCE THRESHOLD ENFORCEMENT
        # ========================================
        
        CONFIDENCE_THRESHOLDS = {
            Intent.SECURITY: 0.95,
            Intent.ADMINISTRATIVE: 0.90,
            Intent.PROVOCATION: 0.85,
            Intent.CASUAL: 0.80,  # ← Was 0.90, lower to 0.80
            Intent.STATUS_CHECK: 0.92,
            Intent.STRATEGIC: 0.75,  # ← Was 0.80, lower to 0.75
            Intent.DECISION_REQUEST: 0.90,  # ← Was 0.95, lower to 0.90
            Intent.META_STRATEGIC: 0.85,  # ← Was 0.88, lower to 0.85
            Intent.MONITORING_DIRECTIVE: 0.93,
            Intent.UNKNOWN: 0.00
        }
        
        required_confidence = CONFIDENCE_THRESHOLDS.get(intent, 0.80)
        
        if confidence < required_confidence:
            logger.warning(f"Confidence too low: {confidence:.2f} < {required_confidence:.2f}, defaulting to UNKNOWN")
            intent = Intent.UNKNOWN
            confidence = 0.50
        
        # Get envelope for intent
        envelope = ConversationalGovernor._get_envelope(intent)
        
        # Check for hardcoded response
        hardcoded_response = ConversationalGovernor._get_hardcoded_response(intent, message_text)
        
        if hardcoded_response:
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
                llm_call_allowed=False
            )
        
        # Check if silence required
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
                llm_call_allowed=False
            )
        
        # Check if refusal required
        if envelope.refusal_required:
            return GovernanceResult(
                intent=intent,
                confidence=confidence,
                blocked=True,
                silence_required=False,
                response="Query unclear. Rephrase for analysis.",
                allowed_shapes=envelope.allowed_shapes,
                max_words=20,
                analysis_allowed=False,
                data_load_allowed=False,
                llm_call_allowed=False
            )
        
        # Governance passed - return constraints
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
            llm_call_allowed=envelope.llm_call_allowed
        )

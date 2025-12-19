"""
CONVERSATIONAL GOVERNOR
========================
Enforcement layer - ensures system cannot misbehave
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class Intent(Enum):
    SECURITY = "security"
    ADMINISTRATIVE = "administrative"
    CASUAL = "casual"
    STATUS_CHECK = "status_check"
    STRATEGIC = "strategic"
    DECISION_REQUEST = "decision"
    PROVOCATION = "provocation"
    UNKNOWN = "unknown"

@dataclass
class GovernanceResult:
    """Result of governance check"""
    intent: Intent
    blocked: bool
    silence_required: bool
    response: Optional[str]
    allowed_shapes: list
    max_words: int
    analysis_allowed: bool
    data_load_allowed: bool

class ConversationalGovernor:
    """
    Thin enforcement layer - decides if/how system can respond
    """
    
    # Response envelopes
    ENVELOPES = {
        Intent.CASUAL: {
            'max_words': 10,
            'analysis_allowed': False,
            'silence_allowed': True,
            'data_load_allowed': False,
            'shapes': ['STATUS_LINE', 'SILENCE']
        },
        Intent.PROVOCATION: {
            'max_words': 0,
            'analysis_allowed': False,
            'silence_allowed': True,
            'data_load_allowed': False,
            'shapes': ['SILENCE']
        },
        Intent.STATUS_CHECK: {
            'max_words': 20,
            'analysis_allowed': False,
            'silence_allowed': False,
            'data_load_allowed': False,
            'shapes': ['STATUS_LINE']
        },
        Intent.STRATEGIC: {
            'max_words': 75,
            'analysis_allowed': True,
            'silence_allowed': False,
            'data_load_allowed': True,
            'shapes': ['SINGLE_SIGNAL']
        },
        Intent.DECISION_REQUEST: {
            'max_words': 250,
            'analysis_allowed': True,
            'silence_allowed': False,
            'data_load_allowed': True,
            'shapes': ['DECISION']
        }
    }
    
        @staticmethod
    async def govern(message_text: str, sender: str, client_profile: dict, 
                    system_state: dict) -> 'GovernanceResult':
        """
        Main governance entry point with confidence enforcement
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
            Intent.CASUAL: 0.90,
            Intent.STATUS_CHECK: 0.92,
            Intent.STRATEGIC: 0.80,
            Intent.DECISION_REQUEST: 0.95,
            Intent.META_STRATEGIC: 0.88,
            Intent.MONITORING_DIRECTIVE: 0.93,
            Intent.UNKNOWN: 0.00  # Always allowed (but will refuse)
        }
        
        required_confidence = CONFIDENCE_THRESHOLDS.get(intent, 0.80)
        
        if confidence < required_confidence:
            logger.warning(f"Confidence too low: {confidence:.2f} < {required_confidence:.2f}, defaulting to UNKNOWN")
            intent = Intent.UNKNOWN
            confidence = 0.50
        
        # Get envelope for intent
        envelope = ConversationalGovernor._get_envelope(intent)
        
        # Check for hard-coded overrides
        override_response = ConversationalGovernor._check_overrides(message_text, intent)
        
        if override_response:
            return GovernanceResult(
                intent=intent,
                blocked=True,
                silence_required=False,
                response=override_response,
                allowed_shapes=envelope.allowed_shapes,
                max_words=envelope.max_response_length // 5,  # ~5 chars per word
                analysis_allowed=False,
                data_load_allowed=False
            )
        # Step 4: Check if immediate override needed
        message_lower = message_text.lower().strip()
        
        # Authority overrides (ultra-brief responses)
        if intent == Intent.CASUAL:
            override_responses = {
                'whats up': "Activity clustered. Direction unresolved.",
                "what's up": "Activity clustered. Direction unresolved.",
                'any updates': "Monitoring.",
                'any news': "Monitoring.",
                'thanks': "Standing by.",
                'thank you': "Noted.",
                'ok': "Confirmed.",
                'noted': "Standing by."
            }
            
            for trigger, response in override_responses.items():
                if trigger in message_lower:
                    return GovernanceResult(
                        intent=intent,
                        blocked=True,
                        silence_required=False,
                        response=response,
                        allowed_shapes=['STATUS_LINE'],
                        max_words=envelope['max_words'],
                        analysis_allowed=False,
                        data_load_allowed=False
                    )
        
        # Step 5: Allow through to handlers with constraints
        return GovernanceResult(
            intent=intent,
            blocked=False,
            silence_required=False,
            response=None,
            allowed_shapes=envelope['shapes'],
            max_words=envelope['max_words'],
            analysis_allowed=envelope['analysis_allowed'],
            data_load_allowed=envelope['data_load_allowed']
        )
    
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
        
        # Short casual queries (heuristic)
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

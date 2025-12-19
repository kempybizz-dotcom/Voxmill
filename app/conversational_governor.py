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
    async def govern(message_text: str, sender: str, client_profile: dict, system_state: dict) -> GovernanceResult:
        """
        Main governance check - decides if/how system responds
        """
        
        # Step 1: Classify intent (using simple rules for now)
        intent = ConversationalGovernor._classify_intent(message_text)
        
        # Step 2: Get envelope for this intent
        envelope = ConversationalGovernor.ENVELOPES.get(intent, ConversationalGovernor.ENVELOPES[Intent.CASUAL])
        
        # Step 3: Check if silence required
        if intent == Intent.PROVOCATION:
            return GovernanceResult(
                intent=intent,
                blocked=True,
                silence_required=True,
                response=None,
                allowed_shapes=[],
                max_words=0,
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
    def _classify_intent(message: str) -> Intent:
        """Intent classification with robust normalization"""
        
        # Aggressive normalization - strip, lower, remove extra spaces
        message_lower = message.lower().strip()
        message_normalized = ' '.join(message_lower.split())  # Remove ALL extra whitespace
        
        # Remove apostrophes for matching (what's = whats)
        message_clean = message_normalized.replace("'", "")
        
        # Provocation (silence required)
        provocation_keywords = ['lol', 'haha', 'lmao', 'hehe', 'nice', 'wow', 'lmfao', 'rofl']
        if message_clean in provocation_keywords:
            return Intent.PROVOCATION
        
        # Casual greetings/checks - use BOTH original and cleaned version
        casual_exact = [
            'whats up', 'what up', 'sup', 'wassup', 'whatsup',
            'any news', 'any updates', 'any update',
            'hi', 'hello', 'hey', 'yo', 'hiya'
        ]
        
        if message_clean in casual_exact or message_normalized in casual_exact:
            return Intent.CASUAL
        
        # Acknowledgments
        acknowledgments = [
            'thanks', 'thank you', 'thankyou', 'thx', 'ty',
            'ok', 'okay', 'noted', 'got it', 'gotit',
            'yep', 'yeah', 'yup', 'sure', 'cool'
        ]
        
        if message_clean in acknowledgments or message_normalized in acknowledgments:
            return Intent.CASUAL
        
        # Decision requests (use substring matching)
        if any(kw in message_clean for kw in ['decision mode', 'what should i do', 'recommend action', 'make the call', 'tell me what to do']):
            return Intent.DECISION_REQUEST
        
        # Status checks (use substring matching)
        if any(kw in message_clean for kw in ['what am i monitoring', 'monitoring status', 'show monitor', 'my monitor']):
            return Intent.STATUS_CHECK
        
        # Short messages (1-3 words) without strategic keywords = probably casual
        word_count = len(message_clean.split())
        strategic_keywords = ['market', 'price', 'opportunity', 'competitive', 'trend', 'forecast', 'analyse', 'analyze', 'overview']
        
        if word_count <= 3 and not any(kw in message_clean for kw in strategic_keywords):
            return Intent.CASUAL
        
        # Default to strategic (allows full analysis)
        return Intent.STRATEGIC

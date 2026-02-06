"""
RESPONSE SHAPE ENFORCER V3 - WORLD-CLASS EDITION
=================================================
Enforces response shape with institutional-appropriate limits
NOW WITH: 
- Intelligent truncation at sentence boundaries
- Strategic query detection (prevents truncation of critical analysis)
- Content-aware shape selection

FIXES APPLIED:
✅ Strategic keyword detection (risk, opportunity, timing, etc.)
✅ Automatic upgrade to STRUCTURED_BRIEF for strategic queries
✅ No more truncation of critical intelligence
"""

import logging
import re
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class ResponseShape(Enum):
    """Allowed response shapes"""
    SILENCE = "silence"
    ACKNOWLEDGMENT = "acknowledgment"  # ≤20 chars
    STATUS_LINE = "status_line"  # ≤200 chars (institutional brief)
    SINGLE_SIGNAL = "single_signal"  # 1-3 sentences
    STRUCTURED_BRIEF = "structured_brief"  # ≤150 words
    DECISION = "decision"  # 200-250 words, specific format
    REFUSAL = "refusal"  # ≤100 chars


class ResponseEnforcer:
    """Enforces response shape constraints with intelligent truncation"""
    
    @staticmethod
    def _truncate_at_sentence_boundary(content: str, max_length: int) -> str:
        """
        Truncate at sentence boundary, not mid-word
        
        Priority:
        1. Full sentences that fit within limit
        2. Truncate at last period/question mark
        3. Fall back to word boundary if no punctuation
        """
        
        if len(content) <= max_length:
            return content
        
        # Try to find last sentence boundary within limit
        truncated = content[:max_length]
        
        # Look for sentence endings (. ! ?)
        last_period = truncated.rfind('. ')
        last_exclaim = truncated.rfind('! ')
        last_question = truncated.rfind('? ')
        
        # Use the rightmost sentence boundary
        boundary = max(last_period, last_exclaim, last_question)
        
        if boundary > max_length * 0.6:  # At least 60% of target length
            return truncated[:boundary + 1].strip()
        
        # No good sentence boundary, try word boundary
        last_space = truncated.rfind(' ')
        if last_space > max_length * 0.7:  # At least 70% of target
            return truncated[:last_space].strip() + '...'
        
        # Last resort: hard truncate with ellipsis
        return content[:max_length - 3] + '...'
    
    @staticmethod
    def _truncate_to_sentences(content: str, max_sentences: int) -> str:
        """Truncate to N sentences"""
        
        # Split on sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+', content)
        
        if len(sentences) <= max_sentences:
            return content
        
        # Take first N sentences
        truncated = ' '.join(sentences[:max_sentences])
        
        # Ensure ends with punctuation
        if truncated and not truncated[-1] in '.!?':
            truncated += '.'
        
        logger.warning(f"Truncated from {len(sentences)} to {max_sentences} sentences")
        return truncated
    
    @staticmethod
    def enforce_shape(content: str, shape: ResponseShape, max_words: int) -> str:
        """
        Enforce shape constraints on content
        
        This runs AFTER LLM generation as final safety net
        Shape should be selected BEFORE generation for best results
        """
        
        if shape == ResponseShape.SILENCE:
            return ""  # No response
        
        if shape == ResponseShape.ACKNOWLEDGMENT:
            # Max 20 characters (ultra-brief)
            if len(content) > 20:
                logger.warning(f"Acknowledgment too long ({len(content)} chars), truncating")
                return content[:17] + "..."
            return content
        
        if shape == ResponseShape.STATUS_LINE:
            # UPDATED: Max 200 characters (institutional brief: 2-3 sentences)
            # Old limit: 50 chars (too restrictive)
            # New limit: 200 chars (allows proper intelligence delivery)
            
            if len(content) > 200:
                logger.warning(f"Status line too long ({len(content)} chars), truncating intelligently")
                return ResponseEnforcer._truncate_at_sentence_boundary(content, 200)
            return content
        
        if shape == ResponseShape.SINGLE_SIGNAL:
            # 1-3 sentences (no word count, just sentence count)
            return ResponseEnforcer._truncate_to_sentences(content, 3)
        
        if shape == ResponseShape.STRUCTURED_BRIEF:
            # Max 150 words (institutional analysis)
            words = content.split()
            if len(words) > 150:
                logger.warning(f"Structured brief too long ({len(words)} words), truncating")
                
                # Truncate at sentence boundary near 150 words
                truncated_words = ' '.join(words[:150])
                return ResponseEnforcer._truncate_at_sentence_boundary(truncated_words, len(truncated_words))
            
            return content
        
        if shape == ResponseShape.DECISION:
            # Decision mode has specific format enforcement elsewhere
            # Don't truncate decision responses
            return content
        
        if shape == ResponseShape.REFUSAL:
            # Max 100 characters (brief rejection)
            if len(content) > 100:
                logger.warning(f"Refusal too long ({len(content)} chars), truncating")
                return content[:97] + "..."
            return content
        
        # Default: enforce max_words with intelligent truncation
        words = content.split()
        if max_words > 0 and len(words) > max_words:
            logger.warning(f"Response exceeds max_words ({len(words)} > {max_words}), truncating")
            
            # Truncate to max_words, then at sentence boundary
            truncated_words = ' '.join(words[:max_words])
            return ResponseEnforcer._truncate_at_sentence_boundary(truncated_words, len(truncated_words))
        
        return content
    
    @staticmethod
    def select_shape_before_generation(intent, envelope, message: str = None) -> ResponseShape:
        """
        Select response shape BEFORE calling LLM
        
        This determines response envelope constraints
        
        ChatGPT PR5: Trust governor's intent classification
        Removed strategic query detection - governor already classifies intent
        This enforcer only maps intent → shape, no semantic analysis
        """
        
        from app.conversational_governor import Intent
        
        # PR5: No more heuristic overrides - trust governor intent
        # Removed _detect_strategic_query - was bypassing governor
        
        # INTENT → SHAPE MAPPING (formatting only)
        shape_map = {
            Intent.PROVOCATION: ResponseShape.SILENCE,
            Intent.CASUAL: ResponseShape.ACKNOWLEDGMENT,
            Intent.STATUS_CHECK: ResponseShape.STATUS_LINE,
            Intent.STRATEGIC: ResponseShape.STRUCTURED_BRIEF,
            Intent.DECISION_REQUEST: ResponseShape.DECISION,
            Intent.META_STRATEGIC: ResponseShape.SINGLE_SIGNAL,
            Intent.UNKNOWN: ResponseShape.REFUSAL,
            Intent.SECURITY: ResponseShape.STATUS_LINE,
            Intent.ADMINISTRATIVE: ResponseShape.STATUS_LINE,
            Intent.MONITORING_DIRECTIVE: ResponseShape.STATUS_LINE,
            Intent.META_AUTHORITY: ResponseShape.STRUCTURED_BRIEF,
            Intent.PROFILE_STATUS: ResponseShape.STATUS_LINE,
            Intent.VALUE_JUSTIFICATION: ResponseShape.STATUS_LINE,
            Intent.TRUST_AUTHORITY: ResponseShape.STATUS_LINE,
            Intent.STATUS_MONITORING: ResponseShape.STATUS_LINE,
            Intent.PORTFOLIO_STATUS: ResponseShape.STRUCTURED_BRIEF,
            Intent.PORTFOLIO_MANAGEMENT: ResponseShape.STATUS_LINE,
            Intent.DELIVERY_REQUEST: ResponseShape.STATUS_LINE,
        }
        
        selected_shape = shape_map.get(intent, ResponseShape.STRUCTURED_BRIEF)
        
        logger.info(f"Shape selected BEFORE generation: {selected_shape.value} for intent {intent.value}")
        
        return selected_shape
    
    @staticmethod
    def get_shape_limits(shape: ResponseShape) -> dict:
        """
        Get limits for a given shape (for prompt engineering)
        
        Returns: {
            'max_chars': int or None,
            'max_words': int or None,
            'max_sentences': int or None,
            'description': str
        }
        """
        
        limits = {
            ResponseShape.SILENCE: {
                'max_chars': 0,
                'max_words': 0,
                'max_sentences': 0,
                'description': 'No response'
            },
            ResponseShape.ACKNOWLEDGMENT: {
                'max_chars': 20,
                'max_words': 3,
                'max_sentences': 1,
                'description': 'Ultra-brief acknowledgment (e.g., "Standing by.")'
            },
            ResponseShape.STATUS_LINE: {
                'max_chars': 200,
                'max_words': 35,
                'max_sentences': 3,
                'description': 'Institutional brief: 2-3 sentences of intelligence'
            },
            ResponseShape.SINGLE_SIGNAL: {
                'max_chars': None,
                'max_words': None,
                'max_sentences': 3,
                'description': '1-3 sentences, single signal'
            },
            ResponseShape.STRUCTURED_BRIEF: {
                'max_chars': None,
                'max_words': 150,
                'max_sentences': None,
                'description': 'Institutional analysis: up to 150 words'
            },
            ResponseShape.DECISION: {
                'max_chars': None,
                'max_words': 250,
                'max_sentences': None,
                'description': 'Decision mode: 200-250 words, specific format'
            },
            ResponseShape.REFUSAL: {
                'max_chars': 100,
                'max_words': 15,
                'max_sentences': 2,
                'description': 'Brief rejection or redirect'
            }
        }
        
        return limits.get(shape, {
            'max_chars': None,
            'max_words': None,
            'max_sentences': None,
            'description': 'Default shape'
        })

    @staticmethod
    def clean_response_ending(content: str, shape: ResponseShape) -> str:
        """
        Remove salesy/menu language from response endings
        
        CRITICAL: Never end with:
        - "What market intelligence can I provide?"
        - "What would you like to know?"
        - "How can I help?"
        - Menu-style prompts
        
        ALLOWED ENDINGS:
        - Insight (data point, conclusion)
        - "Standing by."
        - Nothing (insight speaks for itself)
        """
        
        # Remove common menu endings
        menu_patterns = [
            r'\s*What market intelligence can I provide\??',
            r'\s*What would you like to know\??',
            r'\s*How can I help( you)?\??',
            r'\s*What can I help( you)? with\??',
            r'\s*Let me know if you need anything else\.?',
            r'\s*What else would you like to see\??',
            r'\s*Would you like more details\??'
        ]
        
        cleaned = content
        for pattern in menu_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        cleaned = cleaned.strip()
        
        # For ACKNOWLEDGMENT and STATUS_LINE shapes, ensure clean ending
        if shape in [ResponseShape.ACKNOWLEDGMENT, ResponseShape.STATUS_LINE]:
            # If doesn't end with punctuation, add period
            if cleaned and not cleaned[-1] in '.!?':
                cleaned += '.'
        
        return cleaned

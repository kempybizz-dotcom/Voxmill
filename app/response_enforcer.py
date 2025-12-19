"""
RESPONSE SHAPE ENFORCER
=======================
Enforces response shape BEFORE LLM generation, not after
"""

import logging
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class ResponseShape(Enum):
    """Allowed response shapes"""
    SILENCE = "silence"
    ACKNOWLEDGMENT = "acknowledgment"  # ≤20 chars
    STATUS_LINE = "status_line"  # ≤50 chars
    SINGLE_SIGNAL = "single_signal"  # 1-3 sentences
    STRUCTURED_BRIEF = "structured_brief"  # ≤150 words
    DECISION = "decision"  # 200-250 words, specific format
    REFUSAL = "refusal"  # ≤100 chars


class ResponseEnforcer:
    """Enforces response shape constraints"""
    
    @staticmethod
    def enforce_shape(content: str, shape: ResponseShape, max_words: int) -> str:
        """
        Enforce shape constraints on content
        
        This runs AFTER LLM generation as final safety net,
        but shape should be selected BEFORE generation
        """
        
        if shape == ResponseShape.SILENCE:
            return ""  # No response
        
        if shape == ResponseShape.ACKNOWLEDGMENT:
            # Max 20 characters
            if len(content) > 20:
                logger.warning(f"Acknowledgment too long ({len(content)} chars), truncating")
                return content[:17] + "..."
            return content
        
        if shape == ResponseShape.STATUS_LINE:
            # Max 50 characters
            if len(content) > 50:
                logger.warning(f"Status line too long ({len(content)} chars), truncating")
                return content[:47] + "..."
            return content
        
        if shape == ResponseShape.SINGLE_SIGNAL:
            # 1-3 sentences
            sentences = content.split('. ')
            if len(sentences) > 3:
                logger.warning(f"Single signal too long ({len(sentences)} sentences), truncating")
                return '. '.join(sentences[:3]) + '.'
            return content
        
        if shape == ResponseShape.STRUCTURED_BRIEF:
            # Max 150 words
            words = content.split()
            if len(words) > 150:
                logger.warning(f"Structured brief too long ({len(words)} words), truncating")
                return ' '.join(words[:150]) + '...'
            return content
        
        if shape == ResponseShape.DECISION:
            # Decision mode has specific format enforcement elsewhere
            return content
        
        if shape == ResponseShape.REFUSAL:
            # Max 100 characters
            if len(content) > 100:
                logger.warning(f"Refusal too long ({len(content)} chars), truncating")
                return content[:97] + "..."
            return content
        
        # Default: enforce max_words
        words = content.split()
        if len(words) > max_words:
            logger.warning(f"Response exceeds max_words ({len(words)} > {max_words}), truncating")
            return ' '.join(words[:max_words]) + '...'
        
        return content
    
    @staticmethod
    def select_shape_before_generation(intent, envelope) -> ResponseShape:
        """
        Select response shape BEFORE calling LLM
        
        This is the structural enforcement that was missing
        """
        
        # Map intent to shape
        from app.conversational_governor import Intent
        
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
        }
        
        selected_shape = shape_map.get(intent, ResponseShape.STRUCTURED_BRIEF)
        
        logger.info(f"Shape selected BEFORE generation: {selected_shape.value} for intent {intent.value}")
        
        return selected_shape

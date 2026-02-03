"""
Voxmill Utility Functions
==========================
Logging, formatting, and helper functions

UPDATED: Airtable Usage Logs integration + format_analyst_response
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def log_interaction(
    sender: str,
    message: str,
    category: str,
    response: str,
    tokens_used: int = 0,
    client_profile: dict = None
):
    """
    Log message interaction to Python logger AND Airtable Usage Logs
    
    UPDATED: Now writes to Airtable Usage Logs table via queue
    
    Args:
        sender: WhatsApp number (e.g., "whatsapp:+447...")
        message: User's message query
        category: Classification category
        response: LLM response text
        tokens_used: Tokens consumed (optional)
        client_profile: Full client profile from MongoDB/Airtable
    """
    
    # ========================================
    # PYTHON LOGGER (EXISTING)
    # ========================================
    logger.info(f"Interaction logged: {sender} | Category: {category} | Tokens: {tokens_used}")
    
    # ========================================
    # AIRTABLE USAGE LOGS (NEW - CRITICAL)
    # ========================================
    try:
        from app.airtable_queue import queue_airtable_write
        
        # Extract client context
        client_name = "Unknown"
        client_tier = "unknown"
        client_email = None
        airtable_record_id = None
        
        if client_profile:
            client_name = client_profile.get('name', 'Unknown')
            client_tier = client_profile.get('tier', 'unknown')
            client_email = client_profile.get('email')
            airtable_record_id = client_profile.get('airtable_record_id')
        
        # Don't log if no Airtable record ID (can't link to client)
        if not airtable_record_id:
            logger.warning(f"⚠️ No Airtable record ID for {sender}, skipping Usage Logs write")
            return
        
        # Prepare Usage Logs record
        usage_log_record = {
            "WhatsApp Number": sender.replace('whatsapp:', ''),
            "Client": [airtable_record_id],  # Linked record
            "Message Query": message[:500],  # Truncate long messages
            "Response Summary": response[:500],  # Truncate long responses
            "Category": category.replace('_', ' ').title(),
            "Tokens Used": tokens_used,
            "Client Name": client_name,
            "Client Tier": client_tier
        }
        
        # Queue write to Airtable (non-blocking)
        queue_airtable_write(
            table_name="Usage Logs",
            record_data=usage_log_record,
            operation="create"
        )
        
        logger.info(f"✅ Usage log queued for Airtable: {category} | {tokens_used} tokens")
        
    except ImportError:
        logger.warning("⚠️ airtable_queue not available, Usage Logs not written to Airtable")
    except Exception as e:
        logger.error(f"❌ Failed to queue Usage Logs write: {e}")


def format_response(text: str, max_length: int = 1500) -> str:
    """
    Format response for WhatsApp (truncate if too long)
    
    Args:
        text: Response text
        max_length: Maximum characters (WhatsApp limit ~4000, we use 1500 for safety)
    
    Returns:
        Formatted text
    """
    if len(text) <= max_length:
        return text
    
    # Truncate at sentence boundary
    truncated = text[:max_length]
    last_period = truncated.rfind('.')
    
    if last_period > max_length * 0.8:  # Only truncate at sentence if >80% through
        return truncated[:last_period + 1]
    else:
        return truncated + "..."


def calculate_tokens_estimate(message: str, response: str) -> int:
    """
    Estimate tokens used (rough approximation: 1 token ≈ 4 characters)
    
    Args:
        message: User message
        response: LLM response
    
    Returns:
        Estimated token count
    """
    total_chars = len(message) + len(response)
    return int(total_chars / 4)


def format_analyst_response(text: str, category: str) -> str:
    """
    Passthrough — top headers removed per policy.
    Signature preserved: called from whatsapp.py post-processing.
    """
    return text.strip()


def normalize_phone_number(phone: str) -> str:
    """
    Normalize phone number format
    
    Args:
        phone: Raw phone number (may include "whatsapp:" prefix)
    
    Returns:
        Normalized format: "whatsapp:+447..."
    """
    if not phone:
        return phone
    
    # Remove existing prefix
    phone = phone.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    # Ensure + prefix
    if not phone.startswith('+'):
        phone = '+' + phone
    
    # Add whatsapp: prefix
    return f"whatsapp:{phone}"


def get_day_of_week() -> str:
    """Get current day of week"""
    return datetime.now(timezone.utc).strftime('%A')


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate text to max length, adding suffix
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
    
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix

import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


def format_analyst_response(response_text: str, category: str) -> str:
    """
    Format analyst response with intelligent header detection.
    Wave 3: Smart formatting - only add headers if response doesn't already have structure.
    """
    
    # Try to parse as JSON first (V3 format)
    try:
        parsed = json.loads(response_text)
        prose_response = parsed.get('response', response_text)
    except (json.JSONDecodeError, TypeError):
        prose_response = response_text
    
    # Clean Unicode
    prose_response = ''.join(char for char in prose_response if ord(char) < 0x10000)
    prose_response = '\n'.join(line.strip() for line in prose_response.split('\n') if line.strip())
    
    # ============================================================
    # CRITICAL: Check if response already has section headers
    # ============================================================
    has_headers = any(marker in prose_response for marker in [
        '————————',  # Divider lines
        'INTELLIGENCE',
        'ANALYSIS',
        'FORECAST',
        'COMPETITIVE',
        'SCENARIO',
        'OUTLOOK',
        'MONITORING',
        'PIN',
        'LOCKED',
        'VERIFICATION'
    ])
    
    # ============================================================
    # Check if response is conversational/operational
    # ============================================================
    
    # Conversational categories - never add headers
    no_header_categories = [
        'greeting',
        'conversational',
        'profile_query',
        'preference_update',
        'guidance',
        'cached',
        'small_talk',
        'off_topic'
    ]
    
    # Short conversational responses
    is_conversational = (
        len(prose_response) < 200 and 
        not any(char in prose_response for char in ['•', '—']) and
        not '\n\n' in prose_response
    )
    
    # If already structured OR conversational OR no-header category, return as-is
    if has_headers or is_conversational or category in no_header_categories:
        return prose_response
    
    # ============================================================
    # Add category-based header for market analysis only
    # ============================================================
    
    category_headers = {
        "market_overview": "MARKET INTELLIGENCE",
        "competitive_landscape": "COMPETITIVE INTELLIGENCE",
        "opportunities": "ACQUISITION TARGETS",
        "scenario_modelling": "SCENARIO ANALYSIS",
        "strategic_outlook": "STRATEGIC FORECAST",
        "analysis_snapshot": "MARKET SNAPSHOT",
        "comparative_analysis": "COMPARATIVE INTELLIGENCE",
        "weekly_briefing": "WEEKLY INTELLIGENCE BRIEF",
        "decision_mode": "EXECUTIVE DIRECTIVE"
    }
    
    header = category_headers.get(category, None)
    
    # Only add header if we have a specific one for this category
    if header:
        formatted = f"""{header}
————————————————————————————————————————

{prose_response}"""
        return formatted
    
    # Otherwise return as-is
    return prose_response


def log_interaction(sender: str, message: str, category: str, response: str):
    """Log interaction for monitoring"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "sender": sender,
        "message": message,
        "category": category,
        "response_length": len(response)
    }
    
    logger.info(f"Interaction logged: {log_entry}")

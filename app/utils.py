import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def format_analyst_response(response_text: str, category: str) -> str:
    """
    Format response in Voxmill Executive Analyst tone.
    Smart length management: only add header if response fits in single message.
    """
    try:
        # Try to parse as JSON first (V3 format)
        parsed = json.loads(response_text)
        prose_response = parsed.get('response', response_text)
        
    except (json.JSONDecodeError, TypeError):
        # Already plain text or malformed
        prose_response = response_text
    
    # Clean Unicode
    prose_response = ''.join(char for char in prose_response if ord(char) < 0x10000)
    prose_response = '\n'.join(line.strip() for line in prose_response.split('\n') if line.strip())
    
    # Handle PDF request
    if category == "send_pdf":
        return "Sending your latest executive market briefing now.\n\n[PDF URL will be provided here]"
    
    # Category headers
    category_headers = {
        'scenario_modelling': 'SCENARIO ANALYSIS',
        'strategic_outlook': 'STRATEGIC OUTLOOK',
        'comparative_analysis': 'COMPARATIVE INTELLIGENCE',
        'weekly_briefing': 'WEEKLY BRIEFING',
        'analysis_snapshot': 'MARKET ANALYSIS',
        'market_overview': 'MARKET OVERVIEW',
        'competitive_landscape': 'COMPETITIVE LANDSCAPE',
        'opportunities': 'OPPORTUNITIES',
        'price_band': 'PRICE INTELLIGENCE',
        'segment_performance': 'SEGMENT PERFORMANCE'
    }
    
    header = category_headers.get(category, 'VOXMILL INTELLIGENCE')
    separator = '—' * 40
    
    # SMART LENGTH LOGIC
    header_block = f"{header}\n{separator}\n\n"
    full_response = f"{header_block}{prose_response}"
    
    # WhatsApp limit: 1500 chars (safety margin)
    MAX_SINGLE_MESSAGE = 1500
    
    if len(full_response) <= MAX_SINGLE_MESSAGE:
        # Fits in one message - include header
        return full_response
    else:
        # Too long - will be split by whatsapp.py
        # Don't add header here, let splitting logic handle it
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

import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

def format_analyst_response(response_text: str, category: str) -> str:
    """
    Format response in Voxmill Executive Analyst tone.
    Handles both JSON (from V3 LLM) and plain text responses.
    Bulletproof extraction with multiple fallback strategies.
    """
    try:
        # Strip markdown code fences and whitespace
        cleaned = response_text.strip()
        
        # Remove common JSON wrapper patterns
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]
        if cleaned.startswith('```'):
            cleaned = cleaned[3:]
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]
        
        # Remove leading zero-width characters and whitespace
        cleaned = cleaned.lstrip('\u200b\ufeff \n\r\t')
        
        # Try to parse as JSON
        parsed = json.loads(cleaned)
        
        # Extract the prose response from JSON
        prose_response = parsed.get('response', response_text)
        
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        # Check if response contains JSON-like structure
        if '{' in response_text and '"response"' in response_text:
            # Try to extract just the response field manually
            try:
                start = response_text.find('"response":')
                if start != -1:
                    # Find the opening quote of the response value
                    start = response_text.find('"', start + 11) + 1
                    # Find the closing quote (accounting for escaped quotes)
                    end = start
                    escape_next = False
                    while end < len(response_text):
                        if escape_next:
                            escape_next = False
                            end += 1
                            continue
                        if response_text[end] == '\\':
                            escape_next = True
                            end += 1
                            continue
                        if response_text[end] == '"':
                            break
                        end += 1
                    prose_response = response_text[start:end]
                    # Unescape JSON strings
                    prose_response = prose_response.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
                else:
                    prose_response = response_text
            except Exception as extraction_error:
                logger.warning(f"Manual JSON extraction failed: {extraction_error}")
                prose_response = response_text
        else:
            # Already plain text
            prose_response = response_text
    
    # Clean Unicode (preserve your existing logic)
    prose_response = ''.join(char for char in prose_response if ord(char) < 0x10000)
    prose_response = '\n'.join(line.strip() for line in prose_response.split('\n') if line.strip())
    
    # Handle PDF request
    if category == "send_pdf":
        return "Sending your latest executive market briefing now.\n\n[PDF URL will be provided here]"
    
    # Add institutional category header
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
    
    # Institutional formatting: header + separator + content
    return f"{header}\n{'—' * 40}\n\n{prose_response}"


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

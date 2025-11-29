import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def format_analyst_response(response_text: str, category: str) -> str:
    """Format response in Voxmill Executive Analyst tone"""
    response_text = ''.join(char for char in response_text if ord(char) < 0x10000)
    response_text = '\n'.join(line.strip() for line in response_text.split('\n') if line.strip())
    
    if category == "send_pdf":
        return "Sending your latest executive market briefing now.\n\n[PDF URL will be provided here]"
    
    return response_text

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

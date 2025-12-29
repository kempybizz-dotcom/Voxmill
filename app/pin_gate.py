"""PIN GATE - Extracted from whatsapp.py"""
import logging
from typing import Optional, Tuple
from app.pin_auth import PINAuthenticator, get_pin_status_message, get_pin_response_message, sync_pin_status_to_airtable

logger = logging.getLogger(__name__)

async def handle_pin_gate(sender: str, message: str, client_profile: dict) -> Optional[str]:
    """
    Handle entire PIN authentication flow.
    Returns response to send, or None if PIN gate passed.
    """
    needs_verification, reason = PINAuthenticator.check_needs_verification(sender)
    
    if not needs_verification:
        return None  # Gate passed
    
    client_name = client_profile.get('name', 'there')
    
    # Handle PIN setup (not set)
    if reason == "not_set":
        if len(message.strip()) == 4 and message.strip().isdigit():
            success, msg = PINAuthenticator.set_pin(sender, message.strip())
            if success:
                await sync_pin_status_to_airtable(sender, "Active")
            return get_pin_response_message(success, msg, client_name)
        else:
            return get_pin_status_message("not_set", client_name)
    
    # Handle locked account
    elif reason == "locked":
        return get_pin_status_message("locked", client_name)
    
    # Handle re-verification
    else:
        if len(message.strip()) == 4 and message.strip().isdigit():
            success, msg = PINAuthenticator.verify_and_unlock(sender, message.strip())
            if success:
                await sync_pin_status_to_airtable(sender, "Active")
            return get_pin_response_message(success, msg, client_name)
        else:
            return get_pin_status_message(reason, client_name)

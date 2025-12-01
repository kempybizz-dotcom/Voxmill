import os
import logging
import httpx
from twilio.rest import Client
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction

logger = logging.getLogger(__name__)

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None

async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler with V3 predictive intelligence
    """
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        # Load client profile
        from app.client_manager import get_client_profile, update_client_history
        client_profile = get_client_profile(sender)
        
        # Get preferred region from profile
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        
        # Load dataset for preferred region
        dataset = load_dataset(area=preferred_region)
        
        # Classify and respond (with client context) - V3 returns metadata
        category, response_text, response_metadata = await classify_and_respond(
            message_text, 
            dataset,
            client_profile=client_profile
        )
        
        # Format response
        formatted_response = format_analyst_response(response_text, category)
        
        # Send via Twilio (with smart chunking)
        await send_twilio_message(sender, formatted_response)
        
        # Log interaction and update client history
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"Message processed: {category} | Confidence: {response_metadata.get('confidence_level')} | Urgency: {response_metadata.get('recommendation_urgency')}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "Unable to process your request at this time. Please try again shortly."
        await send_twilio_message(sender, error_msg)


async def send_twilio_message(recipient: str, message: str):
    """Send message via Twilio WhatsApp API with auto-chunking for long messages"""
    try:
        if not twilio_client:
            logger.error("Twilio client not initialized")
            return
        
        # WhatsApp limit: 1600 characters
        MAX_LENGTH = 1500  # Safety margin
        
        if len(message) <= MAX_LENGTH:
            # Send as single message
            twilio_client.messages.create(
                from_=TWILIO_WHATSAPP_NUMBER,
                to=recipient,
                body=message
            )
            logger.info(f"Message sent successfully to {recipient} ({len(message)} chars)")
        else:
            # Split intelligently at paragraph breaks
            chunks = smart_split_message(message, MAX_LENGTH)
            
            for i, chunk in enumerate(chunks, 1):
                if i == 1:
                    # First chunk - include original header
                    chunk_message = chunk
                else:
                    # Subsequent chunks - add continuation marker
                    chunk_message = f"[Part {i}/{len(chunks)}]\n\n{chunk}"
                
                twilio_client.messages.create(
                    from_=TWILIO_WHATSAPP_NUMBER,
                    to=recipient,
                    body=chunk_message
                )
                
                # Small delay between messages to maintain order
                import asyncio
                await asyncio.sleep(0.5)
            
            logger.info(f"Multi-part message sent successfully to {recipient} ({len(chunks)} parts, {len(message)} total chars)")
                
    except Exception as e:
        logger.error(f"Error sending Twilio message: {str(e)}", exc_info=True)
        raise


def smart_split_message(message: str, max_length: int) -> list:
    """
    Split message intelligently at natural break points.
    Priority: double line breaks > single line breaks > sentences > words
    """
    if len(message) <= max_length:
        return [message]
    
    chunks = []
    remaining = message
    
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining)
            break
        
        # Try to split at double line break (paragraph boundary)
        chunk = remaining[:max_length]
        split_point = chunk.rfind('\n\n')
        
        if split_point == -1:
            # Try single line break
            split_point = chunk.rfind('\n')
        
        if split_point == -1:
            # Try sentence end
            split_point = max(
                chunk.rfind('. '),
                chunk.rfind('! '),
                chunk.rfind('? ')
            )
        
        if split_point == -1:
            # Last resort: split at word boundary
            split_point = chunk.rfind(' ')
        
        if split_point == -1:
            # Absolute last resort: hard cut
            split_point = max_length
        
        chunks.append(remaining[:split_point].strip())
        remaining = remaining[split_point:].strip()
    
    return chunks

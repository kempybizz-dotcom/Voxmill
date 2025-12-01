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
        
        
        # Send via Twilio
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
    """Send message via Twilio WhatsApp API"""
    try:
        if not twilio_client:
            logger.error("Twilio client not initialized")
            return
        
        twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            to=recipient,
            body=message
        )
        
        logger.info(f"Message sent successfully to {recipient}")
                
    except Exception as e:
        logger.error(f"Error sending Twilio message: {str(e)}", exc_info=True)
        raise

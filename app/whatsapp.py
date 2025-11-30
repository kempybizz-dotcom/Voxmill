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
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")  # e.g., "whatsapp:+14155238886"

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None

async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler:
    1. Load latest dataset
    2. Classify message intent
    3. Generate LLM response
    4. Send reply via Twilio
    5. Log interaction
    """
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        dataset = load_dataset()
        
        category, response_text = await classify_and_respond(message_text, dataset)
        
        formatted_response = format_analyst_response(response_text, category)
        
        await send_twilio_message(sender, formatted_response)
        
        log_interaction(sender, message_text, category, formatted_response)
        
        logger.info(f"Message processed successfully: {category}")
        
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

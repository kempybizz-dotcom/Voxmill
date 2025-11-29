import os
import logging
import httpx
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction

logger = logging.getLogger(__name__)

ULTRAMSG_INSTANCE_ID = os.getenv("ULTRAMSG_INSTANCE_ID")
ULTRAMSG_TOKEN = os.getenv("ULTRAMSG_TOKEN")
ULTRAMSG_API_URL = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE_ID}/messages/chat"

async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler:
    1. Load latest dataset
    2. Classify message intent
    3. Generate LLM response
    4. Send reply via UltraMsg
    5. Log interaction
    """
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        dataset = load_dataset()
        
        category, response_text = await classify_and_respond(message_text, dataset)
        
        formatted_response = format_analyst_response(response_text, category)
        
        await send_ultramsg_message(sender, formatted_response)
        
        log_interaction(sender, message_text, category, formatted_response)
        
        logger.info(f"Message processed successfully: {category}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "Unable to process your request at this time. Please try again shortly."
        await send_ultramsg_message(sender, error_msg)

async def send_ultramsg_message(recipient: str, message: str):
    """Send message via UltraMsg API"""
    try:
        payload = {
            "token": ULTRAMSG_TOKEN,
            "to": recipient,
            "body": message
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                ULTRAMSG_API_URL,
                data=payload,
                timeout=30.0
            )
            
            if response.status_code != 200:
                logger.error(f"UltraMsg API error: {response.status_code} - {response.text}")
            else:
                logger.info(f"Message sent successfully to {recipient}")
                
    except Exception as e:
        logger.error(f"Error sending UltraMsg message: {str(e)}", exc_info=True)
        raise

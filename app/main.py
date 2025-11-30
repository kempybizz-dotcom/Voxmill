from fastapi import FastAPI, Request, Form
from fastapi.responses import Response
import logging
from app.whatsapp import handle_whatsapp_message

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

app = FastAPI(title="Voxmill WhatsApp Executive Analyst")

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(
    From: str = Form(...),
    Body: str = Form(...)
):
    """
    Receives WhatsApp messages from Twilio.
    Twilio sends form data with From (sender) and Body (message text).
    """
    try:
        logger.info(f"Received Twilio webhook from {From}: {Body}")
        
        if From and Body:
            await handle_whatsapp_message(From, Body)
        
        # Twilio expects empty 200 response
        return Response(status_code=200)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return Response(status_code=200)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "voxmill-whatsapp-analyst",
        "version": "1.0.0"
    }

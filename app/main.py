from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import logging
from app.whatsapp import handle_whatsapp_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Voxmill WhatsApp Executive Analyst")

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    """
    Receives WhatsApp messages from Meta Cloud API.
    Processes message, loads dataset, classifies intent, sends LLM response.
    """
    try:
        body = await request.json()
        logger.info(f"Received webhook: {body}")
        
        if "entry" not in body:
            logger.warning("Invalid webhook structure - no entry field")
            return JSONResponse(content={"status": "ignored"}, status_code=200)
        
        for entry in body.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])
                
                if not messages:
                    continue
                
                message = messages[0]
                sender = message.get("from")
                message_type = message.get("type")
                
                if message_type != "text":
                    logger.info(f"Ignoring non-text message type: {message_type}")
                    continue
                
                message_text = message.get("text", {}).get("body", "")
                
                if sender and message_text:
                    await handle_whatsapp_message(sender, message_text)
        
        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return JSONResponse(content={"status": "error"}, status_code=200)

@app.get("/webhook/whatsapp")
async def whatsapp_verification(request: Request):
    """
    WhatsApp webhook verification endpoint.
    """
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    import os
    verify_token = os.getenv("WHATSAPP_VERIFY_TOKEN", "voxmill_secure_token")
    
    if mode == "subscribe" and token == verify_token:
        logger.info("Webhook verified successfully")
        return JSONResponse(content=int(challenge), status_code=200)
    else:
        logger.warning("Webhook verification failed")
        raise HTTPException(status_code=403, detail="Verification failed")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "voxmill-whatsapp-analyst",
        "version": "1.0.0"
    }

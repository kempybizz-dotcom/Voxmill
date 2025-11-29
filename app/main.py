from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
from app.whatsapp import handle_whatsapp_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Voxmill WhatsApp Executive Analyst")

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    try:
        body = await request.json()
        logger.info(f"Received webhook")
        
        if "entry" not in body:
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
                    continue
                
                message_text = message.get("text", {}).get("body", "")
                
                if sender and message_text:
                    await handle_whatsapp_message(sender, message_text)
        
        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return JSONResponse(content={"status": "error"}, status_code=200)

@app.get("/webhook/whatsapp")
async def whatsapp_verification(request: Request):
    import os
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    verify_token = os.getenv("WHATSAPP_VERIFY_TOKEN", "voxmill_secure_token")
    
    if mode == "subscribe" and token == verify_token:
        logger.info("Webhook verified")
        return JSONResponse(content=int(challenge), status_code=200)
    else:
        return JSONResponse(content={"error": "verification failed"}, status_code=403)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "voxmill-whatsapp-analyst", "version": "1.0.0"}

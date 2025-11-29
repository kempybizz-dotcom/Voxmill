from fastapi import FastAPI, Request, Form
from fastapi.responses import JSONResponse
import logging
from app.whatsapp import handle_whatsapp_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Voxmill WhatsApp Executive Analyst")

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(
    data: str = Form(...),
    from_: str = Form(None, alias="from")
):
    """
    Receives WhatsApp messages from UltraMsg.
    UltraMsg sends form data, not JSON.
    """
    try:
        logger.info(f"Received UltraMsg webhook from {from_}: {data}")
        
        if from_ and data:
            await handle_whatsapp_message(from_, data)
        
        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        return JSONResponse(content={"status": "error"}, status_code=200)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "voxmill-whatsapp-analyst",
        "version": "1.0.0"
    }
```

---

## 🎯 DEPLOYMENT STEPS

### **1. Update GitHub Files**

- Replace `app/whatsapp.py` with UltraMsg version
- Replace `app/main.py` with UltraMsg version
- Commit: `Switch to UltraMsg for V1 shipping speed`

### **2. Update Render Environment Variables**

Render Dashboard → voxmill-whatsapp → Environment

**Remove:**
- `WHATSAPP_TOKEN`
- `WHATSAPP_PHONE_ID`
- `WHATSAPP_VERIFY_TOKEN`

**Add:**
- `ULTRAMSG_INSTANCE_ID` = your instance ID
- `ULTRAMSG_TOKEN` = your API token

### **3. Configure Webhook in UltraMsg**

UltraMsg Dashboard → Settings → Webhooks

**Webhook URL:**
```
https://voxmill-whatsapp.onrender.com/webhook/whatsapp

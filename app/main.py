from fastapi import FastAPI, Request, Form
from fastapi.responses import Response
import logging
import asyncio
from app.whatsapp import handle_whatsapp_message

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

app = FastAPI(title="Voxmill WhatsApp Executive Analyst")

# Background task reference
background_tasks = set()

@app.on_event("startup")
async def startup_event():
    """
    Start background intelligence engine on server startup
    """
    try:
        from app.scheduler import daily_intelligence_cycle
        
        # Create background task
        task = asyncio.create_task(daily_intelligence_cycle())
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        
        logger.info("✓ Daily intelligence scheduler started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanup on shutdown
    """
    logger.info("Shutting down background tasks...")
    for task in background_tasks:
        task.cancel()


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    """
    Receives WhatsApp messages from Twilio.
    Handles text, media, and edge cases.
    """
    try:
        # Parse form data
        form_data = await request.form()
        
        # Log all received fields
        logger.info(f"Received webhook with fields: {dict(form_data)}")
        
        # Extract sender and message
        sender = (
            form_data.get('From') or 
            form_data.get('from') or 
            form_data.get('sender')
        )
        
        message = (
            form_data.get('Body') or 
            form_data.get('body') or 
            form_data.get('message') or
            form_data.get('text')
        )
        
        # Check for media attachments
        num_media = int(form_data.get('NumMedia', 0))
        
        # HANDLE MEDIA MESSAGES
        if num_media > 0:
            media_response = (
                "I've received your media file. Currently, I focus on text-based market intelligence queries. "
                "For document analysis, please describe what you'd like me to examine or send specific data points as text."
            )
            
            if sender:
                from app.whatsapp import send_twilio_message
                await send_twilio_message(sender, media_response)
            
            return Response(status_code=200)
        
        # Log extracted values
        logger.info(f"Extracted - Sender: {sender}, Message: {message}")
        
        if sender and message:
            await handle_whatsapp_message(sender, message)
            logger.info(f"Successfully processed message from {sender}")
        else:
            logger.warning(f"Missing required fields - Sender: {sender}, Message: {message}")
        
        # Twilio expects empty 200 response
        return Response(status_code=200)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        # Still return 200 to Twilio to prevent retries
        return Response(status_code=200)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "voxmill-whatsapp-analyst",
        "version": "1.0.0",
        "background_tasks_active": len(background_tasks)
    }


@app.post("/trigger-intelligence-cycle")
async def manual_trigger():
    """
    Manual endpoint to trigger intelligence cycle
    Useful for testing before deploying scheduler
    """
    try:
        from app.scrapers.competitor_tracker import track_competitor_prices
        from app.alerts.alert_engine import send_market_alerts
        
        logger.info("Manual intelligence cycle triggered")
        
        # Track competitors
        alerts = await track_competitor_prices(area="Mayfair")
        
        # Send alerts
        if alerts:
            await send_market_alerts(alerts)
        
        return {
            "status": "success",
            "alerts_generated": len(alerts),
            "alerts": alerts
        }
    
    except Exception as e:
        logger.error(f"Error in manual trigger: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": str(e)
        }

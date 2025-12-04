"""
Voxmill WhatsApp Intelligence Service - Main Application
FastAPI backend handling Twilio webhooks, MongoDB data, and 5-layer intelligence stack
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional
import json

from fastapi import FastAPI, Request, Response, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse
import redis

# Import local modules
from app.whatsapp import (
    process_incoming_message,
    send_whatsapp_message,
    handle_first_time_user,
    detect_pdf_request
)
from app.dataset_loader import load_latest_dataset, get_agent_snapshot_history
from app.client_manager import (
    get_client_profile,
    update_client_profile,
    increment_message_count,
    check_rate_limit
)
from app.llm import generate_gpt4_response
from app.models import IncomingMessage, ClientProfile
from app.scheduler import start_scheduler, stop_scheduler
from app.utils import normalize_phone_number

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Voxmill WhatsApp Intelligence API",
    description="AI-powered market intelligence via WhatsApp",
    version="2.0.0"
)

# Initialize Redis client for deduplication
REDIS_URL = os.getenv("REDIS_URL")
redis_client = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

if redis_client:
    logger.info("✅ Redis connected for webhook deduplication")
else:
    logger.warning("⚠️  Redis not configured - duplicate message prevention disabled")

# Environment validation
REQUIRED_ENV_VARS = [
    'TWILIO_ACCOUNT_SID',
    'TWILIO_AUTH_TOKEN',
    'TWILIO_WHATSAPP_NUMBER',
    'OPENAI_API_KEY',
    'MONGODB_URI'
]

missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    logger.error(f"❌ Missing required environment variables: {missing_vars}")
    sys.exit(1)

logger.info("✅ All required environment variables present")

# Global scheduler reference
scheduler = None


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global scheduler
    
    logger.info("🚀 Starting Voxmill WhatsApp Intelligence Service...")
    
    # Start background scheduler for data collection
    try:
        scheduler = start_scheduler()
        logger.info("✅ Background scheduler started")
    except Exception as e:
        logger.error(f"❌ Failed to start scheduler: {e}")
    
    logger.info("✅ Voxmill service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global scheduler
    
    logger.info("🛑 Shutting down Voxmill service...")
    
    if scheduler:
        stop_scheduler(scheduler)
        logger.info("✅ Scheduler stopped")
    
    logger.info("✅ Shutdown complete")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Voxmill WhatsApp Intelligence API",
        "status": "operational",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "redis": "connected" if redis_client else "not configured",
        "scheduler": "running" if scheduler and scheduler.running else "stopped"
    }


@app.get("/health/intelligence")
async def intelligence_health_check():
    """
    FIX #10: Intelligence layer health check endpoint
    Tests all critical dependencies and intelligence modules
    """
    health_status = {
        "overall_status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    overall_healthy = True
    
    # Test MongoDB connection
    try:
        from app.dataset_loader import get_mongodb_client
        client = get_mongodb_client()
        client.admin.command('ping')
        health_status["components"]["mongodb"] = {"status": "healthy", "message": "Connection successful"}
    except Exception as e:
        health_status["components"]["mongodb"] = {"status": "unhealthy", "error": str(e)}
        overall_healthy = False
    
    # Test Redis connection
    if redis_client:
        try:
            redis_client.ping()
            health_status["components"]["redis"] = {"status": "healthy", "message": "Connection successful"}
        except Exception as e:
            health_status["components"]["redis"] = {"status": "unhealthy", "error": str(e)}
            overall_healthy = False
    else:
        health_status["components"]["redis"] = {"status": "not_configured", "message": "Redis not enabled"}
    
    # Test intelligence layer imports
    intelligence_layers = [
        "app.intelligence.trend_detector",
        "app.intelligence.agent_profiler",
        "app.intelligence.micromarket_segmenter",
        "app.intelligence.liquidity_velocity",
        "app.intelligence.cascade_predictor"
    ]
    
    for layer_module in intelligence_layers:
        try:
            __import__(layer_module)
            layer_name = layer_module.split(".")[-1]
            health_status["components"][layer_name] = {"status": "healthy", "message": "Module loaded"}
        except Exception as e:
            layer_name = layer_module.split(".")[-1]
            health_status["components"][layer_name] = {"status": "unhealthy", "error": str(e)}
            overall_healthy = False
    
    # Test OpenAI client
    try:
        import openai
        openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        # Don't actually call API, just verify client initializes
        health_status["components"]["openai"] = {"status": "healthy", "message": "Client initialized"}
    except Exception as e:
        health_status["components"]["openai"] = {"status": "unhealthy", "error": str(e)}
        overall_healthy = False
    
    # Test Twilio client
    try:
        from twilio.rest import Client
        twilio_client = Client(
            os.getenv("TWILIO_ACCOUNT_SID"),
            os.getenv("TWILIO_AUTH_TOKEN")
        )
        # Verify client initializes
        health_status["components"]["twilio"] = {"status": "healthy", "message": "Client initialized"}
    except Exception as e:
        health_status["components"]["twilio"] = {"status": "unhealthy", "error": str(e)}
        overall_healthy = False
    
    health_status["overall_status"] = "healthy" if overall_healthy else "degraded"
    
    return health_status


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Main Twilio WhatsApp webhook endpoint
    FIX #2: Added webhook deduplication via Redis
    FIX #8: Added first-time user race condition fix
    """
    try:
        # Parse incoming webhook data
        form_data = await request.form()
        
        message_sid = form_data.get('MessageSid')
        sender = form_data.get('From', '')
        message_body = form_data.get('Body', '').strip()
        
        # FIX #2: WEBHOOK DEDUPLICATION
        if redis_client and message_sid:
            # Check if we've already processed this message
            cache_key = f"webhook_processed:{message_sid}"
            if redis_client.get(cache_key):
                logger.info(f"⚠️  Duplicate webhook ignored: {message_sid}")
                return PlainTextResponse("OK", status_code=200)
            
            # Mark as processed with 60s TTL (Twilio stops retrying after ~30s)
            redis_client.setex(cache_key, 60, "1")
        
        # Validate required fields
        if not sender or not message_body:
            logger.warning(f"⚠️  Empty message from {sender}")
            return PlainTextResponse("OK", status_code=200)
        
        # Normalize phone number
        normalized_sender = normalize_phone_number(sender)
        
        logger.info(f"📱 Incoming message from {normalized_sender}: {message_body[:100]}")
        
        # Get or create client profile
        client_profile = get_client_profile(normalized_sender)
        
        # FIX #8: IMPROVED FIRST-TIME USER DETECTION
        is_first_time = False
        if not client_profile or not client_profile.get('message_count', 0):
            is_first_time = True
        
        # Check rate limit
        if not check_rate_limit(normalized_sender):
            rate_limit_msg = (
                "⚠️ *Rate Limit Reached*\n\n"
                "You've reached your message limit for this period. "
                "Upgrade your plan for unlimited access.\n\n"
                "Reply UPGRADE for pricing options."
            )
            send_whatsapp_message(normalized_sender, rate_limit_msg)
            return PlainTextResponse("OK", status_code=200)
        
        # Handle first-time users (with NO sleep to avoid race condition)
        if is_first_time:
            handle_first_time_user(normalized_sender)
            # Increment message count immediately to prevent duplicate welcome
            increment_message_count(normalized_sender)
            return PlainTextResponse("OK", status_code=200)
        
        # Increment message count for returning users
        increment_message_count(normalized_sender)
        
        # Check if user is requesting PDF report
        if detect_pdf_request(message_body):
            # TODO: Implement PDF generation and send
            response_text = (
                "📊 *PDF Report Generation*\n\n"
                "Generating your complete market intelligence PDF report...\n\n"
                "This will be available in the next release. "
                "For now, you can access all data via text queries."
            )
            send_whatsapp_message(normalized_sender, response_text)
            return PlainTextResponse("OK", status_code=200)
        
        # Process message in background to avoid Twilio timeout
        background_tasks.add_task(
            process_message_async,
            normalized_sender,
            message_body,
            client_profile
        )
        
        # Return 200 immediately to Twilio
        return PlainTextResponse("OK", status_code=200)
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}", exc_info=True)
        return PlainTextResponse("OK", status_code=200)  # Always return 200 to Twilio


async def process_message_async(sender: str, message_body: str, client_profile: dict):
    """
    Process message asynchronously to avoid webhook timeout
    """
    try:
        # Process the message (includes GPT-4 call with 15s timeout)
        response_text = process_incoming_message(sender, message_body, client_profile)
        
        # Send response back to user
        send_whatsapp_message(sender, response_text)
        
    except Exception as e:
        logger.error(f"❌ Error processing message for {sender}: {e}", exc_info=True)
        error_msg = (
            "⚠️ Sorry, I encountered an error processing your request. "
            "Our team has been notified. Please try again in a moment."
        )
        send_whatsapp_message(sender, error_msg)


@app.get("/webhook/whatsapp")
async def whatsapp_webhook_get(request: Request):
    """
    Handle Twilio webhook verification (GET request)
    """
    return PlainTextResponse("Voxmill WhatsApp Webhook Active", status_code=200)


@app.post("/admin/broadcast")
async def admin_broadcast(request: Request):
    """
    Admin endpoint to send broadcast messages
    Requires admin auth token
    """
    # TODO: Implement admin authentication
    try:
        data = await request.json()
        
        recipients = data.get('recipients', [])
        message = data.get('message', '')
        
        if not recipients or not message:
            raise HTTPException(status_code=400, detail="Missing recipients or message")
        
        sent_count = 0
        failed_count = 0
        
        for recipient in recipients:
            try:
                send_whatsapp_message(recipient, message)
                sent_count += 1
            except Exception as e:
                logger.error(f"Failed to send to {recipient}: {e}")
                failed_count += 1
        
        return {
            "status": "completed",
            "sent": sent_count,
            "failed": failed_count
        }
        
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data/latest")
async def get_latest_data(area: Optional[str] = None):
    """
    Get latest market data for testing/debugging
    """
    try:
        dataset = load_latest_dataset(area)
        
        if not dataset:
            return {"error": "No data found", "area": area}
        
        return {
            "area": dataset.get("metadata", {}).get("area"),
            "timestamp": dataset.get("metadata", {}).get("analysis_timestamp"),
            "property_count": len(dataset.get("properties", [])),
            "has_intelligence": "intelligence" in dataset
        }
        
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/client/{phone}")
async def get_client_info(phone: str):
    """
    Get client profile for testing/debugging
    """
    try:
        normalized_phone = normalize_phone_number(phone)
        profile = get_client_profile(normalized_phone)
        
        if not profile:
            return {"error": "Client not found", "phone": normalized_phone}
        
        # Remove sensitive data
        safe_profile = {
            "phone": profile.get("whatsapp_number"),
            "tier": profile.get("tier"),
            "message_count": profile.get("message_count"),
            "areas_of_interest": profile.get("areas_of_interest"),
            "created_at": profile.get("created_at"),
            "last_active": profile.get("last_active")
        }
        
        return safe_profile
        
    except Exception as e:
        logger.error(f"Error fetching client: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Import routes
try:
    from app.routes.onboarding import router as onboarding_router
    from app.routes.stripe_webhooks import router as stripe_router
    from app.routes.testing import router as testing_router
    
    app.include_router(onboarding_router, prefix="/onboarding", tags=["onboarding"])
    app.include_router(stripe_router, prefix="/stripe", tags=["stripe"])
    app.include_router(testing_router, prefix="/test", tags=["testing"])
    
    logger.info("✅ Additional routes loaded")
except ImportError as e:
    logger.warning(f"⚠️  Could not load additional routes: {e}")


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,  # Disable in production
        log_level="info"
    )

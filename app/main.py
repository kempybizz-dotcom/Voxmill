"""
Voxmill WhatsApp Intelligence Service - Main Application
FastAPI backend handling Twilio webhooks, MongoDB data, and 5-layer intelligence stack
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import PlainTextResponse

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
redis_client = None

try:
    import redis
    if REDIS_URL:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✅ Redis connected for webhook deduplication")
except ImportError:
    logger.warning("⚠️  Redis not installed")
except Exception as e:
    logger.warning(f"⚠️  Redis not configured: {e}")

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


def normalize_phone_number(phone: str) -> str:
    """
    Normalize phone number format for WhatsApp
    Remove whatsapp: prefix and ensure + prefix
    """
    if not phone:
        return phone
    
    # Remove whatsapp: prefix if present (also handle URL-encoded version)
    phone = phone.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    # Ensure it starts with +
    if not phone.startswith('+'):
        phone = '+' + phone
    
    return phone


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 Starting Voxmill WhatsApp Intelligence Service...")
    
    # Start background scheduler for data collection
    try:
        from app.scheduler import daily_intelligence_cycle
        import asyncio
        asyncio.create_task(daily_intelligence_cycle())
        logger.info("✅ Background scheduler started")
    except Exception as e:
        logger.warning(f"⚠️  Scheduler not started: {e}")
    
    logger.info("✅ Voxmill service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down Voxmill service...")
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
        "redis": "connected" if redis_client else "not configured"
    }


@app.get("/health/intelligence")
async def intelligence_health_check():
    """
    Intelligence layer health check endpoint
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
        from pymongo import MongoClient
        MONGODB_URI = os.getenv("MONGODB_URI")
        if MONGODB_URI:
            client = MongoClient(MONGODB_URI)
            client.admin.command('ping')
            health_status["components"]["mongodb"] = {"status": "healthy", "message": "Connection successful"}
        else:
            health_status["components"]["mongodb"] = {"status": "not_configured", "message": "MONGODB_URI not set"}
            overall_healthy = False
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
    With webhook deduplication via Redis
    """
    try:
        # Parse incoming webhook data
        form_data = await request.form()
        
        message_sid = form_data.get('MessageSid')
        sender = form_data.get('From', '')
        message_body = form_data.get('Body', '').strip()
        
        # WEBHOOK DEDUPLICATION
        if redis_client and message_sid:
            cache_key = f"webhook_processed:{message_sid}"
            if redis_client.get(cache_key):
                logger.info(f"⚠️  Duplicate webhook ignored: {message_sid}")
                return PlainTextResponse("OK", status_code=200)
            
            # Mark as processed with 60s TTL
            redis_client.setex(cache_key, 60, "1")
        
        # Validate required fields
        if not sender or not message_body:
            logger.warning(f"⚠️  Empty message from {sender}")
            return PlainTextResponse("OK", status_code=200)
        
        # Normalize phone number
        normalized_sender = normalize_phone_number(sender)
        
        logger.info(f"📱 Incoming message from {normalized_sender}: {message_body[:100]}")
        
        # Check rate limit using actual function signature
        from app.client_manager import check_rate_limit
        allowed, rate_limit_message = check_rate_limit(normalized_sender)
        
        if not allowed:
            # Send rate limit message via Twilio
            from app.whatsapp import send_twilio_message
            await send_twilio_message(normalized_sender, f"⚠️ {rate_limit_message}")
            return PlainTextResponse("OK", status_code=200)
        
        # Process message in background to avoid Twilio timeout
        background_tasks.add_task(
            process_message_async,
            normalized_sender,
            message_body
        )
        
        # Return 200 immediately to Twilio
        return PlainTextResponse("OK", status_code=200)
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}", exc_info=True)
        return PlainTextResponse("OK", status_code=200)


async def process_message_async(sender: str, message_body: str):
    """
    Process message asynchronously to avoid webhook timeout
    """
    try:
        # Import here to avoid startup crashes if module has issues
        from app.whatsapp import handle_whatsapp_message
        
        # Process message - handle_whatsapp_message does everything:
        # - First-time welcome detection
        # - PDF request detection
        # - Intelligence layer loading
        # - Response generation
        # - Message sending
        await handle_whatsapp_message(sender, message_body)
        
    except Exception as e:
        logger.error(f"❌ Error processing message for {sender}: {e}", exc_info=True)
        
        # Try to send error message
        try:
            from app.whatsapp import send_twilio_message
            error_msg = (
                "⚠️ Sorry, I encountered an error processing your request. "
                "Our team has been notified. Please try again in a moment."
            )
            await send_twilio_message(sender, error_msg)
        except:
            logger.error("Failed to send error message to user")


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
    """
    try:
        data = await request.json()
        
        recipients = data.get('recipients', [])
        message = data.get('message', '')
        
        if not recipients or not message:
            raise HTTPException(status_code=400, detail="Missing recipients or message")
        
        from app.whatsapp import send_twilio_message
        
        sent_count = 0
        failed_count = 0
        
        for recipient in recipients:
            try:
                await send_twilio_message(recipient, message)
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
        from app.dataset_loader import load_dataset
        
        dataset = load_dataset(area if area else "Mayfair")
        
        if not dataset or dataset.get('error'):
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
        from app.client_manager import get_client_profile
        
        # Normalize the phone number (remove whatsapp: prefix)
        normalized_phone = normalize_phone_number(phone)
        profile = get_client_profile(normalized_phone)
        
        if not profile:
            return {"error": "Client not found", "phone": normalized_phone}
        
        # Remove sensitive data and MongoDB _id
        safe_profile = {
            "phone": profile.get("whatsapp_number"),
            "tier": profile.get("tier"),
            "total_queries": profile.get("total_queries"),
            "last_region_queried": profile.get("last_region_queried"),
            "created_at": profile.get("created_at").isoformat() if profile.get("created_at") else None,
            "preferences": profile.get("preferences", {})
        }
        
        return safe_profile
        
    except Exception as e:
        logger.error(f"Error fetching client: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Import additional routes (optional, won't crash if missing)
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
        reload=False,
        log_level="info"
    )

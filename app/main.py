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
    )        task.cancel()


@app.get("/")
async def root():
    """
    Root endpoint
    """
    return {
        "service": "Voxmill WhatsApp Intelligence",
        "version": "3.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "whatsapp_webhook": "/webhook/whatsapp",
            "onboarding": "/onboarding/create_client",
            "stripe_webhook": "/stripe/webhook",
            "self_test": "/test/register_self"
        }
    }


@app.get("/health/intelligence")
async def intelligence_health_check():
    """
    Check health of all intelligence layers
    """
    health = {
        'overall_status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'layers': {}
    }
    
    # Test MongoDB connection
    try:
        from app.dataset_loader import mongo_client
        if mongo_client:
            mongo_client.admin.command('ping')
            health['layers']['mongodb'] = {'status': 'healthy', 'message': 'Connected'}
        else:
            health['layers']['mongodb'] = {'status': 'unhealthy', 'message': 'Not configured'}
            health['overall_status'] = 'degraded'
    except Exception as e:
        health['layers']['mongodb'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Redis connection
    try:
        if redis_client:
            redis_client.ping()
            health['layers']['redis'] = {'status': 'healthy', 'message': 'Connected'}
        else:
            health['layers']['redis'] = {'status': 'unhealthy', 'message': 'Not configured'}
            health['overall_status'] = 'degraded'
    except Exception as e:
        health['layers']['redis'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Trend Detector
    try:
        from app.intelligence.trend_detector import detect_market_trends
        trends = detect_market_trends(area="Mayfair", lookback_days=1)
        health['layers']['trend_detector'] = {'status': 'healthy', 'trends_found': len(trends)}
    except Exception as e:
        health['layers']['trend_detector'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Agent Profiler
    try:
        from app.intelligence.agent_profiler import AGENT_ARCHETYPES
        health['layers']['agent_profiler'] = {'status': 'healthy', 'archetypes': len(AGENT_ARCHETYPES)}
    except Exception as e:
        health['layers']['agent_profiler'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Micromarket Segmenter
    try:
        from app.intelligence.micromarket_segmenter import segment_micromarkets
        health['layers']['micromarket_segmenter'] = {'status': 'healthy', 'message': 'Module loaded'}
    except Exception as e:
        health['layers']['micromarket_segmenter'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Liquidity Velocity
    try:
        from app.intelligence.liquidity_velocity import calculate_liquidity_velocity
        health['layers']['liquidity_velocity'] = {'status': 'healthy', 'message': 'Module loaded'}
    except Exception as e:
        health['layers']['liquidity_velocity'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Cascade Predictor
    try:
        from app.intelligence.cascade_predictor import build_agent_network
        health['layers']['cascade_predictor'] = {'status': 'healthy', 'message': 'Module loaded'}
    except Exception as e:
        health['layers']['cascade_predictor'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test OpenAI connection
    try:
        from app.llm import openai_client
        if openai_client:
            health['layers']['openai'] = {'status': 'healthy', 'message': 'Client configured'}
        else:
            health['layers']['openai'] = {'status': 'unhealthy', 'message': 'Not configured'}
            health['overall_status'] = 'degraded'
    except Exception as e:
        health['layers']['openai'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    # Test Twilio connection
    try:
        from app.whatsapp import twilio_client
        if twilio_client:
            health['layers']['twilio'] = {'status': 'healthy', 'message': 'Client configured'}
        else:
            health['layers']['twilio'] = {'status': 'unhealthy', 'message': 'Not configured'}
            health['overall_status'] = 'degraded'
    except Exception as e:
        health['layers']['twilio'] = {'status': 'unhealthy', 'message': str(e)}
        health['overall_status'] = 'degraded'
    
    return health


@app.post("/webhook/whatsapp", response_class=PlainTextResponse)
async def whatsapp_webhook(request: Request):
    """
    Twilio WhatsApp webhook handler with full access control
    
    Flow:
    1. Parse incoming message
    2. Extract sender phone number
    3. Rate limit check (Tier 1: blocked, Tier 2: 10/day, Tier 3: unlimited)
    4. Status check (active/inactive)
    5. Process intelligence request
    6. Send response via Twilio
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
            
            return PlainTextResponse("OK", status_code=200)
        
        # Log extracted values
        logger.info(f"Extracted - Sender: {sender}, Message: {message}")
        
        if not sender:
            logger.warning("No sender information in webhook")
            return PlainTextResponse("OK", status_code=200)
        
        # RATE LIMITING & ACCESS CONTROL
        from app.client_manager import check_rate_limit
        
        allowed, error_msg = check_rate_limit(sender)
        
        if not allowed:
            # Send error message to user
            from app.whatsapp import send_twilio_message
            await send_twilio_message(sender, error_msg)
            logger.info(f"Access denied for {sender}: {error_msg}")
            return PlainTextResponse("OK", status_code=200)
        
        # PROCESS MESSAGE
        if message:
            await handle_whatsapp_message(sender, message)
            logger.info(f"Successfully processed message from {sender}")
        else:
            logger.warning(f"Empty message from {sender}")
        
        # Twilio expects 200 response
        return PlainTextResponse("OK", status_code=200)
    
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}", exc_info=True)
        # Still return 200 to Twilio to prevent retries
        return PlainTextResponse("OK", status_code=200)


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


@app.get("/docs")
async def custom_docs():
    """
    Custom API documentation
    """
    return {
        "title": "Voxmill WhatsApp Intelligence API",
        "version": "3.0.0",
        "endpoints": {
            "onboarding": {
                "POST /onboarding/create_client": {
                    "description": "Create new client profile",
                    "body": {
                        "name": "string",
                        "company": "string",
                        "email": "string",
                        "phone": "string",
                        "tier": "tier_1 | tier_2 | tier_3",
                        "markets": ["array of strings"],
                        "risk_level": "low | moderate | high",
                        "preferences": {
                            "format": "concise | detailed",
                            "detail_level": "light | standard | deep"
                        }
                    }
                }
            },
            "stripe": {
                "POST /stripe/webhook": {
                    "description": "Handle Stripe payment webhooks",
                    "events": [
                        "invoice.payment_succeeded → activate client",
                        "invoice.payment_failed → deactivate client",
                        "customer.subscription.deleted → deactivate client"
                    ]
                }
            },
            "testing": {
                "POST /test/register_self": {
                    "description": "Register founder's phone for self-testing",
                    "body": {
                        "phone": "string",
                        "tier": "tier_3"
                    }
                }
            },
            "whatsapp": {
                "POST /webhook/whatsapp": {
                    "description": "Twilio WhatsApp webhook",
                    "access_control": {
                        "tier_1": "Blocked - no analyst access",
                        "tier_2": "10 messages per day",
                        "tier_3": "Unlimited messages"
                    }
                }
            }
        }
    }


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """
    Custom 404 handler
    """
    return {
        "error": "Endpoint not found",
        "path": request.url.path,
        "available_endpoints": [
            "/",
            "/health",
            "/docs",
            "/webhook/whatsapp",
            "/onboarding/create_client",
            "/stripe/webhook",
            "/test/register_self"
        ]
    }


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    """
    Custom 500 handler
    """
    logger.error(f"Internal server error: {str(exc)}", exc_info=True)
    return {
        "error": "Internal server error",
        "message": "Our team has been notified"
    }

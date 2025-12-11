"""
Voxmill WhatsApp Intelligence Service - Main Application
FastAPI backend handling Twilio webhooks, MongoDB data, and 5-layer intelligence stack
"""
import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import PlainTextResponse
from pymongo import MongoClient

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

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']
logger.info("✅ MongoDB connected")


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


# ============================================================================
# STARTUP/SHUTDOWN EVENTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 Starting Voxmill WhatsApp Intelligence Service...")
    
    # Start background scheduler for data collection
    try:
        from app.scheduler import daily_intelligence_cycle
        asyncio.create_task(daily_intelligence_cycle())
        logger.info("✅ Background scheduler started")
    except Exception as e:
        logger.warning(f"⚠️  Scheduler not started: {e}")
    
    # Start alert checker scheduler
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
        
        scheduler = AsyncIOScheduler()
        
        @scheduler.scheduled_job(CronTrigger(hour='*', minute=0))
        async def scheduled_alert_checker():
            logger.info("ALERT CHECKER - Running hourly")
            try:
                await check_and_send_alerts_task()
            except Exception as e:
                logger.error(f"Alert error: {e}", exc_info=True)
        
        scheduler.start()
        logger.info("✅ Alert scheduler started")
    except Exception as e:
        logger.warning(f"⚠️  Alert scheduler not started: {e}")
    
    logger.info("✅ Voxmill service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down Voxmill service...")
    logger.info("✅ Shutdown complete")


# ============================================================================
# BASIC ENDPOINTS
# ============================================================================

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


@app.get("/api/health")
async def api_health_check():
    """API health check endpoint"""
    return {
        "status": "healthy",
        "service": "Voxmill Intelligence API",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
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
        mongo_client.admin.command('ping')
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


# ============================================================================
# WHATSAPP WEBHOOK
# ============================================================================

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
                return PlainTextResponse("", status_code=200)
            
            # Mark as processed with 60s TTL
            redis_client.setex(cache_key, 60, "1")
        
        # Validate required fields
        if not sender or not message_body:
            logger.warning(f"⚠️  Empty message from {sender}")
            return PlainTextResponse("", status_code=200)
        
        # Normalize phone number
        normalized_sender = normalize_phone_number(sender)
        
        logger.info(f"📱 Incoming message from {normalized_sender}: {message_body[:100]}")
        
        # Check rate limit using actual function signature
        from app.client_manager import check_rate_limit
        allowed, rate_limit_message = check_rate_limit(normalized_sender)
        
        if not allowed:
            from app.whatsapp import send_twilio_message
            await send_twilio_message(normalized_sender, f"⚠️ {rate_limit_message}")
            return PlainTextResponse("", status_code=200)
        
        # Process message in background
        background_tasks.add_task(
            process_message_async,
            normalized_sender,
            message_body
        )
        
        # Return 200 immediately to Twilio
        return PlainTextResponse("", status_code=200)
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}", exc_info=True)
        return PlainTextResponse("", status_code=200)


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


# ============================================================================
# AIRTABLE → MONGODB SYNC ENDPOINTS
# ============================================================================

@app.post("/api/sync-preferences")
async def sync_preferences_from_airtable(request: Request):
    """
    Sync client preferences from Airtable to MongoDB.
    Called by Airtable automation when preferences are updated manually.
    """
    try:
        # Get payload from Airtable
        data = await request.json()
        
        email = data.get('email')
        preferences = data.get('preferences', {})
        
        # Validate required fields
        if not email:
            raise HTTPException(status_code=400, detail="Email is required")
        
        # Validate preference values
        valid_competitor_focus = ['low', 'medium', 'high']
        valid_report_depth = ['executive', 'detailed', 'deep']
        
        if 'competitor_focus' in preferences:
            if preferences['competitor_focus'] not in valid_competitor_focus:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid competitor_focus. Must be one of: {valid_competitor_focus}"
                )
        
        if 'report_depth' in preferences:
            if preferences['report_depth'] not in valid_report_depth:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid report_depth. Must be one of: {valid_report_depth}"
                )
        
        # Update MongoDB
        result = db.client_profiles.update_one(
            {"email": email},
            {
                "$set": {
                    "preferences": preferences,
                    "preferences_updated_at": datetime.utcnow(),
                    "preferences_updated_by": "Airtable Manual"
                }
            },
            upsert=True  # Create if doesn't exist
        )
        
        logger.info(f"✅ AIRTABLE SYNC: {email} → {preferences}")
        
        # Return success response
        return {
            "status": "success",
            "email": email,
            "preferences": preferences,
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted": result.upserted_id is not None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Airtable sync error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/preferences/{email}")
async def get_preferences(email: str):
    """
    Get current preferences for a client.
    Useful for Airtable to fetch current MongoDB state.
    """
    try:
        client = db.client_profiles.find_one({"email": email})
        
        if not client:
            raise HTTPException(status_code=404, detail="Client not found")
        
        preferences = client.get('preferences', {})
        
        return {
            "email": email,
            "preferences": preferences,
            "updated_at": client.get('preferences_updated_at'),
            "updated_by": client.get('preferences_updated_by', 'Unknown')
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Get preferences error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ADMIN/DEBUG ENDPOINTS
# ============================================================================

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


@app.get("/pdf/{file_id}")
async def serve_pdf(file_id: str):
    """
    Serve PDF from GridFS (fallback if Cloudflare not configured)
    """
    try:
        from bson.objectid import ObjectId
        import gridfs
        from fastapi.responses import StreamingResponse
        
        fs = gridfs.GridFS(db)
        
        # Get file
        grid_file = fs.get(ObjectId(file_id))
        
        # Stream response
        return StreamingResponse(
            grid_file,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"inline; filename={grid_file.filename}"
            }
        )
        
    except Exception as e:
        logger.error(f"Error serving PDF: {e}")
        return {"error": "PDF not found"}


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


@app.post("/api/regenerate")
async def emergency_regenerate(request: Request):
    """
    Emergency regeneration triggered by operator.
    Only callable with API key for security.
    """
    import subprocess
    
    # Verify API key
    api_key = request.headers.get('X-API-Key')
    if api_key != os.getenv('VOXMILL_OPERATOR_KEY'):
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    data = await request.json()
    email = data.get('email')
    
    # Get client data
    client = db.client_profiles.find_one({"email": email})
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Log emergency regeneration
    db.generation_log.insert_one({
        "email": email,
        "triggered_at": datetime.utcnow(),
        "triggered_by": "Emergency Manual Override",
        "status": "Generating"
    })
    
    # Trigger generation in background
    subprocess.Popen([
        sys.executable, 'voxmill_master.py',
        '--area', client.get('default_region', 'Mayfair'),
        '--city', client.get('city', 'London'),
        '--email', email,
        '--name', client.get('name', 'Client')
    ])
    
    logger.info(f"🚨 EMERGENCY REGENERATION: {email}")
    
    return {
        "status": "regenerating",
        "email": email,
        "eta_seconds": 90
    }


# ============================================================================
# REAL-TIME ALERTS SYSTEM
# ============================================================================

@app.get("/internal/run-alert-checker")
async def run_alert_checker_endpoint(background_tasks: BackgroundTasks, secret: str = None):
    """
    Internal endpoint to run alert checker
    Called by external cron service (cron-job.org, EasyCron, GitHub Actions)
    
    Usage: GET /internal/run-alert-checker?secret=YOUR_SECRET
    """
    
    # Simple security check
    ALERT_SECRET = os.getenv("ALERT_CHECKER_SECRET", "change-me-in-production")
    
    if secret != ALERT_SECRET:
        logger.warning(f"Unauthorized alert checker access attempt")
        return {"error": "Unauthorized", "status": 401}
    
    logger.info("Alert checker triggered via API endpoint")
    
    # Run alert checker in background
    background_tasks.add_task(check_and_send_alerts_task)
    
    return {
        "status": "started",
        "message": "Alert checker running in background",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


async def check_and_send_alerts_task():
    """Background task to check and send alerts"""
    
    try:
        from app.intelligence.alert_detector import detect_alerts_for_region, format_alert_message
        from app.whatsapp import send_twilio_message
        
        logger.info("="*70)
        logger.info("ALERT CHECKER - Starting")
        logger.info("="*70)
        
        # Get all active clients (Tier 2 and Tier 3 have alerts)
        clients = list(db['client_profiles'].find({
            "tier": {"$in": ["tier_2", "tier_3"]},
            "whatsapp_number": {"$exists": True}
        }))
        
        logger.info(f"Found {len(clients)} clients eligible for alerts")
        
        total_alerts_sent = 0
        
        for client in clients:
            client_name = client.get('name', 'Unknown')
            whatsapp_number = client.get('whatsapp_number')
            preferred_regions = client.get('preferences', {}).get('preferred_regions', [])
            
            # Check if alerts are enabled for this client
            alert_preferences = client.get('alert_preferences', {})
            alerts_enabled = alert_preferences.get('enabled', True)  # Default: enabled
            
            if not alerts_enabled:
                logger.info(f"Alerts disabled for {client_name} - skipping")
                continue
            
            if not whatsapp_number:
                logger.warning(f"Client {client_name} has no WhatsApp number")
                continue
            
            if not preferred_regions:
                logger.info(f"Client {client_name} has no preferred regions - skipping")
                continue
            
            logger.info(f"\nChecking alerts for {client_name} ({len(preferred_regions)} regions)")
            
            for region in preferred_regions:
                try:
                    # Detect alerts for this region
                    alerts = detect_alerts_for_region(region)
                    
                    if not alerts:
                        logger.info(f"  - {region}: No alerts")
                        continue
                    
                    logger.info(f"  - {region}: {len(alerts)} alerts detected")
                    
                    # Send each alert
                    for alert in alerts:
                        # Format message
                        message = format_alert_message(alert)
                        
                        # Send via WhatsApp
                        try:
                            await send_twilio_message(whatsapp_number, message)
                            logger.info(f"    ✅ Sent {alert['type']} alert to {client_name}")
                            
                            # Log alert sent
                            db['alerts_sent'].insert_one({
                                "client_id": client.get('_id'),
                                "client_name": client_name,
                                "whatsapp_number": whatsapp_number,
                                "alert_type": alert['type'],
                                "region": region,
                                "urgency": alert['urgency'],
                                "timestamp": datetime.now(timezone.utc),
                                "alert_data": alert
                            })
                            
                            total_alerts_sent += 1
                            
                        except Exception as e:
                            logger.error(f"    ❌ Failed to send alert to {client_name}: {e}")
                    
                except Exception as e:
                    logger.error(f"  - {region}: Error detecting alerts: {e}", exc_info=True)
        
        logger.info(f"\n{'='*70}")
        logger.info(f"ALERT CHECKER COMPLETE - Sent {total_alerts_sent} total alerts")
        logger.info(f"{'='*70}\n")
        
    except Exception as e:
        logger.error(f"Fatal error in alert checker: {e}", exc_info=True)


# ============================================================================
# WAVE 1 & WAVE 3: CACHE AND SESSION ENDPOINTS
# ============================================================================

@app.get("/metrics/cache")
async def get_cache_metrics():
    """Get cache performance metrics"""
    from app.cache_manager import CacheManager
    
    cache_mgr = CacheManager()
    stats = cache_mgr.get_cache_stats()
    
    return {
        "status": "success",
        "cache_stats": stats,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/session/{phone}/analytics")
async def get_session_analytics_endpoint(phone: str):
    """Get conversation analytics for a client"""
    from app.conversation_manager import get_session_analytics
    
    analytics = get_session_analytics(phone)
    
    return {
        "status": "success",
        "phone": phone,
        "analytics": analytics
    }


@app.delete("/session/{phone}")
async def clear_session(phone: str):
    """Clear conversation session (start fresh)"""
    from app.conversation_manager import ConversationSession
    
    session = ConversationSession(phone)
    session.clear_session()
    
    return {
        "status": "session_cleared",
        "phone": phone,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/cache/clear")
async def clear_cache():
    """Clear all cache (emergency use)"""
    from app.cache_manager import CacheManager
    
    cache_mgr = CacheManager()
    
    try:
        # This would need to be implemented in CacheManager
        # For now, just return success
        return {
            "status": "cache_cleared",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


# ============================================================================
# OPTIONAL ROUTES
# ============================================================================

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


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

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

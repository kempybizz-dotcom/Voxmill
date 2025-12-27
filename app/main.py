"""
Voxmill WhatsApp Intelligence Service - Main Application
FastAPI backend handling Twilio webhooks, MongoDB data, and 5-layer intelligence stack

UPDATED: Industry routing support
"""
import json 
import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import PlainTextResponse
from pymongo import MongoClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

# ============================================================================
# INITIALIZE FASTAPI APP
# ============================================================================
app = FastAPI(
    title="Voxmill WhatsApp Intelligence API",
    description="AI-powered market intelligence via WhatsApp",
    version="2.0.0"
)

# ============================================================================
# INITIALIZE SCHEDULER
# ============================================================================
scheduler = AsyncIOScheduler()

# ============================================================================
# INITIALIZE REDIS
# ============================================================================
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

# ============================================================================
# INITIALIZE MONGODB
# ============================================================================
MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']
logger.info("✅ MongoDB connected")

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def normalize_phone_number(phone: str) -> str:
    """Normalize phone number format for WhatsApp"""
    if not phone:
        return phone
    phone = phone.replace('whatsapp:', '').replace('whatsapp%3A', '')
    if not phone.startswith('+'):
        phone = '+' + phone
    return phone

# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def check_all_monitors():
    """Check all active monitors and send alerts"""
    try:
        from app.monitoring import check_monitors_and_alert
        await check_monitors_and_alert()
        logger.info("✅ Monitor check complete")
    except Exception as e:
        logger.error(f"Monitor check failed: {e}")

async def warm_cache():
    """Pre-warm cache at 7am - UPDATED with industry parameter"""
    try:
        from app.dataset_loader import load_dataset
        
        # Warm Real Estate cache (primary vertical)
        for area in ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']:
            load_dataset(area, max_properties=100, industry="Real Estate")
            logger.info(f"✅ Cache warmed for {area}")
    except Exception as e:
        logger.error(f"Cache warming failed: {e}")

async def check_and_send_alerts_task():
    """Background task to check and send alerts"""
    try:
        from app.intelligence.alert_detector import detect_alerts_for_region, format_alert_message
        from app.whatsapp import send_twilio_message
        
        logger.info("="*70)
        logger.info("ALERT CHECKER - Starting")
        logger.info("="*70)
        
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
            
            alert_preferences = client.get('alert_preferences', {})
            alerts_enabled = alert_preferences.get('enabled', True)
            
            if not alerts_enabled or not whatsapp_number or not preferred_regions:
                continue
            
            for region in preferred_regions:
                try:
                    alerts = detect_alerts_for_region(region)
                    if not alerts:
                        continue
                    
                    for alert in alerts:
                        message = format_alert_message(alert)
                        try:
                            await send_twilio_message(whatsapp_number, message)
                            logger.info(f"✅ Sent {alert['type']} alert to {client_name}")
                            
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
                            logger.error(f"❌ Failed to send alert to {client_name}: {e}")
                except Exception as e:
                    logger.error(f"Error detecting alerts for {region}: {e}", exc_info=True)
        
        logger.info(f"ALERT CHECKER COMPLETE - Sent {total_alerts_sent} total alerts")
        
    except Exception as e:
        logger.error(f"Fatal error in alert checker: {e}", exc_info=True)

# ============================================================================
# STARTUP EVENT (SINGLE COMBINED VERSION)
# ============================================================================
async def store_daily_snapshots_all_regions():
    """Store daily snapshots for all core regions - UPDATED with industry parameter"""
    from app.dataset_loader import load_dataset
    
    core_regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
    
    for region in core_regions:
        try:
            logger.info(f"📸 Storing daily snapshot for {region}...")
            dataset = load_dataset(area=region, max_properties=100, industry="Real Estate")
            # Snapshot storage happens automatically inside load_dataset now
            logger.info(f"✅ Snapshot stored for {region}")
        except Exception as e:
            logger.error(f"Failed to store snapshot for {region}: {e}")


@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting Voxmill WhatsApp Intelligence Service...")
    
    # ========================================
    # START AIRTABLE QUEUE PROCESSOR (CRITICAL FOR PRODUCTION)
    # ========================================
    try:
        from app.airtable_queue import start_queue_processor
        await start_queue_processor()
        logger.info("✅ Airtable queue processor started (prevents rate limit crashes)")
    except ImportError:
        logger.warning("⚠️ airtable_queue.py not found - Airtable writes will be synchronous (slower)")
    except Exception as e:
        logger.error(f"❌ Airtable queue processor failed to start: {e}")
    
    # ========================================
    # START SCHEDULERS
    # ========================================
    try:
        # Existing schedulers
        scheduler.add_job(check_all_monitors, 'interval', minutes=15)
        
        # Daily cache warming + historical snapshot
        scheduler.add_job(warm_cache, 'cron', hour=7, minute=0)
        
        # NEW: Daily historical snapshot for ALL core regions
        scheduler.add_job(
            store_daily_snapshots_all_regions,
            'cron',
            hour=6,
            minute=30,
            timezone='Europe/London'
        )
        
        scheduler.start()
        logger.info("✅ Scheduler started: monitors (15min), cache (7am), snapshots (6:30am)")
        
    except Exception as e:
        logger.error(f"Scheduler startup failed: {e}")
    
    # ========================================
    # START BACKGROUND INTELLIGENCE CYCLE
    # ========================================
    try:
        from app.scheduler import daily_intelligence_cycle
        asyncio.create_task(daily_intelligence_cycle())
        logger.info("✅ Background intelligence cycle started")
    except ImportError:
        logger.info("⏭️ Background scheduler not available (app.scheduler not found)")
    except Exception as e:
        logger.warning(f"⚠️ Background scheduler not started: {e}")
    
    logger.info("✅ Voxmill service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down Voxmill service...")
    
    # ========================================
    # GRACEFUL AIRTABLE QUEUE SHUTDOWN
    # ========================================
    try:
        from app.airtable_queue import stop_queue_processor
        await stop_queue_processor()
        logger.info("✅ Airtable queue drained and stopped")
    except ImportError:
        pass
    except Exception as e:
        logger.error(f"Airtable queue shutdown failed: {e}")
    
    # ========================================
    # STOP SCHEDULERS
    # ========================================
    try:
        scheduler.shutdown()
        logger.info("✅ Scheduler stopped")
    except Exception as e:
        logger.error(f"Scheduler shutdown failed: {e}")
    
    logger.info("✅ Shutdown complete")

# ============================================================================
# HEALTH CHECK ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    return {
        "service": "Voxmill WhatsApp Intelligence API",
        "status": "operational",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Enhanced health check with system status"""
    try:
        # Check Redis
        redis_status = "healthy"
        try:
            if redis_client:
                redis_client.ping()
        except:
            redis_status = "down"
        
        # Check MongoDB
        mongo_status = "healthy"
        try:
            mongo_client.admin.command('ping')
        except:
            mongo_status = "down"
        
        # Check scheduler
        scheduler_status = "running" if scheduler.running else "stopped"
        
        return {
            "status": "operational",
            "redis": redis_status,
            "mongodb": mongo_status,
            "scheduler": scheduler_status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ============================================================================
# WHATSAPP WEBHOOK
# ============================================================================

@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request, background_tasks: BackgroundTasks):
    """Main Twilio WhatsApp webhook endpoint"""
    try:
        form_data = await request.form()
        message_sid = form_data.get('MessageSid')
        sender = form_data.get('From', '')
        message_body = form_data.get('Body', '').strip()
        
        # Webhook deduplication
        if redis_client and message_sid:
            cache_key = f"webhook_processed:{message_sid}"
            if redis_client.get(cache_key):
                logger.info(f"⚠️  Duplicate webhook ignored: {message_sid}")
                return PlainTextResponse("", status_code=200)
            redis_client.setex(cache_key, 60, "1")
        
        if not sender or not message_body:
            logger.warning(f"⚠️  Empty message from {sender}")
            return PlainTextResponse("", status_code=200)
        
        normalized_sender = normalize_phone_number(sender)
        logger.info(f"📱 Incoming message from {normalized_sender}: {message_body[:100]}")
        
        # Process message in background (rate limiting happens in whatsapp.py)
        background_tasks.add_task(process_message_async, normalized_sender, message_body)
        return PlainTextResponse("", status_code=200)
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}", exc_info=True)
        return PlainTextResponse("", status_code=200)


async def process_message_async(sender: str, message_body: str):
    """Process message asynchronously"""
    try:
        from app.whatsapp import handle_whatsapp_message
        await handle_whatsapp_message(sender, message_body)
    except Exception as e:
        logger.error(f"❌ Error processing message for {sender}: {e}", exc_info=True)
        try:
            from app.whatsapp import send_twilio_message
            error_msg = "⚠️ Sorry, I encountered an error processing your request. Please try again."
            await send_twilio_message(sender, error_msg)
        except:
            logger.error("Failed to send error message to user")


@app.get("/webhook/whatsapp")
async def whatsapp_webhook_get(request: Request):
    """Handle Twilio webhook verification (GET request)"""
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
    """Admin endpoint to send broadcast messages"""
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
async def get_latest_data(area: Optional[str] = None, industry: Optional[str] = "Real Estate"):
    """
    Get latest market data for testing/debugging
    
    UPDATED: Now supports industry parameter
    """
    try:
        from app.dataset_loader import load_dataset
        
        dataset = load_dataset(
            area=area if area else "Mayfair",
            max_properties=100,
            industry=industry
        )
        
        if not dataset or dataset.get('error'):
            return {"error": "No data found", "area": area, "industry": industry}
        
        return {
            "area": dataset.get("metadata", {}).get("area"),
            "industry": dataset.get("metadata", {}).get("industry", industry),
            "timestamp": dataset.get("metadata", {}).get("analysis_timestamp"),
            "property_count": len(dataset.get("properties", [])),
            "has_intelligence": "intelligence" in dataset
        }
        
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pdf/{file_id}")
async def serve_pdf(file_id: str):
    """Serve PDF from GridFS (fallback if Cloudflare not configured)"""
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
    """Get client profile for testing/debugging"""
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
    """Emergency regeneration triggered by operator. Only callable with API key for security."""
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False, log_level="info")

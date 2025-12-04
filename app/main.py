"""
VOXMILL WHATSAPP INTELLIGENCE BACKEND
======================================
Complete FastAPI backend with:
- WhatsApp webhook handler
- Client onboarding
- Stripe payment webhooks
- Rate limiting
- Self-test registration
- Background intelligence scheduler
"""

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import Response, PlainTextResponse
import logging
import asyncio
import redis
from app.whatsapp import handle_whatsapp_message

# Import new route modules
from app.routes import onboarding, stripe_webhooks, testing

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Voxmill WhatsApp Executive Analyst",
    description="Institutional-grade market intelligence via WhatsApp",
    version="3.0.0"
)

# Include routers
app.include_router(onboarding.router)
app.include_router(stripe_webhooks.router)
app.include_router(testing.router)

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


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "voxmill-whatsapp-analyst",
        "version": "3.0.0",
        "background_tasks_active": len(background_tasks)
    }


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

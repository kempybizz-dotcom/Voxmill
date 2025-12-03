"""
Stripe Webhook Handler with Welcome Messages
"""
from fastapi import APIRouter, Request, HTTPException, Header
from pymongo import MongoClient
from datetime import datetime, timezone
import os
import logging

router = APIRouter(prefix="/stripe", tags=["stripe"])
logger = logging.getLogger(__name__)

# Stripe configuration
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


async def send_welcome_message(whatsapp_number: str, tier: str, name: str):
    """
    Send welcome message to newly activated client
    """
    try:
        from app.whatsapp import send_twilio_message
        
        # Tier-specific welcome messages
        welcome_messages = {
            "tier_1": f"""Welcome to Voxmill Intelligence, {name}.

Your Tier 1 access is now active.

You have access to:
- Real-time market overview
- Competitive intelligence
- Opportunity identification
- Price corridor analysis

Ask me anything about luxury markets. Try:
- "Market overview"
- "Top opportunities"
- "Competitive landscape"

Available 24/7 at this number.""",

            "tier_2": f"""Welcome to Voxmill Intelligence, {name}.

Your Tier 2 Analyst Desk is now active.

You have full access to:
- Real-time market intelligence
- Competitive dynamics analysis
- Trend detection (14-day windows)
- Strategic recommendations
- Liquidity velocity tracking
- Up to 10 analyses per day

Your intelligence is personalized to your preferences and will learn from our conversations.

Ask me anything. Try:
- "What's the market outlook?"
- "Analyze competitive positioning"
- "Show me liquidity trends"

Available 24/7.""",

            "tier_3": f"""Welcome to Voxmill Intelligence, {name}.

Your Tier 3 Strategic Partner access is now active.

You have unlimited access to our complete intelligence suite:

REAL-TIME ANALYSIS:
- Market overview & trends
- Competitive landscape
- Opportunity identification

PREDICTIVE INTELLIGENCE:
- Agent behavioral profiling (85-91% confidence)
- Multi-wave cascade forecasting
- Liquidity velocity tracking
- Micromarket segmentation

SCENARIO MODELING:
- "What if Knight Frank drops 10%?"
- Strategic response recommendations
- Risk/opportunity mapping

No message limits. Full institutional-grade intelligence.

Ask me anything, anytime. Examples:
- "Strategic outlook for Mayfair"
- "What if Savills raises prices 8%?"
- "Analyze liquidity velocity"
- "Predict cascade effects"

Your dedicated intelligence partner, available 24/7."""
        }
        
        message = welcome_messages.get(tier, welcome_messages["tier_1"])
        
        await send_twilio_message(whatsapp_number, message)
        logger.info(f"Welcome message sent to {whatsapp_number} (Tier: {tier})")
        
    except Exception as e:
        logger.error(f"Error sending welcome message: {str(e)}", exc_info=True)


@router.post("/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="Stripe-Signature")
):
    """
    Handle Stripe webhook events for payment status changes
    Sends welcome message on successful payment
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        # Get raw body
        payload = await request.body()
        
        # Parse as JSON (add signature verification in production)
        import json
        event = json.loads(payload)
        
        clients_collection = db['clients']
        
        # Handle different event types
        event_type = event.get('type')
        event_data = event.get('data', {}).get('object', {})
        
        # Extract customer email from event
        customer_email = event_data.get('customer_email') or event_data.get('email')
        
        if not customer_email:
            logger.warning(f"No customer email found in event {event_type}")
            return {"status": "error", "message": "No customer email"}
        
        # Find client by email
        client = clients_collection.find_one({"email": customer_email})
        
        if not client:
            logger.warning(f"Client not found for email: {customer_email}")
            return {"status": "error", "message": "Client not found"}
        
        # Handle event types
        if event_type == 'invoice.payment_succeeded':
            # Activate client
            clients_collection.update_one(
                {"email": customer_email},
                {
                    "$set": {
                        "status": "active",
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            logger.info(f"Client activated: {customer_email}")
            
            # Send welcome message
            whatsapp_number = client.get('whatsapp_number')
            tier = client.get('tier', 'tier_1')
            name = client.get('name', 'there')
            
            if whatsapp_number:
                await send_welcome_message(whatsapp_number, tier, name)
            
        elif event_type == 'invoice.payment_failed':
            # Deactivate client
            clients_collection.update_one(
                {"email": customer_email},
                {
                    "$set": {
                        "status": "inactive",
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            logger.info(f"Client deactivated (payment failed): {customer_email}")
            
        elif event_type == 'customer.subscription.deleted':
            # Deactivate client
            clients_collection.update_one(
                {"email": customer_email},
                {
                    "$set": {
                        "status": "inactive",
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            logger.info(f"Client deactivated (subscription deleted): {customer_email}")
            
        else:
            logger.info(f"Unhandled event type: {event_type}")
        
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"Stripe webhook error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

"""
Stripe Webhook Handler
"""
from fastapi import APIRouter, Request, HTTPException, Header
from pymongo import MongoClient
from datetime import datetime, timezone
import os
import logging
import stripe

router = APIRouter(prefix="/stripe", tags=["stripe"])
logger = logging.getLogger(__name__)

# Stripe configuration
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")

if STRIPE_API_KEY:
    stripe.api_key = STRIPE_API_KEY

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


@router.post("/webhook")
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(None, alias="Stripe-Signature")
):
    """
    Handle Stripe webhook events for payment status changes
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        # Get raw body
        payload = await request.body()
        
        # Verify webhook signature
        try:
            event = stripe.Webhook.construct_event(
                payload, stripe_signature, STRIPE_WEBHOOK_SECRET
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Invalid signature")
        
        clients_collection = db['clients']
        
        # Handle different event types
        event_type = event['type']
        event_data = event['data']['object']
        
        # Extract customer email from event
        customer_email = event_data.get('customer_email') or event_data.get('email')
        
        if not customer_email:
            # Try to get from customer object
            customer_id = event_data.get('customer')
            if customer_id:
                customer = stripe.Customer.retrieve(customer_id)
                customer_email = customer.get('email')
        
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

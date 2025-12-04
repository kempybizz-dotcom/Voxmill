import logging
from pymongo import MongoClient
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def normalize_phone_number(phone: str) -> str:
    """
    Normalize phone number by removing whatsapp: prefix and ensuring + prefix
    """
    if not phone:
        return phone
    
    # Remove whatsapp: prefix if present (also handle URL-encoded version)
    phone = phone.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    # Ensure it starts with +
    if not phone.startswith('+'):
        phone = '+' + phone
    
    return phone


def get_client_profile(whatsapp_number: str) -> dict:
    """Load client profile from MongoDB"""
    try:
        if not mongo_client:
            return {}
        
        db = mongo_client['Voxmill']
        collection = db['client_profiles']
        
        # Normalize number (remove whatsapp: prefix)
        normalized_number = normalize_phone_number(whatsapp_number)
        
        profile = collection.find_one({"whatsapp_number": normalized_number})
        
        if not profile:
            # Create default profile for new client
            default_profile = {
                "whatsapp_number": normalized_number,
                "created_at": datetime.now(timezone.utc),
                "preferences": {
                    "preferred_regions": ["Mayfair"],
                    "competitor_set": [],
                    "risk_appetite": "balanced",
                    "budget_range": {"min": 0, "max": 100000000},
                    "insight_depth": "standard"
                },
                "query_history": [],
                "last_region_queried": "Mayfair",
                "total_queries": 0,
                "tier": "tier_3",  # Default to tier_3 for testing
                "status": "active"
            }
            collection.insert_one(default_profile)
            logger.info(f"Created new client profile: {normalized_number}")
            return default_profile
        
        return profile
        
    except Exception as e:
        logger.error(f"Error loading client profile: {str(e)}")
        return {}


def check_rate_limit(whatsapp_number: str) -> tuple[bool, str]:
    """
    Check if client has exceeded daily message limit
    Returns: (allowed, message)
    """
    try:
        if not mongo_client:
            return (True, "")  # Allow if DB unavailable
        
        db = mongo_client['Voxmill']
        collection = db['client_profiles']  # Use client_profiles, not clients
        
        # Normalize number
        normalized_number = normalize_phone_number(whatsapp_number)
        
        client = collection.find_one({"whatsapp_number": normalized_number})
        
        if not client:
            # Auto-create profile on first message
            logger.info(f"New client detected: {normalized_number}")
            return (True, "")  # Allow first message to trigger profile creation
        
        # Check status
        if client.get('status') == 'inactive':
            return (False, "Your intelligence access is inactive. Contact Voxmill to restore service.")
        
        # Check tier
        tier = client.get('tier', 'tier_1')
        
        if tier == 'tier_1':
            return (False, "Your plan does not include the analyst line. Contact Voxmill to upgrade.")
        
        # Rate limiting for Tier 2
        if tier == 'tier_2':
            today = datetime.now(timezone.utc).date()
            last_message_date = client.get('last_message_date')
            daily_count = client.get('daily_message_count', 0)
            
            # Reset counter if new day
            if last_message_date:
                last_date = last_message_date.date() if hasattr(last_message_date, 'date') else last_message_date
                if last_date != today:
                    daily_count = 0
            
            # Check limit
            TIER_2_DAILY_LIMIT = 10
            if daily_count >= TIER_2_DAILY_LIMIT:
                return (False, "You've reached today's analysis limit for your tier. Upgrade to Tier 3 for unlimited access.")
            
            # Increment counter
            collection.update_one(
                {"whatsapp_number": normalized_number},
                {
                    "$set": {
                        "daily_message_count": daily_count + 1,
                        "last_message_date": datetime.now(timezone.utc)
                    }
                }
            )
        
        # Tier 3 has unlimited access
        return (True, "")
        
    except Exception as e:
        logger.error(f"Rate limit check error: {str(e)}", exc_info=True)
        return (True, "")  # Allow on error


def update_client_history(whatsapp_number: str, query: str, category: str, region: str):
    """Log query to client history"""
    try:
        if not mongo_client:
            return
        
        db = mongo_client['Voxmill']
        collection = db['client_profiles']
        
        # Normalize number
        normalized_number = normalize_phone_number(whatsapp_number)
        
        collection.update_one(
            {"whatsapp_number": normalized_number},
            {
                "$push": {
                    "query_history": {
                        "$each": [{
                            "timestamp": datetime.now(timezone.utc),
                            "query": query,
                            "category": category,
                            "region": region
                        }],
                        "$slice": -50  # Keep last 50 queries
                    }
                },
                "$set": {
                    "last_region_queried": region
                },
                "$inc": {
                    "total_queries": 1
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Error updating client history: {str(e)}")

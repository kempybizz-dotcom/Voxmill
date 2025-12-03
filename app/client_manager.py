import logging
from pymongo import MongoClient
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

def get_client_profile(whatsapp_number: str) -> dict:
    """Load client profile from MongoDB"""
    try:
        if not mongo_client:
            return {}
        
        db = mongo_client['Voxmill']
        collection = db['client_profiles']
        
        profile = collection.find_one({"whatsapp_number": whatsapp_number})
        
        if not profile:
            # Create default profile for new client
            default_profile = {
                "whatsapp_number": whatsapp_number,
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
                "tier": "intelligence_access"
            }
            collection.insert_one(default_profile)
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
        clients = db['clients']
        
        client = clients.find_one({"whatsapp_number": whatsapp_number})
        
        if not client:
            return (False, "This line is reserved for active Voxmill clients.")
        
        # Check status
        if client.get('status') != 'active':
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
            clients.update_one(
                {"whatsapp_number": whatsapp_number},
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
        
        collection.update_one(
            {"whatsapp_number": whatsapp_number},
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

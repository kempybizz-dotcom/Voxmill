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


def get_client_tier(whatsapp_number: str) -> str:
    """
    Get client tier - NOW RETURNS SINGLE TIER FOR ALL
    """
    # ✅ V3.1: Everyone gets full access (simplified for startup phase)
    return "premium_access"


def check_rate_limit(whatsapp_number: str) -> bool:
    """
    Check if client has exceeded rate limit
    ✅ V3.1: Unlimited queries for all clients
    """
    return True  # Always allow (no limits during startup)


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

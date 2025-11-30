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

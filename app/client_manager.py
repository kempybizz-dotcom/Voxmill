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
    """
    Load client profile from MongoDB
    
    ✅ UPDATED: No hardcoded markets - Airtable is source of truth
    """
    try:
        if not mongo_client:
            return {}
        
        db = mongo_client['Voxmill']
        collection = db['client_profiles']
        
        # Normalize number
        normalized_number = normalize_phone_number(whatsapp_number)
        
        # Try to find profile
        profile = collection.find_one({"whatsapp_number": normalized_number})
        
        if not profile:
            # Create profile WITH email field
            # Extract email from number or use placeholder
            # Format: +447780565645 → user_447780565645@temp.voxmill.uk
            temp_email = f"user_{normalized_number.replace('+', '')}@temp.voxmill.uk"
            
            # ========================================
            # MINIMAL MONGODB PROFILE
            # ========================================
            # Airtable is source of truth for:
            # - Industry
            # - Tier
            # - Preferred regions/markets
            # - Account status
            # MongoDB only stores:
            # - Usage tracking
            # - Query history
            # - Conversation state
            # ========================================
            
            default_profile = {
                "whatsapp_number": normalized_number,
                "email": temp_email,
                "created_at": datetime.now(timezone.utc),
                
                # ✅ UPDATED: Empty preferences - Airtable populates these
                "preferences": {
                    "preferred_regions": [],  # ✅ Empty - Airtable is truth
                    "competitor_set": [],
                    "risk_appetite": "balanced",
                    "budget_range": {"min": 0, "max": 100000000},
                    "insight_depth": "standard",
                    "competitor_focus": "medium",
                    "report_depth": "detailed"
                },
                
                # ✅ UPDATED: No default region - will be set on first query
                "query_history": [],
                "last_region_queried": None,  # ✅ None - will be set on first query
                "total_queries": 0,
                
                # ✅ Minimal defaults - Airtable overwrites these
                "tier": "tier_1",  # Will be overwritten by Airtable sync
                "status": "unknown"  # Will be overwritten by Airtable sync
            }
            
            collection.insert_one(default_profile)
            logger.info(f"✅ Created new MongoDB profile: {normalized_number} (Airtable will populate)")
            return default_profile
        
        # ========================================
        # MIGRATION: Add missing fields
        # ========================================
        
        # MIGRATION: If profile exists but missing email, add it
        if not profile.get('email'):
            temp_email = f"user_{normalized_number.replace('+', '')}@temp.voxmill.uk"
            collection.update_one(
                {"_id": profile['_id']},
                {"$set": {"email": temp_email}}
            )
            profile['email'] = temp_email
            logger.info(f"Added email to existing profile: {normalized_number}")
        
        # MIGRATION: Add default preference fields if missing
        if 'preferences' in profile:
            needs_update = False
            updates = {}
            
            if 'competitor_focus' not in profile['preferences']:
                updates['preferences.competitor_focus'] = 'medium'
                needs_update = True
            
            if 'report_depth' not in profile['preferences']:
                updates['preferences.report_depth'] = 'detailed'
                needs_update = True
            
            # ✅ MIGRATION: Ensure preferred_regions is a list (not null)
            if 'preferred_regions' not in profile['preferences']:
                updates['preferences.preferred_regions'] = []
                needs_update = True
            elif profile['preferences']['preferred_regions'] is None:
                updates['preferences.preferred_regions'] = []
                needs_update = True
            
            if needs_update:
                collection.update_one(
                    {"_id": profile['_id']},
                    {"$set": updates}
                )
                # Update in-memory profile
                for key, value in updates.items():
                    if '.' in key:
                        parent_key, child_key = key.split('.', 1)
                        if parent_key in profile:
                            profile[parent_key][child_key] = value
                
                logger.info(f"Added default preferences to profile: {normalized_number}")
        
        return profile
        
    except Exception as e:
        logger.error(f"Error loading client profile: {str(e)}")
        return {}


def update_client_history(whatsapp_number: str, query: str, category: str, region: str):
    """
    Log query to client history
    
    NOTE: This function is called BEFORE rate limiting in whatsapp.py to ensure
    accurate counter for rate limit enforcement. Do not move this call.
    """
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

"""
Self-Test Routes for Founder
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from datetime import datetime, timezone
import os
import logging

router = APIRouter(prefix="/test", tags=["testing"])
logger = logging.getLogger(__name__)

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


class SelfTestRequest(BaseModel):
    phone: str
    tier: str = "tier_3"


@router.post("/register_self")
async def register_self_test(request: SelfTestRequest):
    """
    Register founder's phone for self-testing
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        clients_collection = db['clients']
        
        # Normalize phone
        phone_normalized = request.phone
        if not phone_normalized.startswith('whatsapp:'):
            phone_normalized = f"whatsapp:{phone_normalized}"
        
        # Check if already exists
        existing = clients_collection.find_one({"whatsapp_number": phone_normalized})
        
        if existing:
            # Update to active
            clients_collection.update_one(
                {"whatsapp_number": phone_normalized},
                {
                    "$set": {
                        "status": "active",
                        "tier": request.tier,
                        "updated_at": datetime.now(timezone.utc)
                    }
                }
            )
            return {"message": "Self-test client updated to active", "phone": phone_normalized}
        
        # Create new self-test client
        client_doc = {
            "whatsapp_number": phone_normalized,
            "name": "Self-Test User",
            "company": "Voxmill",
            "email": "test@voxmill.com",
            "phone": request.phone,
            "tier": request.tier,
            "status": "active",
            "markets": ["Mayfair"],
            "risk_level": "high",
            "preferences": {
                "format": "detailed",
                "detail_level": "deep",
                "preferred_regions": ["Mayfair"]
            },
            "query_history": [],
            "total_queries": 0,
            "daily_message_count": 0,
            "last_message_date": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        clients_collection.insert_one(client_doc)
        
        logger.info(f"Self-test client registered: {phone_normalized}")
        
        return {
            "message": "Self-test client registered",
            "phone": phone_normalized,
            "tier": request.tier,
            "status": "active"
        }
        
    except Exception as e:
        logger.error(f"Self-test registration error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

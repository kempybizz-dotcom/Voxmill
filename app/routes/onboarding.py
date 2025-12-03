"""
Client Onboarding Routes
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from datetime import datetime, timezone
from pymongo import MongoClient
import os
import logging

router = APIRouter(prefix="/onboarding", tags=["onboarding"])
logger = logging.getLogger(__name__)

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


class ClientPreferences(BaseModel):
    format: str = "detailed"
    detail_level: str = "standard"
    preferred_regions: List[str] = ["Mayfair"]


class CreateClientRequest(BaseModel):
    name: str
    company: str
    email: EmailStr
    phone: str
    tier: str
    markets: List[str]
    risk_level: str
    preferences: Optional[ClientPreferences] = ClientPreferences()


class CreateClientResponse(BaseModel):
    success: bool
    client_id: str
    message: str


@router.post("/create_client", response_model=CreateClientResponse)
async def create_client(request: CreateClientRequest):
    """
    Create new client profile
    """
    try:
        if not db:
            raise HTTPException(status_code=500, detail="Database not connected")
        
        clients_collection = db['clients']
        
        # Normalize phone number
        phone_normalized = request.phone
        if not phone_normalized.startswith('whatsapp:'):
            phone_normalized = f"whatsapp:{phone_normalized}"
        
        # Check if client already exists
        existing = clients_collection.find_one({"whatsapp_number": phone_normalized})
        if existing:
            raise HTTPException(status_code=409, detail="Client already exists")
        
        # Validate tier
        if request.tier not in ['tier_1', 'tier_2', 'tier_3']:
            raise HTTPException(status_code=400, detail="Invalid tier")
        
        # Create client document
        client_doc = {
            "whatsapp_number": phone_normalized,
            "name": request.name,
            "company": request.company,
            "email": request.email,
            "phone": request.phone,
            "tier": request.tier,
            "status": "inactive",  # Set to inactive until first payment
            "markets": request.markets,
            "risk_level": request.risk_level,
            "preferences": request.preferences.dict(),
            "query_history": [],
            "total_queries": 0,
            "daily_message_count": 0,
            "last_message_date": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Insert into database
        result = clients_collection.insert_one(client_doc)
        
        logger.info(f"Client created: {request.name} ({phone_normalized})")
        
        return CreateClientResponse(
            success=True,
            client_id=str(result.inserted_id),
            message=f"Client {request.name} created successfully. Status: inactive (awaiting payment)"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating client: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

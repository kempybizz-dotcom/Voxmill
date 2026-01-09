"""
AIRTABLE AUTO-SYNC
==================
Automatically populate Airtable fields from system events

NEW CONTROL PLANE INTEGRATION:
- Updated field names to match new schema
- Removed deprecated fields
- Uses Accounts/Permissions/Preferences tables
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from app.airtable_queue import queue_airtable_update

logger = logging.getLogger(__name__)


async def sync_usage_metrics(
    whatsapp_number: str,
    record_id: str,
    table_name: str,
    event_type: str,
    metadata: Dict = None
):
    """
    Auto-update Airtable usage metrics
    
    NEW: Works with Control Plane schema (Accounts, Permissions, Preferences tables)
    
    Event types:
    - message_sent: User sent message
    - intelligence_delivered: System responded
    - decision_mode: Decision mode triggered
    - pin_verified: PIN success
    - pin_failed: PIN failure
    - preference_changed: Settings updated
    """
    
    from app.client_manager import get_client_profile
    
    # Get current profile from MongoDB (has latest counts)
    client = get_client_profile(whatsapp_number)
    
    if not client:
        logger.warning(f"Client not found for auto-sync: {whatsapp_number}")
        return
    
    # ========================================
    # BUILD UPDATE FIELDS (NEW SCHEMA)
    # ========================================
    
    updates = {}
    
    # ========================================
    # ALWAYS UPDATE (Every Event)
    # ========================================
    
    # NOTE: New schema doesn't have 'Last Active' or 'Mongo Sync Status'
    # These fields don't exist in the new Control Plane
    # We only update fields that exist in the new schema
    
    # ========================================
    # MESSAGE SENT
    # ========================================
    
    if event_type == 'message_sent':
        # NOTE: Usage tracking moved to Permissions table
        # We need to update Permissions table, not Accounts table
        
        usage = client.get('usage_metrics', {})
        
        # Queue update to Permissions table (not Accounts)
        permissions_updates = {}
        
        # NOTE: New schema uses 'monthly_message_limit' (not 'Monthly Message Limit')
        # But this is a permission setting, not usage tracking
        # We should track usage in MongoDB only, not Airtable
        
        logger.debug(f"Message usage tracked in MongoDB only (not synced to Airtable)")
    
    # ========================================
    # PIN EVENTS
    # ========================================
    
    elif event_type == 'pin_verified':
        # Update Accounts table
        updates['PIN State'] = 'verified'  # NEW: lowercase enum value
        updates['PIN Last Verified'] = datetime.now(timezone.utc).isoformat()
        # NOTE: 'PIN Failed Attempts' doesn't exist in new schema
    
    elif event_type == 'pin_failed':
        # NOTE: PIN failed attempts tracking removed from new schema
        # Track in MongoDB only
        logger.debug(f"PIN failure tracked in MongoDB only (not synced to Airtable)")
    
    # ========================================
    # PREFERENCE CHANGED
    # ========================================
    
    elif event_type == 'preference_changed':
        if metadata:
            # NOTE: Preferences are in separate Preferences table
            # We need the preferences record_id, not the accounts record_id
            
            # Get preferences record ID from client profile
            prefs_record_id = client.get('airtable_preferences_record_id')
            
            if prefs_record_id:
                pref_updates = {}
                
                # Update active_market_id (requires market record ID lookup)
                if 'regions' in metadata:
                    new_region = metadata['regions'][0] if isinstance(metadata['regions'], list) else metadata['regions']
                    
                    # Get market record ID from Markets table
                    market_record_id = get_market_record_id(new_region, client.get('industry'))
                    
                    if market_record_id:
                        # NEW: active_market_id is a linked record (array of IDs)
                        pref_updates['active_market_id'] = [market_record_id]
                
                # NOTE: competitor_focus and report_depth don't exist in new Preferences schema
                # They were removed from the simplified Control Plane
                
                if pref_updates:
                    await queue_airtable_update(
                        table_name='Preferences',
                        record_id=prefs_record_id,
                        fields=pref_updates,
                        priority='normal'
                    )
                    
                    logger.info(f"📊 Preferences updated for {whatsapp_number}")
    
    # ========================================
    # TRIAL EXPIRY
    # ========================================
    
    elif event_type == 'trial_expired':
        # Update Account Status to 'cancelled' or 'paused'
        updates['Account Status'] = 'cancelled'  # NEW: lowercase enum value
    
    # ========================================
    # SUBSCRIPTION CANCELLED
    # ========================================
    
    elif event_type == 'subscription_cancelled':
        updates['Account Status'] = 'cancelled'  # NEW: lowercase enum value
    
    # ========================================
    # QUEUE UPDATE (Rate-limit safe)
    # ========================================
    
    if updates:
        await queue_airtable_update(
            table_name=table_name,  # Should be 'Accounts'
            record_id=record_id,
            fields=updates,
            priority='normal'
        )
        
        logger.info(f"📊 Auto-synced {len(updates)} fields to Airtable for {whatsapp_number}")


def get_market_record_id(market_name: str, industry: str) -> Optional[str]:
    """
    Get Markets table record ID for a market name
    
    NEW: Required for updating active_market_id (linked record field)
    """
    
    import os
    import requests
    
    AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
    
    # Map industry to schema enum
    industry_map = {
        'Private Real Estate': 'real_estate',
        'Hedge Funds': 'hedge_fund',
        'Family Offices': 'family_office',
        'Private Equity': 'private_equity',
        'Luxury Automotive': 'luxury_assets',
        'Art & Collectibles': 'art_collectibles',
        'Yacht Brokers': 'yachting'
    }
    
    industry_code = industry_map.get(industry, 'real_estate')
    
    try:
        headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Markets"
        
        # Find market by name and industry
        formula = f"AND({{market_name}}='{market_name}', {{industry}}='{industry_code}')"
        params = {'filterByFormula': formula}
        
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        if response.status_code == 200:
            records = response.json().get('records', [])
            if records:
                return records[0]['id']  # Return record ID
        
        logger.warning(f"Market not found in Airtable: {market_name} ({industry})")
        return None
    
    except Exception as e:
        logger.error(f"Get market record ID failed: {e}")
        return None


def calculate_engagement_level(client: Dict) -> str:
    """
    Calculate engagement level based on usage patterns
    
    NOTE: This field doesn't exist in new Control Plane schema
    Kept for MongoDB analytics only
    
    Returns: 'High' | 'Medium' | 'Low'
    """
    
    total_messages = client.get('total_queries', 0)
    
    # Calculate messages per day
    created_at = client.get('created_at')
    if created_at:
        try:
            # ✅ FIX: Ensure timezone-aware datetime
            if isinstance(created_at, str):
                from dateutil import parser as dateutil_parser
                created_at = dateutil_parser.parse(created_at)
            
            # ✅ FIX: Make timezone-aware if naive
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            
            # ✅ FIX: Ensure datetime.now() is also timezone-aware
            now = datetime.now(timezone.utc)
            
            days_active = (now - created_at).days
            if days_active < 1:
                days_active = 1
            
            messages_per_day = total_messages / days_active
            
            if messages_per_day >= 5:
                return 'High'
            elif messages_per_day >= 1:
                return 'Medium'
            else:
                return 'Low'
        except Exception as e:
            logger.error(f"Engagement level calculation failed: {e}")
            # Fallback to total-based calculation
    
    # Fallback: use total only
    if total_messages >= 50:
        return 'High'
    elif total_messages >= 10:
        return 'Medium'
    else:
        return 'Low'


async def sync_ai_fields(
    whatsapp_number: str,
    record_id: str,
    table_name: str
):
    """
    Update AI-generated fields (run periodically, not on every message)
    
    NOTE: Most AI fields removed from new Control Plane schema
    This function is deprecated but kept for backward compatibility
    """
    
    logger.debug(f"AI field sync skipped (fields removed from new Control Plane schema)")
    pass

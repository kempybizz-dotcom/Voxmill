#!/usr/bin/env python3
"""
WHATSAPP SELF-SERVICE PREFERENCE MANAGER
=========================================
Allows clients to update their own preferences via WhatsApp

✅ FIXED: No hardcoded markets - validates against Airtable Markets table
✅ FIXED: Industry-agnostic - works across all verticals
✅ FIXED: Proper market validation using existing functions

Examples:
- "Add Chelsea to my reports"
- "Focus more on competitors next week"
- "Change delivery to 6am"
- "Increase report depth"
- "Switch to Manchester" (industry-agnostic)

Uses GPT-4 to:
1. Detect if message is a preference change request
2. Extract the specific changes requested
3. Validate markets against Airtable
4. Update MongoDB and Airtable
5. Confirm professionally to client
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from pymongo import MongoClient
from openai import OpenAI

logger = logging.getLogger(__name__)

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def detect_preference_request(message: str, client_profile: Dict) -> Dict:
    """
    Use GPT-4 to detect if message is a preference change request
    
    Returns:
    {
        "is_preference_request": true/false,
        "intent": "add_regions" | "change_settings" | "adjust_content" | etc,
        "changes": {
            "regions": ["Chelsea", "Belgravia"],
            "report_depth": "detailed",
            "competitor_focus": "high"
        },
        "confirmation_message": "Professional response to client"
    }
    """
    
    if not openai_client:
        raise ValueError("OpenAI API key not configured")
    
    # Current client settings for context
    current_regions = client_profile.get('preferences', {}).get('preferred_regions', [])
    current_tier = client_profile.get('tier', 'tier_3')
    industry = client_profile.get('industry', 'real_estate')
    
    # Industry display name
    industry_display = {
        'real_estate': 'Real Estate',
        'automotive': 'Automotive',
        'healthcare': 'Healthcare',
        'hospitality': 'Hospitality',
        'yachting': 'Yachting',
        'private_equity': 'Private Equity'
    }.get(industry, industry.title())
    
    system_prompt = f"""You are analyzing a WhatsApp message from a {industry_display} intelligence client to determine if they're requesting changes to their service preferences.

RESPOND ONLY WITH VALID JSON. NO OTHER TEXT.

CLIENT CONTEXT:
- Name: {client_profile.get('name')}
- Industry: {industry_display}
- Current Markets: {', '.join(current_regions) if current_regions else 'None'}
- Service Tier: {current_tier}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL: PREFERENCE REQUEST DETECTION RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A message is a PREFERENCE REQUEST if it contains ANY of these indicators:

STRONG SIGNALS (90%+ confidence):
- "update my preferences" / "change my settings"
- "in my next report" / "in my reports" / "in future reports"
- "from now on" / "going forward" / "starting next week"
- "I'd like [X] in my report"
- "I want [X] in my next report"
- "can you add [X]" / "please include [X]"

COMPETITOR FOCUS SIGNALS:
- "more competitors" / "focus on competitors"
- "more competitive intelligence" / "competitor analysis"
- "strategic moves against competitors"
- "deeper competitor insights" / "competitor depth"
- "less competitor info" / "reduce competitor analysis"

REPORT DEPTH SIGNALS:
- "more detail" / "deeper analysis" / "detailed reports"
- "executive summary" / "brief version" / "concise report"
- "comprehensive report" / "full deep dive"

MARKET/REGION SIGNALS (INDUSTRY-AGNOSTIC):
- "add [market]" / "include [market]" / "cover [market]"
- "remove [market]" / "stop covering [market]"
- "switch to [market]" / "focus on [market]" / "change to [market]"

KEY DISTINCTION:
PREFERENCE REQUESTS (is_preference_request = TRUE):
✓ "I'd like more competitors in my next report"
✓ "Focus more on competitors going forward"
✓ "Add Chelsea to my coverage" (Real Estate)
✓ "Switch to Manchester" (any industry)
✓ "Focus on Monaco" (Yachting)

MARKET QUERIES (is_preference_request = FALSE):
✗ "Who are the main competitors?"
✗ "What's the competitive landscape?"
✗ "Analyze competitor positioning"

TASK:
Analyze if this is a preference change request (vs. a one-time market question).

If it IS a preference request, extract the specific changes and respond with JSON:
{{
    "is_preference_request": true,
    "intent": "add_regions" | "remove_regions" | "change_regions" | "change_depth" | "adjust_focus" | "delivery_settings" | "other",
    "changes": {{
        "regions": ["Market1", "Market2"],
        "report_depth": "executive" | "detailed" | "deep",
        "competitor_focus": "low" | "medium" | "high"
    }},
    "confirmation_message": "Professional confirmation message"
}}

If it's NOT a preference request, respond with:
{{
    "is_preference_request": false,
    "intent": "market_query",
    "original_query": "the message"
}}

EXAMPLES:

Message: "I'd like more competitors in my next report"
Response: {{"is_preference_request": true, "intent": "adjust_focus", "changes": {{"competitor_focus": "high"}}, "confirmation_message": "✅ PREFERENCES UPDATED\\n\\n• Competitor Analysis: HIGH\\n\\nStanding by."}}

Message: "Switch to Manchester"
Response: {{"is_preference_request": true, "intent": "change_regions", "changes": {{"regions": ["Manchester"]}}, "confirmation_message": "✅ PREFERENCES UPDATED\\n\\n• Coverage Areas: Manchester\\n\\nStanding by."}}

Message: "What's happening in Mayfair?"
Response: {{"is_preference_request": false, "intent": "market_query", "original_query": "What's happening in Mayfair?"}}
"""

    user_prompt = f"""CLIENT MESSAGE:
"{message}"

Analyze this message and determine if it's a preference change request."""
    
    response = openai_client.chat.completions.create(
        model="gpt-4-turbo-preview",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        max_tokens=1000,
        temperature=0.3,
        timeout=15.0
    )
    
    # Parse response
    response_text = response.choices[0].message.content.strip()
    
    # Clean potential markdown formatting
    if response_text.startswith('```json'):
        response_text = response_text.replace('```json', '').replace('```', '').strip()
    
    result = json.loads(response_text)
    return result


def apply_preference_changes(whatsapp_number: str, changes: Dict, original_message: str = "") -> bool:
    """
    Apply preference changes to BOTH MongoDB AND Airtable
    
    ✅ FIXED: Distinguishes REPLACE vs MERGE for regions based on user intent
    
    Flow:
    1. Find client in MongoDB
    2. Update Airtable (source of truth) 
    3. Read back actual values from Airtable
    4. Update MongoDB (backup/cache)
    5. Return success
    
    Args:
        whatsapp_number: Client's WhatsApp number (with or without 'whatsapp:' prefix)
        changes: Dict with keys like 'competitor_focus', 'report_depth', 'regions', 'industry', 'allowed_modules'
        original_message: Original user message (for intent detection)
    
    Returns:
        True if successful
    """
    
    if db is None:
        logger.error("MongoDB not connected")
        return False
    
    # ✅ STEP 1: Find client in MongoDB
    normalized_number = whatsapp_number.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    client = db['client_profiles'].find_one({
        "$or": [
            {"whatsapp_number": whatsapp_number},
            {"whatsapp_number": normalized_number}
        ]
    })
    
    if not client:
        logger.error(f"Client not found for: {whatsapp_number}")
        return False
    
    logger.info(f"Found client: {client.get('_id')}, applying changes: {changes}")
    
    # ✅ STEP 2: Update Airtable (source of truth)
    airtable_success = False
    
    try:
        AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
        AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
        
        if AIRTABLE_API_KEY and AIRTABLE_BASE_ID:
            import requests
            
            # Get Airtable record ID
            record_id = client.get('airtable_record_id')
            table_name = client.get('airtable_table', 'Accounts')
            
            if not record_id:
                logger.error("No Airtable record ID found")
                return False
            
            # Build URL
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}/{record_id}"
            headers = {
                'Authorization': f'Bearer {AIRTABLE_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            # ========================================
            # BUILD UPDATE PAYLOAD (WRITABLE FIELDS ONLY)
            # ========================================
            airtable_updates = {}
            
            # ✅ FIX: Removed deprecated fields (Competitor Focus, Report Depth)
            # These fields were removed from new Control Plane schema
            # They're still tracked in MongoDB for backward compatibility
            
            # NOTE: In new Control Plane, these preferences don't exist
            # If user requests depth/focus changes, store in MongoDB only
            if 'competitor_focus' in changes:
                logger.debug(f"Competitor focus preference stored in MongoDB only (not in Airtable)")
            
            if 'report_depth' in changes:
                logger.debug(f"Report depth preference stored in MongoDB only (not in Airtable)")
            
            # ========================================
            # CRITICAL FIX: REPLACE vs MERGE FOR REGIONS
            # ========================================
            if 'regions' in changes:
                message_lower = original_message.lower()
                
                # Detect user intent: REPLACE or MERGE
                is_replace = any(keyword in message_lower for keyword in [
                    'switch to', 'focus on', 'change to', 'move to',
                    'dont focus', 'don\'t focus', 'stop focusing',
                    'switch from', 'stop covering'
                ])
                
                # Get current regions from client profile
                current_regions_str = client.get('preferences', {}).get('preferred_regions', [])
                if isinstance(current_regions_str, list):
                    current_regions = current_regions_str
                else:
                    current_regions = [r.strip() for r in str(current_regions_str).split(',') if r.strip()]
                
                if is_replace:
                    # REPLACE: User wants to change focus entirely
                    new_regions_list = changes['regions'] if isinstance(changes['regions'], list) else [changes['regions']]
                    airtable_updates['active_market'] = new_regions_list[0]  # First region becomes active market
                    logger.info(f"🔄 REPLACE market: {new_regions_list[0]}")
                else:
                    # MERGE: User wants to ADD regions (not supported for active_market)
                    # Just take the first new region
                    new_regions_list = changes['regions'] if isinstance(changes['regions'], list) else [changes['regions']]
                    airtable_updates['active_market'] = new_regions_list[0]
                    logger.info(f"➕ SET market: {new_regions_list[0]}")
                
                # Store for confirmation
                changes['_actual_regions'] = airtable_updates.get('active_market', '')
            
            # ========================================
            # UPDATE AIRTABLE RECORD
            # ========================================
            if airtable_updates:
                update_payload = {"fields": airtable_updates}
                
                update_response = requests.patch(
                    url,
                    headers=headers,
                    json=update_payload,
                    timeout=10
                )
                
                if update_response.status_code == 200:
                    logger.info(f"✅ Updated Airtable: {airtable_updates}")
                    airtable_success = True
                    
                    # ========================================
                    # READ BACK ACTUAL VALUES FOR CONFIRMATION
                    # ========================================
                    updated_record = update_response.json()
                    actual_market = updated_record['fields'].get('active_market', '')
                    actual_competitor_focus = updated_record['fields'].get('Competitor Focus', '')
                    actual_report_depth = updated_record['fields'].get('Report Depth', '')
                    
                    # Store actual values for confirmation message
                    changes['_actual_regions'] = actual_market
                    changes['_actual_competitor_focus'] = actual_competitor_focus
                    changes['_actual_report_depth'] = actual_report_depth
                    
                    logger.info(f"📖 Airtable echo: Market={actual_market}, Focus={actual_competitor_focus}, Depth={actual_report_depth}")
                else:
                    logger.warning(f"Airtable update failed: {update_response.status_code} - {update_response.text}")
        else:
            logger.warning("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured")
    
    except Exception as e:
        logger.error(f"Airtable update error: {e}", exc_info=True)
    
    # ✅ STEP 3: Update MongoDB (backup/cache)
    update_data = {}
    
    if 'regions' in changes:
        # Use actual market from Airtable (if available)
        if '_actual_regions' in changes:
            actual_market = changes['_actual_regions']
            update_data['active_market'] = actual_market
            update_data['preferences.preferred_regions'] = [actual_market] if actual_market else []
        else:
            # Fallback to requested regions
            new_region = changes['regions'][0] if isinstance(changes['regions'], list) else changes['regions']
            update_data['active_market'] = new_region
            update_data['preferences.preferred_regions'] = [new_region]
    
    if 'report_depth' in changes:
        update_data['preferences.report_depth'] = changes['report_depth']
    
    if 'competitor_focus' in changes:
        update_data['preferences.competitor_focus'] = changes['competitor_focus']
    
    if 'delivery_time' in changes:
        update_data['delivery_preferences.delivery_time'] = changes['delivery_time']
    
    # ========================================
    # SYNC INDUSTRY TO MONGODB
    # ========================================
    if 'industry' in changes:
        update_data['industry'] = changes['industry']
    
    # ========================================
    # SYNC ALLOWED MODULES TO MONGODB
    # ========================================
    if 'allowed_modules' in changes:
        modules = changes['allowed_modules']
        if isinstance(modules, str):
            modules = [m.strip() for m in modules.split(',')]
        update_data['allowed_intelligence_modules'] = modules
    
    # Handle any other custom fields
    for key, value in changes.items():
        if key not in ['regions', 'report_depth', 'competitor_focus', 'delivery_time', 'industry', 'allowed_modules'] and not key.startswith('_'):
            update_data[f'preferences.{key}'] = value
    
    mongodb_success = False
    
    if update_data:
        update_data['updated_at'] = datetime.now(timezone.utc)
        
        result = db['client_profiles'].update_one(
            {"_id": client['_id']},
            {"$set": update_data}
        )
        
        logger.info(f"✅ Updated MongoDB: matched={result.matched_count}, modified={result.modified_count}")
        mongodb_success = result.modified_count > 0
    
    # ✅ STEP 4: Return success (either Airtable OR MongoDB succeeded)
    return airtable_success or mongodb_success


def handle_whatsapp_preference_message(from_number: str, message: str) -> Optional[str]:
    """
    Main handler for WhatsApp preference changes
    
    ✅ FIXED: Validates markets against Markets table (is_active + Selectable)
    ✅ FIXED: Industry-agnostic market validation
    """
    
    if db is None:
        return None
    
    # ✅ Normalize phone number and find client
    normalized_number = from_number.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    client = db['client_profiles'].find_one({
        "$or": [
            {"whatsapp_number": from_number},
            {"whatsapp_number": normalized_number}
        ]
    })
    
    if not client:
        return None
    
    logger.info(f"Client found: {client.get('_id')}")
    
    # ✅ CHECK FOR ALERT PREFERENCES FIRST
    alert_response = handle_alert_preferences(from_number, message)
    if alert_response:
        return alert_response
    
    # Detect if this is a preference request
    try:
        analysis = detect_preference_request(message, client)
    except Exception as e:
        logger.error(f"Error analyzing message: {e}", exc_info=True)
        return None
    
    # If NOT a preference request, return None
    if not analysis.get('is_preference_request'):
        return None
    
    # It IS a preference request - get changes
    changes = analysis.get('changes', {})
    
    if not changes:
        # Settings inquiry without specific changes
        return analysis.get('confirmation_message', 
            "I can help update your preferences. What would you like to change?\n\n"
            "• Competitor Focus (low/medium/high)\n"
            "• Report Depth (executive/detailed/deep)\n"
            "• Coverage Markets")
    
    # ========================================
    # MARKET VALIDATION (WRITE-TIME)
    # ========================================
    
    if 'regions' in changes:
        # Get client industry
        industry = client.get('industry', 'real_estate')  # ✅ FIXED: lowercase default
        
        # Get requested market
        new_regions = changes['regions']
        new_market = new_regions[0] if isinstance(new_regions, list) else new_regions
        
        # ✅ QUERY AIRTABLE MARKETS TABLE FOR VALIDATION
        from app.whatsapp import get_available_markets_from_db
        
        available_markets = get_available_markets_from_db(industry)
        
        if not available_markets:
            # No markets configured for this industry
            industry_display = {
                'real_estate': 'Real Estate',
                'automotive': 'Automotive',
                'healthcare': 'Healthcare',
                'hospitality': 'Hospitality',
                'yachting': 'Yachting'
            }.get(industry, industry.title())
            
            logger.warning(f"Market validation failed: no markets configured for {industry}")
            
            return f"""NO MARKETS CONFIGURED

Your account is set up for {industry_display} intelligence.

No {industry_display.lower()} markets are currently active.

Contact intel@voxmill.uk to activate market coverage.

Standing by."""
        
        # Check if requested market is available
        if new_market not in available_markets:
            logger.warning(f"Market validation failed: {new_market} not available for {industry}")
            
            markets_list = ', '.join(available_markets[:5])  # Show first 5
            
            return f"""NO ACTIVE COVERAGE

Market "{new_market}" is not currently available in your coverage.

Available markets: {markets_list}

Contact intel@voxmill.uk to activate additional markets.

Standing by."""
    
    # ========================================
    # MARKET VALIDATED - PROCEED WITH UPDATE
    # ========================================
    
    success = apply_preference_changes(from_number, changes, message)
    
    if success:
        # Log the change
        try:
            db['preference_changes'].insert_one({
                "whatsapp_number": from_number,
                "client_name": client.get('name'),
                "original_message": message,
                "changes_applied": changes,
                "timestamp": datetime.now(timezone.utc),
                "source": "whatsapp_self_service"
            })
        except Exception as e:
            logger.error(f"Failed to log preference change: {e}")
        
# ========================================
        # AUTO-SYNC TO AIRTABLE (FIXED - NO NESTED EVENT LOOPS)
        # ========================================
        
        try:
            from app.airtable_auto_sync import sync_usage_metrics
            
            # ✅ FIX: Use asyncio.create_task() instead of run_until_complete()
            # This avoids "event loop already running" error
            asyncio.create_task(
                sync_usage_metrics(
                    whatsapp_number=from_number,
                    record_id=client.get('airtable_record_id'),
                    table_name=client.get('airtable_table', 'Accounts'),
                    event_type='preference_changed',
                    metadata=changes
                )
            )
            
            logger.info(f"✅ Preference change queued for Airtable sync")
            
        except Exception as e:
            logger.error(f"Airtable auto-sync failed (non-critical): {e}")
        
        # ========================================
        # SIMPLIFIED CONFIRMATION (NO MARKETING)
        # ========================================
        
        # Build change summary
        change_lines = []
        
        # ✅ FIX: Removed _actual_* fields (deprecated from Control Plane)
        # These preferences stored in MongoDB only
        if 'competitor_focus' in changes:
            focus = changes['competitor_focus'].lower()
            change_lines.append(f"• Competitor Focus: {focus}")
        
        if 'report_depth' in changes:
            depth = changes['report_depth'].lower()
            change_lines.append(f"• Report Depth: {depth}")
        
        if 'regions' in changes:
            # ✅ Use _actual_regions if available (from Airtable echo)
            market = changes.get('_actual_regions', changes['regions'][0] if isinstance(changes['regions'], list) else changes['regions'])
            change_lines.append(f"• Coverage Area: {market}")
        
        # WORLD-CLASS: Clean, executive-level confirmation
        confirmation = f"""✅ PREFERENCES UPDATED

{chr(10).join(change_lines)}

Standing by."""
        
        logger.info(f"✅ Preferences updated successfully for {from_number}")
        return confirmation
    else:
        logger.error(f"❌ Failed to update preferences for {from_number}")
        return "Unable to update preferences.\n\nStanding by."

def enhanced_whatsapp_handler(from_number: str, message: str) -> str:
    """
    Enhanced WhatsApp handler that checks for preference requests first,
    then falls back to normal market intelligence queries
    """
    
    # Step 1: Check if it's a preference request
    preference_response = handle_whatsapp_preference_message(from_number, message)
    
    # If it's a preference request, return that response
    if preference_response is not None:
        return preference_response
    
    # Step 2: Otherwise, use normal WhatsApp analyst
    from app.whatsapp import handle_whatsapp_message
    return handle_whatsapp_message(from_number, message)


def handle_alert_preferences(whatsapp_number: str, message: str) -> Optional[str]:
    """
    Handle alert preference updates
    
    Examples:
    - "Enable price drop alerts"
    - "Turn off inventory alerts"  
    - "Alert me when competitors adjust pricing"
    """
    
    if db is None:
        return None
    
    # Normalize phone number
    normalized_number = whatsapp_number.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    client = db['client_profiles'].find_one({
        "$or": [
            {"whatsapp_number": whatsapp_number},
            {"whatsapp_number": normalized_number}
        ]
    })
    
    if not client:
        return None
    
    # Check tier (only Tier 2+ get alerts)
    tier = client.get('tier', 'tier_1')
    if tier == 'tier_1':
        return """Real-Time Alerts are available on Tier 2 and Tier 3 plans.

Upgrade to receive:
- Price drop alerts (>5% adjustments)
- Inventory surge notifications
- Competitor behavior shift signals
- New opportunity detection

Contact intel@voxmill.uk to upgrade."""
    
    # Simple keyword detection for alert preferences
    message_lower = message.lower()
    
    alert_enabled = any(word in message_lower for word in ['enable', 'turn on', 'activate', 'start'])
    alert_disabled = any(word in message_lower for word in ['disable', 'turn off', 'deactivate', 'stop'])
    
    if alert_enabled or alert_disabled:
        # Update alert preferences
        db['client_profiles'].update_one(
            {"_id": client['_id']},
            {
                "$set": {
                    "alert_preferences.enabled": alert_enabled,
                    "alert_preferences.updated_at": datetime.now(timezone.utc)
                }
            }
        )
        
        status = "ENABLED" if alert_enabled else "DISABLED"
        
        return f"""✅ ALERT PREFERENCES UPDATED

Real-Time Alerts: {status}

You will {"receive" if alert_enabled else "not receive"} WhatsApp notifications for:
- Price drops (>5%)
- Inventory surges (5+ new listings)
- Competitor behavior shifts (>30% inventory change)
- New opportunities (below-median pricing)

Your preferences can be changed anytime."""
    
    # If just asking about alerts (not changing)
    if 'alert' in message_lower:
        current_status = client.get('alert_preferences', {}).get('enabled', True)
        
        return f"""ALERT SETTINGS

Current Status: {"ENABLED" if current_status else "DISABLED ❌"}

Alert Types:
- Price Drop Alerts (>5% adjustments)
- Inventory Surge Alerts (5+ new listings)
- Competitor Behavior Alerts (>30% change)
- New Opportunity Alerts (below-median)

To change: Send "Enable alerts" or "Disable alerts"
"""
    
    return None

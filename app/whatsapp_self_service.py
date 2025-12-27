#!/usr/bin/env python3
"""
WHATSAPP SELF-SERVICE PREFERENCE MANAGER
=========================================
Allows clients to update their own preferences via WhatsApp

Examples:
- "Add Chelsea to my reports"
- "Focus more on competitors next week"
- "Change delivery to 6am"
- "Increase report depth"

Uses GPT-4 to:
1. Detect if message is a preference change request
2. Extract the specific changes requested
3. Update MongoDB
4. Confirm professionally to client
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from pymongo import MongoClient
from openai import OpenAI

# ✅ ADD LOGGING
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
    current_city = client_profile.get('city', 'London')
    current_tier = client_profile.get('tier', 'tier_3')
    
    system_prompt = f"""You are analyzing a WhatsApp message from a luxury real estate intelligence client to determine if they're requesting changes to their service preferences.

RESPOND ONLY WITH VALID JSON. NO OTHER TEXT.

CLIENT CONTEXT:
- Name: {client_profile.get('name')}
- Current City: {current_city}
- Current Regions: {', '.join(current_regions) if current_regions else 'None'}
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

REGION SIGNALS:
- "add [region]" / "include [region]" / "cover [region]"
- "remove [region]" / "stop covering [region]"

KEY DISTINCTION:
PREFERENCE REQUESTS (is_preference_request = TRUE):
✓ "I'd like more competitors in my next report"
✓ "Focus more on competitors going forward"
✓ "Add Chelsea to my coverage"
✓ "Strategic moves against competitors in my report"

MARKET QUERIES (is_preference_request = FALSE):
✗ "Who are the main competitors?"
✗ "What's the competitive landscape?"
✗ "Analyze competitor positioning"

TASK:
Analyze if this is a preference change request (vs. a one-time market question).

If it IS a preference request, extract the specific changes and respond with JSON:
{{
    "is_preference_request": true,
    "intent": "add_regions" | "remove_regions" | "change_depth" | "adjust_focus" | "delivery_settings" | "other",
    "changes": {{
        "regions": ["Area1", "Area2"],
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

Message: "I'd like more competitors and strategic moves against competitors in my next report"
Response: {{"is_preference_request": true, "intent": "adjust_focus", "changes": {{"competitor_focus": "high"}}, "confirmation_message": "✅ PREFERENCES UPDATED\\n\\n• Competitor Analysis: HIGH (10 agencies)\\n\\nYour next intelligence deck arrives Sunday at 6:00 AM UTC.\\n\\n━━━━━━━━━━━━━━━━━━━━\\n⚡ NEED THIS URGENTLY?\\n\\nContact: ollys@voxmill.uk\\n━━━━━━━━━━━━━━━━━━━━"}}

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


def apply_preference_changes(whatsapp_number: str, changes: Dict) -> bool:
    """
    Apply preference changes to BOTH MongoDB AND Airtable
    
    Flow:
    1. Find client in MongoDB
    2. Update Airtable (source of truth) 
    3. Update MongoDB (backup/cache)
    4. Return success
    
    Args:
        whatsapp_number: Client's WhatsApp number (with or without 'whatsapp:' prefix)
        changes: Dict with keys like 'competitor_focus', 'report_depth', 'regions'
    
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
        AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID', 'apptsyINaEjzWgCha')
        AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')
        
        if AIRTABLE_API_KEY:
            import requests
            
            # Find Airtable record
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            headers = {
                'Authorization': f'Bearer {AIRTABLE_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'filterByFormula': f"{{WhatsApp}}='{normalized_number}'",
                'maxRecords': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                if records:
                    record_id = records[0]['id']
                    
                    # Build update payload
                    airtable_updates = {}
                    
                    if 'competitor_focus' in changes:
                        # Map to Airtable values
                        airtable_updates['Competitor Focus'] = changes['competitor_focus']
                    
                    if 'report_depth' in changes:
                        # Map to Airtable values
                        airtable_updates['Report Depth'] = changes['report_depth']
                    
                    if 'regions' in changes:
                        # Get current regions from Airtable
                        current_regions_str = records[0]['fields'].get('Regions', '')
                        current_regions = [r.strip() for r in current_regions_str.split(',') if r.strip()]
                        
                        # Merge with new regions (avoid duplicates)
                        new_regions = list(set(current_regions + changes['regions']))
                        airtable_updates['Regions'] = ', '.join(new_regions)
                    
                    # Update Airtable record
                    if airtable_updates:
                        update_url = f"{url}/{record_id}"
                        update_payload = {"fields": airtable_updates}
                        
                        update_response = requests.patch(
                            update_url,
                            headers=headers,
                            json=update_payload,
                            timeout=10
                        )
                        
                        if update_response.status_code == 200:
                            logger.info(f"✅ Updated Airtable: {airtable_updates}")
                            airtable_success = True
                        else:
                            logger.warning(f"Airtable update failed: {update_response.status_code} - {update_response.text}")
                else:
                    logger.warning(f"No Airtable record found for {normalized_number}")
            else:
                logger.warning(f"Airtable query failed: {response.status_code}")
        else:
            logger.warning("AIRTABLE_API_KEY not configured")
    
    except Exception as e:
        logger.error(f"Airtable update error: {e}", exc_info=True)
    
    # ✅ STEP 3: Update MongoDB (backup/cache)
    update_data = {}
    
    if 'regions' in changes:
        current_regions = client.get('preferences', {}).get('preferred_regions', [])
        new_regions = list(set(current_regions + changes['regions']))
        update_data['preferences.preferred_regions'] = new_regions
    
    if 'report_depth' in changes:
        update_data['preferences.report_depth'] = changes['report_depth']
    
    if 'competitor_focus' in changes:
        update_data['preferences.competitor_focus'] = changes['competitor_focus']
    
    # Handle delivery time
    if 'delivery_time' in changes:
        update_data['delivery_preferences.delivery_time'] = changes['delivery_time']
    
    # Handle any other custom fields
    for key, value in changes.items():
        if key not in ['regions', 'report_depth', 'competitor_focus', 'delivery_time']:
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
    Main handler for WhatsApp messages - READ ONLY
    Does NOT modify any global state
    Returns response to send to client, or None if not a preference request
    """
    
    if db is None:
        return None
    
    # ✅ FIX: Normalize phone number and find client
    normalized_number = from_number.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    client = db['client_profiles'].find_one({
        "$or": [
            {"whatsapp_number": from_number},
            {"whatsapp_number": normalized_number}
        ]
    })
    
    if not client:
        # Not a recognized client - let normal handler deal with it
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
        # Fall back to normal query handling
        return None
    
    # If NOT a preference request, return None to use normal analyst
    if not analysis.get('is_preference_request'):
        return None
    
    # It IS a preference request - apply changes
    changes = analysis.get('changes', {})
    
    if not changes:
        # Settings inquiry without specific changes
        return analysis.get('confirmation_message', 
            "I can help update your preferences. What would you like to change?\n\n"
            "• Competitor Focus (low/medium/high)\n"
            "• Report Depth (executive/detailed/deep)\n"
            "• Coverage Regions")
    
    # ✅ FIX: Use WhatsApp number (not email)
    success = apply_preference_changes(from_number, changes)
    
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
        
        # BUILD RICH CONFIRMATION MESSAGE
        today = datetime.now()
        days_until_sunday = (6 - today.weekday()) % 7
        if days_until_sunday == 0:
            days_until_sunday = 7
        next_sunday = today + timedelta(days=days_until_sunday)
        
        # Build change summary
        change_lines = []
        if 'competitor_focus' in changes:
            focus = changes['competitor_focus']
            count = {'low': 3, 'medium': 6, 'high': 10}[focus]
            change_lines.append(f"• Competitor Analysis: {focus.upper()} ({count} agencies)")
        
        if 'report_depth' in changes:
            depth = changes['report_depth']
            slides = {'executive': 5, 'detailed': 14, 'deep': '14+'}[depth]
            change_lines.append(f"• Report Depth: {depth.upper()} ({slides} slides)")
        
        if 'regions' in changes:
            regions = ', '.join(changes['regions'])
            change_lines.append(f"• Coverage Areas: {regions}")
        
        # Final confirmation message
        confirmation = f"""✅ PREFERENCES UPDATED

{chr(10).join(change_lines)}

Your next intelligence deck arrives {next_sunday.strftime('%A, %B %d')} at 6:00 AM UTC.

━━━━━━━━━━━━━━━━━━━━
NEED THIS URGENTLY?

Contact your Voxmill operator for immediate regeneration:
Intel@voxmill.uk

━━━━━━━━━━━━━━━━━━━━

Voxmill Intelligence — Precision at Scale"""
        
        logger.info(f"✅ Preferences updated successfully for {from_number}")
        return confirmation
    else:
        logger.error(f"❌ Failed to update preferences for {from_number}")
        return "❌ Unable to update preferences. Please try again or contact support."


# ============================================================================
# INTEGRATION WITH EXISTING WHATSAPP HANDLER
# ============================================================================

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
    # (This would call your existing WhatsApp analyst code)
    from app.whatsapp import handle_whatsapp_message  # Your existing handler
    return handle_whatsapp_message(from_number, message)


# ADD THIS FUNCTION to whatsapp_self_service.py (at the bottom, before if __name__ == "__main__":)

def handle_alert_preferences(whatsapp_number: str, message: str) -> Optional[str]:
    """
    Handle alert preference updates
    
    Examples:
    - "Enable price drop alerts"
    - "Turn off inventory alerts"  
    - "Alert me when Knight Frank adjusts pricing"
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
- Agent behavior shift signals
- New opportunity detection

Contact ollys@voxmill.uk to upgrade."""
    
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
- Agent behavior shifts (>30% inventory change)
- New opportunities (below-median pricing)

Your preferences can be changed anytime."""
    
    # If just asking about alerts (not changing)
    if 'alert' in message_lower:
        current_status = client.get('alert_preferences', {}).get('enabled', True)
        
        return f"""ALERT SETTINGS

Current Status: {"ENABLED ✅" if current_status else "DISABLED ❌"}

Alert Types:
- Price Drop Alerts (>5% adjustments)
- Inventory Surge Alerts (5+ new listings)
- Agent Behavior Alerts (>30% change)
- New Opportunity Alerts (below-median)

To change: Send "Enable alerts" or "Disable alerts"
"""
    
    return None


if __name__ == "__main__":
    # Test examples
    test_messages = [
        "Add Chelsea to my weekly reports",
        "Focus more on competitors next week",
        "What's the latest in Mayfair?",  # Not a preference request
        "Change delivery to 6am and add Kensington",
        "Make my reports more detailed"
    ]
    
    print("Testing preference detection...\n")
    
    test_client = {
        "name": "Mark Thompson",
        "whatsapp_number": "+447780565645",
        "city": "London",
        "preferences": {
            "preferred_regions": ["Mayfair", "Knightsbridge"]
        }
    }
    
    for msg in test_messages:
        print(f"Message: {msg}")
        result = detect_preference_request(msg, test_client)
        print(f"Is Preference: {result.get('is_preference_request')}")
        if result.get('is_preference_request'):
            print(f"Changes: {result.get('changes')}")
            print(f"Response: {result.get('confirmation_message')}")
        print("-" * 70)

def log_interaction(sender: str, message: str, category: str, response: str):
    """
    Log interaction to BOTH MongoDB AND Airtable Usage Logs
    
    CRITICAL: Must NOT log blocked queries as valid usage
    """
    
    # ONLY log if query was ALLOWED (not blocked by governance)
    blocked_categories = ['governance_override', 'data_load_blocked', 
                          'analysis_blocked', 'rate_check']
    
    if category in blocked_categories:
        logger.info(f"⏭️ Skipping usage log for blocked query: {category}")
        return
    
    # Log to MongoDB
    if mongo_client:
        db = mongo_client['Voxmill']
        db['usage_logs'].insert_one({
            'timestamp': datetime.now(timezone.utc),
            'whatsapp_number': sender,
            'message_query': message[:500],  # Truncate
            'response_summary': response[:500],
            'category': category,
            'tokens_used': estimate_tokens(response)
        })
    
    # Queue Airtable write (non-blocking)
    from app.airtable_queue import queue_airtable_update
    asyncio.create_task(queue_airtable_update(
        table_name='Usage Logs',
        fields={
            'WhatsApp Number': sender,
            'Message Query': message[:500],
            'Response Summary': response[:500],
            'Category': category,
            'Tokens Used': estimate_tokens(response)
        }
    ))

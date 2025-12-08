#!/usr/bin/env python3
"""
WHATSAPP SELF-SERVICE PREFERENCE MANAGER
=========================================
Allows clients to update their own preferences via WhatsApp
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List
from pymongo import MongoClient
from openai import OpenAI

# ✅ FIX: Add logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def get_next_sunday():
    """Calculate next Sunday's date"""
    today = datetime.now()
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        days_until_sunday = 7
    return today + timedelta(days=days_until_sunday)


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
    current_competitor_focus = client_profile.get('preferences', {}).get('competitor_focus', 'medium')
    current_report_depth = client_profile.get('preferences', {}).get('report_depth', 'detailed')
    
    system_prompt = f"""You are analyzing a WhatsApp message to determine if the user wants to CHANGE THEIR SERVICE PREFERENCES.

RESPOND ONLY WITH VALID JSON. NO OTHER TEXT.

CLIENT CONTEXT:
- Name: {client_profile.get('name')}
- Current City: {current_city}
- Current Regions: {', '.join(current_regions) if current_regions else 'None'}
- Current Competitor Focus: {current_competitor_focus}
- Current Report Depth: {current_report_depth}
- Service Tier: {current_tier}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL: This message has ALREADY passed keyword detection for preference-related terms.
Your job is to CONFIRM the intent and EXTRACT specific changes.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREFERENCE REQUEST INDICATORS:
✓ "I need more competitors in my next report" → TRUE, changes: {{"competitor_focus": "high"}}
✓ "Add Chelsea to my coverage" → TRUE, changes: {{"regions": ["Chelsea"]}}
✓ "Make my reports more detailed" → TRUE, changes: {{"report_depth": "deep"}}
✓ "I want to update my preferences" → TRUE, changes: {{}} (ask for specifics)

NOT PREFERENCE REQUESTS:
✗ "Who are my main competitors?" → FALSE (market query)
✗ "What's the competitive landscape?" → FALSE (analysis request)

If the message requests a SETTINGS CHANGE, respond:
{{
    "is_preference_request": true,
    "intent": "adjust_focus" | "change_depth" | "add_regions" | "general_update",
    "changes": {{
        "competitor_focus": "low" | "medium" | "high",
        "report_depth": "executive" | "detailed" | "deep",
        "regions": ["Area1"]
    }},
    "confirmation_message": "Professional confirmation"
}}

If it's just a QUESTION, respond:
{{
    "is_preference_request": false,
    "intent": "market_query"
}}

EXAMPLES:

Message: "I need more competitors in my next report"
Response: {{"is_preference_request": true, "intent": "adjust_focus", "changes": {{"competitor_focus": "high"}}, "confirmation_message": "..."}}

Message: "I want to update my preferences"
Response: {{"is_preference_request": true, "intent": "general_update", "changes": {{}}, "confirmation_message": "What would you like to update? Competitor focus, report depth, or coverage regions?"}}

Message: "Who are my competitors?"
Response: {{"is_preference_request": false, "intent": "market_query"}}
"""

    user_prompt = f"""CLIENT MESSAGE:
"{message}"

Analyze this message and determine if it's a preference change request."""
    
    try:
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
        
        response_text = response.choices[0].message.content.strip()
        
        # Clean markdown formatting
        if response_text.startswith('```json'):
            response_text = response_text.replace('```json', '').replace('```', '').strip()
        
        result = json.loads(response_text)
        return result
    
    except Exception as e:
        logger.error(f"GPT-4 analysis failed: {e}", exc_info=True)
        raise


def apply_preference_changes(client_email: str, changes: Dict) -> bool:
    """Apply preference changes to MongoDB"""
    
    if db is None:
        logger.error("MongoDB not connected")
        return False
    
    logger.info(f"Applying preferences for {client_email}: {changes}")
    
    update_data = {}
    
    # Handle region changes
    if 'regions' in changes:
        current_profile = db['client_profiles'].find_one({"email": client_email})
        if not current_profile:
            logger.error(f"Client not found: {client_email}")
            return False
        
        current_regions = current_profile.get('preferences', {}).get('preferred_regions', [])
        new_regions = list(set(current_regions + changes['regions']))
        update_data['preferences.preferred_regions'] = new_regions
    
    # Handle report depth
    if 'report_depth' in changes:
        update_data['preferences.report_depth'] = changes['report_depth']
    
    # Handle competitor focus
    if 'competitor_focus' in changes:
        update_data['preferences.competitor_focus'] = changes['competitor_focus']
    
    # Handle delivery time
    if 'delivery_time' in changes:
        update_data['delivery_preferences.delivery_time'] = changes['delivery_time']
    
    # Handle any other custom fields
    for key, value in changes.items():
        if key not in ['regions', 'report_depth', 'competitor_focus', 'delivery_time']:
            update_data[f'preferences.{key}'] = value
    
    # Update MongoDB
    if update_data:
        update_data['updated_at'] = datetime.now(timezone.utc)
        
        result = db['client_profiles'].update_one(
            {"email": client_email},
            {"$set": update_data}
        )
        
        logger.info(f"MongoDB update result: matched={result.matched_count}, modified={result.modified_count}")
        return result.modified_count > 0
    
    return False


def handle_whatsapp_preference_message(from_number: str, message: str) -> Optional[str]:
    """
    Main handler for WhatsApp messages
    Returns response to send to client, or None if not a preference request
    """
    
    if db is None:
        logger.warning("MongoDB not connected")
        return None
    
    # ✅ PASS 1: KEYWORD PRE-FILTER (No API call)
    message_lower = message.lower()
    
    preference_keywords = [
        'my next report', 'my reports', 'future reports',
        'my preferences', 'update my preferences', 'change my preferences',
        'more competitors', 'less competitors', 'fewer competitors',
        'more detail', 'less detail', 'deeper analysis',
        'add region', 'remove region', 'stop covering',
        'change delivery', 'delivery time', 'report depth',
        'competitor focus', 'executive summary', 'detailed report'
    ]
    
    has_preference_keyword = any(kw in message_lower for kw in preference_keywords)
    
    if not has_preference_keyword:
        return None
    
    logger.info(f"🎯 Preference keyword detected in: '{message[:50]}'")
    
    # ✅ PASS 2: Find client (handle both phone formats)
    normalized_number = from_number.replace('whatsapp:', '').replace('whatsapp%3A', '')
    
    client = db['client_profiles'].find_one({
        "$or": [
            {"whatsapp_number": from_number},
            {"whatsapp_number": normalized_number}
        ]
    })
    
    if not client:
        logger.warning(f"Client not found for: {from_number}")
        return None
    
    # ✅ Check for email field
    if not client.get('email'):
        logger.error(f"Client profile missing email: {from_number}")
        return (
            "⚠️ Your profile is incomplete. "
            "Please contact support: ollys@voxmill.uk"
        )
    
    # ✅ PASS 3: GPT-4 Intent Detection
    try:
        analysis = detect_preference_request(message, client)
    except Exception as e:
        logger.error(f"Error analyzing message: {e}", exc_info=True)
        # Fallback response
        return (
            "I can help update your preferences.\n\n"
            "Please specify:\n"
            "• Competitor Focus: 'Set competitor focus to HIGH'\n"
            "• Report Depth: 'Set report depth to DETAILED'\n"
            "• Coverage Regions: 'Add Chelsea to my reports'"
        )
    
    # If NOT a preference request (GPT-4 disagreed with keywords)
    if not analysis.get('is_preference_request'):
        logger.warning(f"GPT-4 rejected preference request: '{message}'")
        return None
    
    # Extract changes
    changes = analysis.get('changes', {})
    
    # If no specific changes, ask for clarification
    if not changes:
        return (
            "I can help update your preferences. What would you like to change?\n\n"
            "• Competitor Focus (low/medium/high)\n"
            "• Report Depth (executive/detailed/deep)\n"
            "• Coverage Regions"
        )
    
    # ✅ Apply changes
    logger.info(f"Applying preference changes for {client['email']}: {changes}")
    
    success = apply_preference_changes(client['email'], changes)
    
    if success:
        # Log the change
        try:
            db['preference_changes'].insert_one({
                "client_email": client['email'],
                "client_name": client.get('name'),
                "whatsapp_number": from_number,
                "original_message": message,
                "changes_applied": changes,
                "timestamp": datetime.now(timezone.utc),
                "source": "whatsapp_self_service"
            })
        except Exception as e:
            logger.error(f"Failed to log preference change: {e}")
        
        # Build confirmation message
        next_sunday = get_next_sunday()
        
        change_lines = []
        if 'competitor_focus' in changes:
            focus = changes['competitor_focus']
            count = {'low': 3, 'medium': 6, 'high': 10}.get(focus, 6)
            change_lines.append(f"• Competitor Analysis: {focus.upper()} ({count} agencies)")
        
        if 'report_depth' in changes:
            depth = changes['report_depth']
            slides = {'executive': 5, 'detailed': 14, 'deep': '14+'}.get(depth, 14)
            change_lines.append(f"• Report Depth: {depth.upper()} ({slides} slides)")
        
        if 'regions' in changes:
            regions = ', '.join(changes['regions'])
            change_lines.append(f"• Coverage Areas: {regions}")
        
        confirmation = f"""✅ PREFERENCES UPDATED

{chr(10).join(change_lines)}

Your next intelligence deck arrives {next_sunday.strftime('%A, %B %d')} at 6:00 AM UTC.

━━━━━━━━━━━━━━━━━━━━
⚡ NEED THIS URGENTLY?

Contact: intel@voxmill.uk

━━━━━━━━━━━━━━━━━━━━

Voxmill Intelligence — Precision at Scale"""
        
        logger.info(f"✅ Preferences updated for {client['email']}")
        return confirmation
    else:
        logger.error(f"❌ Failed to update preferences for {client['email']}")
        return "❌ Unable to update preferences. Please try again or contact support."

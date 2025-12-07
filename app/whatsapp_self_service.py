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
from datetime import datetime, timezone
from typing import Dict, Optional, List
from pymongo import MongoClient
from openai import OpenAI

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

TASK:
Analyze if this is a preference change request (vs. a market question).

If it IS a preference request, extract the specific changes and respond with JSON:
{{
    "is_preference_request": true,
    "intent": "add_regions" | "remove_regions" | "change_depth" | "adjust_focus" | "delivery_settings" | "other",
    "changes": {{
        "regions": ["Area1", "Area2"],
        "report_depth": "executive" | "detailed" | "deep",
        "competitor_focus": "low" | "medium" | "high",
        "delivery_time": "06:00" | "12:00" | "18:00"
    }},
    "confirmation_message": "Professional confirmation message to send client"
}}

If it's NOT a preference request (just a question), respond with:
{{
    "is_preference_request": false,
    "intent": "market_query",
    "original_query": "the message"
}}

EXAMPLES:

Message: "Add Chelsea to my weekly reports"
Response: {{"is_preference_request": true, "intent": "add_regions", "changes": {{"regions": ["Chelsea"]}}, "confirmation_message": "Perfect. I've added Chelsea to your coverage areas. Your next report will include comprehensive Chelsea market analysis alongside Mayfair and Knightsbridge."}}

Message: "Focus more on my competitors next week"
Response: {{"is_preference_request": true, "intent": "adjust_focus", "changes": {{"competitor_focus": "high"}}, "confirmation_message": "Understood. I've increased competitor analysis depth for your upcoming reports. You'll receive enhanced competitive positioning intelligence, market share analysis, and strategic threat assessments."}}

Message: "What's happening in Mayfair this week?"
Response: {{"is_preference_request": false, "intent": "market_query", "original_query": "What's happening in Mayfair this week?"}}
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


def apply_preference_changes(client_email: str, changes: Dict) -> bool:
    """Apply preference changes to MongoDB"""
    
    if not db:
        return False
    
    update_data = {}
    
    # Handle region changes
    if 'regions' in changes:
        current_profile = db['client_profiles'].find_one({"email": client_email})
        current_regions = current_profile.get('preferences', {}).get('preferred_regions', [])
        
        # Add new regions (avoid duplicates)
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
        
        return result.modified_count > 0
    
    return False


def handle_whatsapp_preference_message(from_number: str, message: str) -> str:
    """
    Main handler for WhatsApp messages
    Returns response to send to client
    """
    
    if not db:
        return "Service temporarily unavailable. Please try again later."
    
    # Find client by WhatsApp number
    client = db['client_profiles'].find_one({"whatsapp_number": from_number})
    
    if not client:
        return "Number not recognized. Please contact support@voxmill.uk to link your account."
    
    # Detect if this is a preference request
    try:
        analysis = detect_preference_request(message, client)
    except Exception as e:
        print(f"Error analyzing message: {e}")
        # Fall back to normal query handling
        return None  # Signal to use normal WhatsApp analyst
    
    # If NOT a preference request, return None to use normal analyst
    if not analysis.get('is_preference_request'):
        return None
    
    # It IS a preference request - apply changes
    changes = analysis.get('changes', {})
    
    if changes:
        success = apply_preference_changes(client['email'], changes)
        
        if success:
            # Log the change
            db['preference_changes'].insert_one({
                "client_email": client['email'],
                "client_name": client.get('name'),
                "whatsapp_number": from_number,
                "original_message": message,
                "changes_applied": changes,
                "timestamp": datetime.now(timezone.utc),
                "source": "whatsapp_self_service"
            })
            
            # Return confirmation message
            return analysis.get('confirmation_message', 
                "✅ Preferences updated successfully. Changes will be reflected in your next report.")
        else:
            return "❌ Unable to update preferences. Please try again or contact support."
    
    # No changes detected
    return "I didn't detect any specific preference changes in your message. Could you clarify what you'd like to adjust?"


def handle_preference_update(client_email, new_preferences):
    """Update preferences with premium confirmation message"""
    
    # Update MongoDB
    db.client_profiles.update_one(
        {"email": client_email},
        {"$set": {"preferences": new_preferences}}
    )
    
    # Get next Sunday date
    from datetime import datetime, timedelta
    today = datetime.now()
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        days_until_sunday = 7
    next_sunday = today + timedelta(days=days_until_sunday)
    
    # Build confirmation message
    changes = []
    if 'competitor_focus' in new_preferences:
        focus = new_preferences['competitor_focus']
        count = {'low': 3, 'medium': 6, 'high': 10}[focus]
        changes.append(f"• Competitor Analysis: {focus.upper()} ({count} agencies)")
    
    if 'report_depth' in new_preferences:
        depth = new_preferences['report_depth']
        slides = {'executive': 5, 'detailed': 14, 'deep': '14+'}[depth]
        changes.append(f"• Report Depth: {depth.upper()} ({slides} slides)")
    
    message = f"""✅ PREFERENCES UPDATED

{chr(10).join(changes)}

Your next intelligence deck arrives {next_sunday.strftime('%A, %B %d')} at 6:00 AM UTC.

━━━━━━━━━━━━━━━━━━━━
⚡ NEED THIS URGENTLY?

Contact your Voxmill operator for immediate regeneration:
📧 operator@voxmill.uk
📱 WhatsApp: +44 XXXX XXXXXX

━━━━━━━━━━━━━━━━━━━━

Voxmill Intelligence — Precision at Scale"""
    
    return message


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


# ============================================================================
# EXAMPLES OF MESSAGES IT CAN HANDLE
# ============================================================================

EXAMPLE_MESSAGES = """
CLIENT MESSAGES THE SYSTEM CAN UNDERSTAND:

ADDING REGIONS:
✓ "Add Chelsea to my reports"
✓ "Include Knightsbridge and Belgravia next week"
✓ "Start covering Notting Hill"
✓ "Can you add Kensington to my coverage?"

ADJUSTING DEPTH:
✓ "Make my reports more detailed"
✓ "I want deeper analysis going forward"
✓ "Switch to executive summary only"
✓ "Give me the full deep dive version"

COMPETITOR FOCUS:
✓ "Focus more on my competitors"
✓ "I want more competitive intelligence"
✓ "Less competitor info, more market data"
✓ "Deep dive on competitive landscape"

DELIVERY SETTINGS:
✓ "Send my reports at 6am instead"
✓ "Change delivery to morning"
✓ "I prefer evening reports"

REMOVING REGIONS:
✓ "Stop covering Mayfair"
✓ "Remove Chelsea from my reports"
✓ "Don't need Belgravia anymore"

MIXED REQUESTS:
✓ "Add Chelsea and increase competitor focus"
✓ "Send reports at 6am and add Kensington coverage"

STILL HANDLES NORMAL QUERIES:
✓ "What's happening in Mayfair?"
✓ "Send me the latest PDF"
✓ "Show me recent sales data"
→ These go to normal WhatsApp analyst
"""


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
        "email": "mark@fund.com",
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

"""
VOXMILL WHATSAPP HANDLER
========================
Handles incoming WhatsApp messages with V3 predictive intelligence + welcome messages + Airtable API integration
"""

import os
import logging
import httpx
import hashlib
import re
import requests
import pytz
from datetime import datetime, timezone, timedelta
from twilio.rest import Client
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction
from app.whatsapp_self_service import handle_whatsapp_preference_message
from app.conversation_manager import ConversationSession, resolve_reference, generate_contextualized_prompt
from app.security import SecurityValidator
from app.cache_manager import CacheManager
from app.client_manager import get_client_profile, update_client_history
from app.pin_auth import (
    PINAuthenticator,
    get_pin_status_message,
    get_pin_response_message
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

# Initialize Twilio client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None


def get_time_appropriate_greeting(client_name: str = "there") -> str:
    """Generate time-appropriate greeting based on UK time"""
    # Get current time in UK
    uk_tz = pytz.timezone('Europe/London')
    uk_time = datetime.now(uk_tz)
    hour = uk_time.hour
    
    # Extract first name
    first_name = client_name.split()[0] if client_name != "there" else ""
    
    # Time-based greetings
    if 5 <= hour < 12:
        greeting = f"Good morning{', ' + first_name if first_name else ''}."
    elif 12 <= hour < 17:
        greeting = f"Good afternoon{', ' + first_name if first_name else ''}."
    elif 17 <= hour < 22:
        greeting = f"Good evening{', ' + first_name if first_name else ''}."
    else:
        greeting = f"Evening{', ' + first_name if first_name else ''}."
    
    return greeting


async def send_first_time_welcome(sender: str, client_profile: dict):
    """Send welcome message to first-time users"""
    try:
        tier = client_profile.get('tier', 'tier_1')
        name = client_profile.get('name', 'there')
        
        # Split name to get first name only
        first_name = name.split()[0] if name != 'there' else 'there'
        
        # Get time-appropriate greeting
        greeting = get_time_appropriate_greeting(first_name)
        
        # Tier-specific welcome messages
        welcome_messages = {
            "tier_1": f"""{greeting}

Welcome to Voxmill Intelligence.

Your Tier 1 access is now active.

You have access to:
- Real-time market overview
- Competitive intelligence
- Opportunity identification
- Price corridor analysis

Ask me anything about luxury markets. Try:
- "Market overview"
- "Top opportunities"
- "Competitive landscape"

Available 24/7 at this number.""",

            "tier_2": f"""{greeting}

Welcome to Voxmill Intelligence.

Your Tier 2 Analyst Desk is now active.

You have full access to:
- Real-time market intelligence
- Competitive dynamics analysis
- Trend detection (14-day windows)
- Strategic recommendations
- Liquidity velocity tracking
- Up to 50 analyses per hour

Your intelligence is personalized to your preferences and will learn from our conversations.

Ask me anything. Try:
- "What's the market outlook?"
- "Analyze competitive positioning"
- "Show me liquidity trends"

Available 24/7.""",

            "tier_3": f"""{greeting}

Welcome to Voxmill Intelligence.

Your Tier 3 Strategic Partner access is now active.

You have unlimited access to our complete intelligence suite:

REAL-TIME ANALYSIS:
- Market overview & trends
- Competitive landscape
- Opportunity identification

PREDICTIVE INTELLIGENCE:
- Agent behavioral profiling (85-91% confidence)
- Multi-wave cascade forecasting
- Liquidity velocity tracking
- Micromarket segmentation

SCENARIO MODELING:
- "What if Knight Frank drops 10%?"
- Strategic response recommendations
- Risk/opportunity mapping

No message limits. Full institutional-grade intelligence.

Ask me anything, anytime. Examples:
- "Strategic outlook for Mayfair"
- "What if Savills raises prices 8%?"
- "Analyze liquidity velocity"
- "Predict cascade effects"

Your dedicated intelligence partner, available 24/7."""
        }
        
        message = welcome_messages.get(tier, welcome_messages["tier_1"])
        
        await send_twilio_message(sender, message)
        logger.info(f"Welcome message sent to {sender} (Tier: {tier})")
        
        # Small delay before processing their first query
        import asyncio
        await asyncio.sleep(1.5)
        
    except Exception as e:
        logger.error(f"Error sending welcome message: {str(e)}", exc_info=True)


async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler with V3 predictive intelligence + edge case handling + 
    PDF delivery + welcome messages + rate limiting + spam protection + Airtable API integration
    """
    
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        # ========================================
        # EDGE CASE HANDLING - FIRST LINE OF DEFENSE
        # ========================================
        
        # Case 1: Empty message
        if not message_text or not message_text.strip():
            await send_twilio_message(
                sender, 
                "I didn't receive a message. Please send your market intelligence query."
            )
            return
        
        # Case 2: Message too short (likely accidental)
        if len(message_text.strip()) < 2:
            await send_twilio_message(
                sender,
                "I didn't catch that. Ask me about market analysis, competitive intelligence, or strategic forecasting."
            )
            return
        
        # Case 3: Meaningless messages (dots, question marks, etc.)
        if message_text.strip() in ['...', '???', '!!!', '?', '.', '!', '..', '??', '!!']:
            await send_twilio_message(
                sender,
                "I didn't catch that. Ask me about market analysis, competitive intelligence, or strategic forecasting."
            )
            return
        
        # Case 4: Only emojis/symbols (no actual text)
        text_only = re.sub(r'[^\w\s]', '', message_text)
        if len(text_only.strip()) < 2:
            await send_twilio_message(
                sender,
                "I specialise in market intelligence analysis. What would you like to explore? (Market overview, opportunities, competitive landscape, scenario modelling)"
            )
            return
        
        
        # ========================================
        # LOAD CLIENT PROFILE WITH AIRTABLE API
        # ========================================
        
        # Try MongoDB cache first
        client_profile = get_client_profile(sender)
        
        # Check if cache is stale (older than 1 hour)
        should_refresh = False
        if client_profile:
            updated_at = client_profile.get('updated_at')
            if updated_at:
                # Handle both datetime objects and ISO strings
                if isinstance(updated_at, str):
                    from dateutil import parser
                    updated_at = parser.parse(updated_at)
                
                # Make timezone-aware if needed
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                
                cache_age_minutes = (datetime.now(timezone.utc) - updated_at).total_seconds() / 60
                
                # Refresh if older than 60 minutes
                if cache_age_minutes > 60:
                    should_refresh = True
                    logger.info(f"Cache stale ({int(cache_age_minutes)} mins old), refreshing from Airtable")
        
        # ALWAYS fetch from Airtable on first message OR if stale
        if not client_profile or should_refresh or client_profile.get('total_queries', 0) == 0:
            try:
                AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
                AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
                AIRTABLE_TABLE = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')
                
                if AIRTABLE_API_KEY and AIRTABLE_BASE_ID:
                    # Clean WhatsApp number - remove 'whatsapp:' prefix
                    whatsapp_search = sender.replace('whatsapp:', '')
                    whatsapp_clean = whatsapp_search.replace(' ', '')
                    
                    # Search Airtable
                    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
                    headers = {
                        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                        "Content-Type": "application/json"
                    }
                    
                    # Try exact match first
                    params = {
                        "filterByFormula": f"{{WhatsApp Number}}='{whatsapp_search}'"
                    }
                    
                    logger.info(f"🔍 DEBUG: Searching Airtable for {whatsapp_search}")
                    logger.info(f"🔍 DEBUG: URL: {url}")
                    logger.info(f"🔍 DEBUG: Formula: {params.get('filterByFormula')}")
                    
                    response = requests.get(url, headers=headers, params=params, timeout=10)
                    
                    logger.info(f"🔍 DEBUG: Response status: {response.status_code}")
                    
                    # If no match, try with spaces removed
                    if response.status_code == 200 and not response.json().get('records'):
                        logger.info(f"No exact match, trying space-removed matching")
                        params = {
                            "filterByFormula": f"SUBSTITUTE({{WhatsApp Number}}, ' ', '')='{whatsapp_clean}'"
                        }
                        response = requests.get(url, headers=headers, params=params, timeout=10)
                    
                    if response.status_code == 200:
                        records = response.json().get('records', [])
                        logger.info(f"🔍 DEBUG: Found {len(records)} records")
                        
                        if records:
                            fields = records[0]['fields']
                            logger.info(f"🔍 DEBUG: Name={fields.get('Name')}, Email={fields.get('Email')}")
                            
                            # Map Subscription Status → Tier
                            subscription_status = (fields.get('Subscription Status', 'Trial') or 'Trial').lower()
                            
                            status_to_tier = {
                                'trial': 'tier_1',
                                'active': 'tier_3',
                                'paused': 'tier_1',
                                'cancelled': 'tier_1'
                            }
                            
                            tier = status_to_tier.get(subscription_status, 'tier_3')
                            
                            # Trigger PIN re-verification on subscription change
                            old_status = client_profile.get('subscription_status', 'trial') if client_profile else 'trial'
                            
                            if old_status in ['paused', 'cancelled'] and subscription_status == 'active':
                                # Subscription reactivated - trigger PIN re-verification
                                PINAuthenticator.trigger_reverification_on_subscription_change(sender)
                                logger.info(f"🔄 PIN re-verification triggered for {sender} (subscription reactivated)")
                                
                                # Sync to Airtable
                                from app.pin_auth import sync_pin_status_to_airtable
                                import asyncio
                                asyncio.create_task(sync_pin_status_to_airtable(sender, "Requires Re-verification", "Subscription reactivated"))
                            
                            # Preserve query history
                            old_history = client_profile.get('query_history', []) if client_profile else []
                            old_total = client_profile.get('total_queries', 0) if client_profile else 0
                            
                            client_profile = {
                                'whatsapp_number': sender,
                                'email': fields.get('Email', ''),
                                'name': fields.get('Name', 'Valued Client'),
                                'company': fields.get('Company', ''),
                                'tier': tier,
                                'preferences': {
                                    'preferred_regions': [fields.get('Preferred City', 'London')],
                                    'preferred_city': fields.get('Preferred City', 'London'),
                                    'competitor_focus': (fields.get('Competitor Focus', 'Medium') or 'Medium').lower(),
                                    'report_depth': (fields.get('Report Depth', 'Detailed') or 'Detailed').lower(),
                                    'update_frequency': 'weekly'
                                },
                                'monthly_message_limit': fields.get('Monthly Message Limit', 10000),
                                'messages_used_this_month': fields.get('Messages Used This Month', 0),
                                'subscription_status': subscription_status,
                                'stripe_customer_id': '',
                                'airtable_record_id': records[0]['id'],
                                'airtable_last_pin_verified': fields.get('Last PIN Verified'),
                                'total_queries': old_total,
                                'query_history': old_history,
                                'created_at': client_profile.get('created_at', datetime.now(timezone.utc)) if client_profile else datetime.now(timezone.utc),
                                'updated_at': datetime.now(timezone.utc)
                            }
                            
                            # Update MongoDB cache
                            from pymongo import MongoClient
                            MONGODB_URI = os.getenv('MONGODB_URI')
                            if MONGODB_URI:
                                mongo_client = MongoClient(MONGODB_URI)
                                db = mongo_client['Voxmill']
                                db['client_profiles'].update_one(
                                    {'whatsapp_number': sender},
                                    {'$set': client_profile},
                                    upsert=True
                                )
                            
                            logger.info(f"✅ Client refreshed from Airtable: {fields.get('Name')} (Status: {subscription_status}, Tier: {tier})")
                        else:
                            logger.info(f"❌ No Airtable record for {whatsapp_search}")
                    elif response.status_code == 404:
                        logger.error(f"❌ Airtable 404: Check BASE_ID={AIRTABLE_BASE_ID} and TABLE={AIRTABLE_TABLE}")
                    else:
                        logger.warning(f"Airtable API error: {response.status_code}")
                        logger.error(f"🔍 DEBUG: Error response: {response.text}")
                            
            except Exception as e:
                logger.error(f"Airtable API error: {e}", exc_info=True)
        
       # ========================================
        # WHITELIST CHECK - BLOCK UNAUTHORIZED NUMBERS
        # ========================================
        
        # If no Airtable record found, block access
        if not client_profile or not client_profile.get('airtable_record_id'):
            logger.warning(f"🚫 UNAUTHORIZED ACCESS ATTEMPT: {sender}")
            
            await send_twilio_message(
                sender,
                "This number is not authorized for Voxmill Intelligence.\n\n"
                "For institutional access, contact:\n"
                "📧 ollys@voxmill.uk"
            )
            return

        # ========================================
        # FORCE SYNC PIN TIMESTAMP FROM AIRTABLE (EVEN IF CACHED)
        # ========================================
        
        # Always check Airtable for PIN verification status (lightweight call)
        try:
            AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
            AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
            AIRTABLE_TABLE = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')
            
            if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and client_profile.get('airtable_record_id'):
                # Fetch only the PIN field from Airtable (lightweight)
                url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}/{client_profile['airtable_record_id']}"
                headers = {
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    fields = response.json().get('fields', {})
                    airtable_last_pin = fields.get('Last PIN Verified')
                    
                    if airtable_last_pin:
                        # Sync to MongoDB pin_auth
                        from dateutil import parser
                        last_verified_dt = parser.parse(airtable_last_pin)
                        
                        if last_verified_dt.tzinfo is None:
                            last_verified_dt = last_verified_dt.replace(tzinfo=timezone.utc)
                        
                        from pymongo import MongoClient
                        MONGODB_URI = os.getenv('MONGODB_URI')
                        if MONGODB_URI:
                            mongo_client = MongoClient(MONGODB_URI)
                            db = mongo_client['Voxmill']
                            
                            db['pin_auth'].update_one(
                                {'phone_number': sender},
                                {
                                    '$set': {
                                        'last_verified_at': last_verified_dt,
                                        'synced_from_airtable': True,
                                        'updated_at': datetime.now(timezone.utc)
                                    }
                                },
                                upsert=False
                            )
                            
                            logger.info(f"✅ PIN timestamp synced from Airtable: {last_verified_dt}")
        except Exception as e:
            logger.debug(f"PIN sync skipped: {e}")
        
       # ========================================
# PIN AUTHENTICATION - SECURITY LAYER
# ========================================

# Check if user needs PIN verification
needs_verification, reason = PINAuthenticator.check_needs_verification(sender)

client_name = client_profile.get('name', 'there')

if needs_verification:
    # User needs to set or enter PIN
    
    if reason == "not_set":
        # First time - needs to set PIN
        if len(message_text.strip()) == 4 and message_text.strip().isdigit():
            # User is sending their new PIN
            success, message = PINAuthenticator.set_pin(sender, message_text.strip())
            
            if not success:
                response = get_pin_response_message(success, message, client_name)
                await send_twilio_message(sender, response)
                return  # Wait for valid PIN
            
            # Sync to Airtable
            from app.pin_auth import sync_pin_status_to_airtable
            await sync_pin_status_to_airtable(sender, "Active")
            
            # PIN set successfully - RESPOND AND STOP
            unlock_response = "Access verified. Standing by."
            await send_twilio_message(sender, unlock_response)
            
            # Update session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=unlock_response,
                metadata={'category': 'pin_setup'}
            )
            
            logger.info(f"✅ PIN setup complete - silenced")
            return  # ← CRITICAL: Stop here, don't process as query
            
        else:
            # Ask for PIN setup
            response = get_pin_status_message(reason, client_name)
            await send_twilio_message(sender, response)
            return
    
    elif reason == "locked":
        # Account locked - send locked message
        response = get_pin_status_message("locked", client_name)
        await send_twilio_message(sender, response)
        return
    
    else:
        # Re-verification needed (inactivity or subscription change)
        
        if len(message_text.strip()) == 4 and message_text.strip().isdigit():
            # User is entering PIN
            success, message = PINAuthenticator.verify_and_unlock(sender, message_text.strip())
            
            if not success:
                response = get_pin_response_message(success, message, client_name)
                await send_twilio_message(sender, response)
                
                # Sync failed attempt or locked status
                from app.pin_auth import sync_pin_status_to_airtable
                if message == "locked":
                    await sync_pin_status_to_airtable(sender, "Locked", "Too many failed attempts")
                return  # Wait for correct PIN
            
            # Sync successful verification
            from app.pin_auth import sync_pin_status_to_airtable
            await sync_pin_status_to_airtable(sender, "Active")
            
            # PIN verified successfully - RESPOND AND STOP
            unlock_response = "Access verified. Standing by."
            await send_twilio_message(sender, unlock_response)
            
            # Update session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=unlock_response,
                metadata={'category': 'pin_unlock'}
            )
            
            logger.info(f"✅ PIN verified - silenced")
            return  # ← CRITICAL: Stop here, don't process as query
            
        else:
            # Ask for PIN
            response = get_pin_status_message(reason, client_name)
            await send_twilio_message(sender, response)
            return

# ========================================
# PIN COMMANDS - MANUAL LOCK/RESET
# ========================================

message_lower = message_text.lower().strip()

# Lock command (flexible matching)
lock_keywords = ['lock intelligence', 'lock access', 'lock account', 'lock my account',
                'lock it', 'lock this', 'lock down', 'secure account']
if any(kw in message_lower for kw in lock_keywords) or message_lower == 'lock':
    success, message = PINAuthenticator.manual_lock(sender)
    
    if success:
        response = """INTELLIGENCE LINE LOCKED

Your access has been secured.

Enter your 4-digit code to unlock."""
        
        # Sync to Airtable
        from app.pin_auth import sync_pin_status_to_airtable
        await sync_pin_status_to_airtable(sender, "Requires Re-verification", "Manual lock")
    else:
        response = "Unable to lock. Please try again."
    
    await send_twilio_message(sender, response)
    return

# PIN verification request (NEW - handles "verify my pin", "re-verify", etc.)
verify_keywords = ['verify pin', 'verify my pin', 'reverify', 're-verify', 'verify code', 'verify access']
if any(kw in message_lower for kw in verify_keywords):
    response = """PIN VERIFICATION

Enter your 4-digit access code to verify your account."""
    
    await send_twilio_message(sender, response)
    return

# Reset PIN command (more flexible)
reset_keywords = ['reset pin', 'change pin', 'reset code', 'reset my pin', 'change my pin', 
                 'reset access code', 'new pin', 'update pin']
if any(kw in message_lower for kw in reset_keywords):
    # SET STATE FLAG - user is in PIN reset flow
    conversation = ConversationSession(sender)
    conversation.update_session(
        user_message=message_text,
        assistant_response="PIN_RESET_INITIATED",
        metadata={'pin_flow_state': 'awaiting_reset'}
    )
    
    response = """PIN RESET

To reset your access code, reply with:

OLD_PIN NEW_PIN

Example: 1234 5678"""
    
    await send_twilio_message(sender, response)
    return

# CHECK IF USER IS IN PIN RESET FLOW
conversation = ConversationSession(sender)
last_metadata = conversation.get_last_metadata()

if last_metadata and last_metadata.get('pin_flow_state') == 'awaiting_reset':
    # User is in PIN reset flow - treat ANY 4 or 8 digit input as PIN attempt
    digits_only = ''.join(c for c in message_text if c.isdigit())
    
    if len(digits_only) == 8:
        # Format: OLD_PIN NEW_PIN without space
        old_pin = digits_only[:4]
        new_pin = digits_only[4:]
        
        success, message = PINAuthenticator.reset_pin_request(sender, old_pin, new_pin)
        
        # CLEAR STATE
        conversation.update_session(
            user_message=message_text,
            assistant_response="PIN_RESET_COMPLETE" if success else "PIN_RESET_FAILED",
            metadata={'pin_flow_state': None}
        )
        
        if success:
            response = """PIN RESET SUCCESSFUL

Your new access code is active.

Standing by."""
            
            from app.pin_auth import sync_pin_status_to_airtable
            await sync_pin_status_to_airtable(sender, "Active")
        else:
            response = f"{message}\n\nTry again: OLD_PIN NEW_PIN"
        
        await send_twilio_message(sender, response)
        return
    
    elif len(digits_only) == 4:
        # User sent only new PIN - remind them of format
        response = """PIN RESET

Please send both OLD and NEW PIN:

OLD_PIN NEW_PIN

Example: 1234 5678"""
        
        await send_twilio_message(sender, response)
        return

# Handle PIN reset (format: "1234 5678" with space)
if len(message_text.strip()) == 9 and ' ' in message_text:
    parts = message_text.strip().split()
    if len(parts) == 2 and all(p.isdigit() and len(p) == 4 for p in parts):
        old_pin, new_pin = parts
        success, message = PINAuthenticator.reset_pin_request(sender, old_pin, new_pin)
        
        # CLEAR STATE
        conversation = ConversationSession(sender)
        conversation.update_session(
            user_message=message_text,
            assistant_response="PIN_RESET_COMPLETE" if success else "PIN_RESET_FAILED",
            metadata={'pin_flow_state': None}
        )
        
        if success:
            response = """PIN RESET SUCCESSFUL

Your new access code is active.

Standing by."""
            
            from app.pin_auth import sync_pin_status_to_airtable
            await sync_pin_status_to_airtable(sender, "Active")
        else:
            response = f"{message}"
        
        await send_twilio_message(sender, response)
        return

# ========================================
# MONITORING STATUS QUERIES - GUARANTEED SUCCESS PATH
# ========================================

# Handle "show monitors" / "what am I monitoring" / "monitoring status" queries
status_keywords = ['show monitor', 'what am i monitoring', 'monitoring status', 
                   'my monitor', 'active monitor', 'current monitor', 'monitoring']

if any(kw in message_lower for kw in status_keywords):
    try:
        from app.monitoring import show_monitors
        response = await show_monitors(sender)
        await send_twilio_message(sender, response)
        
        # Update conversation session
        conversation = ConversationSession(sender)
        conversation.update_session(
            user_message=message_text,
            assistant_response=response,
            metadata={'category': 'monitoring_status'}
        )
        
        # Log interaction
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        log_interaction(sender, message_text, "monitoring_status", response)
        update_client_history(sender, message_text, "monitoring_status", preferred_region)
        logger.info(f"✅ Monitoring status query handled successfully")
        return
        
    except Exception as e:
        # GUARANTEED FALLBACK - NEVER ERROR
        logger.error(f"Monitoring query failed: {e}")
        
        fallback_response = """MONITORING STATUS

Signal cache synchronizing. Your active monitoring directives will display momentarily.

To create a new monitor:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%"

Standing by."""
        
        await send_twilio_message(sender, fallback_response)
        
        # Update conversation session
        conversation = ConversationSession(sender)
        conversation.update_session(
            user_message=message_text,
            assistant_response=fallback_response,
            metadata={'category': 'monitoring_status_fallback'}
        )
        
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        update_client_history(sender, message_text, "monitoring_status_fallback", preferred_region)
        logger.info(f"✅ Monitoring status fallback sent")
        return

# ========================================
# PORTFOLIO TRACKING COMMANDS
# ========================================

portfolio_keywords = ['my portfolio', 'my properties', 'add property', 'portfolio summary']

if any(kw in message_lower for kw in portfolio_keywords):
    from app.portfolio import get_portfolio_summary, add_property_to_portfolio
    
    if 'add property' in message_lower:
        # Guide user through adding property
        response = """ADD PROPERTY TO PORTFOLIO

Reply with property details:

Format: "Property: [address], Purchase: £[amount], Date: [YYYY-MM-DD], Region: [region]"

Example: "Property: 123 Park Lane, Purchase: £2500000, Date: 2023-01-15, Region: Mayfair" """
        
        await send_twilio_message(sender, response)
        return
    
    # Show portfolio summary
    portfolio = get_portfolio_summary(sender)
    
    if portfolio.get('error'):
        response = """No properties in portfolio.

Add a property: "Add property [details]" """
        await send_twilio_message(sender, response)
        return
    
    # Format portfolio response
    prop_list = []
    for prop in portfolio['properties'][:5]:
        prop_list.append(
            f"• {prop['address']}: £{prop['current_estimate']:,.0f} "
            f"({prop['gain_loss_pct']:+.1f}%)"
        )
    
    response = f"""PORTFOLIO SUMMARY

{chr(10).join(prop_list)}

Total Value: £{portfolio['total_current_value']:,.0f}
Total Gain/Loss: £{portfolio['total_gain_loss']:,.0f} ({portfolio['total_gain_loss_pct']:+.1f}%)

Properties: {portfolio['property_count']}"""
    
    await send_twilio_message(sender, response)
    return

# ========================================
# EXPORT/SHARING COMMANDS
# ========================================

# Export command detection
if 'export' in message_lower or 'send pdf' in message_lower or 'email report' in message_lower or 'share' in message_lower:
    # Generate shareable link to last analysis
    
    conversation = ConversationSession(sender)
    last_metadata = conversation.get_last_metadata()
    
    if not last_metadata or not last_metadata.get('category'):
        response = "No recent analysis to export. Run an analysis first, then ask to export it."
        await send_twilio_message(sender, response)
        return
    
    # Generate shareable link (simplified - you'd upload to Cloudflare/S3)
    import hashlib
    import time
    
    export_id = hashlib.md5(f"{sender}{time.time()}".encode()).hexdigest()[:12]
    
    # Store export in MongoDB
    db['exports'].insert_one({
        'export_id': export_id,
        'whatsapp_number': sender,
        'analysis_category': last_metadata.get('category'),
        'analysis_region': last_metadata.get('region'),
        'created_at': datetime.now(timezone.utc),
        'expires_at': datetime.now(timezone.utc) + timedelta(days=7),
        'content': conversation.get_session()['messages'][-1]['assistant']
    })
    
    share_url = f"https://voxmill.uk/share/{export_id}"
    
    response = f"""ANALYSIS EXPORT

Share link (valid 7 days):
{share_url}

Recipients can view analysis without login.

Note: This is a shareable snapshot. Real-time updates remain in your WhatsApp intelligence line."""
    
    await send_twilio_message(sender, response)
    return

# ========================================
# SILENT MONITORING COMMANDS
# ========================================

monitor_keywords = ['monitor', 'watch', 'track', 'alert me', 'notify me',
                   'keep an eye', 'keep watch', 'keep monitoring',
                   'flag if', 'let me know if', 'tell me if',
                   'stop monitor', 'resume monitor', 'extend monitor', 'confirm']
is_monitor_request = any(kw in message_lower for kw in monitor_keywords)

if is_monitor_request:
    from app.monitoring import handle_monitor_request
    response = await handle_monitor_request(sender, message_text, client_profile)
    await send_twilio_message(sender, response)
    return

# ========================================
# AUTHORIZED - Continue processing
# ========================================
logger.info(f"✅ AUTHORIZED: {client_profile.get('name')} ({client_profile.get('tier')})") 

# ============================================================
# WAVE 1: Security validation
# ============================================================
security_validator = SecurityValidator()

# Validate incoming message for prompt injection
is_safe, sanitized_input, threats = security_validator.validate_input(message_text)

if not is_safe:
    logger.warning(f"Security violation detected from {sender}: {threats}")
    await send_twilio_message(
        sender,
        "⚠️ Your message contains suspicious content and cannot be processed. Please rephrase your query."
    )
    return {"status": "blocked", "reason": "security_violation"}

# Use sanitized input if threats were detected but sanitized
if threats:
    logger.info(f"Input sanitized: {threats}")
    message_text = sanitized_input

# Normalize query EARLY
message_normalized = normalize_query(message_text)

# ========================================
# HANDLE SIMPLE GREETINGS FIRST (NO DATA NEEDED)
# ========================================
        
        greeting_keywords = ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 
                            'good evening', 'morning', 'afternoon', 'evening', 'sup', 'yo',
                            'helo', 'hola', 'heya', 'howdy', 'greetings']
        
        # Check if message is ONLY a greeting
        message_clean = message_normalized.lower().strip()
        is_simple_greeting = (
            message_clean in greeting_keywords or 
            len(message_clean) <= 10 and any(kw in message_clean for kw in greeting_keywords)
        )
        
        is_first_time = client_profile.get('total_queries', 0) == 0
        
        if is_simple_greeting and not is_first_time:
            # Get client name
            client_name = client_profile.get('name', 'there')
            first_name = client_name.split()[0] if client_name != 'there' else 'there'
            
            # Time-appropriate personalized greeting
            greeting = get_time_appropriate_greeting(first_name)
            
            # Returning user gets question, first-time gets "standing by"
            if client_profile.get('total_queries', 0) == 1:
                # First real interaction (after welcome)
                greeting_response = f"""{greeting}

Voxmill Intelligence standing by."""
            else:
                # Regular returning user
                greeting_response = f"""{greeting}

What can I analyze for you today?"""
            
            await send_twilio_message(sender, greeting_response)
            
            # Update conversation session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=greeting_response,
                metadata={'category': 'greeting'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "greeting", greeting_response)
            update_client_history(sender, message_text, "greeting", "None")
            
            logger.info(f"✅ Simple greeting handled")
            return
        
        # ========================================
        # GRATITUDE / CLOSING LANGUAGE DETECTION
        # ========================================
        
        acknowledgment_keywords = ['thanks', 'thank you', 'cheers', 'appreciate it',
                                  'much appreciated', 'perfect', 'great', 'brilliant',
                                  'thx', 'ty', 'tysm',
                                  'yep', 'yeah', 'ok', 'okay', 'alright', 'got it',
                                  'understood', 'fine', 'cool', 'sure', 'right', 'noted']
        
        # Check if message is ONLY acknowledgment (very short, 1-3 words)
        is_acknowledgment_only = (
            any(kw == message_clean for kw in acknowledgment_keywords) or
            (len(message_clean.split()) <= 3 and any(kw in message_clean for kw in acknowledgment_keywords))
        )
        
        if is_acknowledgment_only:
            # Brief professional acknowledgment
            acknowledgment_response = "Standing by."
            
            await send_twilio_message(sender, acknowledgment_response)
            
            # Update conversation session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=acknowledgment_response,
                metadata={'category': 'acknowledgment'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "acknowledgment", acknowledgment_response)
            update_client_history(sender, message_text, "acknowledgment", "None")
            
            logger.info(f"✅ Acknowledgment handled")
            return
        
       # ========================================
        # META-STRATEGIC QUESTIONS DETECTION
        # ========================================
        
        meta_strategic_keywords = ['what\'s missing', 'whats missing', 'what am i not seeing', 
                                   'gaps', 'blind spots', 'what don\'t i know', 'what dont i know',
                                   'what\'s the gap', 'whats the gap', 'what am i missing']
        
        is_meta_strategic = any(kw in message_lower for kw in meta_strategic_keywords)
        
        if is_meta_strategic:
            # Flag for LLM to use Meta-Strategic Protocol
            logger.info(f"✅ Meta-strategic question detected: {message_text[:50]}")
            # Continue to LLM with deterministic routing flag
            # The LLM will see this and apply hard constraints (4 bullets, 6 words max)
        
        # ========================================
        # BREVITY PHRASES - HARD GATE (NO LLM)
        # ========================================
        
        brevity_triggers = {
            'what\'s up': 'Market activity clustered. Direction unresolved.',
            'whats up': 'Market activity clustered. Direction unresolved.',
            'feels noisy': 'Noise usually precedes directional move.',
            'feels quiet': 'Quiet usually precedes a move. Direction depends on who breaks first.',
            'it feels noisy': 'Noise usually precedes directional move.',
            'it feels quiet': 'Quiet usually precedes a move. Direction depends on who breaks first.',
            'noisy today': 'Noise usually precedes directional move.',
            'quiet today': 'Quiet usually precedes a move. Direction depends on who breaks first.',
            'noisy right now': 'Noise usually precedes directional move.',
            'quiet right now': 'Quiet usually precedes a move. Direction depends on who breaks first.',
            'interesting': 'Standing by.',
            'hmm': 'Standing by.',
            'noted': 'Standing by.',
            'i see': 'Standing by.',
            'ok then': 'Standing by.',
            'fair': 'Standing by.'
        }
        
        # Check if ANY brevity trigger appears IN the message (substring match, not exact)
        message_clean_lower = message_clean.lower()
        
        triggered_phrase = None
        brevity_response = None
        
        for trigger, response in brevity_triggers.items():
            if trigger in message_clean_lower:
                triggered_phrase = trigger
                brevity_response = response
                break  # Stop at first match
        
        if triggered_phrase:
            await send_twilio_message(sender, brevity_response)
            
            # Update conversation session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=brevity_response,
                metadata={'category': 'brevity_phrase'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "brevity_phrase", brevity_response)
            update_client_history(sender, message_text, "brevity_phrase", "None")
            
            logger.info(f"✅ Brevity phrase '{triggered_phrase}' detected in '{message_text[:30]}' (hard-gated, no LLM)")
            return  # ← CRITICAL: Stop here, never call LLM
        
        # ========================================
        # POST-DECISION CONSEQUENCE QUESTIONS
        # ========================================
        
        # Check if last interaction was Decision Mode
        conversation = ConversationSession(sender)
        last_metadata = conversation.get_last_metadata()
        last_category = last_metadata.get('category', '') if last_metadata else ''
        
        consequence_keywords = ['backfire', 'worst case', 'what if this fails', 'downside', 
                                'risk if wrong', 'if i\'m wrong', 'what could go wrong',
                                'what if it backfires', 'if this backfires']
        
        is_consequence_query = any(kw in message_lower for kw in consequence_keywords)
        
        if last_category == 'decision_mode' and is_consequence_query:
            # User asking about consequences after Decision Mode
            # Route to BRIEF risk summary (5 lines max)
            
            risk_response = """PRIMARY DOWNSIDE SCENARIO:

Price volatility exposure if market reverses.
Liquidity penalty if exit required <14 days.
Opportunity cost vs alternative deployment.

Risk mitigated by: timing discipline, exit readiness."""
            
            await send_twilio_message(sender, risk_response)
            
            # Update session
            conversation.update_session(
                user_message=message_text,
                assistant_response=risk_response,
                metadata={'category': 'post_decision_risk'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "post_decision_risk", risk_response)
            update_client_history(sender, message_text, "post_decision_risk", "None")
            
            logger.info(f"✅ Post-decision consequence query handled (brief risk summary)")
            return  # ← CRITICAL: Don't route to full LLM
        
        # Check for webhook duplication
        cache_mgr = CacheManager()
        webhook_key = f"{sender}:{message_text[:50]}"
        
        if cache_mgr.check_webhook_duplicate(webhook_key):
            logger.info(f"Duplicate webhook ignored: {webhook_key}")
            return {"status": "duplicate_ignored"}
        
        # ============================================================
        # WAVE 3: Initialize conversation session
        # ============================================================
        conversation = ConversationSession(sender)
        
        # Check if this is a follow-up query
        is_followup, context_hints = conversation.detect_followup_query(message_normalized)
        
        if is_followup:
            logger.info(f"Follow-up detected: {context_hints}")
            # Resolve ambiguous references
            message_normalized = resolve_reference(message_normalized, context_hints)
            logger.info(f"Resolved query: {message_normalized}")
        
        # ========================================
        # RATE LIMITING - PREVENT COST EXPLOSION
        # ========================================
        
        query_history = client_profile.get('query_history', [])
        
        # SPAM PROTECTION: Minimum 2 seconds between messages
        if query_history:
            last_query_time = query_history[-1].get('timestamp')
            if last_query_time:
                # Fix timezone-naive datetime issue
                if last_query_time.tzinfo is None:
                    last_query_time = last_query_time.replace(tzinfo=timezone.utc)
                
                seconds_since_last = (datetime.now(timezone.utc) - last_query_time).total_seconds()
                
                if seconds_since_last < 2:
                    # Too fast - silently ignore (likely accidental double-tap)
                    logger.warning(f"Spam protection triggered for {sender} ({seconds_since_last:.1f}s since last)")
                    return
        
        # RATE LIMITING: Queries per hour by tier
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        
        # Fix timezone for all query timestamps in history
        recent_queries = []
        for q in query_history:
            timestamp = q.get('timestamp')
            if timestamp:
                # Make timezone-aware if needed
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                
                # Check if within last hour
                if timestamp > one_hour_ago:
                    recent_queries.append(q)
        
        tier = client_profile.get('tier', 'tier_1')
        limits = {
            'tier_1': 10,   # 10/hour
            'tier_2': 50,   # 50/hour
            'tier_3': 200   # 200/hour (not truly unlimited to prevent abuse)
        }
        
        max_queries = limits.get(tier, 10)
        
        if len(recent_queries) >= max_queries:
            # Calculate time until reset
            oldest_timestamp = min(q['timestamp'] for q in recent_queries)
            
            # Fix timezone-naive datetime issue
            if oldest_timestamp.tzinfo is None:
                oldest_timestamp = oldest_timestamp.replace(tzinfo=timezone.utc)
            
            time_until_reset = (oldest_timestamp + timedelta(hours=1) - datetime.now(timezone.utc))
            minutes_until_reset = int(time_until_reset.total_seconds() / 60)
            
            rate_limit_msg = f"""⚠️ RATE LIMIT REACHED

Your {tier.replace('_', ' ').title()} plan allows {max_queries} queries per hour.

Reset in: {minutes_until_reset} minutes

Need more queries? Upgrade or contact:
📧 ollys@voxmill.uk"""
            
            await send_twilio_message(sender, rate_limit_msg)
            logger.warning(f"Rate limit hit for {sender} ({tier}): {len(recent_queries)}/{max_queries}")
            return
        
        # ========================================
        # MESSAGE LENGTH LIMIT - PREVENT COST EXPLOSION
        # ========================================
        
        MAX_MESSAGE_LENGTH = 500
        
        if len(message_text) > MAX_MESSAGE_LENGTH:
            await send_twilio_message(
                sender,
                f"Message too long ({len(message_text)} characters). "
                f"Please keep queries under {MAX_MESSAGE_LENGTH} characters for optimal analysis."
            )
            logger.warning(f"Message too long from {sender}: {len(message_text)} chars")
            return
        
        # ========================================
        # PREFERENCE SELF-SERVICE
        # ========================================
        
        try:
            logger.info(f"🔍 Checking if message is preference request: {message_text[:50]}")
            pref_response = handle_whatsapp_preference_message(sender, message_text)
            
            if pref_response:
                logger.info(f"✅ Preference request detected, sending confirmation")
                await send_twilio_message(sender, pref_response)
                
                # Log the preference change
                update_client_history(sender, message_text, "preference_update", "Self-Service")
                
                logger.info(f"✅ Preference updated via WhatsApp for {sender}")
                return
            else:
                logger.info(f"❌ Not a preference request, continuing to normal analyst")
                
        except Exception as e:
            logger.error(f"❌ ERROR in preference handler: {e}", exc_info=True)
            # Continue to normal processing
        
        # ========================================
        # FIRST-TIME USER WELCOME
        # ========================================
        
        is_first_time = client_profile.get('total_queries', 0) == 0
        
        if is_first_time:
            await send_first_time_welcome(sender, client_profile)
        
        # ========================================
        # META-QUESTIONS ABOUT CLIENT PROFILE
        # ========================================
        
        meta_keywords = ['who am i', 'what is my name', 'my profile', 'client profile', 
                         'my details', 'know about me', 'aware of my', 'what do you know']
        
        is_meta_question = any(kw in message_normalized.lower() for kw in meta_keywords)
        
        if is_meta_question:
            client_name = client_profile.get('name', 'Unknown')
            client_email = client_profile.get('email', 'Not on file')
            client_company = client_profile.get('company', 'Not specified')
            tier = client_profile.get('tier', 'tier_1')
            preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
            
            tier_display = {
                'tier_1': 'Basic',
                'tier_2': 'Premium', 
                'tier_3': 'Enterprise'
            }[tier]
            
            greeting = get_time_appropriate_greeting(client_name)
            
            profile_response = f"""{greeting}

CLIENT PROFILE
————————————————————————————————————————

Name: {client_name}
Company: {client_company if client_company else 'Individual Client'}
Service Tier: {tier_display}
Preferred Region: {preferred_region}
Contact: {client_email}

Your intelligence is personalized to your preferences and tier.

What market intelligence can I provide?"""
            
            await send_twilio_message(sender, profile_response)
            
            # Update conversation session
            conversation.update_session(
                user_message=message_text,
                assistant_response=profile_response,
                metadata={'category': 'profile_query'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "profile_query", profile_response)
            update_client_history(sender, message_text, "profile_query", "None")
            
            logger.info(f"✅ Profile query handled")
            return
        
        # ========================================
        # PDF REQUEST DETECTION
        # ========================================
        
        pdf_keywords = ['send pdf', 'full report', 'send report', 'pdf please', 'get report', 
                       'view report', 'send me the pdf', 'can i see the pdf', 'full briefing',
                       'executive briefing', 'complete report', 'detailed report']
        
        is_pdf_request = any(keyword in message_normalized.lower() for keyword in pdf_keywords)
        
        if is_pdf_request:
            preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
            await send_pdf_report(sender, preferred_region)
            return
        
        # ========================================
        # GET PREFERRED REGION
        # ========================================
        
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        
        # ============================================================
        # WAVE 1: Check response cache FIRST
        # ============================================================
        cached_response = cache_mgr.get_response_cache(
            query=message_normalized,
            region=preferred_region,
            client_tier=client_profile.get('tier', 'tier_1')
        )
        
        if cached_response:
            logger.info(f"Cache hit for query: {message_normalized[:50]}")
            cached_text = cached_response.get('response', cached_response)
            await send_twilio_message(sender, cached_response)
            
            # Still update conversation session
            conversation.update_session(
                user_message=message_text,
                assistant_response=cached_response,
                metadata={'cached': True, 'region': preferred_region}
            )
            
            # Log interaction
            log_interaction(sender, message_text, "cached", cached_response)
            update_client_history(sender, message_text, "cached", preferred_region)
            
            return {"status": "success", "source": "cache"}
        
        # ========================================
        # COMPARISON QUERY DETECTION
        # ========================================
        
        comparison_keywords = ['compare', 'vs', 'versus', 'difference between', 'which is better']
        is_comparison = any(kw in message_normalized.lower() for kw in comparison_keywords)
        
        comparison_datasets = []
        
        if is_comparison:
            # Extract regions mentioned
            all_regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington', 
                          'Notting Hill', 'Marylebone', 'South Kensington']
            mentioned_regions = [r for r in all_regions if r.lower() in message_normalized.lower()]
            
            if len(mentioned_regions) >= 2:
                logger.info(f"Comparison query detected: {mentioned_regions}")
                
                # Load datasets for comparison (skip first, it's loaded as primary below)
                for region in mentioned_regions[1:]:
                    try:
                        comp_dataset = load_dataset(area=region)
                        comparison_datasets.append(comp_dataset)
                        logger.info(f"Loaded comparison dataset for {region}")
                    except Exception as e:
                        logger.warning(f"Failed to load dataset for {region}: {e}")
                
                # Update preferred_region to first mentioned
                preferred_region = mentioned_regions[0]
        
        # ========================================
        # LOAD PRIMARY DATASET FOR ANALYSIS
        # ========================================
        
        dataset = load_dataset(area=preferred_region)
        
        # DEBUG: Log what we got
        logger.info(f"📊 Dataset loaded: area={dataset.get('metadata', {}).get('area')}, "
                   f"properties={len(dataset.get('properties', []))}")
        
        # Check if data exists
        property_count = len(dataset.get('properties', []))
        metadata = dataset.get('metadata', {})
        requested_region = preferred_region
        
        # Core regions we have data for
        core_regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        
        # Flag unavailable data for LLM to handle gracefully
        if property_count == 0 and requested_region not in core_regions:
            logger.info(f"No data for {requested_region} - flagging for LLM graceful degradation")
            
            # Intelligent alternative suggestions
            similar_regions = {
                'Westminster': 'Mayfair or Belgravia',
                'South Ken': 'Kensington or Chelsea',
                'South Kensington': 'Kensington or Chelsea',
                'Notting Hill': 'Kensington',
                'Marylebone': 'Mayfair',
                'Paddington': 'Mayfair or Kensington',
                'Fitzrovia': 'Marylebone or Mayfair'
            }
            
            suggestion = similar_regions.get(requested_region, 'Mayfair, Knightsbridge, or Chelsea')
            
            # Pass to LLM with context - let Conversational Intelligence Layer handle response
            dataset['metadata']['data_unavailable'] = True
            dataset['metadata']['requested_region'] = requested_region
            dataset['metadata']['suggested_alternatives'] = suggestion
            dataset['metadata']['core_regions'] = core_regions
            
            # DO NOT return here - continue to LLM for intelligent handling
        
        # If we have data, log and continue normally
        if property_count > 0:
            logger.info(f"✅ Dataset loaded for {requested_region}: {property_count} properties")
        
        # Check data freshness and add warning if stale
        data_timestamp = metadata.get('analysis_timestamp')
        data_freshness_warning = ""
        
        if data_timestamp:
            # Convert string to datetime if needed
            if isinstance(data_timestamp, str):
                try:
                    from dateutil import parser
                    data_timestamp = parser.parse(data_timestamp)
                except:
                    # If parsing fails, skip freshness check
                    data_timestamp = None
            
            if data_timestamp:
                # Make timezone-aware if needed
                if data_timestamp.tzinfo is None:
                    data_timestamp = data_timestamp.replace(tzinfo=timezone.utc)
                
                data_age_hours = (datetime.now(timezone.utc) - data_timestamp).total_seconds() / 3600
                
                if data_age_hours > 48:  # Data older than 2 days
                    data_freshness_warning = f"\n\n━━━━━━━━━━━━━━━━━━━━\n⚠️ Data last updated {int(data_age_hours)} hours ago"
                    logger.warning(f"Stale data for {preferred_region}: {int(data_age_hours)} hours old")
        
        # ========================================
        # INTELLIGENT QUERY PRE-PROCESSING
        # ========================================
        
        # FILTER QUERIES: "properties under £3M", "apartments over £5M"
        filter_keywords = ['under', 'below', 'above', 'over', 'between', 'cheaper than', 'more expensive than']
        has_price_filter = any(kw in message_normalized.lower() for kw in filter_keywords)
        
        if has_price_filter:
            # Extract price from query (handles £3M, 3M, £3,000,000, etc.)
            price_match = re.search(r'£?(\d+(?:,\d{3})*(?:\.\d+)?)\s*M?', message_normalized, re.IGNORECASE)
            
            if price_match:
                price_str = price_match.group(1).replace(',', '')
                price_threshold = float(price_str)
                
                # Check if it's in millions
                if 'M' in message_normalized.upper() or price_threshold < 100:
                    price_threshold *= 1_000_000
                
                # Filter properties
                original_count = len(dataset['properties'])
                
                if 'under' in message_normalized.lower() or 'below' in message_normalized.lower() or 'cheaper' in message_normalized.lower():
                    dataset['properties'] = [
                        p for p in dataset['properties'] 
                        if p.get('price', 0) > 0 and p.get('price', 0) < price_threshold
                    ]
                    filter_type = "under"
                elif 'above' in message_normalized.lower() or 'over' in message_normalized.lower() or 'expensive' in message_normalized.lower():
                    dataset['properties'] = [
                        p for p in dataset['properties'] 
                        if p.get('price', 0) > price_threshold
                    ]
                    filter_type = "over"
                
                filtered_count = len(dataset['properties'])
                logger.info(f"Price filter applied: {original_count} → {filtered_count} properties {filter_type} £{price_threshold/1_000_000:.1f}M")
        
        # SUPERLATIVE QUERIES: "cheapest", "most expensive", "best value"
        superlative_keywords = ['cheapest', 'most expensive', 'best value', 'lowest price', 'highest price', 'best deal']
        has_superlative = any(kw in message_normalized.lower() for kw in superlative_keywords)
        
        if has_superlative:
            # Extract property type if mentioned
            property_types = ['apartment', 'penthouse', 'house', 'flat', 'studio', 
                            '1-bed', '2-bed', '3-bed', '4-bed', '5-bed', 
                            '1 bed', '2 bed', '3 bed', '4 bed', '5 bed']
            mentioned_type = next((pt for pt in property_types if pt.replace('-', ' ') in message_normalized.lower()), None)
            
            # Filter by type if mentioned
            if mentioned_type:
                dataset['properties'] = [
                    p for p in dataset['properties']
                    if mentioned_type.replace('-', ' ').replace(' bed', '') in p.get('property_type', '').lower()
                ]
                logger.info(f"Filtered to {len(dataset['properties'])} {mentioned_type} properties")
            
            # Sort by price
            if 'cheapest' in message_normalized.lower() or 'lowest' in message_normalized.lower() or 'best value' in message_normalized.lower():
                dataset['properties'] = sorted(
                    [p for p in dataset['properties'] if p.get('price', 0) > 0],
                    key=lambda x: x.get('price', float('inf'))
                )[:10]  # Top 10
                logger.info(f"Sorted by lowest price, showing top 10")
            elif 'expensive' in message_normalized.lower() or 'highest' in message_normalized.lower():
                dataset['properties'] = sorted(
                    dataset['properties'],
                    key=lambda x: x.get('price', 0),
                    reverse=True
                )[:10]  # Top 10
                logger.info(f"Sorted by highest price, showing top 10")
        
        # ========================================
        # ADD V3 INTELLIGENCE LAYERS
        # ========================================
        
        # Layer 1: Trend Detection
        try:
            from app.intelligence.trend_detector import detect_market_trends
            trends = detect_market_trends(area=preferred_region, lookback_days=14)
            if trends:
                dataset['detected_trends'] = trends
        except (ImportError, Exception) as e:
            logger.debug(f"Trend detection unavailable: {str(e)}")
        
        # Layer 2: Agent Profiling
        try:
            from app.intelligence.agent_profiler import get_agent_profiles
            agent_profiles = get_agent_profiles(area=preferred_region)
            if agent_profiles:
                dataset['agent_profiles'] = agent_profiles
        except (ImportError, Exception) as e:
            logger.debug(f"Agent profiler unavailable: {str(e)}")
        
        # Layer 3: Micro-Market Segmentation
        try:
            from app.intelligence.micromarket_segmenter import segment_micromarkets
            micromarkets = segment_micromarkets(dataset.get('properties', []), preferred_region)
            if micromarkets and 'error' not in micromarkets:
                dataset['micromarkets'] = micromarkets
        except (ImportError, Exception) as e:
            logger.debug(f"Micromarket segmenter unavailable: {str(e)}")
        
        # Layer 4: Liquidity Velocity
        try:
            from app.intelligence.liquidity_velocity import calculate_liquidity_velocity
            from app.dataset_loader import load_historical_snapshots
            historical = load_historical_snapshots(area=preferred_region, days=30)
            if historical:
                velocity = calculate_liquidity_velocity(dataset.get('properties', []), historical)
                if velocity and 'error' not in velocity:
                    dataset['liquidity_velocity'] = velocity
        except (ImportError, Exception) as e:
            logger.debug(f"Liquidity velocity unavailable: {str(e)}")
        
        # Layer 5: Cascade Prediction (if scenario query)
        try:
            from app.intelligence.cascade_predictor import build_agent_network, predict_cascade
            
            scenario_keywords = ['what if', 'simulate', 'scenario', 'predict', 'cascade']
            is_scenario_query = any(keyword in message_normalized.lower() for keyword in scenario_keywords)
            
            if is_scenario_query:
                network = build_agent_network(area=preferred_region, lookback_days=90)
                
                if not network.get('error'):
                    # Extract scenario from query
                    agent_pattern = r'(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|[\w\s&]+?)\s+(?:drops?|reduces?|lowers?|cuts?|increases?|raises?)(?:\s+by)?\s+(\d+\.?\d*)%'
                    match = re.search(agent_pattern, message_normalized, re.IGNORECASE)
                    
                    if match:
                        initiating_agent = match.group(1).strip()
                        magnitude = float(match.group(2))
                        
                        if any(word in message_normalized.lower() for word in ['drop', 'reduce', 'lower', 'cut']):
                            magnitude = -magnitude
                        
                        cascade = predict_cascade(network, initiating_agent, magnitude)
                        if not cascade.get('error'):
                            dataset['cascade_prediction'] = cascade
                            logger.info(f"Cascade predicted: {initiating_agent} {magnitude:+.1f}% -> {cascade['total_affected_agents']} agents affected")
        except (ImportError, Exception) as e:
            logger.debug(f"Cascade predictor unavailable: {str(e)}")
        
        # ========================================
        # WAVE 4 INTELLIGENCE LAYERS
        # ========================================
        
        # Layer 6: Liquidity Window Prediction
        try:
            from app.intelligence.liquidity_window_predictor import predict_liquidity_windows
            from app.dataset_loader import load_historical_snapshots
            
            # Check if we have liquidity velocity data
            if 'liquidity_velocity' in dataset and not dataset['liquidity_velocity'].get('error'):
                # Load historical data for window prediction
                historical = load_historical_snapshots(area=preferred_region, days=60)
                
                if historical and len(historical) >= 10:
                    windows = predict_liquidity_windows(
                        area=preferred_region,
                        current_velocity=dataset['liquidity_velocity'],
                        historical_data=historical
                    )
                    
                    if not windows.get('error'):
                        dataset['liquidity_windows'] = windows
                        logger.info(f"✅ Liquidity windows predicted: {windows['total_windows']} windows detected")
        except (ImportError, Exception) as e:
            logger.debug(f"Liquidity window predictor unavailable: {str(e)}")
        
        # Layer 7: Behavioral Clustering
        try:
            from app.intelligence.behavioral_clustering import cluster_agents_by_behavior
            
            # Check if we have agent profiles
            if 'agent_profiles' in dataset and dataset['agent_profiles']:
                clusters = cluster_agents_by_behavior(
                    area=preferred_region,
                    agent_profiles=dataset['agent_profiles']
                )
                
                if not clusters.get('error'):
                    dataset['behavioral_clusters'] = clusters
                    logger.info(f"✅ Behavioral clustering complete: {len(clusters.get('clusters', []))} clusters identified")
        except (ImportError, Exception) as e:
            logger.debug(f"Behavioral clustering unavailable: {str(e)}")
        
        # ========================================
        # MANUAL MODE OVERRIDE - ADD THIS NEW SECTION
        # ========================================
        
        # Check for manual mode kill-switch
        if 'manual only' in message_normalized.lower() or 'no inference' in message_normalized.lower():
            # Flag for LLM to disable all inference
            dataset['metadata']['manual_mode'] = True
            logger.info(f"Manual mode activated for {sender} - disabling inference")
        
        # ========================================
        # GPT-4 ANALYSIS
        # ========================================
        
        category, response_text, response_metadata = await classify_and_respond(
            message_normalized,
            dataset,
            client_profile=client_profile,
            comparison_datasets=comparison_datasets if comparison_datasets else None
        )
        
        # Format response
        formatted_response = format_analyst_response(response_text, category)
        
        # ============================================================
        # WAVE 1: Validate response for hallucinations
        # ============================================================
        from app.validation import HallucinationDetector
        
        hallucination_detector = HallucinationDetector()
        
        # Validate response (correct signature)
        is_valid, violations, corrections = hallucination_detector.validate_response(
            response_text=formatted_response,
            dataset=dataset,
            category=category
        )
        
        # Calculate confidence score
        confidence_score = HallucinationDetector.calculate_confidence_score(violations)
        
        # Add warning if low confidence
        if not is_valid and confidence_score < 0.5:
            logger.error(f"Hallucination detected (confidence: {confidence_score}): {violations}")
            formatted_response = f"{formatted_response}\n\n⚠️ Note: Response generated with limited data coverage. Please verify critical details."
            
            # Log the hallucination event
            from app.validation import log_hallucination_event
            log_hallucination_event(violations, formatted_response)
        
        # ============================================================
        # WAVE 1: Cache the response
        # ============================================================
        cache_mgr.set_response_cache(
            query=message_normalized,
            region=preferred_region,
            client_tier=client_profile.get('tier', 'tier_1'),
            category=category,
            response_text=formatted_response,
            metadata=response_metadata
        )
        
        # ============================================================
        # WAVE 1: Validate response output (no prompt leakage)
        # ============================================================
        
        # Security validation
        from app.security import ResponseValidator
        
        response_safe, reason = ResponseValidator.validate_response(formatted_response)
        
        if not response_safe:
            logger.critical(f"LLM output failed security validation: {reason}")
            formatted_response = "An error occurred processing your request. Please try again."
        
        # Add data freshness warning if needed
        formatted_response += data_freshness_warning
        
        # ========================================
        # SEND RESPONSE
        # ========================================
        
        await send_twilio_message(sender, formatted_response)
        
        # ============================================================
        # WAVE 3: Update conversation session
        # ============================================================
        conversation.update_session(
            user_message=message_text,
            assistant_response=formatted_response,
            metadata={
                'category': category if 'category' in locals() else 'unknown',
                'region': preferred_region,
                'confidence': confidence_score if 'confidence_score' in locals() else None,
                'cached': False
            }
        )
        
        # Log interaction and update client history
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"Message processed: {category} | Confidence: {response_metadata.get('confidence_level')} | Urgency: {response_metadata.get('recommendation_urgency')}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "System encountered an error processing your request. Please try rephrasing your query or contact support if this persists."
        await send_twilio_message(sender, error_msg)


async def send_pdf_report(sender: str, area: str):
    """Generate and send PDF report link to client (NO EMOJIS VERSION)"""
    try:
        logger.info(f"PDF report requested by {sender} for {area}")
        
        # Check if PDF storage is configured
        try:
            from app.pdf_storage import get_latest_pdf_for_client, upload_pdf_to_cloud
            storage_available = True
        except ImportError:
            storage_available = False
        
        if not storage_available:
            logger.warning("PDF storage module not configured")
            message = (
                "PDF delivery is being configured for your account.\n\n"
                "In the meantime, I can provide comprehensive intelligence via text.\n\n"
                "Ask me about market overview, opportunities, or strategic outlook."
            )
            await send_twilio_message(sender, message)
            return
        
        # Try to get existing PDF URL
        pdf_url = get_latest_pdf_for_client(sender, area)
        
        if not pdf_url:
            # No existing PDF found, check temp directory
            pdf_path = "/tmp/Voxmill_Executive_Intelligence_Deck.pdf"
            
            if os.path.exists(pdf_path):
                pdf_url = upload_pdf_to_cloud(pdf_path, sender, area)
            else:
                # No PDF available
                await send_twilio_message(
                    sender,
                    f"Your {area} executive briefing is being prepared.\n\n"
                    "Reports are generated daily at midnight GMT. "
                    "The latest report will be available shortly.\n\n"
                    "In the meantime, I can provide real-time intelligence. "
                    "Ask me about market overview, opportunities, or competitive landscape."
                )
                return
        
        if pdf_url:
            date_str = datetime.now().strftime('%B %d, %Y')
            
            # NO EMOJIS VERSION - Professional institutional format
            message = (
                "EXECUTIVE INTELLIGENCE BRIEFING\n"
                "————————————————————————————————————————\n\n"
                f"{area} Market Analysis\n"
                f"Generated: {date_str}\n\n"
                f"View your report:\n{pdf_url}\n\n"
                "Link valid for 7 days\n"
                "14-page institutional-grade analysis"
            )
            
            await send_twilio_message(sender, message)
            logger.info(f"PDF report sent successfully to {sender}")
        else:
            await send_twilio_message(
                sender,
                f"Unable to access your {area} report at this time. "
                "Our team has been notified and will resolve this shortly."
            )
            
    except Exception as e:
        logger.error(f"Error sending PDF report: {str(e)}", exc_info=True)
        await send_twilio_message(sender, "Error generating report link. Our team has been notified.")


def normalize_query(text: str) -> str:
    """Normalize common typos and variations"""
    corrections = {
        'markrt': 'market',
        'overveiw': 'overview',
        'overviw': 'overview',
        'competitve': 'competitive',
        'competetive': 'competitive',
        'oppertunities': 'opportunities',
        'oportunities': 'opportunities',
        'analyise': 'analyse',
        'analize': 'analyse',
        'scenerio': 'scenario',
        'forcast': 'forecast',
        'forceast': 'forecast',
        'whats': 'what is',
        'whta': 'what',
        'teh': 'the',
        'adn': 'and',
        'hte': 'the',
        'reportt': 'report',
        'reprot': 'report'
    }
    
    normalized = text
    for typo, correct in corrections.items():
        pattern = re.compile(re.escape(typo), re.IGNORECASE)
        normalized = pattern.sub(correct, normalized)
    
    return normalized


async def send_twilio_message(recipient: str, message: str):
    """Send message via Twilio WhatsApp API with intelligent chunking"""
    try:
        if not twilio_client:
            logger.error("Twilio client not initialized")
            return
        
        # CRITICAL: Validate message is not None
        if message is None:
            logger.error(f"Cannot send None message to {recipient}")
            message = "An error occurred processing your request. Please try again."
        
        # Convert to string if needed
        message = str(message)
        
        # ENSURE WHATSAPP PREFIX
        from_number = TWILIO_WHATSAPP_NUMBER
        if not from_number.startswith('whatsapp:'):
            from_number = f'whatsapp:{from_number}'
        
        to_number = recipient
        if not to_number.startswith('whatsapp:'):
            to_number = f'whatsapp:{to_number}'
        
        MAX_LENGTH = 1500
        
        if len(message) <= MAX_LENGTH:
            # Short message - send as-is
            twilio_client.messages.create(
                from_=from_number,
                to=to_number,
                body=message
            )
            logger.info(f"Message sent successfully to {to_number} ({len(message)} chars)")
        else:
            # Long message - intelligent splitting
            chunks = smart_split_message(message, MAX_LENGTH)
            
            for i, chunk in enumerate(chunks, 1):
                twilio_client.messages.create(
                    from_=from_number,
                    to=to_number,
                    body=chunk
                )
                
                # Small delay to maintain order
                import asyncio
                await asyncio.sleep(0.5)
            
            logger.info(f"Multi-part message sent to {to_number} ({len(chunks)} parts, {len(message)} total chars)")
                
    except Exception as e:
        logger.error(f"Error sending Twilio message: {str(e)}", exc_info=True)
        raise


def smart_split_message(message: str, max_length: int) -> list:
    """Split message intelligently at natural break points with part numbers"""
    if len(message) <= max_length:
        return [message]
    
    chunks = []
    remaining = message
    
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining.strip())
            break
        
        # Extract a chunk of max_length
        chunk = remaining[:max_length]
        
        # Find best split point (priority order)
        split_point = -1
        
        # 1. Try double line break (major section boundary)
        double_break = chunk.rfind('\n\n')
        if double_break > max_length * 0.5:
            split_point = double_break
        
        # 2. Try single line break
        if split_point == -1:
            single_break = chunk.rfind('\n')
            if single_break > max_length * 0.5:
                split_point = single_break
        
        # 3. Try sentence end
        if split_point == -1:
            sentence_end = max(
                chunk.rfind('. '),
                chunk.rfind('! '),
                chunk.rfind('? ')
            )
            if sentence_end > max_length * 0.4:
                split_point = sentence_end + 1
        
        # 4. Try bullet point
        if split_point == -1:
            bullet = chunk.rfind('\n•')
            if bullet > max_length * 0.4:
                split_point = bullet
        
        # 5. Last resort: word boundary
        if split_point == -1:
            word_break = chunk.rfind(' ')
            if word_break > max_length * 0.3:
                split_point = word_break
        
        # Absolute fallback: hard cut
        if split_point == -1:
            split_point = max_length
        
        # Add chunk
        chunks.append(remaining[:split_point].strip())
        remaining = remaining[split_point:].strip()
    
    # Post-process: If first chunk is tiny, merge with second
    if len(chunks) > 1 and len(chunks[0]) < 200:
        chunks[0] = f"{chunks[0]}\n\n{chunks[1]}"
        chunks.pop(1)
    
    # Add part numbers if multiple chunks
    if len(chunks) > 1:
        numbered_chunks = []
        for i, chunk in enumerate(chunks, 1):
            numbered_chunks.append(f"[Part {i}/{len(chunks)}]\n\n{chunk}")
        return numbered_chunks
    
    return chunks

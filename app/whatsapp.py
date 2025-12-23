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
from dateutil import parser as dateutil_parser
import requests
import pytz
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta
import asyncio

from twilio.rest import Client

from app.instant_response import InstantIntelligence, should_use_instant_response
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


# HELPER FUNCTIONS

def parse_regions(regions_raw):
    """Parse Airtable Regions field into proper list"""
    if not regions_raw:
        return ['Mayfair']
    
    # Airtable returns string, convert to list
    if isinstance(regions_raw, str):
        # Handle comma-separated or single region
        regions = [r.strip() for r in regions_raw.split(',') if r.strip()]
        
        # Expansion map for abbreviations
        region_expansion = {
            'M': 'Mayfair',
            'K': 'Knightsbridge',
            'C': 'Chelsea',
            'B': 'Belgravia',
            'W': 'Kensington'
        }
        
        # Expand single letters
        regions = [region_expansion.get(r, r) if len(r) == 1 else r for r in regions]
        
        # Filter invalid (must be at least 3 chars)
        regions = [r for r in regions if len(r) > 2]
        
        # Fallback
        if not regions:
            return ['Mayfair']
        
        return regions
    elif isinstance(regions_raw, list):
        # Already a list (shouldn't happen with Airtable, but safe)
        return regions_raw if regions_raw else ['Mayfair']
    else:
        return ['Mayfair']


def get_client_from_airtable(sender: str) -> dict:
    """Check Trial Users table first, then Clients table"""
    
    AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
    
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable credentials missing")
        return None
    
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    
    # Normalize phone for search
    search_number = sender.replace('whatsapp:', '').replace('whatsapp%3A', '')
    if not search_number.startswith('+'):
        search_number = '+' + search_number
    
    # ========================================
    # CHECK 1: TRIAL USERS TABLE
    # ========================================
    
    try:
        trial_table_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Trial%20Users"
        
        params = {
            'filterByFormula': f"{{WhatsApp Number}}='{search_number}'"
        }
        
        response = requests.get(trial_table_url, headers=headers, params=params, timeout=5)
        
        if response.status_code == 200:
            records = response.json().get('records', [])
            
            if records:
                trial_record = records[0]
                fields = trial_record.get('fields', {})
                
                # Check if trial expired
                trial_end_date = fields.get('Trial End Date')
                
                if trial_end_date:
                    trial_end = dateutil_parser.parse(trial_end_date)
                    
                    if trial_end.tzinfo is None:
                        trial_end = trial_end.replace(tzinfo=timezone.utc)
                    
                    if datetime.now(timezone.utc) > trial_end:
                        # Trial expired
                        logger.warning(f"Trial expired for {sender}")
                        return {
                            'subscription_status': 'Trial',
                            'trial_expired': True,
                            'name': fields.get('Name', 'there'),
                            'airtable_record_id': None,
                            'table': 'Trial Users'
                        }
                
                # Parse regions properly
                regions = parse_regions(fields.get('Regions'))
                
                # Trial active
                logger.info(f"✅ Trial user found: {fields.get('Name', sender)}")
                return {
                    'name': fields.get('Name', 'there'),
                    'email': fields.get('Email', ''),
                    'subscription_status': 'Trial',
                    'tier': 'tier_2',
                    'trial_expired': False,
                    'airtable_record_id': trial_record['id'],
                    'table': 'Trial Users',
                    'preferences': {
                        'preferred_regions': regions,
                        'competitor_focus': 'medium',
                        'report_depth': 'detailed'
                    },
                    'usage_metrics': {
                        'messages_used_this_month': 0,
                        'monthly_message_limit': 50,
                        'total_messages_sent': 0
                    }
                }
    
    except Exception as e:
        logger.error(f"Error checking Trial Users table: {e}")
    
    # ========================================
    # CHECK 2: MAIN CLIENTS TABLE
    # ========================================
    
    try:
        clients_table_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Clients"
        
        params = {
            'filterByFormula': f"{{WhatsApp Number}}='{search_number}'"
        }
        
        response = requests.get(clients_table_url, headers=headers, params=params, timeout=5)
        
        if response.status_code == 200:
            records = response.json().get('records', [])
            
            if records:
                client_record = records[0]
                fields = client_record.get('fields', {})
                
                subscription_status = fields.get('Subscription Status', 'Active')
                
                # Map status to tier
                status_to_tier = {
                    'Active': 'tier_3',
                    'Premium': 'tier_3',
                    'Basic': 'tier_1',
                    'Cancelled': 'tier_1',
                    'Suspended': 'tier_1'
                }
                
                tier = status_to_tier.get(subscription_status, 'tier_1')
                
                # Parse regions properly
                regions = parse_regions(fields.get('Regions'))
                
                logger.info(f"✅ Client found: {fields.get('Name', sender)} ({subscription_status})")
                
                return {
                    'name': fields.get('Name', 'there'),
                    'email': fields.get('Email', ''),
                    'subscription_status': subscription_status,
                    'tier': tier,
                    'trial_expired': False,
                    'airtable_record_id': client_record['id'],
                    'table': 'Clients',
                    'preferences': {
                        'preferred_regions': regions,
                        'competitor_focus': fields.get('Competitor Focus', 'medium'),
                        'report_depth': fields.get('Report Depth', 'detailed')
                    },
                    'usage_metrics': {
                        'messages_used_this_month': fields.get('Messages Used This Month', 0),
                        'monthly_message_limit': fields.get('Monthly Message Limit', 10000),
                        'total_messages_sent': fields.get('Total Messages Sent', 0)
                    }
                }
    
    except Exception as e:
        logger.error(f"Error checking Clients table: {e}")
    
    # Not found in either table
    return None


def safe_get_last_metadata(conversation) -> dict:
    """Safely get last metadata with fallback for cache issues"""
    try:
        return conversation.get_last_metadata()
    except (AttributeError, Exception):
        try:
            session = conversation.get_session()
            messages = session.get('messages', [])
            return messages[-1].get('metadata', {}) if messages else {}
        except:
            return {}


def safe_detect_followup(conversation, message_normalized):
    """Safely detect followup with fallback"""
    try:
        return conversation.detect_followup_query(message_normalized)
    except (AttributeError, Exception):
        # Method not available - skip followup detection
        return False, {}


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
                    updated_at = dateutil_parser.parse(updated_at)
                
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
            # Fetch from Airtable (checks both Trial Users and Clients tables)
            client_profile_airtable = get_client_from_airtable(sender)
            
            if client_profile_airtable:
                # Merge with MongoDB profile (preserve history)
                old_history = client_profile.get('query_history', []) if client_profile else []
                old_total = client_profile.get('total_queries', 0) if client_profile else 0
                
                client_profile = {
                    'whatsapp_number': sender,
                    'name': client_profile_airtable['name'],
                    'email': client_profile_airtable['email'],
                    'tier': client_profile_airtable['tier'],
                    'subscription_status': client_profile_airtable['subscription_status'],
                    'airtable_record_id': client_profile_airtable['airtable_record_id'],
                    'airtable_table': client_profile_airtable['table'],
                    'preferences': client_profile_airtable['preferences'],
                    'usage_metrics': client_profile_airtable['usage_metrics'],
                    'trial_expired': client_profile_airtable.get('trial_expired', False),
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
                
                logger.info(f"✅ Client refreshed from Airtable: {client_profile['name']} ({client_profile['airtable_table']})")
        
        # ========================================
        # GET PREFERRED REGION (EARLY DEFINITION)
        # ========================================
        
        preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])
        
        # Ensure it's a list (defensive programming)
        if isinstance(preferred_regions, str):
            logger.warning(f"⚠️ preferred_regions is string, not list: '{preferred_regions}'")
            preferred_regions = [preferred_regions] if len(preferred_regions) > 2 else ['Mayfair']
        
        preferred_region = preferred_regions[0] if preferred_regions else 'Mayfair'
        
        # CRITICAL: Validate and fix corrupted region data
        if not preferred_region or len(preferred_region) < 3:
            logger.error(f"❌ CORRUPTED region in client_profile: '{preferred_region}'")
            
            # Hard-coded expansion map for single-letter corruptions
            region_expansion = {
                'M': 'Mayfair',
                'K': 'Knightsbridge', 
                'C': 'Chelsea',
                'B': 'Belgravia',
                'W': 'Kensington'
            }
            
            # Try to expand
            if preferred_region in region_expansion:
                preferred_region = region_expansion[preferred_region]
                logger.info(f"✅ EXPANDED '{preferred_regions[0]}' → '{preferred_region}'")
            else:
                # Ultimate fallback
                preferred_region = 'Mayfair'
                logger.warning(f"❌ Could not expand '{preferred_regions[0]}', defaulting to Mayfair")
        
        logger.info(f"✅ Final preferred_region: '{preferred_region}'")
        
        # ========================================
        # HANDLE TRIAL EXPIRATION
        # ========================================
        
        if client_profile and client_profile.get('trial_expired'):
            logger.warning(f"🚫 TRIAL EXPIRED: {sender}")
            
            trial_expired_msg = """TRIAL PERIOD EXPIRED

Your 24-hour trial access has concluded.

To continue using Voxmill Intelligence, contact:
📧 info@voxmill.uk

Thank you for trying our service."""
            
            await send_twilio_message(sender, trial_expired_msg)
            return
        
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
            
            # Get correct table name
            airtable_table = client_profile.get('airtable_table', 'Clients')
            
            if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and client_profile.get('airtable_record_id'):
                # Fetch only the PIN field from Airtable (lightweight)
                url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{airtable_table.replace(' ', '%20')}/{client_profile['airtable_record_id']}"
                headers = {
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                }
                
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    fields = response.json().get('fields', {})
                    airtable_last_pin = fields.get('PIN Last Verified')
                    
                    if airtable_last_pin:
                        # Sync to MongoDB pin_auth
                        last_verified_dt = dateutil_parser.parse(airtable_last_pin)
                        
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
        last_metadata = safe_get_last_metadata(conversation)

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
        # GREETING DETECTION - PERSONALIZED RESPONSE
        # ========================================
        
        greeting_keywords = ['hello', 'hi', 'hey', 'good morning', 'good afternoon', 'good evening', 'morning', 'afternoon', 'evening']
        
        if any(kw == message_lower for kw in greeting_keywords):
            # User is greeting - respond with personalized greeting
            client_name = client_profile.get('name', 'there')
            greeting_response = get_time_appropriate_greeting(client_name)
            
            await send_twilio_message(sender, greeting_response)
            
            # Update session
            try:
                conversation = ConversationSession(sender)
                conversation.update_session(
                    user_message=message_text,
                    assistant_response=greeting_response,
                    metadata={'category': 'greeting'}
                )
            except Exception:
                pass
            
            logger.info(f"✅ Greeting handled: {greeting_response}")
            return

        # ========================================
        # GOVERNANCE LAYER - WORLD-CLASS ENFORCEMENT
        # ========================================
        
        from app.conversational_governor import ConversationalGovernor, Intent
        
        # ========================================
        # PREPARE CONVERSATION CONTEXT FOR AUTO-SCOPING
        # ========================================
        
        conversation = ConversationSession(sender)
        conversation_entities = conversation.get_last_mentioned_entities()
        
        conversation_context = {
            'regions': conversation_entities.get('regions', []),
            'agents': conversation_entities.get('agents', []),
            'topics': conversation_entities.get('topics', [])
        }
        
        logger.info(f"📍 Conversation context for governance: {conversation_context}")
        
        # ========================================
        # GOVERNANCE LAYER - WITH MANDATE AWARENESS
        # ========================================
        
        # Classify intent and check if response allowed
        governance_result = await ConversationalGovernor.govern(
            message_text=message_text,
            sender=sender,
            client_profile=client_profile,
            system_state={
                'subscription_active': client_profile.get('subscription_status') == 'Active',
                'pin_unlocked': True,  # Already checked above
                'quota_remaining': 100,  # Calculate from usage
                'monitoring_active': len(client_profile.get('active_monitors', [])) > 0
            },
            conversation_context=conversation_context  # NEW: Pass context for auto-scoping
        )
        
        # Log auto-scoping results
        if governance_result.auto_scoped:
            logger.info(f"✅ Auto-scoped query: semantic_category={governance_result.semantic_category}")
        
        # ========================================
        # CHECKPOINT 6: AUTHORITY POLICY GATE
        # ========================================
        
        # If governance blocks response, stop here
        if governance_result.blocked:
            if governance_result.silence_required:
                logger.info(f"✅ SILENCE protocol - no response sent")
                return
            else:
                # Send governance-controlled response
                logger.info(f"🚫 Governance blocked: intent={governance_result.intent.value}, "
                           f"semantic_category={governance_result.semantic_category}, "
                           f"response='{governance_result.response}'")
                
                await send_twilio_message(sender, governance_result.response)
                # Update conversation session
                try:
                    conversation = ConversationSession(sender)
                    conversation.update_session(
                        user_message=message_text,
                        assistant_response=governance_result.response,
                        metadata={'category': 'governance_override', 'intent': governance_result.intent.value}
                    )
                except Exception as session_error:
                    logger.warning(f"Session update failed (non-critical): {session_error}")
                
                # Log interaction
                try:
                    preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
                    log_interaction(sender, message_text, governance_result.intent.value, governance_result.response)
                    update_client_history(sender, message_text, governance_result.intent.value, preferred_region)
                except Exception as log_error:
                    logger.warning(f"Logging failed (non-critical): {log_error}")
                
                logger.info(f"✅ Governance override: {governance_result.intent.value}")
                return
        
        # ========================================
        # EXTRACT GOVERNANCE CONSTRAINTS
        # ========================================
        
        allowed_response_shape = governance_result.allowed_shapes
        max_words = governance_result.max_words
        analysis_allowed = governance_result.analysis_allowed
        data_load_allowed = governance_result.data_load_allowed
        
        logger.info(f"✅ Governance passed: intent={governance_result.intent.value}, "
                   f"confidence={governance_result.confidence:.2f}, "
                   f"semantic_category={governance_result.semantic_category}, "
                   f"auto_scoped={governance_result.auto_scoped}, "
                   f"max_words={max_words}, "
                   f"analysis={analysis_allowed}, "
                   f"data_load={data_load_allowed}")
        
        # ========================================
        # ENFORCE DATA LOADING CONSTRAINT (CRITICAL)
        # ========================================
        
        if not data_load_allowed:
            logger.warning(f"❌ Data loading blocked by governance for intent: {governance_result.intent.value}")
            
            # This should only happen if governance passed but restricted operations
            # Return brief acknowledgment
            await send_twilio_message(sender, "Standing by.")
            
            # Update session
            try:
                conversation = ConversationSession(sender)
                conversation.update_session(
                    user_message=message_text,
                    assistant_response="Standing by.",
                    metadata={'category': 'data_load_blocked', 'intent': governance_result.intent.value}
                )
            except Exception:
                pass
            
            # Log interaction
            try:
                preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
                log_interaction(sender, message_text, "data_load_blocked", "Standing by.")
                update_client_history(sender, message_text, "data_load_blocked", preferred_region)
            except Exception:
                pass
            
            return  # CRITICAL: Stop pipeline here
        
        # ========================================
        # ENFORCE ANALYSIS CONSTRAINT (CRITICAL)
        # ========================================
        
        if not analysis_allowed:
            logger.warning(f"❌ Analysis blocked by governance for intent: {governance_result.intent.value}")
            
            # Return status-only response
            await send_twilio_message(sender, "Monitoring.")
            
            # Update session
            try:
                conversation = ConversationSession(sender)
                conversation.update_session(
                    user_message=message_text,
                    assistant_response="Monitoring.",
                    metadata={'category': 'analysis_blocked', 'intent': governance_result.intent.value}
                )
            except Exception:
                pass
            
            # Log interaction
            try:
                preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
                log_interaction(sender, message_text, "analysis_blocked", "Monitoring.")
                update_client_history(sender, message_text, "analysis_blocked", preferred_region)
            except Exception:
                pass
            
            return  # CRITICAL: Stop pipeline here
            
        # ========================================
        # DOWNSTREAM PROTECTION: BLOCK "QUERY UNCLEAR" FOR MANDATE-RELEVANT
        # ========================================
        
        # This is a safety net - should never be needed if governance works correctly
        if governance_result.semantic_category and governance_result.semantic_category != 'non_domain':
            # Query is mandate-relevant
            if not governance_result.blocked and governance_result.intent == Intent.UNKNOWN:
                logger.critical(f"⚠️ SAFETY NET TRIGGERED: Intent.UNKNOWN leaked through for mandate-relevant query")
                # This should NEVER happen after governance fixes
                # Force to STRATEGIC as last resort
                from app.conversational_governor import Intent
                governance_result.intent = Intent.STRATEGIC
                governance_result.confidence = 0.75
                logger.warning(f"🔄 Forced Intent.UNKNOWN → Intent.STRATEGIC (downstream protection)")

        # ========================================
        # GOVERNANCE PASSED - CONTINUE WITH CONSTRAINTS
        # ========================================
        
        # Store constraints for downstream enforcement
        # These will be checked again before LLM call and after response generation
        
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
                try:
                    conversation = ConversationSession(sender)
                    conversation.update_session(
                        user_message=message_text,
                        assistant_response=response,
                        metadata={'category': 'monitoring_status'}
                    )
                except Exception as session_error:
                    logger.warning(f"Session update failed (non-critical): {session_error}")
                
                # Log interaction
                try:
                    preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
                    log_interaction(sender, message_text, "monitoring_status", response)
                    update_client_history(sender, message_text, "monitoring_status", preferred_region)
                except Exception as log_error:
                    logger.warning(f"Logging failed (non-critical): {log_error}")
                
                logger.info(f"✅ Monitoring status query handled successfully")
                return
                
            except Exception as e:
                # TRULY GUARANTEED FALLBACK
                logger.error(f"Monitoring query failed: {e}")
                
                fallback_response = """MONITORING STATUS

Signal cache synchronizing. Your active monitoring directives will display momentarily.

To create a new monitor:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%"

Standing by."""
                
                # CRITICAL: Wrap EVERYTHING in try-except
                try:
                    await send_twilio_message(sender, fallback_response)
                except Exception as twilio_error:
                    logger.critical(f"Twilio failed in fallback: {twilio_error}")
                    # Last resort: can't even send message
                    return
                
                # Non-critical operations wrapped separately
                try:
                    conversation = ConversationSession(sender)
                    conversation.update_session(
                        user_message=message_text,
                        assistant_response=fallback_response,
                        metadata={'category': 'monitoring_status_fallback'}
                    )
                except Exception:
                    pass  # Silent failure OK
                
                try:
                    preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])
                    preferred_region = preferred_regions[0] if preferred_regions else 'Mayfair'
                    update_client_history(sender, message_text, "monitoring_status_fallback", preferred_region)
                except Exception:
                    pass  # Silent failure OK
                
                logger.info(f"✅ Monitoring status fallback sent")
                return

        # ========================================
        # PORTFOLIO TRACKING COMMANDS
        # ========================================

        portfolio_keywords = ['my portfolio', 'my properties', 'add property', 'portfolio summary']

        if any(kw in message_lower for kw in portfolio_keywords):
            try:
                from app.portfolio import get_portfolio_summary, add_property_to_portfolio, parse_property_from_message
                
                if 'add property' in message_lower:
                    # Check if user is sending property details
                    parsed_property = parse_property_from_message(message_text)
                    
                    if parsed_property:
                        # Add property
                        response = add_property_to_portfolio(sender, parsed_property)
                        await send_twilio_message(sender, response)
                        
                        # Update conversation session
                        conversation = ConversationSession(sender)
                        conversation.update_session(
                            user_message=message_text,
                            assistant_response=response,
                            metadata={'category': 'portfolio_add'}
                        )
                        
                        return
                    else:
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
                
                # Update conversation session
                conversation = ConversationSession(sender)
                conversation.update_session(
                    user_message=message_text,
                    assistant_response=response,
                    metadata={'category': 'portfolio_summary'}
                )
                
                return
                
            except Exception as e:
                logger.error(f"Portfolio command failed: {e}", exc_info=True)
                
                # Fallback response
                fallback_response = """Portfolio system temporarily unavailable.

Your holdings data is safe. Please try again in a moment.

Standing by."""
                
                try:
                    await send_twilio_message(sender, fallback_response)
                except Exception:
                    logger.critical(f"Complete portfolio failure for {sender}")
                
                return
        
        # ========================================
        # EXPORT/SHARING COMMANDS
        # ========================================

        # Export command detection
        if 'export' in message_lower or 'send pdf' in message_lower or 'email report' in message_lower or 'share' in message_lower:
            # Generate shareable link to last analysis
            
            conversation = ConversationSession(sender)
            last_metadata = safe_get_last_metadata(conversation)
            
            if not last_metadata or not last_metadata.get('category'):
                response = "No recent analysis to export. Run an analysis first, then ask to export it."
                await send_twilio_message(sender, response)
                return
            
            # Generate shareable link (simplified - you'd upload to Cloudflare/S3)
            import hashlib
            import time
            
            export_id = hashlib.md5(f"{sender}{time.time()}".encode()).hexdigest()[:12]
            
            # Store export in MongoDB
            from pymongo import MongoClient
            MONGODB_URI = os.getenv('MONGODB_URI')
            mongo_client = MongoClient(MONGODB_URI)
            db = mongo_client['Voxmill']
            
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
        # POST-DECISION CONSEQUENCE QUESTIONS
        # ========================================
        
        # Check if last interaction was Decision Mode
        conversation = ConversationSession(sender)
        last_metadata = safe_get_last_metadata(conversation)
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
        is_followup, context_hints = safe_detect_followup(conversation, message_normalized)
        
        if is_followup:
            logger.info(f"Follow-up detected: {context_hints}")
            # Resolve ambiguous references
            message_normalized = resolve_reference(message_normalized, context_hints)
            logger.info(f"Resolved query: {message_normalized}")
        
        # ========================================
        # RATE LIMITING - PREVENT COST EXPLOSION
        # ========================================
        
        # CRITICAL: Increment counter BEFORE check to prevent off-by-one
        # Log this message as "rate_check" first
        update_client_history(sender, message_text, "rate_check", preferred_region)
        
        # Reload profile with updated counter
        client_profile = get_client_profile(sender)
        query_history = client_profile.get('query_history', [])
        
        # SPAM PROTECTION: Minimum 2 seconds between messages
        if query_history:
            # Get last NON-rate_check query (ignore rate_check sentinel entries)
            last_real_query = None
            for q in reversed(query_history):
                if q.get('category') != 'rate_check':
                    last_real_query = q
                    break
            
            if last_real_query:
                last_query_time = last_real_query.get('timestamp')
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
            # Skip rate_check entries when counting queries
            if q.get('category') == 'rate_check':
                continue
                
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
📧 info@voxmill.uk"""
            
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
            logger.info(f"🔍 BEFORE preference check - preferred_region: '{preferred_region}'")
            logger.info(f"🔍 Checking if message is preference request: {message_text[:50]}")
            
            pref_response = handle_whatsapp_preference_message(sender, message_text)
            
            logger.info(f"🔍 AFTER preference check - preferred_region: '{preferred_region}'")
            
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
        
        # CRITICAL: Validate preferred_region before loading (detect corruption)
        if not preferred_region or len(preferred_region) < 3:
            logger.error(f"❌ CORRUPTED preferred_region detected: '{preferred_region}' (length: {len(preferred_region) if preferred_region else 0})")
            # Force reload from client profile
            preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])
            preferred_region = preferred_regions[0] if preferred_regions else 'Mayfair'
            logger.info(f"✅ FIXED preferred_region to: '{preferred_region}'")
        
        logger.info(f"🎯 Loading dataset for region: '{preferred_region}'")
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
        
        # ========================================
        # WORLD-CLASS: DETECT QUERY TYPE & USE INSTANT RESPONSE
        # ========================================
        
        from app.instant_response import InstantIntelligence, should_use_instant_response
        
        # Determine if we can use instant response
        query_type = 'complex'  # Default
        
        # Detect query patterns
        message_lower = message_normalized.lower()
        
        # Market overview patterns
        overview_patterns = ['market overview', 'what\'s up', 'what\'s the market', 
                            'market status', 'how\'s the market', 'market update',
                            'what\'s happening', 'give me an update']
        
        # Decision mode patterns
        decision_patterns = ['decision mode', 'what should i do', 'recommend action',
                            'tell me what to do', 'make the call', 'your recommendation']
        
        # Trend patterns
        trend_patterns = ['what\'s changed', 'what\'s different', 'trends', 'what\'s new',
                         'movements', 'shifts']
        
        # Timing patterns
        timing_patterns = ['timing', 'when should i', 'optimal time', 'entry window',
                          'exit window', 'liquidity window']
        
        # Agent patterns
        agent_patterns = ['agent', 'agents', 'who\'s moving', 'agent behavior',
                         'knight frank', 'savills', 'hamptons']
        
        # Detect which pattern matches
        is_overview = any(p in message_lower for p in overview_patterns)
        is_decision = any(p in message_lower for p in decision_patterns)
        is_trend = any(p in message_lower for p in trend_patterns)
        is_timing = any(p in message_lower for p in timing_patterns)
        is_agent = any(p in message_lower for p in agent_patterns)
        
        # ========================================
        # ROUTE TO INSTANT RESPONSE (Uses ALL intelligence layers)
        # ========================================
        
        if is_overview:
            # INSTANT MARKET SNAPSHOT (uses all intelligence)
            logger.info(f"🚀 INSTANT MARKET SNAPSHOT for: {message_normalized[:50]}")
            
            formatted_response = InstantIntelligence.get_full_market_snapshot(
                preferred_region,
                dataset,
                client_profile
            )
            
            category = "market_overview"
            
            # Send instant response
            await send_twilio_message(sender, formatted_response)
            
            # Update session
            conversation.update_session(
                user_message=message_text,
                assistant_response=formatted_response,
                metadata={'category': category, 'response_type': 'instant_full_intelligence'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, category, formatted_response)
            update_client_history(sender, message_text, category, preferred_region)
            
            logger.info(f"✅ Instant full-intelligence snapshot sent (<3s)")
            return

        # ========================================
        # CHECK FOR EMPTY/UNAVAILABLE DATASET
        # ========================================
        
        # CRITICAL: Block LLM call if no data available
        if dataset['metadata'].get('is_fallback') or dataset['metadata'].get('property_count', 0) == 0:
            logger.warning(f"No data available for {preferred_region}")
            
            # List available regions
            available_regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
            available_list = ', '.join(available_regions)
            
            fallback_response = f"""INTELLIGENCE UNAVAILABLE

No market data currently available for {preferred_region}.

Available regions:
{available_list}

Request a different region or contact support if this persists.

Standing by."""
            
            await send_twilio_message(sender, fallback_response)
            
            # Update conversation session
            try:
                conversation = ConversationSession(sender)
                conversation.update_session(
                    user_message=message_text,
                    assistant_response=fallback_response,
                    metadata={'category': 'data_unavailable', 'region': preferred_region}
                )
            except Exception:
                pass  # Non-critical
            
            # Log interaction
            try:
                log_interaction(sender, message_text, "data_unavailable", fallback_response)
                update_client_history(sender, message_text, "data_unavailable", preferred_region)
            except Exception:
                pass  # Non-critical
            
            logger.info(f"✅ Empty dataset handled gracefully for {preferred_region}")
            return
        
        elif is_decision:
            # INSTANT DECISION MODE (rule-based)
            logger.info(f"🎯 INSTANT DECISION MODE for: {message_normalized[:50]}")
            
            formatted_response = InstantIntelligence.get_instant_decision(
                preferred_region,
                dataset,
                client_profile
            )
            
            category = "decision_mode"
            
            # Send instant response
            await send_twilio_message(sender, formatted_response)
            
            # Update session
            conversation.update_session(
                user_message=message_text,
                assistant_response=formatted_response,
                metadata={'category': category, 'response_type': 'instant_decision'}
            )
            
            # Log interaction
            log_interaction(sender, message_text, category, formatted_response)
            update_client_history(sender, message_text, category, preferred_region)
            
            logger.info(f"✅ Instant decision sent (<3s)")
            
            # OPTIONAL: Background GPT-4 enhancement
            import asyncio
            asyncio.create_task(
                enhance_response_async(
                    sender,
                    message_normalized,
                    dataset,
                    client_profile,
                    formatted_response
                )
            )
            
            return
        
        elif is_trend:
            # INSTANT TREND ANALYSIS
            logger.info(f"📈 INSTANT TREND ANALYSIS for: {message_normalized[:50]}")
            
            formatted_response = InstantIntelligence.get_trend_analysis(
                preferred_region,
                dataset
            )
            
            category = "trend_analysis"
            
            await send_twilio_message(sender, formatted_response)
            
            conversation.update_session(
                user_message=message_text,
                assistant_response=formatted_response,
                metadata={'category': category, 'response_type': 'instant_trends'}
            )
            
            log_interaction(sender, message_text, category, formatted_response)
            update_client_history(sender, message_text, category, preferred_region)
            
            logger.info(f"✅ Instant trend analysis sent (<3s)")
            return
        
        elif is_timing:
            # INSTANT TIMING ANALYSIS
            logger.info(f"⏰ INSTANT TIMING ANALYSIS for: {message_normalized[:50]}")
            
            formatted_response = InstantIntelligence.get_timing_analysis(
                preferred_region,
                dataset
            )
            
            category = "timing_analysis"
            
            await send_twilio_message(sender, formatted_response)
            
            conversation.update_session(
                user_message=message_text,
                assistant_response=formatted_response,
                metadata={'category': category, 'response_type': 'instant_timing'}
            )
            
            log_interaction(sender, message_text, category, formatted_response)
            update_client_history(sender, message_text, category, preferred_region)
            
            logger.info(f"✅ Instant timing analysis sent (<3s)")
            return
        
        elif is_agent:
            # INSTANT AGENT ANALYSIS
            logger.info(f"🔍 INSTANT AGENT ANALYSIS for: {message_normalized[:50]}")
            
            formatted_response = InstantIntelligence.get_agent_analysis(
                preferred_region,
                dataset
            )
            
            category = "agent_analysis"
            
            await send_twilio_message(sender, formatted_response)
            
            conversation.update_session(
                user_message=message_text,
                assistant_response=formatted_response,
                metadata={'category': category, 'response_type': 'instant_agents'}
            )
            
            log_interaction(sender, message_text, category, formatted_response)
            update_client_history(sender, message_text, category, preferred_region)
            
            logger.info(f"✅ Instant agent analysis sent (<3s)")
            return
        
      # ========================================
        # FALLBACK: COMPLEX QUERIES USE GPT-4
        # ========================================
        
        logger.info(f"🤖 COMPLEX QUERY - Using GPT-4 for: {message_normalized[:50]}")
        
        # ========================================
        # GPT-4 ANALYSIS
        # ========================================
        
        category, response_text, response_metadata = await classify_and_respond(
            message_normalized,
            dataset,
            client_profile=client_profile,
            comparison_datasets=comparison_datasets if comparison_datasets else None
        )
        
        # ========================================
        # FORMAT RESPONSE (STRIP HEADERS FOR AUTHORITY MODE)
        # ========================================
        
        # Check if response is ultra-brief (Authority Mode)
        word_count = len(response_text.split())
        is_authority_response = response_metadata.get('authority_mode', False) or word_count < 50
        
        if is_authority_response:
            # NO headers for brief responses - pure institutional authority
            formatted_response = response_text.strip()
            logger.info(f"✅ Authority response (no headers): {word_count} words")
        else:
            # Standard responses get formatted headers
            formatted_response = format_analyst_response(response_text, category)
            logger.info(f"✅ Standard response (with headers): {word_count} words")

        # ========================================
        # ENFORCE RESPONSE SHAPE (BEFORE PROHIBITION SCAN)
        # ========================================
        
        from app.response_enforcer import ResponseEnforcer, ResponseShape
        
        # Select shape based on intent
        response_shape = ResponseEnforcer.select_shape_before_generation(
            governance_result.intent,
            allowed_response_shape
        )
        
        # Enforce shape constraints
        formatted_response = ResponseEnforcer.enforce_shape(
            formatted_response,
            response_shape,
            max_words
        )
        
        logger.info(f"✅ Shape enforced: {response_shape.value}, {len(formatted_response.split())} words")

        # ========================================
        # PHASE 4: PROHIBITION ENFORCEMENT (CHECKPOINT 10)
        # ========================================
        
        logger.info(f"🔍 Scanning response for Phase 4 prohibitions...")
        
        # Define prohibited patterns with severity levels
        prohibited_patterns = {
            # CRITICAL violations (only truly unacceptable content)
            'disclaimers': {
                'patterns': ["i'm not a financial advisor", "this is not investment advice", "not financial advice", "consult a professional", "as an ai", "i don't have access to", "i cannot", "based on available data", "according to our analysis"],
                'severity': 'CRITICAL',
                'category': 'disclaimers'
            },
            'chatbot_tone': {
                'patterns': ['great question', 'happy to help', 'feel free to', "don't hesitate", 'please let me know', 'absolutely!', 'definitely!', 'would you like me to'],
                'severity': 'CRITICAL',
                'category': 'chatbot_tone'
            },
            
            # HIGH violations (strip and continue - including hedge words)
            'hedging_language': {
                'patterns': ['might', 'could', 'may', 'possibly', 'perhaps', 'likely', 'probably', 'appears to', 'seems to', 'suggests', 'indicates', 'tends to'],
                'severity': 'HIGH',
                'category': 'hedging_language'
            },
            'questions_to_user': {
                'patterns': ['?'],  # Any question mark (except in REFUSAL/UNKNOWN intents)
                'severity': 'HIGH',
                'category': 'question_generation'
            },
            'explanatory_padding': {
                'patterns': ['i analyzed', 'i looked at', 'i examined', 'i considered', 'let me explain', 'to clarify', 'in other words', 'what this means is'],
                'severity': 'HIGH',
                'category': 'explanatory_padding'
            },
            
            # MEDIUM violations (strip quietly)
            'empty_filler': {
                'patterns': ['at this time', 'currently', 'at present', 'as of now', 'going forward', 'in terms of', 'with regard to', "it's worth noting", 'notably', 'furthermore', 'additionally', 'moreover'],
                'severity': 'MEDIUM',
                'category': 'empty_filler'
            }
        }
        
        violations_found = []
        response_lower = formatted_response.lower()
        
        # Scan for violations
        for violation_type, config in prohibited_patterns.items():
            patterns = config['patterns']
            severity = config['severity']
            category = config['category']
            
            for pattern in patterns:
                # Special handling for question marks
                if pattern == '?':
                    question_count = formatted_response.count('?')
                    if question_count > 0 and governance_result.intent not in [Intent.UNKNOWN, Intent.SECURITY]:
                        violations_found.append({
                            'type': violation_type,
                            'pattern': f"question_mark (count: {question_count})",
                            'severity': severity,
                            'category': category
                        })
                        logger.warning(f"⚠️ PROHIBITION VIOLATION: {category} - found {question_count} question marks")
                # Standard pattern matching
                elif pattern in response_lower:
                    violations_found.append({
                        'type': violation_type,
                        'pattern': pattern,
                        'severity': severity,
                        'category': category
                    })
                    logger.warning(f"⚠️ PROHIBITION VIOLATION: {category} - '{pattern}'")
        
        # Enforce violations
        if violations_found:
            # Check for CRITICAL violations
            critical_violations = [v for v in violations_found if v['severity'] == 'CRITICAL']
            
            if critical_violations:
                logger.error(f"🚫 CRITICAL PROHIBITION VIOLATIONS ({len(critical_violations)}): {[v['pattern'] for v in critical_violations]}")
                
                # Use safe fallback - strip violations and continue
                logger.warning(f"⚠️ Stripping {len(critical_violations)} critical violations from response")
                
                for violation in critical_violations:
                    pattern = violation['pattern']
                    # Remove the problematic word
                    formatted_response = re.sub(
                        r'\b' + re.escape(pattern) + r'\b',
                        '',
                        formatted_response,
                        flags=re.IGNORECASE
                    )
                
                # Clean up extra spaces
                formatted_response = ' '.join(formatted_response.split())
                
                # Log the failure
                try:
                    from pymongo import MongoClient
                    MONGODB_URI = os.getenv('MONGODB_URI')
                    if MONGODB_URI:
                        mongo_client = MongoClient(MONGODB_URI)
                        db = mongo_client['Voxmill']
                        
                        db['prohibition_violations'].insert_one({
                            'timestamp': datetime.now(timezone.utc),
                            'sender': sender,
                            'intent': governance_result.intent.value,
                            'violations': critical_violations,
                            'response_snippet': formatted_response[:200],
                            'severity': 'CRITICAL'
                        })
                except Exception as log_error:
                    logger.error(f"Failed to log violation: {log_error}")
            
            else:
                # HIGH/MEDIUM violations - strip patterns
                logger.warning(f"⚠️ Non-critical violations ({len(violations_found)}): stripping patterns")
                
                for violation in violations_found:
                    pattern = violation['pattern']
                    
                    # Skip question marks (can't strip those easily)
                    if 'question_mark' in str(pattern):
                        continue
                    
                    # Replace pattern with empty string (case-insensitive)
                    formatted_response = re.sub(
                        re.escape(pattern),
                        '',
                        formatted_response,
                        flags=re.IGNORECASE
                    )
                
                # Clean up extra spaces
                formatted_response = ' '.join(formatted_response.split())
                
                logger.info(f"✅ Patterns stripped, response cleaned")
        
        else:
            logger.info(f"✅ No prohibition violations detected")
        # ========================================
        # ENVELOPE CONSTRAINT: MAX WORD COUNT
        # ========================================
        
        word_count = len(formatted_response.split())
        
        if word_count > max_words and max_words > 0:
            logger.warning(f"⚠️ Response exceeds max_words: {word_count} > {max_words}")
            
            # Truncate at sentence boundary
            sentences = formatted_response.split('. ')
            truncated = []
            current_words = 0
            
            for sentence in sentences:
                sentence_words = len(sentence.split())
                if current_words + sentence_words <= max_words:
                    truncated.append(sentence)
                    current_words += sentence_words
                else:
                    break
            
            # Reconstruct response
            if truncated:
                formatted_response = '. '.join(truncated)
                # Add period if last sentence was complete
                if not formatted_response.endswith('.'):
                    formatted_response += '.'
                
                logger.info(f"✅ Response truncated to {len(formatted_response.split())} words")
            else:
                # Couldn't truncate intelligently - force hard truncation
                words = formatted_response.split()[:max_words]
                formatted_response = ' '.join(words) + '...'
                logger.warning(f"⚠️ Hard truncation applied")
        
        else:
            logger.info(f"✅ Word count OK: {word_count}/{max_words}")
        
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
        
        # Add data freshness warning if dataset is stale
        data_freshness_warning = ""
        if dataset.get('metadata', {}).get('data_age_hours', 0) > 24:
            data_freshness_warning = "\n\n⚠️ Data >24hrs old"
        
        formatted_response += data_freshness_warning
        
        # ========================================
        # SEND RESPONSE
        # ========================================
        
        await send_twilio_message(sender, formatted_response)
        
     # ========================================
        # SYNC USAGE METRICS BACK TO AIRTABLE
        # ========================================
        
        try:
            AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
            AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
            AIRTABLE_TABLE = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')
            
            if AIRTABLE_API_KEY and AIRTABLE_BASE_ID and client_profile.get('airtable_record_id'):
                
                # Calculate usage metrics
                messages_used = client_profile.get('usage_metrics', {}).get('messages_used_this_month', 0) + 1
                message_limit = client_profile.get('usage_metrics', {}).get('monthly_message_limit', 10000)
                
                # Build update payload - ONLY writable fields
                update_fields = {
                    'Messages Used This Month': int(messages_used)
                }
                
                # Add category-specific fields ONLY if they have values
                if category == 'decision_mode' and response_text:
                    update_fields['Last Decision Mode Trigger'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    # Only update if response exists and isn't empty
                    if len(response_text.strip()) > 0:
                        update_fields['Last Strategic Action Recommended'] = response_text[:500]  # Truncate to 500 chars
                
                # Update Airtable
                url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}/{client_profile['airtable_record_id']}"
                headers = {
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                payload = {"fields": update_fields}
                
                response = requests.patch(url, headers=headers, json=payload, timeout=5)
                
                if response.status_code == 200:
                    logger.info(f"✅ Airtable synced: {messages_used}/{message_limit} messages")
                elif response.status_code == 422:
                    error_details = response.json()
                    logger.error(f"⚠️ Airtable 422 error details: {error_details}")
                    logger.error(f"Payload sent: {payload}")
                else:
                    logger.warning(f"⚠️ Airtable sync failed: {response.status_code} - {response.text}")
                    
        except Exception as e:
            logger.error(f"Airtable sync error: {e}", exc_info=True)
            # Don't fail the whole request - just log

        
        # ========================================
        # UPDATE CONVERSATION SESSION
        # ========================================
        # Entity extraction happens automatically inside update_session()
        conversation.update_session(
            user_message=message_text,
            assistant_response=formatted_response,
            metadata={
                'category': category,
                'region': preferred_region,
                'confidence': confidence_score,
                'cached': False
            }
        )
        
        # Log interaction and update client history
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"✅ Message processed successfully: "
                   f"category={category}, "
                   f"intent={governance_result.intent.value}, "
                   f"semantic_category={governance_result.semantic_category}, "
                   f"confidence={response_metadata.get('confidence_level')}, "
                   f"urgency={response_metadata.get('recommendation_urgency')}, "
                   f"auto_scoped={governance_result.auto_scoped}")

        
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


async def send_twilio_message(to: str, message: str):
    """
    Send WhatsApp message via Twilio with smart chunking
    """
    
    try:
        from twilio.rest import Client
        import asyncio
        
        account_sid = os.getenv('TWILIO_ACCOUNT_SID')
        auth_token = os.getenv('TWILIO_AUTH_TOKEN')
        from_number = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')
        
        if not account_sid or not auth_token:
            logger.error("Twilio credentials missing")
            return
        
        client = Client(account_sid, auth_token)
        
        # Normalize phone number
        if not to.startswith('whatsapp:'):
            to = f'whatsapp:{to}'
        
        # Smart chunking for long messages
        MAX_LENGTH = 1500
        
        if len(message) <= MAX_LENGTH:
            # Single message
            client.messages.create(
                body=message,
                from_=from_number,
                to=to
            )
            logger.info(f"Message sent to {to} ({len(message)} chars)")
        else:
            # Multi-part message
            chunks = smart_split_message(message, MAX_LENGTH)
            
            for i, chunk in enumerate(chunks, 1):
                client.messages.create(
                    body=chunk,  # Already has [Part X/Y] from smart_split_message
                    from_=from_number,
                    to=to
                )
                
                logger.info(f"Chunk {i}/{len(chunks)} sent to {to} ({len(chunk)} chars)")
                
                # Delay between chunks to preserve order
                if i < len(chunks):
                    await asyncio.sleep(0.5)
        
    except Exception as e:
        logger.error(f"Failed to send Twilio message: {e}", exc_info=True)
        raise


async def enhance_response_async(
    sender: str,
    message: str, 
    dataset: Dict, 
    client_profile: Dict,
    instant_response: str
):
    """
    TIER 2: Background GPT-4 enhancement
    
    Runs AFTER instant response sent, provides deeper analysis if needed
    """
    try:
        logger.info(f"🔄 Background enhancement started for {sender}")
        
        # Give GPT-4 more time since user already has response
        from app.llm import classify_and_respond
        
        category, enhanced_text, metadata = await classify_and_respond(
            message,
            dataset,
            client_profile=client_profile,
            comparison_datasets=None
        )
        
        # Only send enhanced response if it adds significant value
        similarity_threshold = 0.7  # Don't send if >70% similar
        
        # Simple similarity check (word overlap)
        instant_words = set(instant_response.lower().split())
        enhanced_words = set(enhanced_text.lower().split())
        overlap = len(instant_words & enhanced_words) / max(len(instant_words), 1)
        
        if overlap < similarity_threshold and len(enhanced_text) > len(instant_response) * 1.3:
            # Enhanced response adds value
            enhancement_msg = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENHANCED ANALYSIS

{enhanced_text}"""
            
            await send_twilio_message(sender, enhancement_msg)
            logger.info(f"✅ Enhanced analysis sent to {sender}")
        else:
            logger.info(f"⏭️ Enhancement skipped (insufficient added value)")
        
    except Exception as e:
        logger.error(f"Background enhancement failed: {e}")
        # Silent failure - user already has instant response


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

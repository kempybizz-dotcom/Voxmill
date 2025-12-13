"""
VOXMILL PIN AUTHENTICATION SYSTEM
==================================
Secure intelligence access with 4-digit PIN protection

Features:
- Bcrypt hashing (never store plain text)
- 5 failed attempt limit (typo-friendly)
- Self-service PIN reset
- Manual lock/unlock
- Auto re-verification after 7 days or subscription changes
"""

import os
import logging
import bcrypt
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# MongoDB connection
MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


class PINAuthenticator:
    """Secure PIN authentication for intelligence access"""
    
    # Constants
    PIN_LENGTH = 4
    MAX_FAILED_ATTEMPTS = 5
    INACTIVITY_DAYS = 7
    
    @staticmethod
    def hash_pin(pin: str) -> str:
        """Securely hash PIN with bcrypt"""
        if not pin or len(pin) != PINAuthenticator.PIN_LENGTH:
            raise ValueError(f"PIN must be exactly {PINAuthenticator.PIN_LENGTH} digits")
        
        if not pin.isdigit():
            raise ValueError("PIN must contain only digits")
        
        # Generate salt and hash
        salt = bcrypt.gensalt(rounds=12)
        hashed = bcrypt.hashpw(pin.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    @staticmethod
    def verify_pin(pin: str, pin_hash: str) -> bool:
        """Verify PIN against stored hash"""
        try:
            return bcrypt.checkpw(pin.encode('utf-8'), pin_hash.encode('utf-8'))
        except Exception as e:
            logger.error(f"PIN verification error: {e}")
            return False
    
    @staticmethod
    def set_pin(whatsapp_number: str, pin: str) -> Tuple[bool, str]:
        """
        Set new PIN for user
        
        Returns: (success, message)
        """
        try:
            # Validate PIN format
            if not pin or len(pin) != PINAuthenticator.PIN_LENGTH:
                return False, f"PIN must be exactly {PINAuthenticator.PIN_LENGTH} digits"
            
            if not pin.isdigit():
                return False, "PIN must contain only digits (0-9)"
            
            # Common weak PINs
            weak_pins = ['0000', '1111', '2222', '3333', '4444', '5555', 
                        '6666', '7777', '8888', '9999', '1234', '4321']
            
            if pin in weak_pins:
                return False, "PIN too simple. Choose a more secure combination."
            
            # Hash PIN
            pin_hash = PINAuthenticator.hash_pin(pin)
            
            # Update database
            if db:
                db['client_profiles'].update_one(
                    {'whatsapp_number': whatsapp_number},
                    {
                        '$set': {
                            'access_pin_hash': pin_hash,
                            'pin_set_at': datetime.now(timezone.utc),
                            'last_verified_at': datetime.now(timezone.utc),
                            'pin_locked': False,
                            'failed_attempts': 0,
                            'require_pin_verification': False
                        }
                    },
                    upsert=True
                )
                
                logger.info(f"✅ PIN set for {whatsapp_number}")
                return True, "PIN set successfully"
            else:
                logger.error("MongoDB not available")
                return False, "System error - please try again"
                
        except Exception as e:
            logger.error(f"Set PIN error: {e}", exc_info=True)
            return False, "System error - please try again"
    
    @staticmethod
    def verify_and_unlock(whatsapp_number: str, pin: str) -> Tuple[bool, str]:
        """
        Verify PIN and unlock intelligence access
        
        Returns: (success, message)
        """
        try:
            if not db:
                return False, "System error"
            
            # Get user profile
            profile = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
            
            if not profile:
                return False, "Profile not found"
            
            # Check if locked
            if profile.get('pin_locked', False):
                return False, "locked"
            
            # Get stored hash
            pin_hash = profile.get('access_pin_hash')
            if not pin_hash:
                return False, "PIN not set"
            
            # Verify PIN
            if PINAuthenticator.verify_pin(pin, pin_hash):
                # Success - reset failed attempts and update verification time
                db['client_profiles'].update_one(
                    {'whatsapp_number': whatsapp_number},
                    {
                        '$set': {
                            'last_verified_at': datetime.now(timezone.utc),
                            'failed_attempts': 0,
                            'require_pin_verification': False
                        }
                    }
                )
                
                logger.info(f"✅ PIN verified for {whatsapp_number}")
                return True, "Access granted"
            else:
                # Failed attempt
                failed_attempts = profile.get('failed_attempts', 0) + 1
                
                update_data = {
                    'failed_attempts': failed_attempts
                }
                
                # Lock after max attempts
                if failed_attempts >= PINAuthenticator.MAX_FAILED_ATTEMPTS:
                    update_data['pin_locked'] = True
                    logger.warning(f"🚫 PIN locked for {whatsapp_number} after {failed_attempts} attempts")
                    return False, "locked"
                
                db['client_profiles'].update_one(
                    {'whatsapp_number': whatsapp_number},
                    {'$set': update_data}
                )
                
                attempts_remaining = PINAuthenticator.MAX_FAILED_ATTEMPTS - failed_attempts
                logger.warning(f"❌ Failed PIN attempt for {whatsapp_number} ({attempts_remaining} remaining)")
                
                return False, f"{attempts_remaining}"
                
        except Exception as e:
            logger.error(f"Verify PIN error: {e}", exc_info=True)
            return False, "System error"
    
    @staticmethod
    def check_needs_verification(whatsapp_number: str) -> Tuple[bool, str]:
        """
        Check if user needs PIN verification
        
        Returns: (needs_verification, reason)
        Reasons: 'not_set', 'inactivity', 'subscription_change', 'manual_lock', 'none'
        """
        try:
            if not db:
                return False, "none"
            
            profile = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
            
            if not profile:
                return False, "none"
            
            # Check if PIN is set
            if not profile.get('access_pin_hash'):
                return True, "not_set"
            
            # Check if manually locked
            if profile.get('pin_locked', False):
                return True, "locked"
            
            # Check if flagged for re-verification
            if profile.get('require_pin_verification', False):
                return True, "subscription_change"
            
            # Check inactivity
            last_verified = profile.get('last_verified_at')
            if last_verified:
                # Make timezone-aware if needed
                if isinstance(last_verified, str):
                    from dateutil import parser
                    last_verified = parser.parse(last_verified)
                
                if last_verified.tzinfo is None:
                    last_verified = last_verified.replace(tzinfo=timezone.utc)
                
                days_since_verification = (datetime.now(timezone.utc) - last_verified).days
                
                if days_since_verification >= PINAuthenticator.INACTIVITY_DAYS:
                    logger.info(f"PIN re-verification needed for {whatsapp_number} (inactive {days_since_verification} days)")
                    return True, "inactivity"
            
            return False, "none"
            
        except Exception as e:
            logger.error(f"Check verification error: {e}", exc_info=True)
            return False, "none"
    
    @staticmethod
    def manual_lock(whatsapp_number: str) -> Tuple[bool, str]:
        """
        Manually lock intelligence access
        
        Returns: (success, message)
        """
        try:
            if not db:
                return False, "System error"
            
            db['client_profiles'].update_one(
                {'whatsapp_number': whatsapp_number},
                {
                    '$set': {
                        'require_pin_verification': True
                    }
                }
            )
            
            logger.info(f"🔒 Manual lock activated for {whatsapp_number}")
            return True, "Intelligence line locked"
            
        except Exception as e:
            logger.error(f"Manual lock error: {e}", exc_info=True)
            return False, "System error"
    
    @staticmethod
    def reset_pin_request(whatsapp_number: str, old_pin: str, new_pin: str) -> Tuple[bool, str]:
        """
        Reset PIN (requires old PIN verification)
        
        Returns: (success, message)
        """
        try:
            if not db:
                return False, "System error"
            
            # Get profile
            profile = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
            
            if not profile:
                return False, "Profile not found"
            
            # Verify old PIN first
            old_pin_hash = profile.get('access_pin_hash')
            if not old_pin_hash:
                return False, "No PIN set"
            
            if not PINAuthenticator.verify_pin(old_pin, old_pin_hash):
                return False, "Old PIN incorrect"
            
            # Set new PIN
            success, message = PINAuthenticator.set_pin(whatsapp_number, new_pin)
            
            if success:
                logger.info(f"🔄 PIN reset for {whatsapp_number}")
                return True, "PIN reset successfully"
            else:
                return False, message
                
        except Exception as e:
            logger.error(f"Reset PIN error: {e}", exc_info=True)
            return False, "System error"
    
    @staticmethod
    def admin_unlock(whatsapp_number: str) -> Tuple[bool, str]:
        """
        Admin unlock (for support team)
        
        Returns: (success, message)
        """
        try:
            if not db:
                return False, "System error"
            
            db['client_profiles'].update_one(
                {'whatsapp_number': whatsapp_number},
                {
                    '$set': {
                        'pin_locked': False,
                        'failed_attempts': 0,
                        'require_pin_verification': True  # Force PIN re-entry
                    }
                }
            )
            
            logger.info(f"🔓 Admin unlock for {whatsapp_number}")
            return True, "Account unlocked - PIN verification required"
            
        except Exception as e:
            logger.error(f"Admin unlock error: {e}", exc_info=True)
            return False, "System error"
    
    @staticmethod
    def trigger_reverification_on_subscription_change(whatsapp_number: str) -> None:
        """
        Flag account for PIN re-verification when subscription status changes
        (Called when subscription goes: paused → active, cancelled → active, etc.)
        """
        try:
            if not db:
                return
            
            db['client_profiles'].update_one(
                {'whatsapp_number': whatsapp_number},
                {
                    '$set': {
                        'require_pin_verification': True
                    }
                }
            )
            
            logger.info(f"🔄 PIN re-verification triggered for {whatsapp_number} (subscription change)")
            
        except Exception as e:
            logger.error(f"Trigger re-verification error: {e}", exc_info=True)


# ============================================================
# CONVENIENCE FUNCTIONS
# ============================================================

def get_pin_status_message(reason: str, client_name: str = "there") -> str:
    """Generate appropriate PIN request message based on reason"""
    
    first_name = client_name.split()[0] if client_name != "there" else "there"
    
    if reason == "not_set":
        return f"""🔐 VOXMILL INTELLIGENCE ACCESS

Welcome to your private intelligence line.

For security, please set a 4-digit access code.

This protects your classified briefings and market intelligence.

Reply with 4 digits (e.g., 1234):"""
    
    elif reason == "inactivity":
        return f"""🔐 ACCESS CODE REQUIRED

Good evening, {first_name}.

Intelligence line locked after 7 days of inactivity.

Enter your 4-digit code:"""
    
    elif reason == "subscription_change":
        return f"""🔐 ACCESS CODE REQUIRED

Welcome back, {first_name}.

Your subscription status has changed.

For security, please re-verify your 4-digit code:"""
    
    elif reason == "locked":
        return f"""🚫 ACCESS SUSPENDED

Your intelligence line has been locked for security.

Too many failed attempts.

Contact support:
📧 ollys@voxmill.uk"""
    
    else:
        return f"""🔐 ACCESS CODE REQUIRED

Enter your 4-digit code:"""


def get_pin_response_message(success: bool, message: str, client_name: str = "there") -> str:
    """Generate response message after PIN attempt"""
    
    first_name = client_name.split()[0] if client_name != "there" else "there"
    
    if success:
        # Successful verification
        from datetime import datetime
        import pytz
        
        uk_tz = pytz.timezone('Europe/London')
        hour = datetime.now(uk_tz).hour
        
        if 5 <= hour < 12:
            greeting = f"Good morning, {first_name}."
        elif 12 <= hour < 17:
            greeting = f"Good afternoon, {first_name}."
        elif 17 <= hour < 22:
            greeting = f"Good evening, {first_name}."
        else:
            greeting = f"Evening, {first_name}."
        
        if message == "PIN set successfully":
            return f"""✅ ACCESS CODE CONFIRMED

Your intelligence line is now secured.

{greeting}

What can I analyze for you today?"""
        else:
            return f"""✅ ACCESS GRANTED

{greeting}

What can I analyze for you?"""
    
    elif message == "locked":
        return get_pin_status_message("locked", client_name)
    
    elif message.isdigit():
        # Failed attempt with remaining count
        attempts_remaining = int(message)
        
        if attempts_remaining == 1:
            return f"""❌ INCORRECT CODE

⚠️ FINAL ATTEMPT REMAINING

Your intelligence line will lock after one more incorrect attempt."""
        else:
            return f"""❌ INCORRECT CODE ({attempts_remaining} attempts remaining)"""
    
    else:
        # Other errors
        return f"❌ {message}"

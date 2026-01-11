"""
VOXMILL PENDING ACTIONS MANAGER
================================
Finite State Machine for destructive operations requiring confirmation

CHATGPT SPEC IMPLEMENTATION:
- Explicit FSM (IDLE → PENDING_CONFIRM → EXECUTING → IDLE)
- Action IDs with 5-minute expiry
- Audit trail for all mutations
- One pending action per client per module
"""

import logging
import secrets
import string
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from enum import Enum
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


class ActionState(Enum):
    """FSM States"""
    IDLE = "idle"
    PENDING_CONFIRM = "pending_confirm"
    EXECUTING = "executing"
    ERROR = "error"


class ActionType(Enum):
    """Supported action types"""
    RESET_PORTFOLIO = "reset_portfolio"
    REMOVE_PROPERTY = "remove_property"
    RESET_MONITORS = "reset_monitors"
    REMOVE_MONITOR = "remove_monitor"


def generate_action_id() -> str:
    """Generate unique action ID (format: P-9F3K2)"""
    chars = string.ascii_uppercase + string.digits
    code = ''.join(secrets.choice(chars) for _ in range(5))
    return f"P-{code}"


class PendingAction:
    """
    Pending action requiring confirmation
    
    FSM: IDLE → PENDING_CONFIRM → EXECUTING → IDLE
    """
    
    EXPIRY_MINUTES = 5
    
    def __init__(self, client_id: str, action_type: ActionType, data: Dict = None):
        self.action_id = generate_action_id()
        self.client_id = client_id
        self.action_type = action_type
        self.state = ActionState.PENDING_CONFIRM
        self.created_at = datetime.now(timezone.utc)
        self.expires_at = self.created_at + timedelta(minutes=self.EXPIRY_MINUTES)
        self.data = data or {}
        self.result = None
    
    def is_expired(self) -> bool:
        """Check if action has expired"""
        return datetime.now(timezone.utc) > self.expires_at
    
    def to_dict(self) -> Dict:
        """Serialize to dict for storage"""
        return {
            'action_id': self.action_id,
            'client_id': self.client_id,
            'action_type': self.action_type.value,
            'state': self.state.value,
            'created_at': self.created_at.isoformat(),
            'expires_at': self.expires_at.isoformat(),
            'data': self.data,
            'result': self.result
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'PendingAction':
        """Deserialize from dict"""
        action = cls.__new__(cls)
        action.action_id = data['action_id']
        action.client_id = data['client_id']
        action.action_type = ActionType(data['action_type'])
        action.state = ActionState(data['state'])
        action.created_at = datetime.fromisoformat(data['created_at'])
        action.expires_at = datetime.fromisoformat(data['expires_at'])
        action.data = data.get('data', {})
        action.result = data.get('result')
        return action


class PendingActionManager:
    """
    Manages pending actions with MongoDB persistence
    
    CHATGPT INVARIANT: One pending destructive action per client at a time
    """
    
    def __init__(self):
        self.collection = db['pending_actions']
        # Create index on client_id for fast lookups
        self.collection.create_index('client_id')
        self.collection.create_index('expires_at', expireAfterSeconds=300)  # Auto-cleanup
    
    def create_action(self, client_id: str, action_type: ActionType, 
                     data: Dict = None) -> PendingAction:
        """
        Create new pending action
        
        CHATGPT RULE: Reject if pending action exists
        """
        
        # Check for existing pending action
        existing = self.get_pending_action(client_id)
        
        if existing and not existing.is_expired():
            logger.warning(f"🚫 Client {client_id} already has pending action: {existing.action_id}")
            raise ValueError(f"Pending action exists: {existing.action_id}. Complete or wait for expiry.")
        
        # Create new action
        action = PendingAction(client_id, action_type, data)
        
        # Store in MongoDB
        self.collection.insert_one(action.to_dict())
        
        logger.info(f"✅ Created pending action: {action.action_id} for {client_id}")
        
        return action
    
    def get_pending_action(self, client_id: str) -> Optional[PendingAction]:
        """Get pending action for client (if exists and not expired)"""
        
        # Find most recent pending action
        doc = self.collection.find_one(
            {
                'client_id': client_id,
                'state': ActionState.PENDING_CONFIRM.value
            },
            sort=[('created_at', -1)]
        )
        
        if not doc:
            return None
        
        action = PendingAction.from_dict(doc)
        
        # Check expiry
        if action.is_expired():
            logger.info(f"⏰ Action {action.action_id} expired, removing")
            self.collection.delete_one({'action_id': action.action_id})
            return None
        
        return action
    
    def confirm_action(self, client_id: str, action_id: str) -> Optional[PendingAction]:
        """
        Confirm action by ID
        
        Returns action if valid, None if invalid/expired
        """
        
        pending = self.get_pending_action(client_id)
        
        if not pending:
            logger.warning(f"🚫 No pending action for {client_id}")
            return None
        
        if pending.action_id != action_id:
            logger.warning(f"🚫 Action ID mismatch: expected {pending.action_id}, got {action_id}")
            return None
        
        # Update state to EXECUTING
        self.collection.update_one(
            {'action_id': action_id},
            {'$set': {'state': ActionState.EXECUTING.value}}
        )
        
        pending.state = ActionState.EXECUTING
        
        logger.info(f"✅ Action {action_id} confirmed, state → EXECUTING")
        
        return pending
    
    def complete_action(self, action_id: str, result: Dict):
        """
        Mark action as completed and write audit log
        
        CHATGPT REQUIREMENT: Audit trail for all mutations
        """
        
        # Get action
        doc = self.collection.find_one({'action_id': action_id})
        
        if not doc:
            logger.error(f"❌ Action {action_id} not found for completion")
            return
        
        action = PendingAction.from_dict(doc)
        
        # Write audit log
        audit_entry = {
            'client_id': action.client_id,
            'action_id': action.action_id,
            'action_type': action.action_type.value,
            'actor': 'user',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': action.data,
            'result': result
        }
        
        db['action_audit_log'].insert_one(audit_entry)
        
        # Remove pending action (transition to IDLE)
        self.collection.delete_one({'action_id': action_id})
        
        logger.info(f"✅ Action {action_id} completed and logged")
    
    def cancel_action(self, client_id: str) -> bool:
        """Cancel pending action (if exists)"""
        
        pending = self.get_pending_action(client_id)
        
        if not pending:
            return False
        
        self.collection.delete_one({'action_id': pending.action_id})
        
        logger.info(f"🗑️ Action {pending.action_id} cancelled")
        
        return True


# Global manager instance
action_manager = PendingActionManager()

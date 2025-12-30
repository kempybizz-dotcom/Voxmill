"""
AIRTABLE AUTO-SYNC
==================
Automatically populate Airtable fields from system events

Integration with existing airtable_queue.py for rate-limit-safe writes
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
    # BUILD UPDATE FIELDS
    # ========================================
    
    updates = {}
    
    # ========================================
    # ALWAYS UPDATE (Every Event)
    # ========================================
    
    updates['Last Active'] = datetime.now(timezone.utc).isoformat()
    updates['Mongo Sync Status'] = datetime.now(timezone.utc).isoformat()
    
    # ========================================
    # MESSAGE SENT
    # ========================================
    
    if event_type == 'message_sent':
        usage = client.get('usage_metrics', {})
        
        # Counters
        updates['Messages Used This Month'] = usage.get('messages_used_this_month', 0)
        updates['Total Messages Sent'] = usage.get('total_messages_sent', 0)
        updates['Last Message Date'] = datetime.now(timezone.utc).isoformat()
        
        # Calculate remaining
        monthly_limit = usage.get('monthly_message_limit', 50)
        used = usage.get('messages_used_this_month', 0)
        updates['Message Limit Remaining'] = max(0, monthly_limit - used)
        
        # Tokens (if provided)
        if metadata and 'tokens_used' in metadata:
            updates['Total Tokens Used'] = usage.get('total_tokens_used', 0) + metadata['tokens_used']
        
        # Engagement Level (AI)
        updates['Engagement Level (AI)'] = calculate_engagement_level(client)
    
    # ========================================
    # INTELLIGENCE DELIVERED
    # ========================================
    
    elif event_type == 'intelligence_delivered':
        if metadata:
            category = metadata.get('category', '')
            
            # Track decision mode
            if category in ['decision_mode', 'decision_request']:
                updates['Last Decision Mode Trigger'] = datetime.now(timezone.utc).isoformat()
            
            # Track strategic actions
            if 'strategic_action' in metadata:
                updates['Last Strategic Action Recommended'] = metadata['strategic_action']
    
    # ========================================
    # PIN EVENTS
    # ========================================
    
    elif event_type == 'pin_verified':
        updates['PIN Status'] = 'Active'
        updates['PIN Last Verified'] = datetime.now(timezone.utc).isoformat()
        updates['PIN Failed Attempts'] = 0  # Reset on success
    
    elif event_type == 'pin_failed':
        # Increment failed attempts
        current_failures = client.get('pin_failed_attempts', 0)
        updates['PIN Failed Attempts'] = current_failures + 1
        
        # Lock if too many failures
        if current_failures + 1 >= 5:
            updates['PIN Status'] = 'Locked'
            updates['PIN Locked Reason'] = 'Too many failed attempts'
    
    # ========================================
    # PREFERENCE CHANGED
    # ========================================
    
    elif event_type == 'preference_changed':
        if metadata:
            # Update preference fields
            if 'regions' in metadata:
                updates['Regions'] = metadata['regions']
            
            if 'competitor_focus' in metadata:
                updates['Competitor Focus'] = metadata['competitor_focus'].capitalize()
            
            if 'report_depth' in metadata:
                updates['Report Depth'] = metadata['report_depth'].capitalize()
    
    # ========================================
    # ONBOARDING COMPLETE
    # ========================================
    
    elif event_type == 'onboarding_complete':
        updates['Onboarding Complete'] = True
    
    # ========================================
    # TRIAL EXPIRY
    # ========================================
    
    elif event_type == 'trial_expired':
        updates['Trial Expiry Date'] = datetime.now(timezone.utc).isoformat()
    
    # ========================================
    # CANCELLATION
    # ========================================
    
    elif event_type == 'subscription_cancelled':
        updates['Cancellation Date'] = datetime.now(timezone.utc).isoformat()
        updates['Subscription Status'] = 'Cancelled'
    
    # ========================================
    # QUEUE UPDATE (Rate-limit safe)
    # ========================================
    
    if updates:
        await queue_airtable_update(
            table_name=table_name,
            record_id=record_id,
            fields=updates,
            priority='normal'
        )
        
        logger.info(f"📊 Auto-synced {len(updates)} fields to Airtable for {whatsapp_number}")


def calculate_engagement_level(client: Dict) -> str:
    """
    Calculate engagement level based on usage patterns
    
    Returns: 'High' | 'Medium' | 'Low'
    """
    
    total_messages = client.get('total_queries', 0)
    
    # Calculate messages per day
    created_at = client.get('created_at')
    if created_at:
        # ✅ FIX: Ensure timezone-aware datetime
        if isinstance(created_at, str):
            from dateutil import parser as dateutil_parser
            created_at = dateutil_parser.parse(created_at)
        
        # Make timezone-aware if naive
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        
        days_active = (datetime.now(timezone.utc) - created_at).days
        if days_active < 1:
            days_active = 1
        
        messages_per_day = total_messages / days_active
        
        if messages_per_day >= 5:
            return 'High'
        elif messages_per_day >= 1:
            return 'Medium'
        else:
            return 'Low'
    
    # Fallback: use total only
    if total_messages >= 50:
        return 'High'
    elif total_messages >= 10:
        return 'Medium'
    else:
        return 'Low'


def calculate_decision_style(client: Dict) -> str:
    """
    Analyze query history to determine decision style
    
    Returns: 'Data-Driven' | 'Intuitive' | 'Balanced'
    """
    
    query_history = client.get('query_history', [])
    
    if len(query_history) < 5:
        return 'Unknown'
    
    # Count categories
    decision_queries = 0
    analytical_queries = 0
    quick_queries = 0
    
    for query in query_history[-20:]:  # Last 20 queries
        category = query.get('category', '')
        
        if category in ['decision_mode', 'decision_request']:
            decision_queries += 1
        elif category in ['trend_analysis', 'risk_analysis']:
            analytical_queries += 1
        elif category in ['market_overview', 'status_check']:
            quick_queries += 1
    
    # Classify
    if decision_queries > analytical_queries and decision_queries > quick_queries:
        return 'Data-Driven'
    elif quick_queries > analytical_queries * 2:
        return 'Intuitive'
    else:
        return 'Balanced'


async def sync_ai_fields(
    whatsapp_number: str,
    record_id: str,
    table_name: str
):
    """
    Update AI-generated fields (run periodically, not on every message)
    
    Fields updated:
    - Client Summary (AI)
    - Decision Style
    - Observed Risk Tolerance
    - Confidence Trajectory
    - Accumulating Bias
    """
    
    from app.client_manager import get_client_profile
    
    client = get_client_profile(whatsapp_number)
    
    if not client:
        return
    
    updates = {}
    
    # Decision Style
    updates['Decision Style'] = calculate_decision_style(client)
    
    # TODO: Add GPT-4 generation for:
    # - Client Summary (AI): 2-3 sentence summary of client behavior
    # - Observed Risk Tolerance: High/Medium/Low based on queries
    # - Confidence Trajectory: Increasing/Stable/Decreasing
    # - Accumulating Bias: Detected patterns in queries
    
    if updates:
        await queue_airtable_update(
            table_name=table_name,
            record_id=record_id,
            fields=updates,
            priority='normal'
        )

"""
Silent Priority Monitoring System - World-Class Edition
Institutional-grade market monitoring with explicit consent and lifecycle management
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import re
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


class MonitorManager:
    """Manages lifecycle of silent monitoring alerts"""
    
    # Tier limits
    TIER_LIMITS = {
        'tier_1': 5,
        'tier_2': 10,
        'tier_3': 999  # Unlimited
    }
    
    @staticmethod
    def parse_monitor_request(message: str, client_profile: dict) -> Tuple[bool, dict]:
        """
        Parse monitoring request and extract parameters with intelligent inference
        
        Returns: (success, monitor_config)
        """
        
        # Extract region
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message.lower()), 
                      client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0])
        
        # Extract agent (optional)
        agents = ['Knight Frank', 'Savills', 'Hamptons', 'Chestertons', 'Strutt & Parker']
        agent = next((a for a in agents if a.lower() in message.lower()), None)
        
        # Extract triggers with intelligent inference
        triggers = []
        
        # Price drop trigger - more flexible patterns
        price_patterns = [
            r'price.+?drop.+?(\d+)%',
            r'drop.+?(\d+)%',
            r'prices?.+?(\d+)%',
            r'(\d+)%.*drop',
            r'alert.*(\d+)%',
            r'if.*(\d+)%'
        ]
        
        for pattern in price_patterns:
            price_match = re.search(pattern, message, re.IGNORECASE)
            if price_match:
                threshold = int(price_match.group(1))
                triggers.append({
                    "type": "price_drop",
                    "threshold": threshold,
                    "unit": "percent"
                })
                break
        
        # Inventory increase trigger
        inventory_patterns = [
            r'inventory.+?increase.+?(\d+)%',
            r'increase.+?(\d+)%',
            r'inventory.*(\d+)%'
        ]
        
        for pattern in inventory_patterns:
            inventory_match = re.search(pattern, message, re.IGNORECASE)
            if inventory_match:
                threshold = int(inventory_match.group(1))
                triggers.append({
                    "type": "inventory_increase",
                    "threshold": threshold,
                    "unit": "percent"
                })
                break
        
        # Liquidity velocity trigger
        velocity_patterns = [
            r'velocity.+?below.+?(\d+)',
            r'liquidity.+?below.+?(\d+)',
            r'velocity.*(\d+)'
        ]
        
        for pattern in velocity_patterns:
            velocity_match = re.search(pattern, message, re.IGNORECASE)
            if velocity_match:
                threshold = int(velocity_match.group(1))
                triggers.append({
                    "type": "velocity_drop",
                    "threshold": threshold,
                    "unit": "absolute"
                })
                break
        
        # SMART DEFAULT: If no specific trigger but monitoring intent is clear
        if not triggers:
            # Keywords indicating monitoring intent without specific threshold
            monitor_keywords = ['monitor', 'watch', 'track', 'alert', 'notify']
            has_monitor_intent = any(kw in message.lower() for kw in monitor_keywords)
            
            # Competitor/agent movement keywords
            movement_keywords = ['move', 'movement', 'change', 'competitor']
            has_movement_intent = any(kw in message.lower() for kw in movement_keywords)
            
            if has_monitor_intent and has_movement_intent and agent:
                # Smart default: monitor for any significant price movement (>2%)
                triggers.append({
                    "type": "price_drop",
                    "threshold": 2,
                    "unit": "percent"
                })
                logger.info(f"Applied smart default: monitoring {agent} for >2% price movement")
        
        if not triggers:
            return False, {}
        
        # Extract duration with smart parsing
        duration_match = re.search(r'(\d+)\s*(?:days?|d)', message, re.IGNORECASE)
        duration_days = int(duration_match.group(1)) if duration_match else None
        
        # Check for time-based keywords
        if 'week' in message.lower() and not duration_days:
            duration_days = 7
        elif 'month' in message.lower() and not duration_days:
            duration_days = 30
        
        # Check for "indefinite"
        if 'indefinite' in message.lower() or 'unlimited' in message.lower():
            duration_days = 365 * 10  # 10 years = effectively indefinite
        
        # Smart default: 7 days if not specified
        if duration_days is None:
            duration_days = 7
        
        return True, {
            'region': region,
            'agent': agent,
            'triggers': triggers,
            'duration_days': duration_days
        }
    
    @staticmethod
    @staticmethod
    async def create_monitor_pending(whatsapp_number: str, config: dict, client_profile: dict) -> str:
        """
        Create monitor in pending state, awaiting confirmation
        
        Returns: confirmation message
        """
        
        # Check tier limits
        tier = client_profile.get('tier', 'tier_1')
        existing_monitors = db['client_profiles'].find_one(
            {'whatsapp_number': whatsapp_number}
        ).get('active_monitors', [])
        
        active_count = len([m for m in existing_monitors if m.get('status') == 'active'])
        
        if active_count >= MonitorManager.TIER_LIMITS[tier]:
            return f"""MONITOR LIMIT REACHED

Your {tier.replace('_', ' ').title()} plan allows {MonitorManager.TIER_LIMITS[tier]} active monitors.

Current active: {active_count}

Stop an existing monitor or upgrade tier."""
        
        # Get baseline data
        from app.dataset_loader import load_dataset
        dataset = load_dataset(area=config['region'])
        
        baseline_data = {
            "avg_price": dataset.get('metrics', {}).get('avg_price', 0),
            "inventory_count": len(dataset.get('properties', [])),
            "velocity_score": dataset.get('liquidity_velocity', {}).get('velocity_score', 0) if 'liquidity_velocity' in dataset else 0,
            "captured_at": datetime.now(timezone.utc)
        }
        
        # Track if defaults were applied
        defaults_applied = config.get('defaults_applied', {})
        
        # Create pending monitor
        monitor_id = f"mon_{int(datetime.now(timezone.utc).timestamp())}"
        
        monitor = {
            "id": monitor_id,
            "created_at": datetime.now(timezone.utc),
            "expires_at": datetime.now(timezone.utc) + timedelta(days=config['duration_days']),
            "duration_days": config['duration_days'],
            "status": "pending_confirmation",
            "pause_reason": None,
            "region": config['region'],
            "agent": config['agent'],
            "triggers": config['triggers'],
            "check_frequency_hours": 6,
            "last_checked": None,
            "next_check": None,
            "total_checks": 0,
            "baseline_data": baseline_data,
            "current_data": None,
            "alerts_sent": 0,
            "last_alert": None,
            "alert_history": [],
            "confirmed_at": None,
            "last_extended": None,
            "user_notes": "",
            "pending_expires": datetime.now(timezone.utc) + timedelta(minutes=5),
            "defaults_applied": defaults_applied
        }
        
        # Save to MongoDB
        db['client_profiles'].update_one(
            {'whatsapp_number': whatsapp_number},
            {'$push': {'active_monitors': monitor}}
        )
        
        # Format trigger list
        trigger_list = "\n".join([
            f"• {t['type'].replace('_', ' ').title()}: ≥{t['threshold']}%" if t['unit'] == 'percent' 
            else f"• {t['type'].replace('_', ' ').title()}: <{t['threshold']}"
            for t in config['triggers']
        ])
        
        agent_str = f"{config['agent']} " if config['agent'] else ""
        duration_str = f"{config['duration_days']} days" if config['duration_days'] < 3650 else "Indefinite"
        
        # Build execution receipt if defaults were applied
        execution_receipt = ""
        if defaults_applied:
            applied_parts = []
            if defaults_applied.get('threshold'):
                applied_parts.append(f"{defaults_applied['threshold']}% threshold")
            if defaults_applied.get('duration'):
                applied_parts.append(f"{defaults_applied['duration']} days")
            
            if applied_parts:
                execution_receipt = f"\n\nDefault parameters applied: {' · '.join(applied_parts)}"
        
        return f"""MONITORING REQUEST RECEIVED
————————————————————————————————————————

Target: {agent_str}{config['region']}
{trigger_list}

Duration: {duration_str}
Check frequency: Every 6 hours
Action on breach: Alert once, then pause

Reply "CONFIRM" to activate.

Customize duration: "CONFIRM, 30 days" / "CONFIRM, 60 days"

Request expires: 5 minutes.{execution_receipt}"""
    
    @staticmethod
    async def confirm_monitor(whatsapp_number: str, message: str = "") -> str:
        """Activate pending monitor after confirmation with optional duration override"""
        
        # Extract custom duration if provided (e.g., "CONFIRM, 24 hours" or "CONFIRM 7 days")
        custom_duration = None
        
        # Pattern 1: "CONFIRM, 24 hours" or "CONFIRM, 7 days"
        duration_match = re.search(r'confirm[,\s]+(\d+)\s*(hour|day|week|month)', message, re.IGNORECASE)
        if duration_match:
            amount = int(duration_match.group(1))
            unit = duration_match.group(2).lower()
            
            if 'hour' in unit:
                custom_duration = amount / 24  # Convert to days
            elif 'day' in unit:
                custom_duration = amount
            elif 'week' in unit:
                custom_duration = amount * 7
            elif 'month' in unit:
                custom_duration = amount * 30
        
        # Find pending monitor
        client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
        monitors = client.get('active_monitors', [])
        
        pending_monitor = None
        for m in monitors:
            if m.get('status') == 'pending_confirmation':
                pending_monitor = m
                break
        
        if not pending_monitor:
            return """No pending monitors found.

Create a new monitor:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%"""
        
        # Check if expired
        pending_expires = pending_monitor.get('pending_expires')
        if pending_expires:
            if isinstance(pending_expires, str):
                from dateutil import parser
                pending_expires = parser.parse(pending_expires)
            
            if pending_expires.tzinfo is None:
                pending_expires = pending_expires.replace(tzinfo=timezone.utc)
            
            if datetime.now(timezone.utc) > pending_expires:
                # Remove expired pending monitor
                db['client_profiles'].update_one(
                    {'whatsapp_number': whatsapp_number},
                    {'$pull': {'active_monitors': {'id': pending_monitor['id']}}}
                )
                
                return """Confirmation expired (5 minutes).

Please create a new monitoring request."""
        
        # Apply custom duration if provided
        now = datetime.now(timezone.utc)
        if custom_duration:
            new_expires_at = now + timedelta(days=custom_duration)
            duration_days = custom_duration
        else:
            new_expires_at = pending_monitor['expires_at']
            duration_days = pending_monitor['duration_days']
        
        next_check = now + timedelta(hours=pending_monitor['check_frequency_hours'])
        
        # Activate monitor
        db['client_profiles'].update_one(
            {
                'whatsapp_number': whatsapp_number,
                'active_monitors.id': pending_monitor['id']
            },
            {
                '$set': {
                    'active_monitors.$.status': 'active',
                    'active_monitors.$.confirmed_at': now,
                    'active_monitors.$.next_check': next_check,
                    'active_monitors.$.expires_at': new_expires_at,
                    'active_monitors.$.duration_days': duration_days,
                    'active_monitors.$.pending_expires': None
                }
            }
        )
        
        # Format response
        trigger_list = "\n".join([
            f"{t['type'].replace('_', ' ').title()} ≥{t['threshold']}%" if t['unit'] == 'percent' 
            else f"{t['type'].replace('_', ' ').title()} <{t['threshold']}"
            for t in pending_monitor['triggers']
        ])
        
        agent_str = f"{pending_monitor['agent']} " if pending_monitor.get('agent') else ""
        expires_str = new_expires_at.strftime('%d %b %Y')
        next_check_str = next_check.strftime('%d %b %H:%M')
        
        baseline = pending_monitor['baseline_data']
        
        return f"""MONITORING ACTIVE
————————————————————————————————————————

{agent_str}{pending_monitor['region']} — {trigger_list}

Expires: {expires_str}
Next check: {next_check_str}

Baseline:
- Avg price: £{baseline['avg_price']:,.0f}
- Inventory: {baseline['inventory_count']} units
{f"- Velocity: {baseline['velocity_score']}/100" if baseline.get('velocity_score', 0) > 0 else ""}

Commands:
"Stop monitoring {pending_monitor['region']}" — Deactivate
"Show monitors" — View all active
"Extend monitoring" — Add 30 days

Standing by."""
    
    @staticmethod
    async def stop_monitor(whatsapp_number: str, region: str = None) -> str:
        """Deactivate monitor(s)"""
        
        if not region:
            # Stop all monitors
            result = db['client_profiles'].update_one(
                {'whatsapp_number': whatsapp_number},
                {'$set': {'active_monitors': []}}
            )
            
            return """ALL MONITORING STOPPED

All monitors deactivated.

Reactivate anytime with:
"Monitor [agent] [region], alert if [condition]" """
        
        # Find specific monitor
        client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
        monitors = client.get('active_monitors', [])
        
        monitor_to_stop = None
        for m in monitors:
            if m.get('region').lower() == region.lower() and m.get('status') in ['active', 'paused']:
                monitor_to_stop = m
                break
        
        if not monitor_to_stop:
            return f"""No active monitor found for {region}.

Use "Show monitors" to see active monitors."""
        
        # Calculate runtime
        runtime_hours = (datetime.now(timezone.utc) - monitor_to_stop['created_at']).total_seconds() / 3600
        runtime_days = int(runtime_hours / 24)
        
        # Remove monitor
        db['client_profiles'].update_one(
            {'whatsapp_number': whatsapp_number},
            {'$pull': {'active_monitors': {'id': monitor_to_stop['id']}}}
        )
        
        agent_str = f"{monitor_to_stop['agent']} " if monitor_to_stop.get('agent') else ""
        
        return f"""MONITORING STOPPED

{agent_str}{monitor_to_stop['region']} monitor deactivated.

Runtime: {runtime_days} days
Alerts sent: {monitor_to_stop['alerts_sent']}

Reactivate anytime."""


async def handle_monitor_request(whatsapp_number: str, message: str, client_profile: dict) -> str:
    """
    Main handler for monitoring requests
    Routes to appropriate function based on message with intelligent intent inference
    """
    
    message_lower = message.lower().strip()
    
    # Check for manual mode override
    if 'manual only' in message_lower or 'no inference' in message_lower:
        return """MANUAL MODE ACTIVATED

Inference disabled. Provide complete parameters:

Format: "Monitor [agent] [region], alert if [condition], [duration]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%, 30 days"

Required:
- Agent (optional): Knight Frank, Savills, Hamptons, etc.
- Region: Mayfair, Knightsbridge, Chelsea, Belgravia, Kensington
- Condition: "prices drop X%" / "inventory increases X%" / "velocity below X"
- Duration: "X days" (or "indefinite")"""
    
    # Confirmation (with or without duration)
    if 'confirm' in message_lower:
        return await MonitorManager.confirm_monitor(whatsapp_number, message)
    
    # Stop monitoring
    if 'stop monitoring' in message_lower or 'stop all' in message_lower:
        # Extract region if specified
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message_lower), None)
        
        return await MonitorManager.stop_monitor(whatsapp_number, region)
    
    # Show monitors
    if 'show monitor' in message_lower or 'list monitor' in message_lower or 'my monitor' in message_lower:
        return await show_monitors(whatsapp_number)
    
    # Resume monitoring
    if 'resume monitor' in message_lower:
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message_lower), None)
        
        if region:
            return await resume_monitor(whatsapp_number, region)
        else:
            return "Specify region: 'Resume monitoring Mayfair'"
    
    # Extend monitoring
    if 'extend monitor' in message_lower:
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message_lower), None)
        
        if region:
            return await extend_monitor(whatsapp_number, region)
        else:
            return "Specify region: 'Extend monitoring Mayfair'"
    
    # Create new monitor with intelligent parsing
    success, config = MonitorManager.parse_monitor_request(message, client_profile)
    
    if not success:
        # REMOVED "Unable to parse" - return None to route to LLM
        return None
    
    return await MonitorManager.create_monitor_pending(whatsapp_number, config, client_profile)


async def show_monitors(whatsapp_number: str) -> str:
    """Show all active monitors"""
    
    client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
    monitors = client.get('active_monitors', [])
    
    active_monitors = [m for m in monitors if m.get('status') in ['active', 'paused']]
    
    if not active_monitors:
        return """No active monitors.

Create one:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%"""
    
    monitor_list = []
    
    for m in active_monitors:
        agent_str = f"{m['agent']} " if m.get('agent') else ""
        status_emoji = "●" if m['status'] == 'active' else "⏸"
        
        trigger_str = ", ".join([
            f"{t['type'].replace('_', ' ')} {t['threshold']}%"
            for t in m['triggers']
        ])
        
        days_left = (m['expires_at'] - datetime.now(timezone.utc)).days
        
        monitor_list.append(
            f"{status_emoji} {agent_str}{m['region']}\n"
            f"   {trigger_str}\n"
            f"   Expires: {days_left}d | Alerts: {m['alerts_sent']}"
        )
    
    return f"""ACTIVE MONITORS ({len(active_monitors)})
————————————————————————————————————————

{chr(10).join(monitor_list)}

Commands:
"Stop monitoring [region]"
"Resume monitoring [region]" (if paused)
"Extend monitoring [region]" """


async def resume_monitor(whatsapp_number: str, region: str) -> str:
    """Resume paused monitor with updated baseline"""
    
    client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
    monitors = client.get('active_monitors', [])
    
    paused_monitor = None
    for m in monitors:
        if m.get('region').lower() == region.lower() and m.get('status') == 'paused':
            paused_monitor = m
            break
    
    if not paused_monitor:
        return f"No paused monitor found for {region}."
    
    # Get new baseline
    from app.dataset_loader import load_dataset
    dataset = load_dataset(area=region)
    
    new_baseline = {
        "avg_price": dataset.get('metrics', {}).get('avg_price', 0),
        "inventory_count": len(dataset.get('properties', [])),
        "velocity_score": dataset.get('liquidity_velocity', {}).get('velocity_score', 0) if 'liquidity_velocity' in dataset else 0,
        "captured_at": datetime.now(timezone.utc)
    }
    
    next_check = datetime.now(timezone.utc) + timedelta(hours=paused_monitor['check_frequency_hours'])
    
    # Update monitor
    db['client_profiles'].update_one(
        {
            'whatsapp_number': whatsapp_number,
            'active_monitors.id': paused_monitor['id']
        },
        {
            '$set': {
                'active_monitors.$.status': 'active',
                'active_monitors.$.pause_reason': None,
                'active_monitors.$.baseline_data': new_baseline,
                'active_monitors.$.next_check': next_check
            }
        }
    )
    
    agent_str = f"{paused_monitor['agent']} " if paused_monitor.get('agent') else ""
    
    return f"""MONITORING RESUMED

{agent_str}{region}

Baseline updated: £{new_baseline['avg_price']:,.0f}
Expires: {paused_monitor['expires_at'].strftime('%d %b')}
Next check: {next_check.strftime('%d %b %H:%M')}

Standing by."""


async def extend_monitor(whatsapp_number: str, region: str, days: int = 30) -> str:
    """Extend monitor expiry by X days"""
    
    client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
    monitors = client.get('active_monitors', [])
    
    monitor_to_extend = None
    for m in monitors:
        if m.get('region').lower() == region.lower() and m.get('status') == 'active':
            monitor_to_extend = m
            break
    
    if not monitor_to_extend:
        return f"No active monitor found for {region}."
    
    new_expiry = monitor_to_extend['expires_at'] + timedelta(days=days)
    
    db['client_profiles'].update_one(
        {
            'whatsapp_number': whatsapp_number,
            'active_monitors.id': monitor_to_extend['id']
        },
        {
            '$set': {
                'active_monitors.$.expires_at': new_expiry,
                'active_monitors.$.last_extended': datetime.now(timezone.utc)
            }
        }
    )
    
    agent_str = f"{monitor_to_extend['agent']} " if monitor_to_extend.get('agent') else ""
    
    return f"""MONITORING EXTENDED

{agent_str}{region}

Extended: +{days} days
New expiry: {new_expiry.strftime('%d %b %Y')}

Standing by."""

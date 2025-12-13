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
        Parse monitoring request and extract parameters
        
        Returns: (success, monitor_config)
        """
        
        # Extract region
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message.lower()), 
                      client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0])
        
        # Extract agent (optional)
        agents = ['Knight Frank', 'Savills', 'Hamptons', 'Chestertons', 'Strutt & Parker']
        agent = next((a for a in agents if a.lower() in message.lower()), None)
        
        # Extract triggers
        triggers = []
        
        # Price drop trigger
        price_drop_match = re.search(r'price.+?drop.+?(\d+)%|drop.+?(\d+)%', message, re.IGNORECASE)
        if price_drop_match:
            threshold = int(price_drop_match.group(1) or price_drop_match.group(2))
            triggers.append({
                "type": "price_drop",
                "threshold": threshold,
                "unit": "percent"
            })
        
        # Inventory increase trigger
        inventory_match = re.search(r'inventory.+?increase.+?(\d+)%|increase.+?(\d+)%', message, re.IGNORECASE)
        if inventory_match:
            threshold = int(inventory_match.group(1) or inventory_match.group(2))
            triggers.append({
                "type": "inventory_increase",
                "threshold": threshold,
                "unit": "percent"
            })
        
        # Liquidity velocity trigger
        velocity_match = re.search(r'velocity.+?below.+?(\d+)|liquidity.+?below.+?(\d+)', message, re.IGNORECASE)
        if velocity_match:
            threshold = int(velocity_match.group(1) or velocity_match.group(2))
            triggers.append({
                "type": "velocity_drop",
                "threshold": threshold,
                "unit": "absolute"
            })
        
        if not triggers:
            return False, {}
        
        # Extract duration (default 30 days)
        duration_match = re.search(r'(\d+)\s*days?', message, re.IGNORECASE)
        duration_days = int(duration_match.group(1)) if duration_match else 30
        
        # Check for "indefinite"
        if 'indefinite' in message.lower() or 'unlimited' in message.lower():
            duration_days = 365 * 10  # 10 years = effectively indefinite
        
        return True, {
            'region': region,
            'agent': agent,
            'triggers': triggers,
            'duration_days': duration_days
        }
    
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
            return f"""❌ MONITOR LIMIT REACHED

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
            "pending_expires": datetime.now(timezone.utc) + timedelta(minutes=5)
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
        
        return f"""🔔 MONITORING REQUEST RECEIVED

Target: {agent_str}{config['region']}
{trigger_list}

CONFIRMATION REQUIRED:

Duration: {duration_str}
Check frequency: Every 6 hours
Action on breach: Alert once, then pause

Reply "CONFIRM" to activate.

To customize duration: "30 days" / "60 days" / "90 days" / "indefinite"

This request expires in 5 minutes."""
    
    @staticmethod
    async def confirm_monitor(whatsapp_number: str, region: str) -> str:
        """Activate pending monitor after confirmation"""
        
        # Find pending monitor for this region
        client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
        monitors = client.get('active_monitors', [])
        
        pending_monitor = None
        for m in monitors:
            if (m.get('status') == 'pending_confirmation' and 
                m.get('region').lower() == region.lower()):
                pending_monitor = m
                break
        
        if not pending_monitor:
            # Try to find any pending monitor (if region not specified)
            for m in monitors:
                if m.get('status') == 'pending_confirmation':
                    pending_monitor = m
                    break
        
        if not pending_monitor:
            return """❌ No pending monitors found.

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
                
                return """❌ Confirmation expired (5 minutes).

Please create a new monitoring request."""
        
        # Activate monitor
        now = datetime.now(timezone.utc)
        next_check = now + timedelta(hours=pending_monitor['check_frequency_hours'])
        
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
        expires_str = pending_monitor['expires_at'].strftime('%d %b %Y, %H:%M GMT')
        next_check_str = next_check.strftime('%d %b %Y, %H:%M GMT')
        
        baseline = pending_monitor['baseline_data']
        
        return f"""✅ MONITORING ACTIVE

{agent_str}{pending_monitor['region']} - {trigger_list}

Started: {now.strftime('%d %b %Y, %H:%M GMT')}
Expires: {expires_str}
Status: Active
Next check: {next_check_str}

Baseline captured:
- Avg price: £{baseline['avg_price']:,.0f}
- Inventory: {baseline['inventory_count']} units
{f"• Velocity: {baseline['velocity_score']}/100" if baseline.get('velocity_score', 0) > 0 else ""}

Commands:
- "Stop monitoring {pending_monitor['region']}" - Deactivate
- "Show monitors" - View all active
- "Extend monitoring" - Add 30 days

You'll only hear from me when thresholds breach.

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
            
            return """🔴 ALL MONITORING STOPPED

All monitors have been deactivated.

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
            return f"""❌ No active monitor found for {region}.

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
        
        return f"""🔴 MONITORING STOPPED

{agent_str}{monitor_to_stop['region']} monitor deactivated.

Total runtime: {runtime_days} days
Alerts sent: {monitor_to_stop['alerts_sent']}

Need to reactivate? Just ask."""


async def handle_monitor_request(whatsapp_number: str, message: str, client_profile: dict) -> str:
    """
    Main handler for monitoring requests
    Routes to appropriate function based on message
    """
    
    message_lower = message.lower().strip()
    
    # Confirmation
    if message_lower == 'confirm':
        # Try to extract region from context if available
        return await MonitorManager.confirm_monitor(whatsapp_number, "")
    
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
            return "Specify region to resume: 'Resume monitoring Mayfair'"
    
    # Extend monitoring
    if 'extend monitor' in message_lower:
        regions = ['Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington']
        region = next((r for r in regions if r.lower() in message_lower), None)
        
        if region:
            return await extend_monitor(whatsapp_number, region)
        else:
            return "Specify region to extend: 'Extend monitoring Mayfair'"
    
    # Create new monitor
    success, config = MonitorManager.parse_monitor_request(message, client_profile)
    
    if not success:
        return """Unable to parse monitoring criteria.

Format examples:
- "Monitor Knight Frank Mayfair, alert if prices drop 5%"
- "Watch Chelsea inventory, notify if increases 20%"
- "Track Knightsbridge liquidity, alert if velocity drops below 60"

Specific request?"""
    
    return await MonitorManager.create_monitor_pending(whatsapp_number, config, client_profile)


async def show_monitors(whatsapp_number: str) -> str:
    """Show all active monitors"""
    
    client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
    monitors = client.get('active_monitors', [])
    
    active_monitors = [m for m in monitors if m.get('status') in ['active', 'paused']]
    
    if not active_monitors:
        return """No active monitors.

Create one with:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor Knight Frank Mayfair, alert if prices drop 5%"""
    
    monitor_list = []
    
    for m in active_monitors:
        agent_str = f"{m['agent']} " if m.get('agent') else ""
        status_emoji = "🟢" if m['status'] == 'active' else "⏸️"
        
        trigger_str = ", ".join([
            f"{t['type'].replace('_', ' ')} {t['threshold']}%"
            for t in m['triggers']
        ])
        
        days_left = (m['expires_at'] - datetime.now(timezone.utc)).days
        
        monitor_list.append(
            f"{status_emoji} {agent_str}{m['region']}\n"
            f"   {trigger_str}\n"
            f"   Expires: {days_left} days | Alerts: {m['alerts_sent']}"
        )
    
    return f"""ACTIVE MONITORS ({len(active_monitors)})
————————————————————————————————————————

{chr(10).join(monitor_list)}

Commands:
- "Stop monitoring [region]"
- "Resume monitoring [region]" (if paused)
- "Extend monitoring [region]" """


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
    
    return f"""✅ MONITORING RESUMED

{agent_str}{region}

New baseline: £{new_baseline['avg_price']:,.0f} (updated to current)
Expires: {paused_monitor['expires_at'].strftime('%d %b %Y')}
Next check: {next_check.strftime('%d %b %Y, %H:%M GMT')}

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
    
    return f"""✅ MONITORING EXTENDED

{agent_str}{region}

Extended by: {days} days
New expiry: {new_expiry.strftime('%d %b %Y, %H:%M GMT')}

Standing by."""

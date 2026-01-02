"""
Silent Priority Monitoring System - World-Class Edition
Institutional-grade market monitoring with explicit consent and lifecycle management

✅ FIXED: No hardcoded markets - queries Airtable Markets & Competitors tables dynamically
✅ FIXED: Industry-agnostic - works across all verticals
✅ FIXED: Proper imports and class structure
"""

import os
import logging
import requests
import re
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from typing import Tuple, Optional
from dateutil import parser as dateutil_parser

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


# ============================================================
# HELPER FUNCTIONS - AIRTABLE QUERIES
# ============================================================

def get_available_competitors_from_airtable(industry_code: str) -> list:
    """
    Query Competitors table for available competitors by industry
    
    ✅ INDUSTRY-AGNOSTIC - Returns competitors from Airtable
    
    Args:
        industry_code: Lowercase industry code (e.g., 'real_estate', 'automotive')
    
    Returns:
        List of competitor names or empty list if none configured
    """
    
    AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
    
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        logger.error("Airtable credentials missing")
        return []
    
    try:
        headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Competitors"
        
        # Query for competitors in this industry that are trackable
        formula = f"AND({{industry}}='{industry_code}', {{is_trackable}}=TRUE())"
        params = {'filterByFormula': formula}
        
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        if response.status_code == 200:
            records = response.json().get('records', [])
            competitors = [r['fields'].get('competitor_name') for r in records if r['fields'].get('competitor_name')]
            
            if not competitors:
                logger.warning(f"⚠️ NO COMPETITORS CONFIGURED in Airtable for industry: {industry_code}")
            
            logger.info(f"✅ Found {len(competitors)} competitors for {industry_code}")
            return competitors
        
        logger.error(f"Competitors table query failed: {response.status_code}")
        return []
    
    except Exception as e:
        logger.error(f"Get available competitors failed: {e}")
        return []


def get_available_markets_from_db(industry_code: str) -> list:
    """
    Query Markets table for available markets by industry
    
    ✅ IMPORTED FROM whatsapp.py to avoid circular import
    
    Args:
        industry_code: Lowercase industry code
    
    Returns:
        List of market names
    """
    from app.whatsapp import get_available_markets_from_db as get_markets
    return get_markets(industry_code)


# ============================================================
# MONITOR MANAGER CLASS
# ============================================================

class MonitorManager:
    """Central manager for monitoring lifecycle"""
    
    TIER_LIMITS = {
        'tier_1': 1,
        'tier_2': 3,
        'tier_3': 10
    }
    
    @staticmethod
    def parse_monitor_request(message: str, client_profile: dict) -> Tuple[bool, dict]:
        """
        Parse monitoring request and extract parameters with intelligent inference
        
        ✅ INDUSTRY AGNOSTIC - Queries Markets & Competitors tables dynamically
        
        Returns: (success, monitor_config)
        """
        
        # ========================================
        # GET INDUSTRY FROM CLIENT PROFILE
        # ========================================
        
        industry = client_profile.get('industry', 'real_estate')
        
        # ========================================
        # QUERY AVAILABLE MARKETS FROM AIRTABLE
        # ========================================
        
        available_markets = get_available_markets_from_db(industry)
        
        if not available_markets:
            logger.error(f"❌ NO MARKETS CONFIGURED for industry: {industry}")
            return False, {'error': 'no_markets_configured'}
        
        # Extract region from message (match against available markets)
        region = next((m for m in available_markets if m.lower() in message.lower()), None)
        
        # Fallback to client's preferred region
        if not region:
            preferred_regions = client_profile.get('preferences', {}).get('preferred_regions', [])
            region = preferred_regions[0] if preferred_regions else None
        
        # Final fallback to first available market
        if not region:
            region = available_markets[0]
        
        # ========================================
        # QUERY AVAILABLE COMPETITORS FROM AIRTABLE
        # ========================================
        
        available_competitors = get_available_competitors_from_airtable(industry)
        
        # Extract agent/competitor from message (match against available competitors)
        agent = next((c for c in available_competitors if c.lower() in message.lower()), None)
        
        # ========================================
        # EXTRACT TRIGGERS (INDUSTRY-AGNOSTIC)
        # ========================================
        
        triggers = []
        
        # Price drop trigger - flexible patterns
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
        
        # Liquidity/velocity trigger (industry-agnostic)
        velocity_patterns = [
            r'velocity.+?below.+?(\d+)',
            r'liquidity.+?below.+?(\d+)',
            r'velocity.*(\d+)',
            r'volume.+?below.+?(\d+)',
            r'activity.+?below.+?(\d+)'
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
        
        # ========================================
        # SMART DEFAULT TRIGGER
        # ========================================
        
        if not triggers:
            # Keywords indicating monitoring intent without specific threshold
            monitor_keywords = ['monitor', 'watch', 'track', 'alert', 'notify']
            has_monitor_intent = any(kw in message.lower() for kw in monitor_keywords)
            
            # Movement keywords (industry-agnostic)
            movement_keywords = ['move', 'movement', 'change', 'competitor', 'shift']
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
            logger.warning(f"No triggers extracted from: {message}")
            return False, {}
        
        # ========================================
        # EXTRACT DURATION
        # ========================================
        
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
        
        logger.info(f"✅ Monitor parsed: industry={industry}, market={region}, agent={agent}, triggers={len(triggers)}, duration={duration_days}d")
        
        return True, {
            'industry': industry,
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
            return f"""MONITOR LIMIT REACHED

Your {tier.replace('_', ' ').title()} plan allows {MonitorManager.TIER_LIMITS[tier]} active monitors.

Current active: {active_count}

Stop an existing monitor or upgrade tier."""
        
        # Get baseline data
        from app.dataset_loader import load_dataset
        dataset = load_dataset(area=config['region'], industry=config['industry'])
        
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
            "industry": config['industry'],
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
        
        return f"""MONITOR REQUEST
————————————————————————————————————————

Target: {agent_str}{config['region']}
{trigger_list}

Duration: {duration_str}
Check frequency: Every 6 hours
Action on breach: Alert once, then pause

Status: Pending confirmation

Reply "CONFIRM" to activate.

Customize duration: "CONFIRM, 30 days" / "CONFIRM, 60 days"

Request expires: 5 minutes.{execution_receipt}"""
    
    @staticmethod
    async def confirm_monitor(whatsapp_number: str, message: str = "") -> str:
        """Activate pending monitor after confirmation with optional duration override"""
        
        # Extract custom duration if provided (e.g., "CONFIRM, 24 hours" or "CONFIRM 7 days")
        custom_duration = None
        
        # Pattern: "CONFIRM, 24 hours" or "CONFIRM, 7 days"
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
                pending_expires = dateutil_parser.parse(pending_expires)
            
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


# ============================================================
# MAIN HANDLER
# ============================================================

async def handle_monitor_request(whatsapp_number: str, message: str, client_profile: dict) -> str:
    """
    Main handler for monitoring requests
    Routes to appropriate function based on message with intelligent intent inference
    """
    
    message_lower = message.lower().strip()
    
    # ========================================
    # GET USER'S INDUSTRY & AVAILABLE MARKETS
    # ========================================
    
    industry = client_profile.get('industry', 'real_estate')
    available_markets = get_available_markets_from_db(industry)
    
    # Check for manual mode override
    if 'manual only' in message_lower or 'no inference' in message_lower:
        markets_str = ', '.join(available_markets) if available_markets else 'No markets configured'
        
        return f"""MANUAL MODE ACTIVATED

Inference disabled. Provide complete parameters:

Format: "Monitor [agent] [region], alert if [condition], [duration]"

Available markets: {markets_str}

Required:
- Agent (optional): competitor name
- Region: market name
- Condition: "prices drop X%" / "inventory increases X%" / "velocity below X"
- Duration: "X days" (or "indefinite")"""
    
    # Confirmation (with or without duration)
    if 'confirm' in message_lower:
        return await MonitorManager.confirm_monitor(whatsapp_number, message)
    
    # Stop monitoring
    if 'stop monitoring' in message_lower or 'stop all' in message_lower:
        # ✅ FIXED: Extract region dynamically from available markets
        region = next((m for m in available_markets if m.lower() in message_lower), None)
        return await MonitorManager.stop_monitor(whatsapp_number, region)
    
    # Show monitors
    if 'show monitor' in message_lower or 'list monitor' in message_lower or 'my monitor' in message_lower:
        return await show_monitors(whatsapp_number)
    
    # Resume monitoring
    if 'resume monitor' in message_lower:
        # ✅ FIXED: Extract region dynamically from available markets
        region = next((m for m in available_markets if m.lower() in message_lower), None)
        
        if region:
            return await resume_monitor(whatsapp_number, region, industry)
        else:
            markets_str = ', '.join(available_markets[:3]) if available_markets else 'No markets'
            return f"Specify region: 'Resume monitoring {available_markets[0] if available_markets else 'MARKET'}'"
    
    # Extend monitoring
    if 'extend monitor' in message_lower:
        # ✅ FIXED: Extract region dynamically from available markets
        region = next((m for m in available_markets if m.lower() in message_lower), None)
        
        if region:
            return await extend_monitor(whatsapp_number, region)
        else:
            return f"Specify region: 'Extend monitoring {available_markets[0] if available_markets else 'MARKET'}'"
    
    # DETECT IMPLICIT MONITORING (no explicit "monitor" command)
    implicit_keywords = ['keep an eye', 'watch for', 'track', 'let me know if', 'watch']
    is_implicit = any(kw in message_lower for kw in implicit_keywords)
    
    if is_implicit:
        # Extract target from message
        target = extract_monitoring_target(message, industry)
        
        if not target:
            return """SURVEILLANCE REQUEST UNCLEAR

Specify target entity or market.

Standing by."""
        
        # CREATE PENDING MONITOR in database
        pending_config = {
            'whatsapp_number': whatsapp_number,
            'target': target,
            'status': 'pending_confirmation',
            'created_at': datetime.now(timezone.utc),
            'message': message
        }
        
        # Store in MongoDB
        try:
            db['pending_monitors'].update_one(
                {'whatsapp_number': whatsapp_number},
                {'$set': pending_config},
                upsert=True
            )
            
            logger.info(f"✅ Pending monitor created for {whatsapp_number}: {target}")
        except Exception as e:
            logger.error(f"Failed to create pending monitor: {e}")
            return "Error initializing surveillance. Please try again."
        
        return f"""SURVEILLANCE INITIATED

Target: {target}
Status: Awaiting activation parameters

Reply with timeframe to activate:
- "24 hours"
- "7 days"
- "30 days"
- "Until I say stop"

Standing by."""
    
    # Create new monitor with intelligent parsing
    success, config = MonitorManager.parse_monitor_request(message, client_profile)
    
    if not success:
        # Helpful error instead of None
        markets_str = ', '.join(available_markets[:3]) if available_markets else 'No markets configured'
        
        return f"""MONITORING REQUEST UNCLEAR

Provide monitoring directive:

Format: "Monitor [target], alert if [condition]"

Examples:
- "Monitor [competitor] {available_markets[0] if available_markets else 'MARKET'}, alert if prices drop 5%"
- "Track inventory, notify if listings increase 10%"

Available markets: {markets_str}

Standing by."""
    
    return await MonitorManager.create_monitor_pending(whatsapp_number, config, client_profile)


# ============================================================
# MONITOR CHECKING BACKGROUND JOB
# ============================================================

async def check_monitors_and_alert():
    """Check all active monitors and send alerts if thresholds breached"""
    
    clients = db['client_profiles'].find({'active_monitors': {'$exists': True}})
    
    for client in clients:
        monitors = client.get('active_monitors', [])
        
        for monitor in monitors:
            if monitor.get('status') != 'active':
                continue
            
            # Check if it's time to check
            next_check = monitor.get('next_check')
            if not next_check or datetime.now(timezone.utc) < next_check:
                continue
            
            # Load current data
            from app.dataset_loader import load_dataset
            industry = monitor.get('industry', 'real_estate')
            dataset = load_dataset(area=monitor['region'], industry=industry)
            
            current_data = {
                "avg_price": dataset.get('metrics', {}).get('avg_price', 0),
                "inventory_count": len(dataset.get('properties', [])),
                "velocity_score": dataset.get('liquidity_velocity', {}).get('velocity_score', 0)
            }
            
            baseline = monitor['baseline_data']
            
            # Check each trigger
            alert_triggered = False
            alert_details = []
            
            for trigger in monitor['triggers']:
                if trigger['type'] == 'price_drop':
                    pct_change = ((current_data['avg_price'] - baseline['avg_price']) / baseline['avg_price']) * 100
                    
                    if pct_change <= -trigger['threshold']:
                        alert_triggered = True
                        alert_details.append(f"Price dropped {abs(pct_change):.1f}% (threshold: {trigger['threshold']}%)")
            
            if alert_triggered:
                # Send alert
                agent_str = f"{monitor['agent']} " if monitor.get('agent') else ""
                alert_msg = f"""🚨 MONITORING ALERT

{agent_str}{monitor['region']}

{chr(10).join(alert_details)}

Baseline: £{baseline['avg_price']:,.0f}
Current: £{current_data['avg_price']:,.0f}

Monitor paused until you resume."""
                
                from app.whatsapp import send_twilio_message
                await send_twilio_message(client['whatsapp_number'], alert_msg)
                
                # Pause monitor
                db['client_profiles'].update_one(
                    {'whatsapp_number': client['whatsapp_number'], 'active_monitors.id': monitor['id']},
                    {'$set': {
                        'active_monitors.$.status': 'paused',
                        'active_monitors.$.pause_reason': 'alert_sent',
                        'active_monitors.$.last_alert': datetime.now(timezone.utc),
                        'active_monitors.$.alerts_sent': monitor.get('alerts_sent', 0) + 1
                    }}
                )
            
            # Update next check time
            next_check_time = datetime.now(timezone.utc) + timedelta(hours=monitor['check_frequency_hours'])
            
            db['client_profiles'].update_one(
                {'whatsapp_number': client['whatsapp_number'], 'active_monitors.id': monitor['id']},
                {'$set': {
                    'active_monitors.$.next_check': next_check_time,
                    'active_monitors.$.last_checked': datetime.now(timezone.utc),
                    'active_monitors.$.total_checks': monitor.get('total_checks', 0) + 1,
                    'active_monitors.$.current_data': current_data
                }}
            )


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def extract_monitoring_target(message: str, industry: str) -> str:
    """
    Extract monitoring target from implicit request
    
    ✅ FIXED: Queries Airtable for competitors and markets dynamically
    """
    message_lower = message.lower()
    
    # Get available competitors and markets from Airtable
    competitors = get_available_competitors_from_airtable(industry)
    markets = get_available_markets_from_db(industry)
    
    # Check for competitor
    for competitor in competitors:
        if competitor.lower() in message_lower:
            return competitor
    
    # Check for market
    for market in markets:
        if market.lower() in message_lower:
            return market
    
    # Fallback - extract capitalized words (proper nouns)
    words = message.split()
    capitalized = [w for w in words if w and w[0].isupper() and len(w) > 2]
    
    if capitalized:
        return ' '.join(capitalized[:3])  # Max 3 words
    
    return None


def extract_duration(message: str) -> Optional[int]:
    """Extract duration in days from confirmation message"""
    message_lower = message.lower()
    
    # Indefinite
    if 'indefinite' in message_lower or 'until i say' in message_lower:
        return -1
    
    # Hours
    hours_match = re.search(r'(\d+)\s*hour', message_lower)
    if hours_match:
        hours = int(hours_match.group(1))
        return max(1, hours // 24)  # Convert to days, minimum 1
    
    # Days
    days_match = re.search(r'(\d+)\s*day', message_lower)
    if days_match:
        return int(days_match.group(1))
    
    # Weeks
    weeks_match = re.search(r'(\d+)\s*week', message_lower)
    if weeks_match:
        return int(weeks_match.group(1)) * 7
    
    # Default to None (require explicit duration)
    return None


async def show_monitors(whatsapp_number: str) -> str:
    """Show all active monitors"""
    
    client = db['client_profiles'].find_one({'whatsapp_number': whatsapp_number})
    monitors = client.get('active_monitors', [])
    
    active_monitors = [m for m in monitors if m.get('status') in ['active', 'paused']]
    
    if not active_monitors:
        return """No active monitors.

Create one:
"Monitor [agent] [region], alert if [condition]"

Example: "Monitor [competitor] [market], alert if prices drop 5%"""
    
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


async def resume_monitor(whatsapp_number: str, region: str, industry: str) -> str:
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
    dataset = load_dataset(area=region, industry=industry)
    
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

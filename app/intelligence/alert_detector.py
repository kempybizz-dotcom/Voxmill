#!/usr/bin/env python3
"""
VOXMILL ALERT DETECTOR
======================
Detects market events worthy of real-time alerts

Alert Types:
1. Price drops (>5% in 24h)
2. Inventory surges (>5 new listings in 24h)
3. Agent behavior shifts (>30% inventory change)
4. New market opportunities (high deal scores)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


def detect_alerts_for_region(area: str, vertical: str = "luxury_real_estate") -> List[Dict]:
    """
    Detect alert-worthy events in a specific region
    
    Returns: List of alerts with type, urgency, and formatted message
    """
    
    if db is None:
        logger.error("MongoDB not connected")
        return []
    
    alerts = []
    now = datetime.now(timezone.utc)
    
    # Get current snapshot (last 2 hours)
    current_snapshot = db['datasets'].find_one({
        "metadata.vertical.name": vertical,
        "metadata.area": area,
        "metadata.analysis_timestamp": {"$gte": now - timedelta(hours=2)}
    }, sort=[("metadata.analysis_timestamp", -1)])
    
    # Get historical snapshot (24 hours ago)
    historical_snapshot = db['datasets'].find_one({
        "metadata.vertical.name": vertical,
        "metadata.area": area,
        "metadata.analysis_timestamp": {
            "$gte": now - timedelta(hours=26),
            "$lte": now - timedelta(hours=22)
        }
    }, sort=[("metadata.analysis_timestamp", -1)])
    
    if not current_snapshot or not historical_snapshot:
        logger.warning(f"No snapshots found for {area} - cannot detect alerts")
        return []
    
    current_props = current_snapshot.get('properties', [])
    historical_props = historical_snapshot.get('properties', [])
    
    # ALERT TYPE 1: Price Drops
    price_drop_alerts = detect_price_drops(current_props, historical_props, area)
    alerts.extend(price_drop_alerts)
    
    # ALERT TYPE 2: Inventory Surges
    inventory_alerts = detect_inventory_changes(current_props, historical_props, area)
    alerts.extend(inventory_alerts)
    
    # ALERT TYPE 3: Agent Behavior Shifts
    agent_alerts = detect_agent_behavior_shifts(current_props, historical_props, area)
    alerts.extend(agent_alerts)
    
    logger.info(f"Detected {len(alerts)} alerts for {area}")
    return alerts


def detect_price_drops(current: List[Dict], historical: List[Dict], area: str) -> List[Dict]:
    """Detect properties with significant price drops"""
    
    alerts = []
    
    # Build historical lookup by address
    hist_lookup = {p.get('address', ''): p for p in historical if p.get('address')}
    
    for curr_prop in current:
        address = curr_prop.get('address', '')
        if not address:
            continue
        
        hist_prop = hist_lookup.get(address)
        if not hist_prop:
            continue
        
        curr_price = curr_prop.get('price', 0)
        hist_price = hist_prop.get('price', 0)
        
        if curr_price and hist_price and curr_price < hist_price:
            price_change_pct = ((curr_price - hist_price) / hist_price) * 100
            
            # Alert on drops >5%
            if price_change_pct < -5:
                alerts.append({
                    "type": "price_drop",
                    "urgency": "immediate" if price_change_pct < -10 else "near_term",
                    "area": area,
                    "property": {
                        "address": address,
                        "current_price": curr_price,
                        "previous_price": hist_price,
                        "change_pct": price_change_pct,
                        "agent": curr_prop.get('agent', 'Unknown'),
                        "property_type": curr_prop.get('property_type', 'Unknown')
                    },
                    "timestamp": datetime.now(timezone.utc)
                })
    
    return alerts


def detect_inventory_changes(current: List[Dict], historical: List[Dict], area: str) -> List[Dict]:
    """Detect significant inventory changes (new listings)"""
    
    alerts = []
    
    # Get unique IDs/addresses
    current_addresses = {p.get('address', '') for p in current if p.get('address')}
    hist_addresses = {p.get('address', '') for p in historical if p.get('address')}
    
    new_listings = current_addresses - hist_addresses
    
    # Alert on 5+ new listings
    if len(new_listings) >= 5:
        new_props = [p for p in current if p.get('address') in new_listings]
        
        alerts.append({
            "type": "inventory_surge",
            "urgency": "near_term",
            "area": area,
            "count": len(new_listings),
            "properties": sorted(new_props, key=lambda x: x.get('price', 0))[:5],
            "timestamp": datetime.now(timezone.utc)
        })
    
    return alerts


def detect_agent_behavior_shifts(current: List[Dict], historical: List[Dict], area: str) -> List[Dict]:
    """Detect agents with significant inventory changes"""
    
    alerts = []
    
    # Count properties by agent
    def count_by_agent(props):
        counts = {}
        for p in props:
            agent = p.get('agent', 'Unknown')
            if agent != 'Unknown':
                counts[agent] = counts.get(agent, 0) + 1
        return counts
    
    current_counts = count_by_agent(current)
    hist_counts = count_by_agent(historical)
    
    # Detect significant changes
    for agent, curr_count in current_counts.items():
        hist_count = hist_counts.get(agent, 0)
        
        if hist_count == 0:
            continue
        
        change_pct = ((curr_count - hist_count) / hist_count) * 100
        
        # Alert on 30%+ change (either direction)
        if abs(change_pct) > 30:
            alert_type = "agent_depletion" if change_pct < 0 else "agent_surge"
            
            alerts.append({
                "type": alert_type,
                "urgency": "near_term",
                "area": area,
                "agent": agent,
                "current_count": curr_count,
                "previous_count": hist_count,
                "change_pct": change_pct,
                "timestamp": datetime.now(timezone.utc)
            })
    
    return alerts


def format_alert_message(alert: Dict) -> str:
    """Format alert as WhatsApp message"""
    
    area = alert['area'].upper()
    
    if alert['type'] == 'price_drop':
        prop = alert['property']
        return f"""🚨 MARKET ALERT — {area}

{prop['agent']} adjusted pricing below target range

IMMEDIATE ACTION:
- *{prop['address']}:* £{prop['current_price']/1000000:.2f}M ({prop['change_pct']:+.1f}%)
- Previous: £{prop['previous_price']/1000000:.2f}M
- Property Type: {prop['property_type']}

⚡ ACTION WINDOW: 24-48 hours before market absorbs adjustment

Historical Context: Price drop of this magnitude signals provider motivation or competitive repositioning."""

    elif alert['type'] == 'inventory_surge':
        top_props = alert['properties'][:3]
        prop_lines = '\n'.join([
            f"• {p.get('address', 'Unknown')}: £{p.get('price', 0)/1000000:.2f}M ({p.get('agent', 'Unknown')})"
            for p in top_props
        ])
        
        return f"""🚨 MARKET ALERT — {area}

{alert['count']} new listings added in last 24 hours

TOP OPPORTUNITIES:
{prop_lines}

⚡ ACTION WINDOW: 7-14 days before absorption

Strategic Context: Inventory surge signals allocation shift or increased provider motivation. Monitor for follow-on competitive adjustments."""

    elif alert['type'] in ['agent_depletion', 'agent_surge']:
        direction = "depleted" if alert['type'] == 'agent_depletion' else "surged"
        context = "Strong buyer absorption signal" if alert['type'] == 'agent_depletion' else "Increased seller motivation or allocation shift"
        
        return f"""🚨 MARKET ALERT — {area}

{alert['agent']} inventory {direction} {abs(alert['change_pct']):.1f}% in 24 hours

CURRENT COUNT: {alert['current_count']} properties
PREVIOUS COUNT: {alert['previous_count']} properties

⚡ POSITIONING: Monitor for competitive response within 7-14 days

Strategic Context: {context}. Market leader behavior suggests directional positioning."""

    else:
        return f"🚨 MARKET ALERT — {area}\n\nAlert type: {alert['type']}"

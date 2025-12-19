"""
VOXMILL HISTORICAL DATA STORAGE
================================
Daily snapshots for intelligence layer consumption

Stores:
- Property snapshots (for velocity calculations)
- Agent behavioral events (for profiling)
- Market metrics (for trend detection)
"""

import os
import logging
from datetime import datetime, timezone
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def store_daily_snapshot(dataset: dict, area: str):
    """
    Store daily market snapshot for historical analysis
    
    Called every time dataset is loaded (but only stores once per day per area)
    """
    
    if not mongo_client:
        logger.warning("MongoDB not available, skipping historical storage")
        return
    
    try:
        db = mongo_client['Voxmill']
        snapshots = db['historical_snapshots']
        
        # Check if we already have a snapshot for today
        today = datetime.now(timezone.utc).date().isoformat()
        
        existing = snapshots.find_one({
            'area': area,
            'date': today
        })
        
        if existing:
            logger.info(f"✅ Snapshot already exists for {area} on {today}")
            return
        
        # Extract key data for storage
        properties = dataset.get('properties', [])
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
        
        # Store compact snapshot
        snapshot = {
            'area': area,
            'date': today,
            'timestamp': datetime.now(timezone.utc),
            
            # Properties (compressed - only essential fields)
            'properties': [
                {
                    'address': p.get('address'),
                    'price': p.get('price'),
                    'price_per_sqft': p.get('price_per_sqft'),
                    'agent': p.get('agent'),
                    'lat': p.get('lat'),
                    'lng': p.get('lng'),
                    'type': p.get('type'),
                    'bedrooms': p.get('bedrooms')
                }
                for p in properties[:200]  # Cap at 200 to save space
            ],
            
            # Metrics
            'metrics': {
                'property_count': len(properties),
                'avg_price': metrics.get('avg_price'),
                'median_price': metrics.get('median_price'),
                'avg_price_per_sqft': metrics.get('avg_price_per_sqft'),
                'total_value': metrics.get('total_value')
            },
            
            # Intelligence (if available)
            'intelligence': {
                'market_sentiment': intelligence.get('market_sentiment'),
                'sentiment_confidence': intelligence.get('sentiment_confidence'),
                'top_agents': intelligence.get('top_agents', [])[:10]
            },
            
            # Liquidity velocity (if calculated)
            'liquidity_velocity': dataset.get('liquidity_velocity', {}) if not dataset.get('liquidity_velocity', {}).get('error') else None,
            
            # Agent profiles (if available)
            'agent_profiles': dataset.get('agent_profiles', [])[:10] if dataset.get('agent_profiles') else None,
            
            # Detected trends (if available)
            'detected_trends': dataset.get('detected_trends', [])[:5] if dataset.get('detected_trends') else None
        }
        
        # Insert snapshot
        snapshots.insert_one(snapshot)
        
        logger.info(f"✅ Stored historical snapshot for {area} on {today} ({len(properties)} properties)")
        
        # CLEANUP: Delete snapshots older than 90 days to save space
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=90)
        
        result = snapshots.delete_many({
            'timestamp': {'$lt': cutoff_date}
        })
        
        if result.deleted_count > 0:
            logger.info(f"🗑️ Cleaned up {result.deleted_count} old snapshots (>90 days)")
        
    except Exception as e:
        logger.error(f"Error storing historical snapshot: {e}", exc_info=True)


def get_historical_snapshots(area: str, days: int = 30) -> list:
    """
    Retrieve historical snapshots for an area
    
    Returns: List of snapshot dicts (newest first)
    """
    
    if not mongo_client:
        return []
    
    try:
        from datetime import timedelta
        
        db = mongo_client['Voxmill']
        snapshots = db['historical_snapshots']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        results = list(snapshots.find({
            'area': area,
            'timestamp': {'$gte': cutoff_date}
        }).sort('timestamp', -1))  # Newest first
        
        logger.info(f"📊 Retrieved {len(results)} historical snapshots for {area} (last {days} days)")
        
        return results
        
    except Exception as e:
        logger.error(f"Error retrieving historical snapshots: {e}", exc_info=True)
        return []


def get_agent_behavioral_history(agent: str, area: str, days: int = 60) -> list:
    """
    Extract agent behavioral events from historical snapshots
    
    Returns: List of events for agent profiling
    """
    
    snapshots = get_historical_snapshots(area, days)
    
    if not snapshots:
        return []
    
    events = []
    
    # Track this agent's properties across snapshots
    agent_history = {}
    
    for snapshot in reversed(snapshots):  # Oldest first
        snapshot_date = snapshot['date']
        properties = snapshot.get('properties', [])
        
        # Find properties by this agent
        agent_props = [p for p in properties if p.get('agent') == agent]
        
        if agent_props:
            avg_price = sum([p['price'] for p in agent_props if p.get('price')]) / len(agent_props)
            
            agent_history[snapshot_date] = {
                'count': len(agent_props),
                'avg_price': avg_price,
                'properties': agent_props
            }
    
    # Detect behavioral events
    dates = sorted(agent_history.keys())
    
    for i in range(1, len(dates)):
        prev_date = dates[i-1]
        curr_date = dates[i]
        
        prev = agent_history[prev_date]
        curr = agent_history[curr_date]
        
        # Price change event
        if prev['avg_price'] > 0:
            price_change_pct = ((curr['avg_price'] - prev['avg_price']) / prev['avg_price']) * 100
            
            if abs(price_change_pct) > 2:  # >2% change
                events.append({
                    'type': 'price_change',
                    'date': curr_date,
                    'magnitude': price_change_pct,
                    'agent_avg': curr['avg_price'],
                    'previous_avg': prev['avg_price'],
                    'days_to_respond': (datetime.fromisoformat(curr_date) - datetime.fromisoformat(prev_date)).days
                })
        
        # Inventory change event
        inventory_change = curr['count'] - prev['count']
        
        if abs(inventory_change) >= 2:  # +/- 2 properties
            events.append({
                'type': 'inventory_change',
                'date': curr_date,
                'change': inventory_change,
                'new_count': curr['count'],
                'previous_count': prev['count']
            })
    
    return events


def check_historical_data_availability(area: str) -> dict:
    """
    Check if enough historical data exists for intelligence layers
    
    Returns: Status dict with recommendations
    """
    
    snapshots = get_historical_snapshots(area, days=90)
    
    status = {
        'area': area,
        'total_snapshots': len(snapshots),
        'date_range': {
            'oldest': snapshots[-1]['date'] if snapshots else None,
            'newest': snapshots[0]['date'] if snapshots else None
        },
        'capabilities': {}
    }
    
    # Check what's possible
    if len(snapshots) >= 2:
        status['capabilities']['liquidity_velocity'] = 'available'
    else:
        status['capabilities']['liquidity_velocity'] = 'unavailable (need 2+ days)'
    
    if len(snapshots) >= 10:
        status['capabilities']['liquidity_windows'] = 'available'
    else:
        status['capabilities']['liquidity_windows'] = f'unavailable (need 10+ days, have {len(snapshots)})'
    
    if len(snapshots) >= 30:
        status['capabilities']['agent_profiling'] = 'available'
        status['capabilities']['cascade_prediction'] = 'available'
    else:
        status['capabilities']['agent_profiling'] = f'limited (optimal: 30+ days, have {len(snapshots)})'
        status['capabilities']['cascade_prediction'] = f'limited (optimal: 30+ days, have {len(snapshots)})'
    
    return status

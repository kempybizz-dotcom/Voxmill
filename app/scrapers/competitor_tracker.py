import os
import logging
from datetime import datetime, timezone
from pymongo import MongoClient
import asyncio
from datetime import timedelta
from app.dataset_loader import load_dataset

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

async def track_competitor_prices(area: str = "Mayfair"):
    """
    Daily scraper: Track all competitor pricing changes
    Stores in MongoDB for historical comparison
    """
    try:
        if not mongo_client:
            logger.error("MongoDB not connected")
            return
        
        db = mongo_client['Voxmill']
        collection = db['price_history']
        
        # Load current market snapshot
        dataset = load_dataset(area=area)
        properties = dataset.get('properties', [])
        
        timestamp = datetime.now(timezone.utc)
        
        # Extract competitor snapshots
        agent_snapshots = {}
        
        for prop in properties:
            agent = prop.get('agent', 'Unknown')
            price = prop.get('price', 0)
            address = prop.get('address', '')
            
            if agent == 'Private' or not price:
                continue
            
            if agent not in agent_snapshots:
                agent_snapshots[agent] = {
                    'agent': agent,
                    'listings': [],
                    'avg_price': 0,
                    'total_inventory': 0,
                    'timestamp': timestamp,
                    'area': area
                }
            
            agent_snapshots[agent]['listings'].append({
                'address': address,
                'price': price,
                'price_per_sqft': prop.get('price_per_sqft', 0),
                'property_type': prop.get('type', 'Unknown'),
                'bedrooms': prop.get('bedrooms', 0)
            })
        
        # Calculate agent metrics
        for agent, snapshot in agent_snapshots.items():
            prices = [l['price'] for l in snapshot['listings']]
            snapshot['avg_price'] = sum(prices) / len(prices) if prices else 0
            snapshot['total_inventory'] = len(snapshot['listings'])
            snapshot['min_price'] = min(prices) if prices else 0
            snapshot['max_price'] = max(prices) if prices else 0
        
        # Store snapshots
        for snapshot in agent_snapshots.values():
            collection.insert_one(snapshot)
        
        logger.info(f"Competitor tracking complete: {len(agent_snapshots)} agents tracked in {area}")
        
        # Detect changes (compare to yesterday)
        alerts = await detect_price_changes(area, agent_snapshots)
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error in competitor tracking: {str(e)}", exc_info=True)
        return []


async def detect_price_changes(area: str, current_snapshots: dict) -> list:
    """
    Compare today's data to yesterday's
    Return list of significant changes (>5% move or inventory shift >20%)
    """
    try:
        db = mongo_client['Voxmill']
        collection = db['price_history']
        
        alerts = []
        
        # Get yesterday's data for comparison
        yesterday_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_end = yesterday_start.replace(hour=23, minute=59, second=59)
        
        for agent, current in current_snapshots.items():
            # Find yesterday's snapshot for this agent
            yesterday = collection.find_one({
                'agent': agent,
                'area': area,
                'timestamp': {
                    '$gte': yesterday_start - timedelta(days=1),
                    '$lte': yesterday_end - timedelta(days=1)
                }
            })
            
            if not yesterday:
                continue  # No historical data yet
            
            # Calculate changes
            price_change_pct = ((current['avg_price'] - yesterday['avg_price']) / yesterday['avg_price']) * 100
            inventory_change_pct = ((current['total_inventory'] - yesterday['total_inventory']) / yesterday['total_inventory']) * 100
            
            # Detect significant moves
            if abs(price_change_pct) >= 5:
                alerts.append({
                    'type': 'price_change',
                    'agent': agent,
                    'area': area,
                    'change_pct': round(price_change_pct, 1),
                    'old_price': yesterday['avg_price'],
                    'new_price': current['avg_price'],
                    'severity': 'high' if abs(price_change_pct) >= 10 else 'medium',
                    'timestamp': current['timestamp']
                })
            
            if abs(inventory_change_pct) >= 20:
                alerts.append({
                    'type': 'inventory_change',
                    'agent': agent,
                    'area': area,
                    'change_pct': round(inventory_change_pct, 1),
                    'old_inventory': yesterday['total_inventory'],
                    'new_inventory': current['total_inventory'],
                    'severity': 'medium',
                    'timestamp': current['timestamp']
                })
        
        # Detect cascades (multiple agents moving same direction within 24h)
        price_drops = [a for a in alerts if a['type'] == 'price_change' and a['change_pct'] < 0]
        if len(price_drops) >= 2:
            alerts.append({
                'type': 'cascade_detected',
                'area': area,
                'agents_involved': [a['agent'] for a in price_drops],
                'avg_drop': sum([a['change_pct'] for a in price_drops]) / len(price_drops),
                'severity': 'critical',
                'timestamp': datetime.now(timezone.utc)
            })
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error detecting price changes: {str(e)}", exc_info=True)
        return []

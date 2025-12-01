import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

def detect_market_trends(area: str = "Mayfair", lookback_days: int = 14) -> list:
    """
    Analyze historical data to detect meaningful trends
    Returns list of trend insights
    """
    try:
        if not mongo_client:
            return []
        
        db = mongo_client['Voxmill']
        price_history = db['price_history']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Get all snapshots for this area in lookback period
        snapshots = list(price_history.find({
            'area': area,
            'timestamp': {'$gte': cutoff_date}
        }).sort('timestamp', 1))
        
        if len(snapshots) < 2:
            return []
        
        trends = []
        
        # Group by agent
        agents = {}
        for snap in snapshots:
            agent = snap['agent']
            if agent not in agents:
                agents[agent] = []
            agents[agent].append(snap)
        
        # Detect trends per agent
        for agent, history in agents.items():
            if len(history) < 2:
                continue
            
            # Price trend detection
            prices = [h['avg_price'] for h in history]
            price_changes = []
            for i in range(1, len(prices)):
                change_pct = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
                if abs(change_pct) >= 3:  # Significant move threshold
                    price_changes.append(change_pct)
            
            # Consistent direction = trend
            if len(price_changes) >= 3:
                avg_change = sum(price_changes) / len(price_changes)
                if all(c < 0 for c in price_changes):
                    trends.append({
                        'type': 'price_trend_down',
                        'agent': agent,
                        'pattern': f"{len(price_changes)} consecutive drops",
                        'avg_change': round(avg_change, 1),
                        'severity': 'high' if abs(avg_change) > 5 else 'medium',
                        'insight': f"{agent} showing sustained downward pressure: {len(price_changes)} drops averaging {abs(avg_change):.1f}% each"
                    })
                elif all(c > 0 for c in price_changes):
                    trends.append({
                        'type': 'price_trend_up',
                        'agent': agent,
                        'pattern': f"{len(price_changes)} consecutive increases",
                        'avg_change': round(avg_change, 1),
                        'severity': 'medium',
                        'insight': f"{agent} showing upward momentum: {len(price_changes)} increases averaging {avg_change:.1f}% each"
                    })
            
            # Inventory velocity
            inventories = [h['total_inventory'] for h in history]
            inventory_change = ((inventories[-1] - inventories[0]) / inventories[0]) * 100
            
            if abs(inventory_change) >= 30:  # 30% change in inventory
                if inventory_change > 0:
                    trends.append({
                        'type': 'inventory_surge',
                        'agent': agent,
                        'change_pct': round(inventory_change, 1),
                        'severity': 'high' if inventory_change > 50 else 'medium',
                        'insight': f"{agent} inventory surge: +{inventory_change:.1f}% in {lookback_days} days—possible distressed assets or market expansion"
                    })
                else:
                    trends.append({
                        'type': 'inventory_drain',
                        'agent': agent,
                        'change_pct': round(inventory_change, 1),
                        'severity': 'medium',
                        'insight': f"{agent} inventory depletion: {inventory_change:.1f}% in {lookback_days} days—strong absorption or supply constraint"
                    })
        
        logger.info(f"Detected {len(trends)} trends for {area} over {lookback_days} days")
        return trends
        
    except Exception as e:
        logger.error(f"Error detecting trends: {str(e)}", exc_info=True)
        return []

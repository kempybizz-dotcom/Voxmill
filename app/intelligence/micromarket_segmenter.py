import os
import logging
import numpy as np
from datetime import datetime, timezone
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def segment_micromarkets(properties: list, area: str) -> dict:
    """
    Segment properties into micro-markets using geographic clustering + price similarity
    
    Args:
        properties: List of property dicts with coordinates and prices
        area: Market name (e.g., "Mayfair")
    
    Returns: Dict with micromarket analysis
    """
    try:
        if not properties or len(properties) < 5:
            return {'error': 'insufficient_data', 'message': 'Need at least 5 properties for segmentation'}
        
        # Filter properties with valid coordinates and prices
       # Filter properties with valid coordinates and prices
        valid_props = []
        for p in properties:
            lat = p.get('lat')
            lng = p.get('lng')
            price = p.get('price', 0)
            
            # Validate coordinates are in valid UK bounds
            if lat and lng and price > 0:
                if 49.0 <= lat <= 61.0 and -8.0 <= lng <= 2.0:
                    valid_props.append(p)
                else:
                    logger.debug(f"Property with invalid UK coordinates: lat={lat}, lng={lng}")
        
        # Try sklearn clustering first (best results)
        try:
            from sklearn.cluster import DBSCAN
            return segment_with_sklearn(valid_props, area)
        except ImportError:
            logger.warning("sklearn not available, using fallback address-based segmentation")
            return segment_by_address(properties, area)
        
    except Exception as e:
        logger.error(f"Error segmenting micromarkets: {str(e)}", exc_info=True)
        return {'error': 'segmentation_failed', 'message': str(e)}


def segment_with_sklearn(properties: list, area: str) -> dict:
    """
    Use DBSCAN clustering on geographic coordinates + price similarity
    """
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler
    
    # Extract features
    coords = np.array([[p['lat'], p['lng']] for p in properties])
    prices = np.array([p['price'] for p in properties])
    
    # Normalize prices to 0-1 scale
    price_normalized = (prices - prices.min()) / (prices.max() - prices.min() + 1)
    
    # Combine geography (70% weight) + price similarity (30% weight)
    features = np.column_stack([
        coords * 0.7,
        price_normalized.reshape(-1, 1) * 0.3
    ])
    
    # Cluster (eps controls sensitivity, min_samples = minimum cluster size)
    clustering = DBSCAN(eps=0.015, min_samples=3).fit(features)
    
    # Group properties by cluster
    clusters = {}
    for idx, label in enumerate(clustering.labels_):
        if label == -1:  # Outlier
            continue
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(properties[idx])
    
    if not clusters:
        logger.warning(f"No clusters formed for {area}, using fallback")
        return segment_by_address(properties, area)
    
    # Analyze each micro-market
    micromarkets = []
    for cluster_id, cluster_props in clusters.items():
        micro = analyze_micromarket_cluster(cluster_id, cluster_props, area)
        micromarkets.append(micro)
    
    # Calculate macro market average for divergence analysis
    macro_avg = sum([m['avg_price'] for m in micromarkets]) / len(micromarkets)
    
    # Detect divergences
    for micro in micromarkets:
        deviation_pct = ((micro['avg_price'] - macro_avg) / macro_avg) * 100
        micro['macro_deviation_pct'] = round(deviation_pct, 1)
        
        if deviation_pct > 15:
            micro['classification'] = 'premium_zone'
            micro['opportunity'] = 'potential_overheating'
        elif deviation_pct < -15:
            micro['classification'] = 'value_zone'
            micro['opportunity'] = 'undervalued_entry_point'
        else:
            micro['classification'] = 'balanced'
            micro['opportunity'] = 'fair_value'
    
    # Sort by average price (highest first)
    micromarkets = sorted(micromarkets, key=lambda x: x['avg_price'], reverse=True)
    
    return {
        'area': area,
        'segmentation_method': 'geographic_clustering',
        'total_micromarkets': len(micromarkets),
        'macro_avg_price': round(macro_avg),
        'micromarkets': micromarkets,
        'analysis_timestamp': datetime.now(timezone.utc).isoformat()
    }


def segment_by_address(properties: list, area: str) -> dict:
    """
    Fallback: Segment by extracting street names from addresses
    """
    street_clusters = {}
    
    for prop in properties:
        address = prop.get('address', '')
        if not address or not prop.get('price'):
            continue
        
        # Extract street name (typically first part before comma)
        street = address.split(',')[0].strip() if ',' in address else address
        
        # Clean common prefixes
        for prefix in ['The ', 'Upper ', 'Lower ', 'North ', 'South ', 'East ', 'West ']:
            if street.startswith(prefix):
                street = street[len(prefix):]
        
        if street not in street_clusters:
            street_clusters[street] = []
        street_clusters[street].append(prop)
    
    # Filter clusters with at least 2 properties
    valid_clusters = {k: v for k, v in street_clusters.items() if len(v) >= 2}
    
    if not valid_clusters:
        return {'error': 'insufficient_clusters', 'message': 'Could not form meaningful micro-markets'}
    
    # Analyze each street cluster
    micromarkets = []
    for street_name, cluster_props in valid_clusters.items():
        micro = analyze_micromarket_cluster(street_name, cluster_props, area)
        micromarkets.append(micro)
    
    # Calculate macro average
    macro_avg = sum([m['avg_price'] for m in micromarkets]) / len(micromarkets)
    
    # Add divergence analysis
    for micro in micromarkets:
        deviation_pct = ((micro['avg_price'] - macro_avg) / macro_avg) * 100
        micro['macro_deviation_pct'] = round(deviation_pct, 1)
        
        if deviation_pct > 15:
            micro['classification'] = 'premium_zone'
            micro['opportunity'] = 'potential_overheating'
        elif deviation_pct < -15:
            micro['classification'] = 'value_zone'
            micro['opportunity'] = 'undervalued_entry_point'
        else:
            micro['classification'] = 'balanced'
            micro['opportunity'] = 'fair_value'
    
    micromarkets = sorted(micromarkets, key=lambda x: x['avg_price'], reverse=True)
    
    return {
        'area': area,
        'segmentation_method': 'address_clustering',
        'total_micromarkets': len(micromarkets),
        'macro_avg_price': round(macro_avg),
        'micromarkets': micromarkets,
        'analysis_timestamp': datetime.now(timezone.utc).isoformat()
    }


def analyze_micromarket_cluster(cluster_id, cluster_props: list, area: str) -> dict:
    """
    Analyze a single micro-market cluster
    """
    prices = [p['price'] for p in cluster_props if p.get('price', 0) > 0]
    
    if not prices:
        return None
    
    avg_price = sum(prices) / len(prices)
    median_price = sorted(prices)[len(prices) // 2]
    
    # Determine dominant street/area name
    if isinstance(cluster_id, int):
        # Geographic cluster - extract most common street
        addresses = [p.get('address', '') for p in cluster_props]
        street_names = [addr.split(',')[0].strip() for addr in addresses if ',' in addr]
        if street_names:
            name = max(set(street_names), key=street_names.count)
        else:
            name = f"Zone {cluster_id + 1}"
    else:
        # Address cluster - use street name directly
        name = cluster_id
    
    # Property type distribution
    types = [p.get('type', 'Unknown') for p in cluster_props]
    type_counts = {}
    for t in types:
        type_counts[t] = type_counts.get(t, 0) + 1
    dominant_type = max(type_counts, key=type_counts.get) if type_counts else 'Unknown'
    
    # Agent distribution
    agents = [p.get('agent', 'Unknown') for p in cluster_props if p.get('agent') != 'Private']
    agent_counts = {}
    for a in agents:
        agent_counts[a] = agent_counts.get(a, 0) + 1
    top_agents = sorted(agent_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    # Price per sqft if available
    sqft_prices = [p.get('price_per_sqft', 0) for p in cluster_props if p.get('price_per_sqft', 0) > 0]
    avg_sqft_price = round(sum(sqft_prices) / len(sqft_prices)) if sqft_prices else None
    
    return {
        'name': name,
        'property_count': len(cluster_props),
        'avg_price': round(avg_price),
        'median_price': round(median_price),
        'price_range': {
            'min': min(prices),
            'max': max(prices),
            'spread_pct': round(((max(prices) - min(prices)) / min(prices)) * 100, 1)
        },
        'avg_price_per_sqft': avg_sqft_price,
        'dominant_type': dominant_type,
        'type_distribution': type_counts,
        'top_agents': [{'agent': agent, 'count': count} for agent, count in top_agents],
        'price_volatility': 'high' if max(prices) > min(prices) * 2 else 'low'
    }


def detect_micromarket_divergence(micromarkets: list, macro_avg: float) -> list:
    """
    Identify micro-markets moving differently from macro trend
    Returns list of divergence insights
    """
    divergences = []
    
    for micro in micromarkets:
        deviation_pct = ((micro['avg_price'] - macro_avg) / macro_avg) * 100
        
        if abs(deviation_pct) > 15:
            divergences.append({
                'micromarket': micro['name'],
                'deviation_pct': round(deviation_pct, 1),
                'direction': 'outperforming' if deviation_pct > 0 else 'underperforming',
                'avg_price': micro['avg_price'],
                'macro_avg': macro_avg,
                'opportunity': 'value_zone' if deviation_pct < -15 else 'overheated',
                'confidence': 'high' if micro['property_count'] > 5 else 'medium',
                'insight': f"{micro['name']} trading {abs(deviation_pct):.1f}% {'above' if deviation_pct > 0 else 'below'} market—{'premium positioning' if deviation_pct > 0 else 'potential value play'}"
            })
    
    return sorted(divergences, key=lambda x: abs(x['deviation_pct']), reverse=True)


def get_micromarket_trends(area: str, lookback_days: int = 30) -> list:
    """
    Analyze how micro-markets have evolved over time
    Requires historical micromarket snapshots in MongoDB
    """
    try:
        if not mongo_client:
            return []
        
        db = mongo_client['Voxmill']
        micromarket_history = db['micromarket_history']
        
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Get historical snapshots
        snapshots = list(micromarket_history.find({
            'area': area,
            'timestamp': {'$gte': cutoff_date}
        }).sort('timestamp', 1))
        
        if len(snapshots) < 2:
            return []
        
        # Compare first vs last snapshot
        first_snapshot = snapshots[0]['micromarkets']
        last_snapshot = snapshots[-1]['micromarkets']
        
        trends = []
        for first_micro in first_snapshot:
            name = first_micro['name']
            # Find matching micro in last snapshot
            last_micro = next((m for m in last_snapshot if m['name'] == name), None)
            
            if last_micro:
                price_change_pct = ((last_micro['avg_price'] - first_micro['avg_price']) / first_micro['avg_price']) * 100
                
                if abs(price_change_pct) > 5:
                    trends.append({
                        'micromarket': name,
                        'price_change_pct': round(price_change_pct, 1),
                        'direction': 'appreciating' if price_change_pct > 0 else 'depreciating',
                        'timeframe_days': lookback_days,
                        'insight': f"{name} {'+' if price_change_pct > 0 else ''}{price_change_pct:.1f}% over {lookback_days} days"
                    })
        
        return sorted(trends, key=lambda x: abs(x['price_change_pct']), reverse=True)
        
    except Exception as e:
        logger.error(f"Error getting micromarket trends: {str(e)}", exc_info=True)
        return []

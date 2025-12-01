import os
import logging
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI") 
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def load_dataset(area: str = "Mayfair", vertical: str = "real_estate") -> dict:
    """
    Load market dataset from MongoDB
    
    Args:
        area: Geographic area (e.g., "Mayfair", "Knightsbridge")
        vertical: Market vertical (default: "real_estate")
    
    Returns: Dataset dict with properties, metrics, intelligence
    """
    try:
        if not mongo_client:
            logger.error("MongoDB client not initialized")
            return _get_fallback_dataset(area)
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        # Find most recent dataset for this area
        dataset = collection.find_one(
            {
                'metadata.area': area,
                'metadata.vertical.name': vertical
            },
            sort=[('metadata.analysis_timestamp', -1)]
        )
        
        if not dataset:
            logger.warning(f"No dataset found for {area}, using fallback")
            return _get_fallback_dataset(area)
        
        # Remove MongoDB _id field
        if '_id' in dataset:
            del dataset['_id']
        
        logger.info(f"Dataset loaded for {area}: {len(dataset.get('properties', []))} properties")
        return dataset
        
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}", exc_info=True)
        return _get_fallback_dataset(area)


def load_historical_snapshots(area: str = "Mayfair", days: int = 30) -> list:
    """
    Load historical property snapshots for velocity calculation
    
    Args:
        area: Market area
        days: Lookback period in days
    
    Returns: List of property snapshot lists (most recent first)
    """
    try:
        if not mongo_client:
            logger.warning("MongoDB not connected")
            return []
        
        db = mongo_client['Voxmill']
        datasets = db['datasets']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get all datasets for this area in the lookback period
        # Sort by timestamp descending (most recent first)
        historical = list(datasets.find({
            'metadata.area': area,
            'metadata.analysis_timestamp': {'$exists': True}
        }).sort('metadata.analysis_timestamp', -1).limit(30))
        
        # Extract just the properties arrays
        snapshots = []
        for dataset in historical:
            properties = dataset.get('properties', [])
            if properties:
                snapshots.append(properties)
        
        logger.info(f"Loaded {len(snapshots)} historical snapshots for {area}")
        return snapshots
        
    except Exception as e:
        logger.error(f"Error loading historical snapshots: {str(e)}", exc_info=True)
        return []


def load_multiple_datasets(areas: list, vertical: str = "real_estate") -> dict:
    """
    Load datasets for multiple areas for comparative analysis
    
    Args:
        areas: List of area names
        vertical: Market vertical
    
    Returns: Dict mapping area -> dataset
    """
    try:
        datasets = {}
        
        for area in areas:
            dataset = load_dataset(area=area, vertical=vertical)
            if dataset and not dataset.get('error'):
                datasets[area] = dataset
        
        logger.info(f"Loaded datasets for {len(datasets)}/{len(areas)} areas")
        return datasets
        
    except Exception as e:
        logger.error(f"Error loading multiple datasets: {str(e)}", exc_info=True)
        return {}


def get_available_areas(vertical: str = "real_estate") -> list:
    """
    Get list of all areas with available datasets
    
    Args:
        vertical: Market vertical
    
    Returns: List of area names
    """
    try:
        if not mongo_client:
            return ["Mayfair"]  # Default fallback
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        # Get distinct areas
        areas = collection.distinct('metadata.area', {
            'metadata.vertical.name': vertical
        })
        
        logger.info(f"Found {len(areas)} areas with datasets")
        return sorted(areas) if areas else ["Mayfair"]
        
    except Exception as e:
        logger.error(f"Error getting available areas: {str(e)}", exc_info=True)
        return ["Mayfair"]


def get_dataset_metadata(area: str = "Mayfair") -> dict:
    """
    Get metadata about a dataset without loading full properties
    
    Args:
        area: Market area
    
    Returns: Metadata dict
    """
    try:
        if not mongo_client:
            return {}
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        dataset = collection.find_one(
            {'metadata.area': area},
            {'metadata': 1, 'metrics': 1, 'kpis': 1, 'intelligence': 1},
            sort=[('metadata.analysis_timestamp', -1)]
        )
        
        if not dataset:
            return {}
        
        return {
            'metadata': dataset.get('metadata', {}),
            'metrics': dataset.get('metrics', dataset.get('kpis', {})),
            'intelligence': dataset.get('intelligence', {}),
            'property_count': dataset.get('metadata', {}).get('property_count', 0)
        }
        
    except Exception as e:
        logger.error(f"Error getting dataset metadata: {str(e)}", exc_info=True)
        return {}


def store_dataset(dataset: dict) -> bool:
    """
    Store a dataset in MongoDB
    
    Args:
        dataset: Complete dataset dict
    
    Returns: True if successful
    """
    try:
        if not mongo_client:
            logger.error("MongoDB not connected, cannot store dataset")
            return False
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        # Ensure timestamp exists
        if 'metadata' not in dataset:
            dataset['metadata'] = {}
        
        if 'analysis_timestamp' not in dataset['metadata']:
            dataset['metadata']['analysis_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        # Insert dataset
        collection.insert_one(dataset)
        
        area = dataset.get('metadata', {}).get('area', 'Unknown')
        logger.info(f"Stored dataset for {area}")
        return True
        
    except Exception as e:
        logger.error(f"Error storing dataset: {str(e)}", exc_info=True)
        return False


def _get_fallback_dataset(area: str) -> dict:
    """
    Return fallback dataset when MongoDB is unavailable
    Used for testing/demo purposes
    """
    return {
        'metadata': {
            'area': area,
            'city': 'London',
            'country': 'UK',
            'vertical': {
                'name': 'real_estate',
                'type': 'luxury_residential'
            },
            'property_count': 0,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat(),
            'data_source': 'fallback'
        },
        'properties': [],
        'metrics': {
            'avg_price': 0,
            'median_price': 0,
            'min_price': 0,
            'max_price': 0,
            'total_inventory': 0
        },
        'kpis': {
            'avg_price': 0,
            'median_price': 0,
            'property_count': 0
        },
        'intelligence': {
            'market_sentiment': 'Unknown',
            'confidence_level': 'low',
            'executive_summary': f'No data available for {area}. Please check data pipeline.',
            'strategic_insights': [],
            'risk_assessment': 'Data unavailable'
        },
        'error': 'no_data_available'
    }


def cleanup_old_datasets(days: int = 90, vertical: str = "real_estate") -> int:
    """
    Remove datasets older than specified days
    
    Args:
        days: Age threshold in days
        vertical: Market vertical
    
    Returns: Number of datasets removed
    """
    try:
        if not mongo_client:
            return 0
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        result = collection.delete_many({
            'metadata.vertical.name': vertical,
            'metadata.analysis_timestamp': {'$lt': cutoff_date.isoformat()}
        })
        
        deleted_count = result.deleted_count
        logger.info(f"Cleaned up {deleted_count} old datasets (>{days} days)")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up datasets: {str(e)}", exc_info=True)
        return 0


def get_dataset_history(area: str, days: int = 30) -> list:
    """
    Get historical dataset metadata for trend analysis
    
    Args:
        area: Market area
        days: Lookback period
    
    Returns: List of metadata dicts sorted by timestamp
    """
    try:
        if not mongo_client:
            return []
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        history = list(collection.find(
            {
                'metadata.area': area,
                'metadata.analysis_timestamp': {'$gte': cutoff_date.isoformat()}
            },
            {
                'metadata': 1,
                'metrics': 1,
                'kpis': 1,
                'intelligence.market_sentiment': 1
            }
        ).sort('metadata.analysis_timestamp', 1))
        
        # Remove _id fields
        for item in history:
            if '_id' in item:
                del item['_id']
        
        logger.info(f"Retrieved {len(history)} historical datasets for {area}")
        return history
        
    except Exception as e:
        logger.error(f"Error getting dataset history: {str(e)}", exc_info=True)
        return []


def get_latest_dataset_timestamp(area: str) -> datetime:
    """
    Get timestamp of most recent dataset for an area
    
    Args:
        area: Market area
    
    Returns: Datetime of latest dataset or None
    """
    try:
        if not mongo_client:
            return None
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        dataset = collection.find_one(
            {'metadata.area': area},
            {'metadata.analysis_timestamp': 1},
            sort=[('metadata.analysis_timestamp', -1)]
        )
        
        if dataset and 'metadata' in dataset:
            timestamp_str = dataset['metadata'].get('analysis_timestamp')
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting latest timestamp: {str(e)}", exc_info=True)
        return None


def check_dataset_freshness(area: str, max_age_hours: int = 24) -> dict:
    """
    Check if dataset is fresh or needs update
    
    Args:
        area: Market area
        max_age_hours: Maximum acceptable age in hours
    
    Returns: Dict with freshness info
    """
    try:
        latest_timestamp = get_latest_dataset_timestamp(area)
        
        if not latest_timestamp:
            return {
                'fresh': False,
                'age_hours': None,
                'status': 'no_data',
                'message': f'No dataset found for {area}'
            }
        
        age = datetime.now(timezone.utc) - latest_timestamp
        age_hours = age.total_seconds() / 3600
        
        is_fresh = age_hours <= max_age_hours
        
        return {
            'fresh': is_fresh,
            'age_hours': round(age_hours, 1),
            'last_update': latest_timestamp.isoformat(),
            'status': 'fresh' if is_fresh else 'stale',
            'message': f'Dataset is {age_hours:.1f} hours old'
        }
        
    except Exception as e:
        logger.error(f"Error checking dataset freshness: {str(e)}", exc_info=True)
        return {
            'fresh': False,
            'age_hours': None,
            'status': 'error',
            'message': str(e)
        }

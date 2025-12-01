import os
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

def load_dataset(area: str = "Mayfair", vertical: str = "uk-real-estate"):
    """
    Load the latest Voxmill dataset from MongoDB for specified area.
    Falls back to Mayfair if area not found.
    """
    try:
        if not mongo_client:
            logger.error("MongoDB client not initialized - MONGODB_URI missing")
            raise Exception("Database connection not configured")
        
        db = mongo_client['Voxmill']
        collection = db['datasets']
        
        # Try to get dataset for requested area
        dataset_doc = collection.find_one(
            {"area": area, "vertical": vertical},
            sort=[("timestamp", -1)]
        )
        
        # Fallback to Mayfair if area not found
        if not dataset_doc and area != "Mayfair":
            logger.warning(f"No dataset found for {area}, falling back to Mayfair")
            dataset_doc = collection.find_one(
                {"area": "Mayfair", "vertical": vertical},
                sort=[("timestamp", -1)]
            )
        
        if not dataset_doc:
            logger.error(f"No dataset found for {area} or Mayfair in MongoDB")
            raise Exception(f"No dataset available")
        
        dataset = dataset_doc.get('data', {})
        
        total_props = len(dataset.get('properties', []))
        
        logger.info(f"Dataset loaded from MongoDB: {total_props} properties for {area}")
        return dataset
        
    except Exception as e:
        logger.error(f"Error loading dataset from MongoDB: {str(e)}")
        raise

    def load_historical_snapshots(area: str = "Mayfair", days: int = 30) -> list:
    """
    Load historical property snapshots for velocity calculation
    
    Args:
        area: Market area
        days: Lookback period in days
    
    Returns: List of property snapshot lists
    """
    try:
        if not mongo_client:
            logger.warning("MongoDB not connected")
            return []
        
        from datetime import timedelta, timezone
        
        db = mongo_client['Voxmill']
        datasets = db['datasets']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get all datasets for this area in the lookback period
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

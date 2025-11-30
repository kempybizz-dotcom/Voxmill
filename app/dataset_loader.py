import os
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

def load_dataset(area: str = "Mayfair", vertical: str = "uk-real-estate"):
    """
    Load the latest Voxmill dataset from MongoDB.
    Falls back to demo data if MongoDB unavailable.
    """
    try:
        if not mongo_client:
            logger.error("MongoDB client not initialized - MONGODB_URI missing")
            raise Exception("Database connection not configured")
        
        db = mongo_client['Voxmill']  # Capital V
        collection = db['datasets']
        
        # Get latest dataset for this area
        dataset_doc = collection.find_one(
            {"area": area, "vertical": vertical},
            sort=[("timestamp", -1)]
        )
        
        if not dataset_doc:
            logger.error(f"No dataset found for {area} in MongoDB")
            raise Exception(f"No dataset available for {area}")
        
        dataset = dataset_doc['data']
        
        # Get property count from the actual data structure
        total_props = 0
        if 'metadata' in dataset and 'property_count' in dataset['metadata']:
            total_props = dataset['metadata']['property_count']
        elif 'kpis' in dataset and 'total_properties' in dataset['kpis']:
            total_props = dataset['kpis']['total_properties']
        elif 'properties' in dataset:
            total_props = len(dataset['properties'])
        
        logger.info(f"Dataset loaded from MongoDB: {total_props} properties for {area}")
        return dataset
        
    except Exception as e:
        logger.error(f"Error loading dataset from MongoDB: {str(e)}")
        raise

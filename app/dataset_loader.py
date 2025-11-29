import json
import logging
import os

logger = logging.getLogger(__name__)

def load_dataset() -> dict:
    """
    Load latest dataset from JSON file.
    Reads from /tmp/voxmill_analysis.json (shared with cron job).
    Always loads fresh data — no caching.
    """
    try:
        
        dataset_path = "/tmp/voxmill_analysis.json"
        
        if not os.path.exists(dataset_path):
            logger.error(f"Dataset file not found: {dataset_path}")
            raise Exception("Dataset file missing - cron job may not have run yet")
        
        with open(dataset_path, "r") as f:
            dataset = json.load(f)
        
        property_count = len(dataset.get('properties', []))
        logger.info(f"Dataset loaded from {dataset_path}: {property_count} properties")
        return dataset
        
    except FileNotFoundError:
        logger.error("voxmill_analysis.json not found in /tmp")
        raise Exception("Dataset file missing")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in dataset: {str(e)}")
        raise Exception("Dataset format error")
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}", exc_info=True)
        raise

import json
import logging
import os

logger = logging.getLogger(__name__)

def load_dataset() -> dict:
    """
    Load latest dataset from JSON file.
    Always loads fresh data — no caching.
    """
    try:
        dataset_path = os.path.join(os.path.dirname(__file__), "..", "latest_dataset.json")
        
        with open(dataset_path, "r") as f:
            dataset = json.load(f)
        
        property_count = len(dataset.get('properties', []))
        logger.info(f"Dataset loaded: {property_count} properties")
        return dataset
        
    except FileNotFoundError:
        logger.error("latest_dataset.json not found")
        raise Exception("Dataset file missing")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in dataset: {str(e)}")
        raise Exception("Dataset format error")
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}", exc_info=True)
        raise

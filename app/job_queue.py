"""
Job Queue Management
Utilities for pushing jobs to Redis queue
"""

import os
import json
import redis
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL')
redis_client = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None


def queue_pdf_job(area: str, city: str, client_email: str, client_name: str, vertical: str = 'uk-real-estate') -> bool:
    """
    Queue a PDF generation job
    
    Returns: True if queued successfully
    """
    if not redis_client:
        logger.warning("Redis not available - cannot queue job")
        return False
    
    job = {
        'area': area,
        'city': city,
        'client_email': client_email,
        'client_name': client_name,
        'vertical': vertical,
        'queued_at': datetime.now().isoformat()
    }
    
    try:
        redis_client.lpush('voxmill:pdf_jobs', json.dumps(job))
        logger.info(f"✅ Job queued for {area}")
        return True
    except Exception as e:
        logger.error(f"Failed to queue job: {e}")
        return False


def get_queue_length() -> int:
    """Get number of jobs in queue"""
    if not redis_client:
        return 0
    return redis_client.llen('voxmill:pdf_jobs'

"""
AIRTABLE WRITE QUEUE
====================
Batches Airtable writes to avoid rate limits
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
import os
import requests

logger = logging.getLogger(__name__)

# Global queue
_write_queue = asyncio.Queue()
_queue_processor = None

AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')


async def queue_airtable_update(
    table_name: str,
    record_id: str,
    fields: Dict,
    priority: str = 'normal'
):
    """
    Queue an Airtable write (non-blocking)
    
    Args:
        table_name: 'Clients' or 'Trial Users'
        record_id: Airtable record ID
        fields: Fields to update
        priority: 'high' or 'normal'
    """
    
    await _write_queue.put({
        'table': table_name,
        'record_id': record_id,
        'fields': fields,
        'priority': priority,
        'queued_at': datetime.now(timezone.utc)
    })
    
    logger.debug(f"📋 Queued Airtable update: {table_name}/{record_id}")


async def _process_queue():
    """
    Background worker: processes Airtable writes
    
    Rate limit: Max 5 requests/second (safe for all Airtable plans)
    """
    
    while True:
        try:
            # Get next item (blocks if queue empty)
            item = await _write_queue.get()
            
            # Execute write
            try:
                url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{item['table'].replace(' ', '%20')}/{item['record_id']}"
                headers = {
                    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                payload = {"fields": item['fields']}
                
                response = requests.patch(url, headers=headers, json=payload, timeout=5)
                
                if response.status_code == 200:
                    logger.debug(f"✅ Airtable updated: {item['table']}/{item['record_id']}")
                else:
                    logger.warning(f"⚠️ Airtable update failed: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"❌ Airtable write error: {e}")
            
            # Rate limit: 5 requests/second = 200ms between requests
            await asyncio.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Queue processor error: {e}")
            await asyncio.sleep(1)


async def start_queue_processor():
    """Start background queue processor"""
    global _queue_processor
    
    if _queue_processor is None:
        _queue_processor = asyncio.create_task(_process_queue())
        logger.info("✅ Airtable queue processor started")

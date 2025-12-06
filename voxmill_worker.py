"""
Voxmill Queue Worker
Processes PDF generation jobs from Redis queue sequentially
"""

import os
import sys
import json
import time
import redis
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL')

def main():
    """Poll Redis queue and process jobs sequentially"""
    
    if not REDIS_URL:
        logger.error("REDIS_URL not configured")
        sys.exit(1)
    
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    
    logger.info("="*70)
    logger.info("VOXMILL QUEUE WORKER STARTED")
    logger.info("="*70)
    logger.info(f"Redis: {REDIS_URL}")
    logger.info(f"Polling queue: voxmill:pdf_jobs")
    logger.info("="*70)
    
    while True:
        try:
            # Block for 10 seconds waiting for job
            job_data = redis_client.brpop('voxmill:pdf_jobs', timeout=10)
            
            if job_data:
                queue_name, job_json = job_data
                job = json.loads(job_json)
                
                logger.info(f"\n📋 JOB RECEIVED: {job.get('area', 'Unknown')}")
                
                # Call voxmill_master.py with job parameters
                import subprocess
                result = subprocess.run([
                    sys.executable,
                    'voxmill_master.py',
                    '--area', job['area'],
                    '--city', job.get('city', 'London'),
                    '--email', job.get('client_email', 'none'),
                    '--name', job.get('client_name', 'Client')
                ])
                
                if result.returncode == 0:
                    logger.info(f"✅ JOB COMPLETE: {job['area']}")
                else:
                    logger.error(f"❌ JOB FAILED: {job['area']}")
            
        except KeyboardInterrupt:
            logger.info("\n🛑 Worker stopped by user")
            break
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
            time.sleep(5)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
VOXMILL ALERT CHECKER (CRON JOB)
=================================
Runs every hour to detect and send real-time alerts to clients

Schedule: 0 * * * * (every hour)
"""

import os
import sys
import logging
import asyncio
from datetime import datetime, timezone
from pymongo import MongoClient

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.intelligence.alert_detector import detect_alerts_for_region, format_alert_message
from app.whatsapp import send_twilio_message

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


async def check_and_send_alerts():
    """
    Main alert checking function
    
    1. Get all active clients with alert preferences
    2. For each client's preferred regions, detect alerts
    3. Send alerts via WhatsApp
    4. Log alerts sent
    """
    
    if db is None:
        logger.error("MongoDB not connected - cannot check alerts")
        return
    
    logger.info("=" * 70)
    logger.info("VOXMILL ALERT CHECKER - Starting run")
    logger.info("=" * 70)
    
    # Get all active clients (Tier 2 and Tier 3 have alerts)
    clients = list(db['client_profiles'].find({
        "tier": {"$in": ["tier_2", "tier_3"]},
        "whatsapp_number": {"$exists": True}
    }))
    
    logger.info(f"Found {len(clients)} clients eligible for alerts")
    
    total_alerts_sent = 0
    
    for client in clients:
        client_name = client.get('name', 'Unknown')
        whatsapp_number = client.get('whatsapp_number')
        preferred_regions = client.get('preferences', {}).get('preferred_regions', [])
        
        if not whatsapp_number:
            logger.warning(f"Client {client_name} has no WhatsApp number")
            continue
        
        if not preferred_regions:
            logger.info(f"Client {client_name} has no preferred regions - skipping")
            continue
        
        logger.info(f"\nChecking alerts for {client_name} ({len(preferred_regions)} regions)")
        
        for region in preferred_regions:
            try:
                # Detect alerts for this region
                alerts = detect_alerts_for_region(region)
                
                if not alerts:
                    logger.info(f"  - {region}: No alerts")
                    continue
                
                logger.info(f"  - {region}: {len(alerts)} alerts detected")
                
                # Send each alert
                for alert in alerts:
                    # Format message
                    message = format_alert_message(alert)
                    
                    # Send via WhatsApp
                    try:
                        await send_twilio_message(whatsapp_number, message)
                        logger.info(f"    ✅ Sent {alert['type']} alert to {client_name}")
                        
                        # Log alert sent
                        db['alerts_sent'].insert_one({
                            "client_id": client.get('_id'),
                            "client_name": client_name,
                            "whatsapp_number": whatsapp_number,
                            "alert_type": alert['type'],
                            "region": region,
                            "urgency": alert['urgency'],
                            "timestamp": datetime.now(timezone.utc),
                            "alert_data": alert
                        })
                        
                        total_alerts_sent += 1
                        
                    except Exception as e:
                        logger.error(f"    ❌ Failed to send alert to {client_name}: {e}")
                
            except Exception as e:
                logger.error(f"  - {region}: Error detecting alerts: {e}", exc_info=True)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"ALERT CHECKER COMPLETE - Sent {total_alerts_sent} total alerts")
    logger.info(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(check_and_send_alerts())

import asyncio
import logging
from datetime import datetime
from app.scrapers.competitor_tracker import track_competitor_prices
from app.alerts.alert_engine import send_market_alerts

logger = logging.getLogger(__name__)

async def daily_intelligence_cycle():
    """
    Run daily at 6 AM GMT:
    1. Scrape competitor data
    2. Detect changes
    3. Send alerts
    """
    while True:
        try:
            now = datetime.now()
            
            # Check if it's 6 AM
            if now.hour == 6 and now.minute < 10:
                logger.info("Starting daily intelligence cycle...")
                
                # Track competitors
                alerts = await track_competitor_prices(area="Mayfair")
                
                # Send alerts if any detected
                if alerts:
                    await send_market_alerts(alerts)
                    logger.info(f"Intelligence cycle complete: {len(alerts)} alerts generated")
                else:
                    logger.info("Intelligence cycle complete: No significant changes detected")
                
                # Sleep for 1 hour to avoid re-triggering
                await asyncio.sleep(3600)
            else:
                # Check every 10 minutes
                await asyncio.sleep(600)
        
        except Exception as e:
            logger.error(f"Error in daily intelligence cycle: {str(e)}", exc_info=True)
            await asyncio.sleep(600)

# Add to app/main.py startup:
# import asyncio
# from app.scheduler import daily_intelligence_cycle
# 
# @app.on_event("startup")
# async def start_scheduler():
#     asyncio.create_task(daily_intelligence_cycle())

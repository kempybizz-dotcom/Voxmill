import os
import logging
from datetime import datetime, timezone
from pymongo import MongoClient
from app.whatsapp import send_twilio_message

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

async def send_market_alerts(alerts: list):
    """
    Send WhatsApp alerts to subscribed clients
    Only sends if client has alerts enabled in their profile
    """
    try:
        if not mongo_client or not alerts:
            return
        
        db = mongo_client['Voxmill']
        clients = db['client_profiles']
        
        # Get all clients with alerts enabled
        subscribed_clients = clients.find({
            'preferences.alerts_enabled': True
        })
        
        for client in subscribed_clients:
            whatsapp_number = client['whatsapp_number']
            preferred_regions = client.get('preferences', {}).get('preferred_regions', [])
            
            # Filter alerts relevant to this client's regions
            relevant_alerts = [
                a for a in alerts 
                if a.get('area') in preferred_regions
            ]
            
            if not relevant_alerts:
                continue
            
            # Format alert message
            alert_message = format_alert_message(relevant_alerts)
            
            # Send via WhatsApp
            await send_twilio_message(whatsapp_number, alert_message)
            
            logger.info(f"Alerts sent to {whatsapp_number}: {len(relevant_alerts)} alerts")
        
    except Exception as e:
        logger.error(f"Error sending alerts: {str(e)}", exc_info=True)


def format_alert_message(alerts: list) -> str:
    """
    Format alerts into executive WhatsApp message
    """
    severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    sorted_alerts = sorted(alerts, key=lambda x: severity_order.get(x.get('severity', 'low'), 3))
    
    message_parts = ["MARKET ALERT", "—" * 40, ""]
    
    for alert in sorted_alerts[:5]:  # Max 5 alerts per message
        alert_type = alert.get('type')
        
        if alert_type == 'price_change':
            direction = "↓" if alert['change_pct'] < 0 else "↑"
            message_parts.append(
                f"{direction} {alert['agent']}: {abs(alert['change_pct'])}% "
                f"(£{alert['old_price']:,.0f} → £{alert['new_price']:,.0f})"
            )
        
        elif alert_type == 'inventory_change':
            direction = "↓" if alert['change_pct'] < 0 else "↑"
            message_parts.append(
                f"{direction} {alert['agent']} inventory: {abs(alert['change_pct'])}% "
                f"({alert['old_inventory']} → {alert['new_inventory']} listings)"
            )
        
        elif alert_type == 'cascade_detected':
            message_parts.append(
                f"⚠ CASCADE DETECTED\n"
                f"Agents: {', '.join(alert['agents_involved'])}\n"
                f"Avg drop: {abs(alert['avg_drop']):.1f}%"
            )
    
    message_parts.append("")
    message_parts.append("Text 'ANALYSE' for full intelligence briefing.")
    
    return "\n".join(message_parts)

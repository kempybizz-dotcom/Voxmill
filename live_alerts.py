"""
VOXMILL LIVE ALERTS SYSTEM
===========================
Daily market monitoring with instant alert notifications
Detects: Price drops, new hot deals, market anomalies, competitor movements

RUNS AS CRON JOB: Monitors markets daily, sends alerts when thresholds hit
"""

import os
import sys
import json
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# Import data collector and analyzer
import subprocess

# Configuration
ALERT_DB_FILE = "/tmp/voxmill_alert_history.json"
SENDER_EMAIL = os.environ.get('VOXMILL_EMAIL')
SENDER_PASSWORD = os.environ.get('VOXMILL_EMAIL_PASSWORD')

# Alert thresholds (customize per client)
ALERT_THRESHOLDS = {
    'price_drop_percent': 10,  # Alert if property drops >10%
    'new_hot_deals_threshold': 3,  # Alert if 3+ new hot deals appear
    'avg_price_change_percent': 5,  # Alert if market avg moves >5%
    'exceptional_deal_score': 9.0,  # Alert on any property scoring 9.0+
    'market_volatility_spike': 1.5  # Alert if volatility increases 1.5x
}

# ============================================================================
# ALERT HISTORY MANAGEMENT
# ============================================================================

def load_alert_history():
    """Load previous market state for comparison"""
    
    if not os.path.exists(ALERT_DB_FILE):
        return {}
    
    try:
        with open(ALERT_DB_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_alert_history(data):
    """Save current market state for next comparison"""
    
    with open(ALERT_DB_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# ============================================================================
# ANOMALY DETECTION
# ============================================================================

def detect_market_alerts(current_data, previous_data, client_config):
    """Detect alert-worthy market changes"""
    
    alerts = []
    
    area = client_config['area']
    prev_key = f"{area}_{client_config['vertical']}"
    
    current_metrics = current_data['metrics']
    current_props = current_data['properties']
    
    # First run - no comparison possible
    if prev_key not in previous_data:
        return []
    
    prev_metrics = previous_data[prev_key]['metrics']
    prev_props_ids = set(previous_data[prev_key].get('property_ids', []))
    
    # 1. EXCEPTIONAL NEW DEALS
    exceptional_deals = [p for p in current_props if p['deal_score'] >= ALERT_THRESHOLDS['exceptional_deal_score']]
    
    if exceptional_deals:
        for deal in exceptional_deals[:3]:  # Top 3
            # Check if it's new (wasn't in previous scan)
            deal_id = deal.get('listing_id', deal['address'])
            if deal_id not in prev_props_ids:
                alerts.append({
                    'type': 'EXCEPTIONAL_DEAL',
                    'severity': 'HIGH',
                    'title': f"🔥 EXCEPTIONAL DEAL ALERT: {area}",
                    'message': f"New property scored {deal['deal_score']}/10\n{deal['address']}\n£{deal['price']:,} | {deal['beds']}bd/{deal['baths']}ba | £{deal['price_per_sqft']:,}/sqft\n\nThis is {int((1 - deal['price_per_sqft']/current_metrics['avg_ppsf'])*100)}% below market average.",
                    'data': deal
                })
    
    # 2. MARKET PRICE MOVEMENT
    price_change_pct = ((current_metrics['avg_price'] - prev_metrics['avg_price']) / prev_metrics['avg_price']) * 100
    
    if abs(price_change_pct) >= ALERT_THRESHOLDS['avg_price_change_percent']:
        direction = "increased" if price_change_pct > 0 else "dropped"
        alerts.append({
            'type': 'MARKET_SHIFT',
            'severity': 'MEDIUM',
            'title': f"📊 MARKET SHIFT ALERT: {area}",
            'message': f"Average price has {direction} {abs(price_change_pct):.1f}% in 24 hours.\n\nPrevious: £{prev_metrics['avg_price']:,}\nCurrent: £{current_metrics['avg_price']:,}\n\nThis represents a significant market movement.",
            'data': {'prev': prev_metrics['avg_price'], 'current': current_metrics['avg_price']}
        })
    
    # 3. HOT DEAL VOLUME SPIKE
    new_hot_deals = current_metrics['hot_deals'] - prev_metrics.get('hot_deals', 0)
    
    if new_hot_deals >= ALERT_THRESHOLDS['new_hot_deals_threshold']:
        alerts.append({
            'type': 'DEAL_VOLUME_SPIKE',
            'severity': 'MEDIUM',
            'title': f"💰 DEAL VOLUME SPIKE: {area}",
            'message': f"{new_hot_deals} new hot deals detected in the last 24 hours.\n\nPrevious: {prev_metrics.get('hot_deals', 0)} deals\nCurrent: {current_metrics['hot_deals']} deals\n\nIncreased inventory presents acquisition opportunities.",
            'data': {'increase': new_hot_deals}
        })
    
    # 4. PRICE/SQFT ANOMALIES
    ppsf_change_pct = ((current_metrics['avg_ppsf'] - prev_metrics['avg_ppsf']) / prev_metrics['avg_ppsf']) * 100
    
    if abs(ppsf_change_pct) >= ALERT_THRESHOLDS['avg_price_change_percent']:
        direction = "increased" if ppsf_change_pct > 0 else "decreased"
        alerts.append({
            'type': 'PPSF_MOVEMENT',
            'severity': 'MEDIUM',
            'title': f"📏 PRICE/SQFT ALERT: {area}",
            'message': f"Average £/sqft has {direction} {abs(ppsf_change_pct):.1f}%.\n\nPrevious: £{prev_metrics['avg_ppsf']:,}/sqft\nCurrent: £{current_metrics['avg_ppsf']:,}/sqft\n\nPricing dynamics are shifting.",
            'data': {'prev': prev_metrics['avg_ppsf'], 'current': current_metrics['avg_ppsf']}
        })
    
    # 5. INDIVIDUAL PROPERTY PRICE DROPS
    # (This requires tracking individual properties - simplified version)
    significant_drops = [p for p in current_props if 'SIGNIFICANT_UNDERPRICING' in p.get('anomaly_flags', [])]
    
    if significant_drops:
        for prop in significant_drops[:2]:  # Top 2
            alerts.append({
                'type': 'PRICE_ANOMALY',
                'severity': 'HIGH',
                'title': f"⚠️ PRICING ANOMALY: {area}",
                'message': f"Property detected at significantly below-market pricing:\n\n{prop['address']}\n£{prop['price']:,} | £{prop['price_per_sqft']:,}/sqft\n\nThis is a statistical outlier—potential acquisition target.",
                'data': prop
            })
    
    return alerts

# ============================================================================
# ALERT EMAIL GENERATION
# ============================================================================

def generate_alert_email_html(alerts, client_name, area, city):
    """Generate HTML alert email"""
    
    alert_items = ""
    for alert in alerts:
        severity_color = {
            'HIGH': '#D4AF37',
            'MEDIUM': '#B8960C',
            'LOW': '#8B7209'
        }.get(alert['severity'], '#D4AF37')
        
        alert_items += f"""
        <div style="background-color: #0A0A0A; border-left: 4px solid {severity_color}; padding: 20px; margin: 15px 0;">
            <p style="color: {severity_color}; font-size: 14px; font-weight: bold; margin: 0 0 10px 0;">
                {alert['title']}
            </p>
            <p style="color: #E8E8E8; font-size: 13px; line-height: 20px; margin: 0; white-space: pre-line;">
                {alert['message']}
            </p>
        </div>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin: 0; padding: 0; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; background-color: #0A0A0A;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #0A0A0A;">
            <tr>
                <td align="center" style="padding: 40px 20px;">
                    <table width="600" cellpadding="0" cellspacing="0" style="background-color: #1A1A1A; border: 2px solid #D4AF37;">
                        
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #1A1A1A 0%, #0A0A0A 100%); padding: 30px; border-bottom: 3px solid #D4AF37;">
                                <table cellpadding="0" cellspacing="0">
                                    <tr>
                                        <td style="padding-right: 15px; vertical-align: middle;">
                                            <img src="cid:voxmill_logo" alt="Voxmill" width="50" height="50" style="display: block;" />
                                        </td>
                                        <td style="vertical-align: middle;">
                                            <h1 style="margin: 0; color: #D4AF37; font-size: 22px; font-weight: bold; letter-spacing: 2px;">
                                                🚨 LIVE MARKET ALERT
                                            </h1>
                                            <p style="margin: 5px 0 0 0; color: #E8E8E8; font-size: 11px; letter-spacing: 1px;">
                                                VOXMILL INTELLIGENCE — {datetime.now().strftime("%B %d, %Y %H:%M")}
                                            </p>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                        
                        <!-- Body -->
                        <tr>
                            <td style="padding: 30px;">
                                <p style="color: #E8E8E8; font-size: 15px; line-height: 22px; margin: 0 0 20px 0;">
                                    {client_name},
                                </p>
                                
                                <p style="color: #E8E8E8; font-size: 14px; line-height: 22px; margin: 0 0 20px 0;">
                                    <b style="color: #D4AF37;">{len(alerts)} critical market event(s)</b> detected in <b style="color: #D4AF37;">{area}, {city}</b> requiring immediate attention.
                                </p>
                                
                                {alert_items}
                                
                                <div style="background-color: #D4AF37; padding: 20px; margin: 25px 0; border-radius: 4px;">
                                    <p style="color: #0A0A0A; font-size: 13px; font-weight: bold; margin: 0 0 10px 0;">
                                        ⚡ RECOMMENDED ACTION
                                    </p>
                                    <p style="color: #0A0A0A; font-size: 12px; line-height: 18px; margin: 0;">
                                        Review these opportunities within the next 4-6 hours. Market conditions are dynamic—early action provides competitive advantage.
                                    </p>
                                </div>
                                
                                <p style="color: #E8E8E8; font-size: 13px; line-height: 20px; margin: 30px 0 0 0;">
                                    Best,<br>
                                    <b style="color: #D4AF37;">Voxmill Intelligence</b><br>
                                    <span style="color: #B8960C; font-size: 11px;">Automated Market Monitoring System</span>
                                </p>
                            </td>
                        </tr>
                        
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: #0A0A0A; padding: 15px 30px; border-top: 1px solid #4A4A4A;">
                                <p style="color: #6B5507; font-size: 10px; line-height: 14px; margin: 0; text-align: center;">
                                    This is an automated alert from Voxmill's market monitoring system<br>
                                    You're receiving this because you have an active intelligence subscription
                                </p>
                            </td>
                        </tr>
                        
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return html

def send_alert_email(alerts, client_email, client_name, area, city):
    """Send alert email to client"""
    
    print(f"\n📧 SENDING ALERT EMAIL")
    print(f"   To: {client_email}")
    print(f"   Alerts: {len(alerts)}")
    
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print(f"   ⚠️ Email credentials not configured (skipping)")
        return False
    
    try:
        msg = MIMEMultipart('related')
        msg['From'] = f"Voxmill Live Alerts <{SENDER_EMAIL}>"
        msg['To'] = client_email
        msg['Subject'] = f"🚨 {len(alerts)} Market Alert(s) — {area}"
        
        html_content = generate_alert_email_html(alerts, client_name, area, city)
        
        msg_alternative = MIMEMultipart('alternative')
        msg.attach(msg_alternative)
        
        part_html = MIMEText(html_content, 'html')
        msg_alternative.attach(part_html)
        
        # Embed logo
        logo_path = os.path.join(os.path.dirname(__file__), 'voxmill_logo.png')
        if os.path.exists(logo_path):
            with open(logo_path, 'rb') as logo_file:
                logo_image = MIMEImage(logo_file.read())
                logo_image.add_header('Content-ID', '<voxmill_logo>')
                msg.attach(logo_image)
        
        # Send
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        print(f"   ✅ Alert email sent")
        return True
        
    except Exception as e:
        print(f"   ❌ Alert email failed: {str(e)}")
        return False

# ============================================================================
# MONITOR EXECUTION
# ============================================================================

def monitor_market(client_config):
    """Monitor a specific market for a client"""
    
    print("\n" + "="*70)
    print("VOXMILL LIVE ALERTS MONITOR")
    print("="*70)
    print(f"Client: {client_config['name']}")
    print(f"Market: {client_config['area']}, {client_config['city']}")
    print(f"Vertical: {client_config['vertical']}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    try:
        # Run data collection
        print(f"\n[1/3] Collecting current market data...")
        result = subprocess.run(
            [sys.executable, 'data_collector.py', 
             client_config['vertical'], 
             client_config['area'], 
             client_config['city']],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Data collection failed")
            return False
        
        # Run AI analysis
        print(f"\n[2/3] Analyzing for anomalies...")
        result = subprocess.run(
            [sys.executable, 'ai_analyzer.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Analysis failed")
            return False
        
        # Load current data
        with open('/tmp/voxmill_intelligence.json', 'r') as f:
            current_data = json.load(f)
        
        # Load previous data
        previous_data = load_alert_history()
        
        # Detect alerts
        print(f"\n[3/3] Detecting alert-worthy events...")
        alerts = detect_market_alerts(current_data, previous_data, client_config)
        
        if alerts:
            print(f"\n🚨 {len(alerts)} ALERT(S) DETECTED:")
            for alert in alerts:
                print(f"   • {alert['type']}: {alert['title']}")
            
            # Send alert email
            send_alert_email(
                alerts,
                client_config['email'],
                client_config['name'],
                client_config['area'],
                client_config['city']
            )
        else:
            print(f"\n✅ No alerts detected (market stable)")
        
        # Save current state for next comparison
        area_key = f"{client_config['area']}_{client_config['vertical']}"
        previous_data[area_key] = {
            'timestamp': datetime.now().isoformat(),
            'metrics': current_data['metrics'],
            'property_ids': [p.get('listing_id', p['address']) for p in current_data['properties']]
        }
        save_alert_history(previous_data)
        
        print(f"\n✅ Monitoring cycle complete")
        return True
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# CLI
# ============================================================================

def main():
    """Main CLI"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Voxmill Live Alerts Monitoring System')
    
    parser.add_argument('--vertical', type=str, required=True)
    parser.add_argument('--area', type=str, required=True)
    parser.add_argument('--city', type=str, required=True)
    parser.add_argument('--email', type=str, required=True)
    parser.add_argument('--name', type=str, required=True)
    
    args = parser.parse_args()
    
    client_config = {
        'vertical': args.vertical,
        'area': args.area,
        'city': args.city,
        'email': args.email,
        'name': args.name
    }
    
    success = monitor_market(client_config)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

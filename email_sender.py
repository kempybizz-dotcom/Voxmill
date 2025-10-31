"""
VOXMILL SIMPLE EMAIL SENDER
============================
LexCura-inspired simplicity
Clean black, minimal gold, perfect rendering
No bullshit complexity
"""

import smtplib
import os
import sys
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
DEFAULT_PDF_PATH = "/tmp/Voxmill_Elite_Intelligence.pdf"

def validate_environment():
    """Validate email credentials"""
    sender_email = os.environ.get('VOXMILL_EMAIL')
    sender_password = os.environ.get('VOXMILL_EMAIL_PASSWORD')
    
    if not sender_email:
        raise EnvironmentError("VOXMILL_EMAIL not set in environment")
    if not sender_password:
        raise EnvironmentError("VOXMILL_EMAIL_PASSWORD not set (use Gmail App Password)")
    
    logger.info(f"✅ Environment validated: {sender_email}")
    return sender_email, sender_password

def create_simple_email(recipient_name, area, city):
    """Create simple, elegant email like LexCura"""
    
    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Voxmill Market Intelligence</title>
    <style>
        body {{
            margin: 0;
            padding: 0;
            background-color: #000000;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            background-color: #0A0A0A;
        }}
        .header {{
            padding: 60px 40px 50px 40px;
            text-align: center;
            border-top: 3px solid #CBA135;
        }}
        .logo {{
            width: 50px;
            height: 50px;
            background: #CBA135;
            transform: rotate(45deg);
            margin: 0 auto 20px auto;
            position: relative;
        }}
        .logo-v {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%) rotate(-45deg);
            font-family: Georgia, serif;
            font-size: 24px;
            font-weight: bold;
            color: #0A0A0A;
        }}
        .brand {{
            font-size: 22px;
            font-weight: 700;
            color: #FFFFFF;
            letter-spacing: 3px;
            margin-bottom: 6px;
        }}
        .tagline {{
            font-size: 10px;
            color: #CBA135;
            letter-spacing: 2px;
            text-transform: uppercase;
        }}
        .content {{
            padding: 40px;
            color: #FFFFFF;
        }}
        .greeting {{
            font-size: 16px;
            margin-bottom: 30px;
            color: #E8E8E8;
        }}
        .title {{
            font-size: 26px;
            font-weight: 700;
            color: #FFFFFF;
            margin-bottom: 12px;
            line-height: 1.3;
        }}
        .subtitle {{
            font-size: 11px;
            color: #CBA135;
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-bottom: 30px;
        }}
        .body-text {{
            font-size: 15px;
            color: #E8E8E8;
            line-height: 1.7;
            margin-bottom: 30px;
        }}
        .highlight-box {{
            background-color: #111111;
            border-left: 3px solid #CBA135;
            padding: 25px;
            margin: 30px 0;
        }}
        .highlight-title {{
            font-size: 10px;
            color: #CBA135;
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-bottom: 15px;
            font-weight: 700;
        }}
        .highlight-item {{
            font-size: 14px;
            color: #E8E8E8;
            margin-bottom: 10px;
            padding-left: 15px;
            position: relative;
        }}
        .highlight-item:before {{
            content: "▸";
            position: absolute;
            left: 0;
            color: #CBA135;
        }}
        .cta {{
            text-align: center;
            margin: 40px 0 30px 0;
        }}
        .cta-button {{
            display: inline-block;
            background: #CBA135;
            color: #000000;
            text-decoration: none;
            padding: 15px 40px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 2px;
            text-transform: uppercase;
        }}
        .signature {{
            margin-top: 40px;
            padding-top: 25px;
            border-top: 1px solid #222222;
        }}
        .sig-name {{
            font-size: 15px;
            color: #CBA135;
            font-weight: 600;
            margin-bottom: 4px;
        }}
        .sig-title {{
            font-size: 12px;
            color: #999999;
        }}
        .footer {{
            padding: 30px 40px;
            text-align: center;
            border-top: 1px solid #222222;
        }}
        .footer-text {{
            font-size: 11px;
            color: #666666;
            line-height: 1.6;
            margin-bottom: 12px;
        }}
        .confidential {{
            font-size: 10px;
            color: #CBA135;
            letter-spacing: 2px;
            text-transform: uppercase;
        }}
        
        @media screen and (max-width: 600px) {{
            .header, .content, .footer {{
                padding-left: 20px !important;
                padding-right: 20px !important;
            }}
            .title {{
                font-size: 22px !important;
            }}
            .brand {{
                font-size: 20px !important;
            }}
            .highlight-box {{
                padding: 20px !important;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        
        <div class="header">
            <div class="logo">
                <div class="logo-v">V</div>
            </div>
            <div class="brand">VOXMILL</div>
            <div class="tagline">Market Intelligence</div>
        </div>
        
        <div class="content">
            
            <div class="greeting">Dear {recipient_name},</div>
            
            <div class="title">Market Intelligence Snapshot<br>{area}</div>
            <div class="subtitle">Weekly Precision Report • {city}</div>
            
            <div class="body-text">
                Following our conversation — I've attached this week's <strong style="color: #CBA135;">Voxmill Market Intelligence</strong> 
                report for <strong>{area}, {city}</strong>. This analysis provides executive insights into current market positioning.
            </div>
            
            <div class="highlight-box">
                <div class="highlight-title">📊 Report Highlights</div>
                <div class="highlight-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
                <div class="highlight-item">Competitor landscape analysis and market positioning</div>
                <div class="highlight-item">Executive intelligence with actionable insights</div>
                <div class="highlight-item">Pricing trends and anomaly detection</div>
            </div>
            
            <div class="cta">
                <a href="#" class="cta-button">View Full Report</a>
            </div>
            
            <div class="body-text" style="text-align: center; color: #999999; font-size: 14px;">
                Have a look at the attached PDF. I'll follow up in 24-48 hours to discuss anything that stands out.
            </div>
            
            <div class="signature">
                <div class="sig-name">Olly</div>
                <div class="sig-title">Voxmill Market Intelligence</div>
            </div>
            
        </div>
        
        <div class="footer">
            <div class="footer-text">
                © {datetime.now().year} Voxmill Automations • Confidential Market Intelligence<br>
                This report contains proprietary analysis for authorized recipients only
            </div>
            <div class="confidential">Voxmill Automations</div>
        </div>
        
    </div>
</body>
</html>
"""

def send_simple_email(recipient_email, recipient_name, area, city, pdf_path=None):
    """Send simple, production-ready email"""
    
    logger.info("="*70)
    logger.info("VOXMILL SIMPLE EMAIL SENDER")
    logger.info("="*70)
    
    try:
        # Step 1: Validate
        logger.info("Step 1/5: Validating environment...")
        sender_email, sender_password = validate_environment()
        
        # Step 2: Check PDF
        if pdf_path is None:
            pdf_path = DEFAULT_PDF_PATH
        
        logger.info(f"Step 2/5: Checking PDF at {pdf_path}...")
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(f"PDF not found at {pdf_path}")
        
        pdf_size = pdf_file.stat().st_size
        logger.info(f"✅ PDF found: {pdf_size:,} bytes")
        
        # Step 3: Create message
        logger.info("Step 3/5: Creating email...")
        msg = MIMEMultipart('related')
        msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
        msg['To'] = recipient_email
        msg['Subject'] = f"Market intelligence snapshot — {area}"
        
        html_content = create_simple_email(recipient_name, area, city)
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        logger.info("✅ HTML created")
        
        # Step 4: Attach PDF
        logger.info("Step 4/5: Attaching PDF...")
        with open(pdf_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            
            area_clean = area.replace(' ', '_').replace(',', '')
            city_clean = city.replace(' ', '_').replace(',', '')
            filename = f"Voxmill_{city_clean}_{area_clean}_Intelligence.pdf"
            
            part.add_header('Content-Disposition', f'attachment; filename={filename}')
            msg.attach(part)
        
        logger.info(f"✅ PDF attached as {filename}")
        
        # Step 5: Send
        logger.info(f"Step 5/5: Sending to {recipient_email}...")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        logger.info("="*70)
        logger.info(f"✅ EMAIL SENT SUCCESSFULLY")
        logger.info(f"   To: {recipient_email}")
        logger.info(f"   Subject: Market intelligence snapshot — {area}")
        logger.info("="*70)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ ERROR: {str(e)}")
        raise

def send_email(recipient_email, recipient_name, area, city, pdf_path=None):
    """Integration function"""
    return send_simple_email(recipient_email, recipient_name, area, city, pdf_path)

def main():
    if len(sys.argv) < 4:
        print("Usage: python email_sender_SIMPLE.py <recipient_email> <recipient_name> <area> [city] [pdf_path]")
        sys.exit(1)
    
    recipient_email = sys.argv[1]
    recipient_name = sys.argv[2]
    area = sys.argv[3]
    city = sys.argv[4] if len(sys.argv) > 4 else "London"
    pdf_path = sys.argv[5] if len(sys.argv) > 5 else None
    
    try:
        send_simple_email(recipient_email, recipient_name, area, city, pdf_path)
        sys.exit(0)
    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

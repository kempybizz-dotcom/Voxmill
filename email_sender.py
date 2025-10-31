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
    """Create Fortune-500 luxury email template"""
    
    return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>Voxmill Market Intelligence</title>
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Inter:wght@400;600&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            margin: 0;
            padding: 0;
            background-color: #0C0C0C;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
        }}
        .email-wrapper {{
            background-color: #0C0C0C;
            padding: 0;
        }}
        .container {{
            max-width: 640px;
            margin: 0 auto;
            background-color: #0C0C0C;
        }}
        .header {{
            background-color: #0C0C0C;
            padding: 40px 32px 32px 32px;
            text-align: center;
            position: relative;
        }}
        .header::before {{
            content: "V";
            position: absolute;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            font-family: 'Playfair Display', Georgia, serif;
            font-size: 120px;
            font-weight: 700;
            color: #B08D57;
            opacity: 0.05;
            pointer-events: none;
            z-index: 0;
        }}
        .logo-container {{
            position: relative;
            z-index: 1;
            margin-bottom: 24px;
        }}
        .logo {{
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, #B08D57 0%, #CBA135 100%);
            transform: rotate(45deg);
            margin: 0 auto;
            position: relative;
            box-shadow: 0 6px 20px rgba(176, 141, 87, 0.25);
        }}
        .logo-v {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%) rotate(-45deg);
            font-family: 'Playfair Display', Georgia, serif;
            font-size: 22px;
            font-weight: 700;
            color: #0C0C0C;
            line-height: 1;
        }}
        .brand {{
            font-family: 'Inter', sans-serif;
            font-size: 11px;
            font-weight: 600;
            color: #B08D57;
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-top: 8px;
        }}
        .header-divider {{
            width: 100%;
            height: 1px;
            background-color: #2E2E2E;
            margin-top: 32px;
        }}
        .content {{
            background-color: #111111;
            padding: 40px 32px;
            color: #D8D8D8;
        }}
        .greeting {{
            font-size: 15px;
            color: #D8D8D8;
            margin-bottom: 32px;
            font-weight: 400;
        }}
        .tagline {{
            font-family: 'Inter', sans-serif;
            font-size: 11px;
            font-weight: 600;
            color: #B08D57;
            letter-spacing: 2px;
            text-transform: uppercase;
            text-align: center;
            margin-bottom: 16px;
        }}
        .title {{
            font-family: 'Playfair Display', Georgia, serif;
            font-size: 28px;
            font-weight: 700;
            color: #F8F8F8;
            text-align: center;
            margin-bottom: 12px;
            line-height: 1.3;
        }}
        .subtitle {{
            font-size: 12px;
            font-weight: 400;
            color: #999999;
            text-align: center;
            letter-spacing: 1px;
            text-transform: uppercase;
            margin-bottom: 32px;
        }}
        .body-text {{
            font-size: 15px;
            color: #D8D8D8;
            line-height: 1.7;
            margin-bottom: 28px;
        }}
        .highlight-box {{
            background-color: #0B0B0B;
            border-left: 3px solid #B08D57;
            border-radius: 0 4px 4px 0;
            padding: 28px 24px;
            margin: 32px 0;
            box-shadow: 0 0 20px rgba(203, 161, 53, 0.08);
        }}
        .highlight-title {{
            font-size: 11px;
            font-weight: 700;
            color: #B08D57;
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-bottom: 18px;
        }}
        .highlight-item {{
            font-size: 14px;
            color: #D8D8D8;
            margin-bottom: 12px;
            padding-left: 18px;
            position: relative;
            line-height: 1.6;
        }}
        .highlight-item:before {{
            content: "—";
            position: absolute;
            left: 0;
            color: #B08D57;
            font-weight: 600;
        }}
        .highlight-item:last-child {{
            margin-bottom: 0;
        }}
        .cta {{
            text-align: center;
            margin: 40px 0 28px 0;
        }}
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #B08D57 0%, #CBA135 100%);
            color: #FFFFFF;
            text-decoration: none;
            padding: 14px 28px;
            border-radius: 6px;
            font-family: 'Inter', sans-serif;
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 1px;
            text-transform: uppercase;
            box-shadow: 0 4px 16px rgba(176, 141, 87, 0.3);
            transition: all 0.3s ease;
        }}
        .cta-note {{
            text-align: center;
            color: #B08D57;
            font-size: 12px;
            font-weight: 600;
            margin-top: 16px;
            margin-bottom: 12px;
        }}
        .followup-text {{
            text-align: center;
            color: #999999;
            font-size: 14px;
            line-height: 1.6;
            margin-top: 24px;
        }}
        .signature {{
            margin-top: 44px;
            padding-top: 28px;
            border-top: 1px solid #2E2E2E;
        }}
        .sig-name {{
            font-size: 15px;
            color: #F8F8F8;
            font-weight: 600;
            margin-bottom: 4px;
        }}
        .sig-title {{
            font-size: 12px;
            color: #999999;
            font-weight: 400;
        }}
        .footer {{
            background-color: #0A0A0A;
            padding: 32px 32px 40px 32px;
            text-align: center;
            border-top: 1px solid #2E2E2E;
        }}
        .footer-text {{
            font-size: 11px;
            color: #777777;
            line-height: 1.7;
            margin-bottom: 16px;
        }}
        .footer-brand {{
            font-size: 10px;
            font-weight: 600;
            color: #B08D57;
            letter-spacing: 2px;
            text-transform: uppercase;
        }}
        
        @media screen and (max-width: 640px) {{
            .container {{
                width: 100% !important;
            }}
            .header, .content, .footer {{
                padding-left: 24px !important;
                padding-right: 24px !important;
            }}
            .title {{
                font-size: 24px !important;
            }}
            .highlight-box {{
                padding: 24px 20px !important;
            }}
            .cta-button {{
                padding: 13px 24px !important;
                font-size: 10px !important;
            }}
        }}
        
        @media (prefers-color-scheme: dark) {{
            .email-wrapper, .container, .header, .content, .footer {{
                background-color: #0C0C0C !important;
            }}
        }}
    </style>
</head>
<body>
    <div class="email-wrapper">
        <div class="container">
            
            <!-- Header -->
            <div class="header">
                <div class="logo-container">
                    <div class="logo">
                        <div class="logo-v">V</div>
                    </div>
                </div>
                <div class="brand">VOXMILL MARKET INTELLIGENCE</div>
                <div class="header-divider"></div>
            </div>
            
            <!-- Content -->
            <div class="content">
                
                <div class="greeting">{recipient_name},</div>
                
                <div class="tagline">Weekly Precision Report</div>
                <div class="title">Market Intelligence Snapshot<br/>{area}</div>
                <div class="subtitle">{city} • {datetime.now().strftime('%B %d, %Y')}</div>
                
                <div class="body-text">
                    Following our conversation — I've attached this week's Voxmill Market Intelligence 
                    report for <strong style="color: #B08D57;">{area}, {city}</strong>. 
                    This analysis provides executive-level insights into current market positioning and competitive dynamics.
                </div>
                
                <!-- Highlight Box -->
                <div class="highlight-box">
                    <div class="highlight-title">Report Highlights</div>
                    <div class="highlight-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
                    <div class="highlight-item">Competitor landscape analysis and strategic market positioning</div>
                    <div class="highlight-item">Executive intelligence briefing with actionable insights</div>
                    <div class="highlight-item">Pricing trend analysis and market anomaly detection</div>
                </div>
                
                <!-- CTA -->
                <div class="cta">
                    <a href="#" class="cta-button" style="pointer-events: none; cursor: default;">View Full Report</a>
                </div>
                
                <div class="cta-note">
                    📎 Full report attached above
                </div>
                
                <div class="followup-text">
                    I'll follow up within 24–48 hours to discuss strategic implications 
                    and how this intelligence can enhance your competitive positioning.
                </div>
                
                <!-- Signature -->
                <div class="signature">
                    <div class="sig-name">Olly</div>
                    <div class="sig-title">Voxmill Market Intelligence</div>
                </div>
                
            </div>
            
            <!-- Footer -->
            <div class="footer">
                <div class="footer-text">
                    © {datetime.now().year} Voxmill Automations • Confidential Market Intelligence<br/>
                    This briefing contains proprietary analysis for authorized recipients only
                </div>
                <div class="footer-brand">Voxmill Automations</div>
            </div>
            
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

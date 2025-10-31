"""
VOXMILL PRODUCTION EMAIL SENDER
================================
Production-hardened email delivery for Render cron jobs
Bronze/gold luxury aesthetic matching PDF
Robust error handling + logging
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

# Configure logging for Render cron visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Email configuration
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
DEFAULT_PDF_PATH = "/tmp/Voxmill_Elite_Intelligence.pdf"

def validate_environment():
    """Validate required environment variables exist"""
    
    sender_email = os.environ.get('VOXMILL_EMAIL')
    sender_password = os.environ.get('VOXMILL_EMAIL_PASSWORD')
    
    if not sender_email:
        raise EnvironmentError(
            "VOXMILL_EMAIL environment variable not set. "
            "Configure in Render dashboard: Environment → Add Secret"
        )
    
    if not sender_password:
        raise EnvironmentError(
            "VOXMILL_EMAIL_PASSWORD environment variable not set. "
            "Use Gmail App Password (not regular password). "
            "Generate at: https://myaccount.google.com/apppasswords"
        )
    
    logger.info(f"✅ Environment validated: {sender_email}")
    return sender_email, sender_password

def create_luxury_template(recipient_name, area, city):
    """Create bronze/gold luxury email template matching PDF aesthetic"""
    
    return f"""
<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="x-apple-disable-message-reformatting">
    <title>Voxmill Market Intelligence</title>
    
    <style>
        /* Reset and base */
        html, body {{
            margin: 0 !important;
            padding: 0 !important;
            height: 100% !important;
            width: 100% !important;
            background-color: #0B0B0B !important;
        }}
        
        * {{
            -ms-text-size-adjust: 100%;
            -webkit-text-size-adjust: 100%;
            box-sizing: border-box;
        }}
        
        table, td {{
            mso-table-lspace: 0pt !important;
            mso-table-rspace: 0pt !important;
        }}
        
        table {{
            border-spacing: 0 !important;
            border-collapse: collapse !important;
            table-layout: fixed !important;
            margin: 0 auto !important;
        }}
        
        img {{
            -ms-interpolation-mode: bicubic;
        }}
        
        a {{
            text-decoration: none;
        }}
        
        /* Container */
        .email-container {{
            max-width: 640px;
            margin: 0 auto;
            background-color: #0B0B0B;
            color: #FFFFFF;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }}
        
        /* Header */
        .header {{
            padding: 50px 40px 40px 40px;
            text-align: center;
            background-color: #0B0B0B;
            border-top: 3px solid #B08D57;
        }}
        
        .logo-diamond {{
            width: 64px;
            height: 64px;
            background: linear-gradient(135deg, #B08D57 0%, #CBA135 50%, #B08D57 100%);
            transform: rotate(45deg);
            margin: 0 auto 24px auto;
            position: relative;
            border-radius: 4px;
            box-shadow: 0 8px 32px rgba(176, 141, 87, 0.3);
        }}
        
        .logo-v {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%) rotate(-45deg);
            font-family: Georgia, 'Times New Roman', serif;
            font-size: 32px;
            font-weight: bold;
            color: #0B0B0B;
            line-height: 1;
        }}
        
        .brand-name {{
            font-size: 26px;
            font-weight: 700;
            color: #FFFFFF;
            letter-spacing: 3px;
            margin-bottom: 8px;
        }}
        
        .brand-tagline {{
            font-size: 11px;
            color: #B08D57;
            letter-spacing: 2px;
            text-transform: uppercase;
            font-weight: 500;
            opacity: 0.9;
        }}
        
        /* Main content */
        .main-content {{
            padding: 40px 40px 50px 40px;
            background-color: #0B0B0B;
        }}
        
        .greeting {{
            font-size: 16px;
            color: #EAEAEA;
            margin-bottom: 32px;
            font-weight: 400;
        }}
        
        .headline {{
            font-size: 28px;
            font-weight: 700;
            color: #FFFFFF;
            text-align: center;
            margin-bottom: 16px;
            line-height: 1.3;
            letter-spacing: -0.5px;
        }}
        
        .subheadline {{
            font-size: 12px;
            color: #B08D57;
            text-align: center;
            margin-bottom: 36px;
            letter-spacing: 2px;
            text-transform: uppercase;
            font-weight: 600;
            opacity: 0.9;
        }}
        
        .description {{
            font-size: 15px;
            color: #EAEAEA;
            line-height: 1.7;
            margin-bottom: 32px;
            text-align: center;
            font-weight: 400;
        }}
        
        /* Executive summary box - matches PDF aesthetic */
        .executive-box {{
            background-color: #121212;
            border: 1px solid rgba(176, 141, 87, 0.25);
            border-left: 3px solid #B08D57;
            border-radius: 8px;
            padding: 28px 26px;
            margin: 32px 0;
            box-shadow: 0 0 20px rgba(203, 161, 53, 0.08);
        }}
        
        .executive-title {{
            font-size: 11px;
            color: #B08D57;
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 18px;
            font-weight: 700;
        }}
        
        .executive-item {{
            font-size: 14px;
            color: #EAEAEA;
            margin-bottom: 12px;
            padding-left: 20px;
            position: relative;
            line-height: 1.6;
            font-weight: 400;
        }}
        
        .executive-item:before {{
            content: "▸";
            position: absolute;
            left: 0;
            color: #CBA135;
            font-size: 14px;
        }}
        
        .executive-item:last-child {{
            margin-bottom: 0;
        }}
        
        /* CTA */
        .cta-section {{
            text-align: center;
            margin: 40px 0 36px 0;
        }}
        
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #B08D57 0%, #CBA135 100%);
            color: #FFFFFF;
            text-decoration: none;
            padding: 16px 40px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 1.5px;
            text-transform: uppercase;
            box-shadow: 0 4px 16px rgba(176, 141, 87, 0.3);
        }}
        
        .follow-up {{
            font-size: 15px;
            color: #AFAFAF;
            text-align: center;
            margin-top: 28px;
            line-height: 1.7;
            font-weight: 400;
        }}
        
        /* Signature */
        .signature {{
            margin-top: 44px;
            padding-top: 28px;
            border-top: 1px solid #2E2E2E;
        }}
        
        .sig-name {{
            font-size: 16px;
            color: #B08D57;
            font-weight: 600;
            margin-bottom: 6px;
        }}
        
        .sig-title {{
            font-size: 13px;
            color: #AFAFAF;
            margin-bottom: 4px;
            font-weight: 400;
        }}
        
        .sig-company {{
            font-size: 11px;
            color: #6B6B6B;
            letter-spacing: 1px;
            text-transform: uppercase;
            font-weight: 500;
        }}
        
        /* Footer */
        .footer {{
            background-color: #0B0B0B;
            padding: 32px 40px 44px 40px;
            text-align: center;
            border-top: 1px solid #2E2E2E;
        }}
        
        .footer-text {{
            font-size: 11px;
            color: #6B6B6B;
            line-height: 1.7;
            margin-bottom: 16px;
            font-weight: 400;
        }}
        
        .confidential {{
            font-size: 11px;
            color: #B08D57;
            text-transform: uppercase;
            letter-spacing: 2px;
            font-weight: 600;
            opacity: 0.8;
        }}
        
        /* Mobile responsiveness */
        @media screen and (max-width: 640px) {{
            .email-container {{
                width: 100% !important;
                max-width: 100% !important;
            }}
            
            .header,
            .main-content,
            .footer {{
                padding-left: 24px !important;
                padding-right: 24px !important;
            }}
            
            .header {{
                padding-top: 40px !important;
                padding-bottom: 32px !important;
            }}
            
            .headline {{
                font-size: 24px !important;
            }}
            
            .brand-name {{
                font-size: 22px !important;
            }}
            
            .logo-diamond {{
                width: 56px !important;
                height: 56px !important;
            }}
            
            .logo-v {{
                font-size: 28px !important;
            }}
            
            .cta-button {{
                padding: 14px 32px !important;
                font-size: 12px !important;
            }}
            
            .executive-box {{
                padding: 22px 18px !important;
            }}
        }}
        
        /* Dark mode support */
        @media (prefers-color-scheme: dark) {{
            .email-container,
            .header,
            .main-content,
            .footer {{
                background-color: #0B0B0B !important;
            }}
        }}
    </style>
</head>

<body style="margin: 0; padding: 0; background-color: #0B0B0B;">
    <div class="email-container">
        
        <!-- Header -->
        <div class="header">
            <div class="logo-diamond">
                <div class="logo-v">V</div>
            </div>
            <div class="brand-name">VOXMILL</div>
            <div class="brand-tagline">Market Intelligence</div>
        </div>
        
        <!-- Main Content -->
        <div class="main-content">
            
            <div class="greeting">{recipient_name},</div>
            
            <div class="headline">Market Intelligence Snapshot<br>{area}</div>
            <div class="subheadline">Weekly Precision Report • {city}</div>
            
            <div class="description">
                Following our conversation — I've attached this week's <strong style="color: #B08D57;">Voxmill Market Intelligence</strong> 
                report for <strong style="color: #CBA135;">{area}, {city}</strong>. 
                This analysis provides executive-level insights into current market positioning.
            </div>
            
            <!-- Executive Summary -->
            <div class="executive-box">
                <div class="executive-title">📊 Report Highlights</div>
                <div class="executive-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
                <div class="executive-item">Competitor landscape analysis and market positioning</div>
                <div class="executive-item">Executive intelligence with actionable insights</div>
                <div class="executive-item">Pricing trends and anomaly detection</div>
            </div>
            
            <!-- CTA -->
            <div class="cta-section">
                <a href="#" class="cta-button">View Full Report</a>
            </div>
            
            <div class="follow-up">
                Have a look at the attached PDF. I'll follow up in 24-48 hours to discuss 
                anything that stands out for your portfolio.
            </div>
            
            <!-- Signature -->
            <div class="signature">
                <div class="sig-name">Olly</div>
                <div class="sig-title">Voxmill Market Intelligence</div>
                <div class="sig-company">Voxmill Automations</div>
            </div>
            
        </div>
        
        <!-- Footer -->
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

def send_luxury_email(recipient_email, recipient_name, area, city, pdf_path=None):
    """Send production-hardened luxury email with PDF attachment"""
    
    logger.info("="*70)
    logger.info("VOXMILL EMAIL SENDER - STARTING")
    logger.info("="*70)
    
    try:
        # Step 1: Validate environment
        logger.info("Step 1/5: Validating environment...")
        sender_email, sender_password = validate_environment()
        
        # Step 2: Check PDF exists
        if pdf_path is None:
            pdf_path = DEFAULT_PDF_PATH
        
        logger.info(f"Step 2/5: Checking PDF at {pdf_path}...")
        
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(
                f"PDF not found at {pdf_path}. "
                f"Ensure PDF generator runs successfully before email sender."
            )
        
        pdf_size = pdf_file.stat().st_size
        logger.info(f"✅ PDF found: {pdf_path} ({pdf_size:,} bytes)")
        
        # Step 3: Create email message
        logger.info("Step 3/5: Creating email message...")
        
        msg = MIMEMultipart('related')
        msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
        msg['To'] = recipient_email
        msg['Subject'] = f"Market intelligence snapshot — {area}"
        
        # Create HTML content
        html_content = create_luxury_template(recipient_name, area, city)
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        logger.info("✅ HTML template created")
        
        # Step 4: Attach PDF
        logger.info("Step 4/5: Attaching PDF...")
        
        with open(pdf_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            
            # Clean filename
            area_clean = area.replace(' ', '_').replace(',', '')
            city_clean = city.replace(' ', '_').replace(',', '')
            filename = f"Voxmill_{city_clean}_{area_clean}_Intelligence.pdf"
            
            part.add_header(
                'Content-Disposition',
                f'attachment; filename={filename}'
            )
            msg.attach(part)
        
        logger.info(f"✅ PDF attached as {filename}")
        
        # Step 5: Send email via SMTP
        logger.info(f"Step 5/5: Sending email to {recipient_email}...")
        logger.info(f"   → Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.set_debuglevel(0)  # Set to 1 for verbose SMTP debug
            server.starttls()
            
            logger.info("   → Authenticating...")
            server.login(sender_email, sender_password)
            
            logger.info("   → Sending message...")
            server.send_message(msg)
        
        logger.info("="*70)
        logger.info(f"✅ EMAIL SENT SUCCESSFULLY")
        logger.info(f"   To: {recipient_email}")
        logger.info(f"   Subject: Market intelligence snapshot — {area}")
        logger.info(f"   PDF: {filename}")
        logger.info("="*70)
        
        return True
        
    except EnvironmentError as e:
        logger.error(f"❌ ENVIRONMENT ERROR: {str(e)}")
        logger.error("Configure in Render: Dashboard → Environment → Add Secret")
        raise
        
    except FileNotFoundError as e:
        logger.error(f"❌ FILE ERROR: {str(e)}")
        raise
        
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"❌ SMTP AUTHENTICATION FAILED")
        logger.error(f"   Error: {str(e)}")
        logger.error(f"   Solution: Use Gmail App Password (not regular password)")
        logger.error(f"   Generate at: https://myaccount.google.com/apppasswords")
        raise
        
    except smtplib.SMTPException as e:
        logger.error(f"❌ SMTP ERROR: {str(e)}")
        raise
        
    except Exception as e:
        logger.error(f"❌ UNEXPECTED ERROR: {str(e)}")
        logger.error(f"   Type: {type(e).__name__}")
        import traceback
        logger.error(traceback.format_exc())
        raise

# Integration function for master script
def send_email(recipient_email, recipient_name, area, city, pdf_path=None):
    """Integration function for voxmill_master.py"""
    return send_luxury_email(recipient_email, recipient_name, area, city, pdf_path)

def main():
    """Test function"""
    
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python email_sender_production.py <recipient_email> <recipient_name> <area> [city] [pdf_path]")
        print("Example: python email_sender_production.py john@agency.com 'John Smith' Mayfair London")
        sys.exit(1)
    
    recipient_email = sys.argv[1]
    recipient_name = sys.argv[2]
    area = sys.argv[3]
    city = sys.argv[4] if len(sys.argv) > 4 else "London"
    pdf_path = sys.argv[5] if len(sys.argv) > 5 else None
    
    try:
        send_luxury_email(recipient_email, recipient_name, area, city, pdf_path)
        sys.exit(0)
    except Exception as e:
        logger.error(f"CRITICAL FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

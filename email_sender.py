"""
VOXMILL EMAIL SENDER - FORTUNE-500 EDITION
===========================================
Black background, bronze accents, real logo
Inline CID attachment support
"""

import smtplib
import os
import sys
import logging
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
DEFAULT_PDF_PATH = "/tmp/Voxmill_Elite_Intelligence.pdf"
DEFAULT_LOGO_PATH = "/mnt/user-data/uploads/voxmill_logo.png"

def validate_environment():
    """Validate email credentials"""
    sender_email = os.environ.get('VOXMILL_EMAIL')
    sender_password = os.environ.get('VOXMILL_EMAIL_PASSWORD')
    
    if not sender_email:
        raise EnvironmentError("VOXMILL_EMAIL not set")
    if not sender_password:
        raise EnvironmentError("VOXMILL_EMAIL_PASSWORD not set")
    
    logger.info(f"✅ Credentials: {sender_email}")
    return sender_email, sender_password

def create_voxmill_email(recipient_name, area, city):
    """Create Fortune-500 email HTML"""
    
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Inter:wght@400;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{margin:0;padding:0;background:#0C0C0C;font-family:'Inter',Arial,sans-serif;}}
.wrap{{max-width:640px;margin:0 auto;background:#0C0C0C;}}
.hdr{{background:#0C0C0C;padding:32px 32px 24px;text-align:center;}}
.logo{{margin:0 auto 10px;display:block;width:60px;height:auto;}}
.brand{{font-size:11px;font-weight:600;color:#B08D57;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:16px;}}
.divider{{width:100%;height:1px;background:#CBA135;}}
.content{{background:#121212;padding:32px;margin:0;border-radius:6px;}}
.greeting{{font-size:15px;color:#C9C9C9;margin-bottom:24px;}}
.tag{{font-size:11px;font-weight:600;color:#B08D57;letter-spacing:1.5px;text-transform:uppercase;text-align:center;margin-bottom:12px;}}
.title{{font-family:'Playfair Display',serif;font-size:26px;font-weight:700;color:#FFFFFF;text-align:center;line-height:1.3;margin-bottom:10px;}}
.sub{{font-size:12px;color:#999999;text-align:center;letter-spacing:1px;text-transform:uppercase;margin-bottom:24px;}}
.txt{{font-size:14px;color:#C9C9C9;line-height:1.7;margin-bottom:20px;}}
.box{{background:#0C0C0C;border-left:3px solid #B08D57;border-radius:0 6px 6px 0;padding:24px 20px;margin:24px 0;}}
.box-title{{font-size:11px;font-weight:700;color:#B08D57;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:14px;}}
.box-item{{font-size:13px;color:#C9C9C9;margin-bottom:10px;padding-left:16px;position:relative;line-height:1.6;}}
.box-item:before{{content:"—";position:absolute;left:0;color:#CBA135;font-weight:600;}}
.cta{{text-align:center;margin:28px 0 20px;}}
.btn{{display:inline-block;background:linear-gradient(135deg,#B08D57,#CBA135);color:#FFF;text-decoration:none;padding:13px 26px;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;box-shadow:0 4px 12px rgba(203,161,53,0.3);}}
.note{{text-align:center;color:#CBA135;font-size:12px;font-weight:600;margin:14px 0 10px;}}
.follow{{text-align:center;color:#999999;font-size:13px;line-height:1.6;margin-top:20px;}}
.sig{{margin-top:32px;padding-top:20px;border-top:1px solid #333;}}
.sig-name{{font-size:14px;color:#FFFFFF;font-weight:600;margin-bottom:4px;}}
.sig-title{{font-size:11px;color:#999999;}}
.ftr{{background:#0C0C0C;padding:24px 32px;text-align:center;border-top:1px solid #CBA135;margin-top:20px;}}
.ftr-txt{{font-size:10px;color:#B08D57;line-height:1.6;letter-spacing:1px;text-transform:uppercase;}}
@media (max-width:640px){{
.hdr,.content,.ftr{{padding-left:20px!important;padding-right:20px!important;}}
.title{{font-size:22px!important;}}
.box{{padding:20px 16px!important;}}
}}
</style>
</head>
<body>
<div class="wrap">
<div class="hdr">
<img src="cid:voxmill_logo" class="logo" alt="Voxmill">
<div class="brand">VOXMILL MARKET INTELLIGENCE</div>
<div class="divider"></div>
</div>
<div class="content">
<div class="greeting">{recipient_name},</div>
<div class="tag">Weekly Precision Report</div>
<div class="title">Market Intelligence Snapshot<br/>{area}</div>
<div class="sub">{city} • {datetime.now().strftime('%B %d, %Y')}</div>
<div class="txt">
Following our conversation — I've attached this week's Voxmill Market Intelligence 
report for <strong style="color:#B08D57">{area}, {city}</strong>. 
This analysis provides executive-level insights into current market positioning.
</div>
<div class="box">
<div class="box-title">Report Highlights</div>
<div class="box-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
<div class="box-item">Competitor landscape analysis and strategic positioning</div>
<div class="box-item">Executive intelligence briefing with actionable insights</div>
<div class="box-item">Pricing trend analysis and market anomaly detection</div>
</div>
<div class="cta">
<a href="cid:voxmill_report_pdf" class="btn">Open Full Report</a>
</div>
<div class="note">📎 Full report attached above</div>
<div class="follow">
I'll follow up within 24–48 hours to discuss strategic implications 
and how this intelligence can enhance your competitive positioning.
</div>
<div class="sig">
<div class="sig-name">Olly</div>
<div class="sig-title">Voxmill Market Intelligence</div>
</div>
</div>
<div class="ftr">
<div class="ftr-txt">
Voxmill Automations — Confidential Market Intelligence | {datetime.now().year}<br/>
This briefing contains proprietary analysis for authorized recipients only
</div>
</div>
</div>
</body>
</html>"""

def send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None):
    """Send Voxmill Fortune-500 email"""
    
    logger.info("=" * 70)
    logger.info("VOXMILL FORTUNE-500 EMAIL SENDER")
    logger.info("=" * 70)
    
    try:
        # Step 1: Validate
        logger.info("\nStep 1/6: Validating credentials...")
        sender_email, sender_password = validate_environment()
        
        # Step 2: Check PDF
        if pdf_path is None:
            pdf_path = DEFAULT_PDF_PATH
        
        logger.info(f"Step 2/6: Checking PDF at {pdf_path}...")
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")
        
        pdf_size = pdf_file.stat().st_size
        logger.info(f"✅ PDF: {pdf_size:,} bytes ({pdf_size/1024:.1f} KB)")
        
        # Step 3: Load logo
        logger.info("Step 3/6: Loading logo...")
        if logo_path is None:
            logo_path = DEFAULT_LOGO_PATH
        
        if not Path(logo_path).exists():
            raise FileNotFoundError(f"Logo not found: {logo_path}")
        
        logger.info(f"✅ Logo: {logo_path}")
        
        # Step 4: Build email
        logger.info("Step 4/6: Building email message...")
        msg = MIMEMultipart('related')
        msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
        msg['To'] = recipient_email
        msg['Subject'] = f"Market intelligence snapshot — {area}"
        
        # HTML content
        html_content = create_voxmill_email(recipient_name, area, city)
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        logger.info("✅ HTML created")
        
        # Step 5: Attach logo as inline image
        logger.info("Step 5/6: Attaching inline logo...")
        with open(logo_path, 'rb') as f:
            logo_img = MIMEImage(f.read())
            logo_img.add_header('Content-ID', '<voxmill_logo>')
            logo_img.add_header('Content-Disposition', 'inline', filename='voxmill_logo.png')
            msg.attach(logo_img)
        
        logger.info("✅ Logo embedded")
        
        # Step 6: Attach PDF
        logger.info("Step 6/6: Attaching PDF...")
        with open(pdf_path, "rb") as f:
            part = MIMEBase('application', 'pdf')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            
            area_clean = area.replace(' ', '_').replace(',', '')
            city_clean = city.replace(' ', '_').replace(',', '')
            filename = f"Voxmill_{city_clean}_{area_clean}_Intelligence.pdf"
            
            part.add_header('Content-ID', '<voxmill_report_pdf>')
            part.add_header('Content-Disposition', f'attachment; filename={filename}')
            msg.attach(part)
        
        logger.info(f"✅ PDF attached: {filename}")
        logger.info(f"✅ PDF button linked")
        
        # Send
        logger.info(f"\nSending to {recipient_email}...")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ EMAIL SENT SUCCESSFULLY")
        logger.info(f"   To: {recipient_email}")
        logger.info(f"   Name: {recipient_name}")
        logger.info(f"   Area: {area}, {city}")
        logger.info("=" * 70 + "\n")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ ERROR: {str(e)}")
        raise

def send_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None):
    """Integration function"""
    return send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path, logo_path)

def main():
    """CLI"""
    if len(sys.argv) < 4:
        print("Usage: python VOXMILL_email_sender.py <email> <name> <area> [city] [pdf] [logo]")
        print("\nExample:")
        print("  python VOXMILL_email_sender.py john@example.com 'John Smith' Mayfair London")
        sys.exit(1)
    
    recipient_email = sys.argv[1]
    recipient_name = sys.argv[2]
    area = sys.argv[3]
    city = sys.argv[4] if len(sys.argv) > 4 else "London"
    pdf_path = sys.argv[5] if len(sys.argv) > 5 else None
    logo_path = sys.argv[6] if len(sys.argv) > 6 else None
    
    try:
        send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path, logo_path)
        sys.exit(0)
    except Exception as e:
        logger.error(f"\nCRITICAL FAILURE: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

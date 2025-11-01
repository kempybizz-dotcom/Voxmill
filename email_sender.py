"""
VOXMILL EXECUTIVE EMAIL SENDER
===============================
Fortune-500 black/bronze design system
Full functionality preserved
"""

import smtplib
import os
import sys
import logging
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
    """Create executive email - EXACT Voxmill design system"""
    
    year = datetime.now().year
    date = datetime.now().strftime('%B %d, %Y')
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Voxmill Market Intelligence</title>
</head>
<body style="margin:0;padding:0;background-color:#0B0B0B;font-family:Arial,Helvetica,sans-serif;">

<!-- OUTER WRAPPER -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#0B0B0B;margin:0;padding:0;">
<tr>
<td align="center" style="padding:0;">

<!-- MAIN CONTAINER 640px -->
<table width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;margin:0 auto;">

<!-- HEADER -->
<tr>
<td style="background-color:#0B0B0B;padding:36px 0 28px;text-align:center;">
<!-- Logo or V text fallback -->
<div style="width:56px;height:56px;margin:0 auto 12px;background:linear-gradient(135deg,#B08D57,#CBA135);transform:rotate(45deg);position:relative;">
<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%) rotate(-45deg);font-family:'Times New Roman',Times,serif;font-size:28px;font-weight:700;color:#0B0B0B;">V</div>
</div>
<div style="font-family:'Times New Roman',Times,serif;font-size:11px;font-weight:700;color:#B08D57;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:16px;">
VOXMILL MARKET INTELLIGENCE
</div>
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="height:1px;background-color:#CBA135;"></td></tr>
</table>
</td>
</tr>

<!-- CONTENT PANEL -->
<tr>
<td style="padding:0 20px 20px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#121212;border:1px solid rgba(203,161,53,0.25);border-radius:14px;">
<tr>
<td style="padding:36px;">

<!-- GREETING -->
<p style="margin:0 0 24px;font-size:15px;color:#EAEAEA;line-height:1.5;">
{recipient_name},
</p>

<!-- TAG LINE -->
<p style="margin:0 0 8px;font-family:'Times New Roman',Times,serif;font-size:11px;font-weight:600;color:#B08D57;letter-spacing:1px;text-transform:uppercase;text-align:center;">
WEEKLY PRECISION REPORT
</p>

<!-- TITLE -->
<h1 style="margin:0 0 8px;font-family:'Times New Roman',Times,serif;font-size:24px;font-weight:700;color:#EAEAEA;text-align:center;line-height:1.3;letter-spacing:1px;">
Market Intelligence Snapshot
</h1>
<p style="margin:0 0 24px;font-size:13px;color:#AFAFAF;text-align:center;text-transform:uppercase;letter-spacing:0.5px;">
{area}, {city} • {date}
</p>

<!-- MAIN PARAGRAPH -->
<p style="margin:0 0 24px;font-size:15px;color:#EAEAEA;line-height:1.7;">
Following our conversation — I've attached this week's Voxmill Market Intelligence report for <strong style="color:#B08D57;">{area}, {city}</strong>. This analysis provides executive-level insights into current market positioning and competitive dynamics.
</p>

<!-- HIGHLIGHT BOX -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:rgba(176,141,87,0.06);border-left:4px solid #B08D57;border-radius:0 8px 8px 0;margin:24px 0;">
<tr>
<td style="padding:24px 20px;">

<!-- BOX TITLE -->
<p style="margin:0 0 16px;font-size:11px;font-weight:700;color:#B08D57;letter-spacing:1px;text-transform:uppercase;">
REPORT HIGHLIGHTS
</p>

<!-- BULLET LIST -->
<table cellpadding="0" cellspacing="0" border="0">
<tr><td style="padding-bottom:10px;font-size:14px;color:#EAEAEA;line-height:1.6;">
<span style="color:#CBA135;font-weight:700;margin-right:8px;">—</span>40+ luxury properties analyzed with AI-powered deal scoring
</td></tr>
<tr><td style="padding-bottom:10px;font-size:14px;color:#EAEAEA;line-height:1.6;">
<span style="color:#CBA135;font-weight:700;margin-right:8px;">—</span>Competitor landscape analysis and strategic market positioning
</td></tr>
<tr><td style="padding-bottom:10px;font-size:14px;color:#EAEAEA;line-height:1.6;">
<span style="color:#CBA135;font-weight:700;margin-right:8px;">—</span>Executive intelligence briefing with actionable insights
</td></tr>
<tr><td style="font-size:14px;color:#EAEAEA;line-height:1.6;">
<span style="color:#CBA135;font-weight:700;margin-right:8px;">—</span>Pricing trend analysis and market anomaly detection
</td></tr>
</table>

</td>
</tr>
</table>

<!-- CTA BUTTON -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:28px 0 20px;">
<tr>
<td align="center">
<a href="cid:voxmill_report_pdf" style="display:inline-block;background:linear-gradient(135deg,#B08D57 0%,#CBA135 100%);color:#FFFFFF;text-decoration:none;padding:11px 20px;border-radius:6px;font-size:13px;font-weight:600;letter-spacing:1px;text-transform:uppercase;">
OPEN FULL REPORT
</a>
</td>
</tr>
</table>

<!-- ATTACHMENT NOTE -->
<p style="margin:0 0 10px;font-size:12px;font-weight:600;color:#CBA135;text-align:center;">
📎 Full report attached above
</p>

<!-- FOLLOW UP -->
<p style="margin:20px 0 0;font-size:13px;color:#AFAFAF;line-height:1.6;text-align:center;">
I'll follow up within 24–48 hours to discuss strategic implications and how this intelligence can enhance your competitive positioning.
</p>

<!-- SIGNATURE -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:32px;padding-top:20px;border-top:1px solid #2E2E2E;">
<tr>
<td>
<p style="margin:0 0 4px;font-size:14px;color:#EAEAEA;font-weight:600;">Olly</p>
<p style="margin:0;font-size:11px;color:#AFAFAF;">Voxmill Market Intelligence</p>
</td>
</tr>
</table>

</td>
</tr>
</table>
</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="background-color:#0B0B0B;padding:24px 20px;text-align:center;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:16px;">
<tr><td style="height:1px;background-color:#CBA135;"></td></tr>
</table>
<p style="margin:0;font-size:10px;color:#B08D57;line-height:1.6;letter-spacing:1px;text-transform:uppercase;">
VOXMILL AUTOMATIONS — CONFIDENTIAL | {year}
</p>
<p style="margin:8px 0 0;font-size:10px;color:#AFAFAF;line-height:1.5;">
This briefing contains proprietary analysis for authorized recipients only
</p>
</td>
</tr>

</table>

</td>
</tr>
</table>

</body>
</html>"""

def send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None):
    """Send executive email"""
    
    logger.info("=" * 70)
    logger.info("VOXMILL EXECUTIVE EMAIL SENDER")
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
        
        logo_exists = Path(logo_path).exists()
        if logo_exists:
            logger.info(f"✅ Logo: {logo_path}")
        else:
            logger.warning(f"⚠️  Logo not found at {logo_path}, using text fallback")
            logo_path = None
        
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
        
        # Step 5: Attach logo (if available)
        if logo_path:
            logger.info("Step 5/6: Attaching inline logo...")
            with open(logo_path, 'rb') as f:
                logo_img = MIMEImage(f.read())
                logo_img.add_header('Content-ID', '<voxmill_logo>')
                logo_img.add_header('Content-Disposition', 'inline', filename='voxmill_logo.png')
                msg.attach(logo_img)
            logger.info("✅ Logo embedded")
        else:
            logger.info("Step 5/6: Skipping logo (using text fallback)")
        
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
        print("Usage: python VOXMILL_FINAL_email_sender.py <email> <name> <area> [city] [pdf] [logo]")
        print("\nExample:")
        print("  python VOXMILL_FINAL_email_sender.py john@example.com 'John Smith' Mayfair London")
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

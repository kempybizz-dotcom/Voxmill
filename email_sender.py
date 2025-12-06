"""
VOXMILL EXECUTIVE EMAIL SENDER V2.1 (PRODUCTION-READY)
========================================================
Matte black + bronze luxury aesthetic
Auto-generates logo if not found
No emojis - pure professional design

✅ V2.1 FIXES:
   • Fixed undefined variable errors (voxmill_email, voxmill_email_password)
   • Added retry logic with exponential backoff (3 attempts)
   • Proper environment variable loading
   • Complete error handling
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
from io import BytesIO
import time

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
DEFAULT_PDF_PATH = "/tmp/Voxmill_Executive_Intelligence_Deck.pdf"
DEFAULT_LOGO_PATH = "/opt/render/project/src/voxmill_logo.png"

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

def generate_logo_svg():
    """Generate Voxmill logo as SVG (fallback if PNG not found)"""
    svg = """<svg width="120" height="120" viewBox="0 0 120 120" xmlns="http://www.w3.org/2000/svg">
    <defs>
        <linearGradient id="goldGradient" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" style="stop-color:#A77B3A;stop-opacity:1" />
            <stop offset="50%" style="stop-color:#BFA670;stop-opacity:1" />
            <stop offset="100%" style="stop-color:#A77B3A;stop-opacity:1" />
        </linearGradient>
        <filter id="glow">
            <feGaussianBlur stdDeviation="4" result="coloredBlur"/>
            <feMerge>
                <feMergeNode in="coloredBlur"/>
                <feMergeNode in="SourceGraphic"/>
            </feMerge>
        </filter>
    </defs>
    <rect width="120" height="120" fill="#0C0C0C"/>
    <path d="M60,20 L85,60 L60,100 L35,60 Z" fill="url(#goldGradient)" stroke="#BFA670" stroke-width="2" filter="url(#glow)"/>
    <text x="60" y="72" font-family="Playfair Display, serif" font-size="52" font-weight="700" fill="#0C0C0C" text-anchor="middle">V</text>
</svg>"""
    return svg.encode('utf-8')

def get_logo_bytes(logo_path):
    """Get logo as bytes - tries PNG first, generates SVG fallback"""
    # Try multiple paths
    possible_paths = [
        logo_path,
        "/opt/render/project/src/voxmill_logo.png",
        "/tmp/voxmill_logo.png",
        "voxmill_logo.png"
    ]
    
    for path in possible_paths:
        if Path(path).exists():
            logger.info(f"✅ Logo found: {path}")
            with open(path, 'rb') as f:
                return f.read(), 'png'
    
    # Generate SVG fallback
    logger.info("⚠️  PNG not found, generating SVG logo...")
    
    # Try to convert SVG to PNG using cairosvg if available
    try:
        import cairosvg
        svg_bytes = generate_logo_svg()
        png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=120, output_height=120)
        logger.info("✅ Generated PNG from SVG")
        return png_bytes, 'png'
    except ImportError:
        logger.info("✅ Using SVG logo (cairosvg not available)")
        return generate_logo_svg(), 'svg+xml'

def create_voxmill_email(recipient_name, area, city):
    """Create executive-grade email matching PDF aesthetic"""
    
    year = datetime.now().year
    date = datetime.now().strftime('%B %d, %Y')
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Voxmill Executive Intelligence</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
</head>
<body style="margin:0;padding:0;background:#000000;font-family:'Inter',Arial,sans-serif;">

<!-- OUTER CONTAINER -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#000000;margin:0;padding:40px 0;">
<tr>
<td align="center">

<!-- MAIN CARD 700px -->
<table width="700" cellpadding="0" cellspacing="0" border="0" style="max-width:700px;background:linear-gradient(135deg,#0C0C0C 0%,#111111 100%);border:3px solid #BFA670;border-radius:0;margin:0 auto;position:relative;">

<!-- GOLD TOP BAR -->
<tr>
<td style="height:3px;background:linear-gradient(90deg,#A77B3A 0%,#BFA670 50%,#A77B3A 100%);"></td>
</tr>

<!-- HEADER WITH LOGO -->
<tr>
<td style="padding:48px 48px 32px;text-align:center;background:linear-gradient(135deg,#0C0C0C 0%,#111111 100%);">
<!-- Logo -->
<img src="cid:voxmill_logo" alt="Voxmill" width="120" height="120" style="display:block;margin:0 auto 20px;filter:drop-shadow(0 8px 40px rgba(198,161,91,0.9));" />
<!-- Label -->
<div style="font-family:'Inter',Arial,sans-serif;font-size:11px;font-weight:600;color:#A77B3A;letter-spacing:0.2em;text-transform:uppercase;margin-bottom:8px;">
CONFIDENTIAL EXECUTIVE BRIEF
</div>
<!-- Title -->
<h1 style="margin:0;font-family:'Playfair Display','Times New Roman',serif;font-size:42px;font-weight:700;color:#BFA670;letter-spacing:0.01em;line-height:1.2;">
Market Intelligence Snapshot
</h1>
<!-- Subtitle -->
<div style="font-family:'Inter',Arial,sans-serif;font-size:16px;color:rgba(234,234,234,0.85);letter-spacing:0.1em;text-transform:uppercase;margin-top:12px;">
<span style="color:#BFA670;font-weight:600;">{area}</span> · {city}
</div>
<div style="font-family:'Inter',Arial,sans-serif;font-size:11px;color:#B8B8B8;letter-spacing:0.15em;margin-top:8px;font-style:italic;">
Precision Intelligence. Strategic Foresight. Capital Deployment Clarity.
</div>
</td>
</tr>

<!-- DIVIDER -->
<tr>
<td style="padding:0 48px;">
<div style="height:2px;background:rgba(191,166,112,0.2);"></div>
</td>
</tr>

<!-- MAIN CONTENT -->
<tr>
<td style="padding:48px;">

<!-- Greeting -->
<p style="margin:0 0 32px;font-family:'Inter',Arial,sans-serif;font-size:16px;color:#EAEAEA;line-height:1.6;">
{recipient_name},
</p>

<!-- Body Text - COLD OUTREACH VERSION -->
<p style="margin:0 0 24px;font-family:'Inter',Arial,sans-serif;font-size:15px;color:rgba(234,234,234,0.85);line-height:1.85;">
We've identified you as a strategic player in the <strong style="color:#BFA670;">{area}</strong> market.
</p>

<p style="margin:0 0 24px;font-family:'Inter',Arial,sans-serif;font-size:15px;color:rgba(234,234,234,0.85);line-height:1.85;">
I've prepared a complimentary <strong style="color:#BFA670;">Voxmill Executive Intelligence</strong> analysis for <strong style="color:#BFA670;">{area}, {city}</strong> — the same institutional-grade intelligence used by leading hedge funds and private equity firms.
</p>

<p style="margin:0 0 28px;font-family:'Inter',Arial,sans-serif;font-size:15px;color:rgba(234,234,234,0.85);line-height:1.85;">
This report includes proprietary market signals we believe could significantly impact your positioning in the next 30-90 days.
</p>

<!-- HIGHLIGHT BOX - Matte Black Card -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:rgba(10,10,10,0.55);border:1px solid rgba(255,215,167,0.06);border-radius:10px;margin:32px 0;">
<tr>
<td style="padding:32px;">

<!-- Box Title -->
<div style="font-family:'Inter',Arial,sans-serif;font-size:12px;font-weight:700;color:#BFA670;letter-spacing:0.8px;text-transform:uppercase;margin-bottom:20px;border-bottom:1px solid rgba(191,166,112,0.2);padding-bottom:12px;">
EXECUTIVE SUMMARY
</div>

<!-- Bullet List -->
<table cellpadding="0" cellspacing="0" border="0" style="width:100%;">
<tr><td style="padding:8px 0;font-family:'Inter',Arial,sans-serif;font-size:14px;color:#EAEAEA;line-height:1.7;">
<span style="color:#BFA670;font-weight:700;margin-right:12px;">—</span>Market Overview — KPI dashboard with velocity & liquidity metrics
</td></tr>
<tr><td style="padding:8px 0;font-family:'Inter',Arial,sans-serif;font-size:14px;color:#EAEAEA;line-height:1.7;">
<span style="color:#BFA670;font-weight:700;margin-right:12px;">—</span>Competitive Landscape — Institutional positioning & market share analysis
</td></tr>
<tr><td style="padding:8px 0;font-family:'Inter',Arial,sans-serif;font-size:14px;color:#EAEAEA;line-height:1.7;">
<span style="color:#BFA670;font-weight:700;margin-right:12px;">—</span>Strategic Forecast — 30/90-day projections with AI-powered insights
</td></tr>
<tr><td style="padding:8px 0;font-family:'Inter',Arial,sans-serif;font-size:14px;color:#EAEAEA;line-height:1.7;">
<span style="color:#BFA670;font-weight:700;margin-right:12px;">—</span>Top Opportunities — Algorithm-flagged acquisition targets
</td></tr>
</table>

</td>
</tr>
</table>

<!-- CTA BUTTON -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:40px 0 32px;">
<tr>
<td align="center">
<!--[if mso]>
<v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="cid:voxmill_report_pdf" style="height:50px;v-text-anchor:middle;width:240px;" arcsize="12%" stroke="f" fillcolor="#BFA670">
<w:anchorlock/>
<center>
<![endif]-->
<a href="cid:voxmill_report_pdf" style="display:inline-block;background:linear-gradient(90deg,#A77B3A 0%,#BFA670 50%,#A77B3A 100%);color:#0C0C0C;text-decoration:none;padding:16px 40px;border-radius:6px;font-family:'Inter',Arial,sans-serif;font-size:13px;font-weight:700;letter-spacing:1px;text-transform:uppercase;box-shadow:0 6px 16px rgba(191,166,112,0.4);">
VIEW FULL INTELLIGENCE DECK
</a>
<!--[if mso]>
</center>
</v:roundrect>
<![endif]-->
</td>
</tr>
</table>

<!-- Attachment Note -->
<div style="text-align:center;margin:0 0 32px;">
<div style="font-family:'Inter',Arial,sans-serif;font-size:11px;font-weight:600;color:#BFA670;letter-spacing:0.5px;">
FULL REPORT ATTACHED
</div>
<div style="font-family:'Inter',Arial,sans-serif;font-size:12px;color:#B8B8B8;margin-top:6px;">
Voxmill_{area.replace(' ','_')}_{city.replace(' ','_')}_Intelligence.pdf
</div>
</div>

<!-- Closing - COLD OUTREACH VERSION -->
<p style="margin:0 0 20px;font-family:'Inter',Arial,sans-serif;font-size:14px;color:rgba(234,234,234,0.75);line-height:1.75;">
If this intelligence proves valuable, I'd welcome a brief call to discuss how Voxmill can provide ongoing competitive advantage in {area} and your other markets.
</p>

<p style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:14px;color:rgba(234,234,234,0.75);line-height:1.75;">
No obligation — simply evaluate the analysis and decide if it's relevant to your strategy.
</p>

<!-- SIGNATURE BLOCK -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:40px;padding-top:24px;border-top:1px solid rgba(191,166,112,0.2);">
<tr>
<td>
<p style="margin:0 0 4px;font-family:'Inter',Arial,sans-serif;font-size:16px;color:#EAEAEA;font-weight:600;">Olly</p>
<p style="margin:0 0 2px;font-family:'Inter',Arial,sans-serif;font-size:12px;color:#BFA670;font-weight:500;">Founder, Voxmill Market Intelligence</p>
<p style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:11px;color:#B8B8B8;">Institutional-Grade Market Analysis</p>
</td>
</tr>
</table>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td style="padding:32px 48px;background:#0C0C0C;border-top:2px solid rgba(191,166,112,0.15);">
<!-- Gold divider -->
<div style="height:1px;background:linear-gradient(90deg,transparent,#BFA670,transparent);margin-bottom:20px;"></div>
<!-- Footer text -->
<p style="margin:0 0 8px;font-family:'Inter',Arial,sans-serif;font-size:10px;color:#A77B3A;text-align:center;letter-spacing:1px;text-transform:uppercase;">
VOXMILL AUTOMATIONS — CONFIDENTIAL INTELLIGENCE | {year}
</p>
<p style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:10px;color:#B8B8B8;text-align:center;line-height:1.6;">
This briefing contains proprietary analysis for authorized recipients only
</p>
</td>
</tr>

<!-- GOLD BOTTOM BAR -->
<tr>
<td style="height:3px;background:linear-gradient(90deg,#A77B3A 0%,#BFA670 50%,#A77B3A 100%);"></td>
</tr>

</table>

</td>
</tr>
</table>

<!-- MOBILE RESPONSIVE -->
<style type="text/css">
@media only screen and (max-width: 750px) {{
table[width="700"] {{
width: 100% !important;
}}
td {{
padding-left: 24px !important;
padding-right: 24px !important;
}}
h1 {{
font-size: 32px !important;
}}
img[width="120"] {{
width: 80px !important;
height: 80px !important;
}}
}}
</style>

</body>
</html>"""


def send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None, max_attempts=3):
    """
    Send executive email with retry logic
    
    ✅ V2.1 FIXED: Proper variable definitions and retry logic
    """
    
    # ✅ FIX: Load credentials at function start
    try:
        sender_email, sender_password = validate_environment()
    except Exception as e:
        logger.error(f"❌ Failed to load credentials: {e}")
        raise
    
    # Set defaults
    if pdf_path is None:
        pdf_path = DEFAULT_PDF_PATH
    if logo_path is None:
        logo_path = DEFAULT_LOGO_PATH
    
    # Validate PDF exists
    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        logger.error(f"❌ PDF not found: {pdf_path}")
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    
    if pdf_file.stat().st_size == 0:
        logger.error(f"❌ PDF is empty: {pdf_path}")
        raise ValueError(f"PDF file is empty: {pdf_path}")
    
    logger.info(f"✅ PDF validated: {pdf_file.stat().st_size:,} bytes")
    
    # Load logo
    logo_bytes, logo_type = get_logo_bytes(logo_path)
    logger.info(f"✅ Logo loaded: {logo_type.upper()}")
    
    # Build email message
    msg = MIMEMultipart('related')
    msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = f"Market Intelligence Snapshot — {area}"
    
    # HTML content
    html_content = create_voxmill_email(recipient_name, area, city)
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
    # Attach logo
    logo_img = MIMEImage(logo_bytes, _subtype=logo_type)
    logo_img.add_header('Content-ID', '<voxmill_logo>')
    logo_img.add_header('Content-Disposition', 'inline', filename=f'voxmill_logo.{logo_type.split("/")[-1]}')
    msg.attach(logo_img)
    
    # Attach PDF
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
    
    logger.info(f"✅ Email message built: {filename}")
    
    # ✅ RETRY LOGIC with exponential backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"\n📧 Send Attempt {attempt}/{max_attempts}")
            
            # Send via SMTP
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(sender_email, sender_password)  # ✅ FIXED: Using correct variable names
                server.send_message(msg)
            
            logger.info(f"✅ EMAIL SENT (attempt {attempt})")
            logger.info(f"   To: {recipient_email}")
            logger.info(f"   Area: {area}, {city}")
            return True  # SUCCESS
            
        except smtplib.SMTPException as e:
            logger.warning(f"⚠️ SMTP error on attempt {attempt}: {e}")
            
            if attempt < max_attempts:
                wait_time = 2 ** attempt  # Exponential: 2s, 4s, 8s
                logger.info(f"   Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ All {max_attempts} attempts failed")
                raise
        
        except Exception as e:
            logger.error(f"❌ Non-retryable error: {e}")
            raise
    
    return False


def send_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None):
    """
    Integration function (legacy compatibility)
    Now delegates to send_voxmill_email with retry logic
    """
    
    logger.info("=" * 70)
    logger.info("VOXMILL EXECUTIVE EMAIL SENDER V2.1")
    logger.info("=" * 70)
    
    try:
        return send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path, logo_path)
    except Exception as e:
        logger.error(f"\n❌ ERROR: {str(e)}")
        raise


def main():
    """CLI"""
    if len(sys.argv) < 4:
        print("Usage: python email_sender.py <email> <name> <area> [city] [pdf] [logo]")
        print("\nExample:")
        print("  python email_sender.py john@example.com 'John Smith' Mayfair London")
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

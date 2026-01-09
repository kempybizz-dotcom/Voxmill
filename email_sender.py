"""
VOXMILL EXECUTIVE EMAIL SENDER V2.2 (MOBILE-OPTIMIZED)
========================================================
✅ V2.2 MOBILE FIXES:
   • Fluid container (600px max, 100% on mobile)
   • Responsive padding (48px → 20px on mobile)
   • Typography scales (42px → 28px on mobile)
   • Executive summary box optimized for narrow screens
   • Touch-friendly CTA button (min 44px height)
   • Proper line-height for readability on small screens
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
    
    logger.info("⚠️  PNG not found, generating SVG logo...")
    
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
    """Create mobile-optimized executive email"""
    
    year = datetime.now().year
    date = datetime.now().strftime('%B %d, %Y')
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="format-detection" content="telephone=no,date=no,address=no,email=no">
<title>Voxmill Executive Intelligence</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">

<style type="text/css">
/* CLIENT-SPECIFIC RESETS */
body, table, td, a {{ -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }}
table, td {{ mso-table-lspace: 0pt; mso-table-rspace: 0pt; }}
img {{ -ms-interpolation-mode: bicubic; border: 0; outline: none; text-decoration: none; }}

/* MOBILE RESPONSIVE */
@media only screen and (max-width: 640px) {{
    /* Container */
    .email-container {{
        width: 100% !important;
        min-width: 100% !important;
    }}
    
    /* Padding reduction */
    .mobile-padding {{
        padding-left: 20px !important;
        padding-right: 20px !important;
    }}
    
    .mobile-padding-sm {{
        padding-left: 16px !important;
        padding-right: 16px !important;
    }}
    
    /* Typography scaling */
    .mobile-title {{
        font-size: 28px !important;
        line-height: 1.3 !important;
    }}
    
    .mobile-subtitle {{
        font-size: 14px !important;
    }}
    
    .mobile-body {{
        font-size: 14px !important;
        line-height: 1.7 !important;
    }}
    
    .mobile-small {{
        font-size: 10px !important;
    }}
    
    /* Logo scaling */
    .mobile-logo {{
        width: 80px !important;
        height: 80px !important;
        margin-bottom: 16px !important;
    }}
    
    /* Button optimization */
    .mobile-button {{
        font-size: 12px !important;
        padding: 14px 24px !important;
        min-height: 44px !important;
    }}
    
    /* Summary box */
    .mobile-summary {{
        padding: 20px !important;
    }}
    
    .mobile-summary-item {{
        font-size: 13px !important;
        padding: 6px 0 !important;
    }}
    
    /* Spacing adjustments */
    .mobile-spacing {{
        padding-top: 24px !important;
        padding-bottom: 24px !important;
    }}
}}
</style>
</head>
<body style="margin:0;padding:0;background:#000000;font-family:'Inter',Arial,sans-serif;">

<!-- OUTER WRAPPER -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#000000;margin:0;padding:20px 0;">
<tr>
<td align="center" style="padding:0;">

<!-- MAIN CONTAINER (600px max, fluid on mobile) -->
<table class="email-container" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;background:linear-gradient(135deg,#0C0C0C 0%,#111111 100%);border:2px solid #BFA670;margin:0 auto;">

<!-- GOLD TOP BAR -->
<tr>
<td style="height:3px;background:linear-gradient(90deg,#A77B3A 0%,#BFA670 50%,#A77B3A 100%);"></td>
</tr>

<!-- HEADER WITH LOGO -->
<tr>
<td class="mobile-padding mobile-spacing" style="padding:40px 32px 28px;text-align:center;background:linear-gradient(135deg,#0C0C0C 0%,#111111 100%);">
<!-- Logo -->
<img src="cid:voxmill_logo" alt="Voxmill" class="mobile-logo" width="100" height="100" style="display:block;margin:0 auto 18px;filter:drop-shadow(0 8px 40px rgba(198,161,91,0.9));" />

<!-- Label -->
<div class="mobile-small" style="font-family:'Inter',Arial,sans-serif;font-size:11px;font-weight:600;color:#A77B3A;letter-spacing:0.18em;text-transform:uppercase;margin-bottom:8px;">
CONFIDENTIAL EXECUTIVE BRIEF
</div>

<!-- Title -->
<h1 class="mobile-title" style="margin:0;font-family:'Playfair Display','Times New Roman',serif;font-size:36px;font-weight:700;color:#BFA670;letter-spacing:0.01em;line-height:1.2;">
Market Intelligence<br/>Snapshot
</h1>

<!-- Subtitle -->
<div class="mobile-subtitle" style="font-family:'Inter',Arial,sans-serif;font-size:15px;color:rgba(234,234,234,0.85);letter-spacing:0.08em;text-transform:uppercase;margin-top:12px;">
<span style="color:#BFA670;font-weight:600;">{area}</span> · {city}
</div>

<div class="mobile-small" style="font-family:'Inter',Arial,sans-serif;font-size:11px;color:#B8B8B8;letter-spacing:0.12em;margin-top:8px;font-style:italic;">
Precision Intelligence. Strategic Foresight.
</div>
</td>
</tr>

<!-- DIVIDER -->
<tr>
<td class="mobile-padding-sm" style="padding:0 32px;">
<div style="height:1px;background:rgba(191,166,112,0.2);"></div>
</td>
</tr>

<!-- MAIN CONTENT -->
<tr>
<td class="mobile-padding mobile-spacing" style="padding:36px 32px;">

<!-- Greeting -->
<p class="mobile-body" style="margin:0 0 24px;font-family:'Inter',Arial,sans-serif;font-size:15px;color:#EAEAEA;line-height:1.6;">
{recipient_name},
</p>

<!-- Body Text -->
<p class="mobile-body" style="margin:0 0 20px;font-family:'Inter',Arial,sans-serif;font-size:14px;color:rgba(234,234,234,0.85);line-height:1.75;">
We've identified you as a strategic player in the <strong style="color:#BFA670;">{area}</strong> market.
</p>

<p class="mobile-body" style="margin:0 0 20px;font-family:'Inter',Arial,sans-serif;font-size:14px;color:rgba(234,234,234,0.85);line-height:1.75;">
I've prepared a complimentary <strong style="color:#BFA670;">Voxmill Executive Intelligence</strong> analysis for <strong style="color:#BFA670;">{area}, {city}</strong> — the same institutional-grade intelligence used by leading hedge funds and private equity firms.
</p>

<p class="mobile-body" style="margin:0 0 24px;font-family:'Inter',Arial,sans-serif;font-size:14px;color:rgba(234,234,234,0.85);line-height:1.75;">
This report includes proprietary market signals we believe could significantly impact your positioning in the next 30-90 days.
</p>

<!-- EXECUTIVE SUMMARY BOX -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:rgba(10,10,10,0.6);border:1px solid rgba(255,215,167,0.08);border-radius:8px;margin:24px 0;">
<tr>
<td class="mobile-summary" style="padding:24px;">

<!-- Box Title -->
<div class="mobile-small" style="font-family:'Inter',Arial,sans-serif;font-size:11px;font-weight:700;color:#BFA670;letter-spacing:0.6px;text-transform:uppercase;margin-bottom:16px;border-bottom:1px solid rgba(191,166,112,0.2);padding-bottom:10px;">
EXECUTIVE SUMMARY
</div>

<!-- Bullet List -->
<table cellpadding="0" cellspacing="0" border="0" style="width:100%;">
<tr><td class="mobile-summary-item" style="padding:6px 0;font-family:'Inter',Arial,sans-serif;font-size:13px;color:#EAEAEA;line-height:1.6;">
<span style="color:#BFA670;font-weight:700;margin-right:10px;">—</span>Market Overview — KPI dashboard with velocity & liquidity metrics
</td></tr>
<tr><td class="mobile-summary-item" style="padding:6px 0;font-family:'Inter',Arial,sans-serif;font-size:13px;color:#EAEAEA;line-height:1.6;">
<span style="color:#BFA670;font-weight:700;margin-right:10px;">—</span>Competitive Landscape — Institutional positioning & market share
</td></tr>
<tr><td class="mobile-summary-item" style="padding:6px 0;font-family:'Inter',Arial,sans-serif;font-size:13px;color:#EAEAEA;line-height:1.6;">
<span style="color:#BFA670;font-weight:700;margin-right:10px;">—</span>Strategic Forecast — 30/90-day projections with AI insights
</td></tr>
<tr><td class="mobile-summary-item" style="padding:6px 0;font-family:'Inter',Arial,sans-serif;font-size:13px;color:#EAEAEA;line-height:1.6;">
<span style="color:#BFA670;font-weight:700;margin-right:10px;">—</span>Top Opportunities — Algorithm-flagged acquisition targets
</td></tr>
</table>

</td>
</tr>
</table>

<!-- CTA BUTTON -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:32px 0 24px;">
<tr>
<td align="center">
<!--[if mso]>
<v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="cid:voxmill_report_pdf" style="height:50px;v-text-anchor:middle;width:240px;" arcsize="12%" stroke="f" fillcolor="#BFA670">
<w:anchorlock/>
<center>
<![endif]-->
<a href="cid:voxmill_report_pdf" class="mobile-button" style="display:inline-block;background:linear-gradient(90deg,#A77B3A 0%,#BFA670 50%,#A77B3A 100%);color:#0C0C0C;text-decoration:none;padding:15px 32px;border-radius:6px;font-family:'Inter',Arial,sans-serif;font-size:12px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase;box-shadow:0 4px 12px rgba(191,166,112,0.4);min-height:44px;line-height:1.4;">
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
<div style="text-align:center;margin:0 0 24px;">
<div class="mobile-small" style="font-family:'Inter',Arial,sans-serif;font-size:11px;font-weight:600;color:#BFA670;letter-spacing:0.4px;">
FULL REPORT ATTACHED
</div>
<div class="mobile-small" style="font-family:'Inter',Arial,sans-serif;font-size:10px;color:#B8B8B8;margin-top:4px;word-break:break-all;">
Voxmill_{area.replace(' ','_')}_{city.replace(' ','_')}.pdf
</div>
</div>

<!-- Closing -->
<p class="mobile-body" style="margin:0 0 16px;font-family:'Inter',Arial,sans-serif;font-size:13px;color:rgba(234,234,234,0.75);line-height:1.7;">
If this intelligence proves valuable, I'd welcome a brief call to discuss how Voxmill can provide ongoing competitive advantage in {area} and your other markets.
</p>

<p class="mobile-body" style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:13px;color:rgba(234,234,234,0.75);line-height:1.7;">
No obligation — simply evaluate the analysis and decide if it's relevant to your strategy.
</p>

<!-- SIGNATURE -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:32px;padding-top:20px;border-top:1px solid rgba(191,166,112,0.2);">
<tr>
<td>
<p class="mobile-body" style="margin:0 0 4px;font-family:'Inter',Arial,sans-serif;font-size:15px;color:#EAEAEA;font-weight:600;">Olly</p>
<p class="mobile-small" style="margin:0 0 2px;font-family:'Inter',Arial,sans-serif;font-size:12px;color:#BFA670;font-weight:500;">Founder, Voxmill Market Intelligence</p>
<p class="mobile-small" style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:10px;color:#B8B8B8;">Institutional-Grade Analysis</p>
</td>
</tr>
</table>

</td>
</tr>

<!-- FOOTER -->
<tr>
<td class="mobile-padding mobile-spacing" style="padding:28px 32px;background:#0C0C0C;border-top:1px solid rgba(191,166,112,0.15);">
<div style="height:1px;background:linear-gradient(90deg,transparent,#BFA670,transparent);margin-bottom:16px;"></div>
<p class="mobile-small" style="margin:0 0 6px;font-family:'Inter',Arial,sans-serif;font-size:10px;color:#A77B3A;text-align:center;letter-spacing:0.8px;text-transform:uppercase;">
VOXMILL — CONFIDENTIAL INTELLIGENCE | {year}
</p>
<p class="mobile-small" style="margin:0;font-family:'Inter',Arial,sans-serif;font-size:9px;color:#B8B8B8;text-align:center;line-height:1.5;">
Proprietary analysis for authorized recipients
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

</body>
</html>"""


def send_voxmill_email(recipient_email, recipient_name, area, city, pdf_path=None, logo_path=None, max_attempts=3):
    """Send executive email with retry logic"""
    
    try:
        sender_email, sender_password = validate_environment()
    except Exception as e:
        logger.error(f"❌ Failed to load credentials: {e}")
        raise
    
    if pdf_path is None:
        pdf_path = DEFAULT_PDF_PATH
    if logo_path is None:
        logo_path = DEFAULT_LOGO_PATH
    
    # Validate PDF
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
    
    # Build message
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
    
    # RETRY LOGIC
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"\n📧 Send Attempt {attempt}/{max_attempts}")
            
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            
            logger.info(f"✅ EMAIL SENT (attempt {attempt})")
            logger.info(f"   To: {recipient_email}")
            logger.info(f"   Area: {area}, {city}")
            return True
            
        except smtplib.SMTPException as e:
            logger.warning(f"⚠️ SMTP error on attempt {attempt}: {e}")
            
            if attempt < max_attempts:
                wait_time = 2 ** attempt
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
    """Integration function (legacy compatibility)"""
    
    logger.info("=" * 70)
    logger.info("VOXMILL EXECUTIVE EMAIL SENDER V2.2 (MOBILE-OPTIMIZED)")
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

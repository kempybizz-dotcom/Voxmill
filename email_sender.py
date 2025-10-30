"""
VOXMILL ELITE EMAIL SENDER — FORTUNE 500 REDESIGN
===================================================
Executive-grade HTML email delivery with PDF attachment
Cinematic black/bronze/gold aesthetic
£10,000/month consultancy visual authority

PRODUCTION-GRADE EMAIL DELIVERY.
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from datetime import datetime

# Email configuration
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = os.environ.get('VOXMILL_EMAIL')
SENDER_PASSWORD = os.environ.get('VOXMILL_EMAIL_PASSWORD')
SENDER_NAME = "Olly - Voxmill Market Intelligence"

PDF_FILE = "/tmp/Voxmill_Elite_Intelligence.pdf"

# ============================================================================
# HTML EMAIL TEMPLATE — ELITE EDITION
# ============================================================================

def generate_html_email(recipient_name, area, city):
    """Generate Fortune-500 grade HTML email"""
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@700&family=Inter:wght@400;600&display=swap');
            
            body {{
                margin: 0;
                padding: 0;
                font-family: 'Inter', 'Helvetica Neue', Helvetica, Arial, sans-serif;
                background-color: #0B0B0B;
                -webkit-font-smoothing: antialiased;
                -moz-osx-font-smoothing: grayscale;
            }}
            
            .elite-heading {{
                font-family: 'Playfair Display', Georgia, serif;
                font-weight: 700;
                letter-spacing: 2px;
            }}
            
            .section-label {{
                font-family: 'Inter', sans-serif;
                font-weight: 600;
                letter-spacing: 2px;
                text-transform: uppercase;
                font-size: 11px;
            }}
            
            @media only screen and (max-width: 640px) {{
                .email-container {{
                    width: 100% !important;
                    padding: 20px !important;
                }}
                .content-box {{
                    padding: 25px !important;
                }}
            }}
        </style>
    </head>
    <body style="margin: 0; padding: 0; background-color: #0B0B0B;">
        
        <!-- Outer wrapper -->
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #0B0B0B; padding: 50px 0;">
            <tr>
                <td align="center">
                    
                    <!-- Main email container -->
                    <table class="email-container" width="640" cellpadding="0" cellspacing="0" border="0" style="background-color: #0B0B0B; max-width: 640px;">
                        
                        <!-- Header with watermark -->
                        <tr>
                            <td style="position: relative; padding: 50px 40px 40px 40px; text-align: center;">
                                
                                <!-- Watermark V (semi-transparent) -->
                                <div style="position: absolute; top: 20px; left: 50%; transform: translateX(-50%); opacity: 0.08; z-index: 0;">
                                    <span style="font-family: 'Playfair Display', serif; font-size: 120px; color: #B08D57; font-weight: 700;">V</span>
                                </div>
                                
                                <!-- Logo -->
                                <div style="position: relative; z-index: 1; margin-bottom: 20px;">
                                    <img src="cid:voxmill_logo" alt="Voxmill" width="50" height="50" style="display: inline-block; vertical-align: middle;" />
                                </div>
                                
                                <!-- Brand name -->
                                <h1 class="elite-heading" style="margin: 0 0 5px 0; color: #B08D57; font-size: 26px; letter-spacing: 3px;">
                                    VOXMILL
                                </h1>
                                <p class="section-label" style="margin: 0; color: #AFAFAF; font-size: 10px; letter-spacing: 2px;">
                                    MARKET INTELLIGENCE
                                </p>
                                
                                <!-- Gold divider -->
                                <div style="margin: 30px auto 0 auto; width: 80px; height: 1px; background: linear-gradient(90deg, transparent 0%, #CBA135 50%, transparent 100%);"></div>
                                
                            </td>
                        </tr>
                        
                        <!-- Main content box -->
                        <tr>
                            <td style="padding: 0 40px;">
                                <table class="content-box" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #121212; border-radius: 12px; overflow: hidden;">
                                    <tr>
                                        <td style="padding: 40px;">
                                            
                                            <!-- Greeting -->
                                            <p style="color: #EAEAEA; font-size: 16px; line-height: 26px; margin: 0 0 25px 0; font-weight: 400;">
                                                {recipient_name},
                                            </p>
                                            
                                            <!-- Main message -->
                                            <p style="color: #EAEAEA; font-size: 15px; line-height: 26px; margin: 0 0 25px 0;">
                                                Following our conversation — I've attached this week's <span style="color: #B08D57; font-weight: 600;">Voxmill Market Intelligence</span> report for <span style="color: #CBA135; font-weight: 600;">{area}, {city}</span>.
                                            </p>
                                            
                                            <!-- Report highlights box -->
                                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: rgba(176,141,87,0.08); border: 1px solid rgba(176,141,87,0.25); border-radius: 8px; margin: 30px 0; box-shadow: 0 0 20px rgba(203,161,53,0.08);">
                                                <tr>
                                                    <td style="padding: 25px 30px;">
                                                        
                                                        <!-- Section header -->
                                                        <p class="section-label" style="color: #B08D57; font-size: 11px; margin: 0 0 18px 0; letter-spacing: 2px; text-shadow: 0 0 15px rgba(203,161,53,0.15);">
                                                            📊 REPORT HIGHLIGHTS
                                                        </p>
                                                        
                                                        <!-- Bullet list -->
                                                        <table cellpadding="0" cellspacing="0" border="0">
                                                            <tr>
                                                                <td style="vertical-align: top; padding: 0 12px 12px 0;">
                                                                    <span style="color: #CBA135; font-size: 14px;">▸</span>
                                                                </td>
                                                                <td style="color: #EAEAEA; font-size: 14px; line-height: 22px; padding-bottom: 12px;">
                                                                    40+ luxury properties analyzed with AI-powered deal scoring
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td style="vertical-align: top; padding: 0 12px 12px 0;">
                                                                    <span style="color: #CBA135; font-size: 14px;">▸</span>
                                                                </td>
                                                                <td style="color: #EAEAEA; font-size: 14px; line-height: 22px; padding-bottom: 12px;">
                                                                    Competitor landscape analysis and market positioning
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td style="vertical-align: top; padding: 0 12px 12px 0;">
                                                                    <span style="color: #CBA135; font-size: 14px;">▸</span>
                                                                </td>
                                                                <td style="color: #EAEAEA; font-size: 14px; line-height: 22px; padding-bottom: 12px;">
                                                                    Executive intelligence with actionable insights
                                                                </td>
                                                            </tr>
                                                            <tr>
                                                                <td style="vertical-align: top; padding: 0 12px 0 0;">
                                                                    <span style="color: #CBA135; font-size: 14px;">▸</span>
                                                                </td>
                                                                <td style="color: #EAEAEA; font-size: 14px; line-height: 22px;">
                                                                    Pricing trends and anomaly detection
                                                                </td>
                                                            </tr>
                                                        </table>
                                                        
                                                    </td>
                                                </tr>
                                            </table>
                                            
                                            <!-- Follow-up message -->
                                            <p style="color: #EAEAEA; font-size: 15px; line-height: 26px; margin: 0 0 25px 0;">
                                                Have a look at the attached PDF. I'll follow up in 24-48 hours to discuss anything that stands out for your portfolio.
                                            </p>
                                            
                                            <!-- Optional CTA button -->
                                            <table cellpadding="0" cellspacing="0" border="0" style="margin: 30px 0 0 0;">
                                                <tr>
                                                    <td style="background: linear-gradient(135deg, #B08D57 0%, #CBA135 100%); border-radius: 8px; padding: 16px 32px; text-align: center;">
                                                        <a href="#" style="color: #FFFFFF; text-decoration: none; font-size: 14px; font-weight: 600; letter-spacing: 1px; text-transform: uppercase;">
                                                            View Full Report
                                                        </a>
                                                    </td>
                                                </tr>
                                            </table>
                                            
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                        
                        <!-- Signature -->
                        <tr>
                            <td style="padding: 35px 40px 20px 40px;">
                                <p style="color: #EAEAEA; font-size: 15px; line-height: 26px; margin: 0;">
                                    Best,<br>
                                    <span style="color: #B08D57; font-weight: 600; font-size: 16px;">Olly</span><br>
                                    <span style="color: #AFAFAF; font-size: 13px;">Voxmill Market Intelligence</span>
                                </p>
                            </td>
                        </tr>
                        
                        <!-- Footer divider -->
                        <tr>
                            <td style="padding: 20px 40px 0 40px;">
                                <div style="height: 1px; background-color: #2E2E2E;"></div>
                            </td>
                        </tr>
                        
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: #0B0B0B; padding: 30px 40px 40px 40px; text-align: center;">
                                <p class="section-label" style="color: #B08D57; font-size: 11px; margin: 0 0 10px 0; letter-spacing: 2px;">
                                    VOXMILL AUTOMATIONS
                                </p>
                                <p style="color: #6B6B6B; font-size: 11px; line-height: 18px; margin: 0;">
                                    Confidential Market Intelligence | {datetime.now().year}<br>
                                    This report contains proprietary analysis for authorized recipients only
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

def generate_plain_text_email(recipient_name, area, city):
    """Generate plain text fallback"""
    
    text = f"""{recipient_name},

Following our conversation — I've attached this week's Voxmill Market Intelligence report for {area}, {city}.

REPORT HIGHLIGHTS:
▸ 40+ luxury properties analyzed with AI-powered deal scoring
▸ Competitor landscape analysis and market positioning
▸ Executive intelligence with actionable insights
▸ Pricing trends and anomaly detection

Have a look at the attached PDF. I'll follow up in 24-48 hours to discuss anything that stands out for your portfolio.

Best,
Olly
Voxmill Market Intelligence

---
VOXMILL AUTOMATIONS
Confidential Market Intelligence | {datetime.now().year}
This report contains proprietary analysis for authorized recipients only
"""
    
    return text

# ============================================================================
# EMAIL SENDING
# ============================================================================

def send_email(recipient_email, recipient_name, area, city, subject=None):
    """Send professional email with PDF attachment"""
    
    print(f"\n📧 SENDING ELITE EMAIL")
    print(f"   To: {recipient_email}")
    print(f"   Subject: Market intelligence snapshot — {area}")
    
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        raise Exception("Email credentials not configured (VOXMILL_EMAIL, VOXMILL_EMAIL_PASSWORD)")
    
    if not os.path.exists(PDF_FILE):
        raise Exception(f"PDF not found: {PDF_FILE}")
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        msg['To'] = recipient_email
        msg['Subject'] = subject or f"Market intelligence snapshot — {area}"
        
        # Generate email content
        text_content = generate_plain_text_email(recipient_name, area, city)
        html_content = generate_html_email(recipient_name, area, city)
        
        # Attach both versions
        part_text = MIMEText(text_content, 'plain')
        part_html = MIMEText(html_content, 'html')
        
        msg.attach(part_text)
        msg.attach(part_html)
        
        # Embed logo as inline image
        logo_path = os.path.join(os.path.dirname(__file__), 'voxmill_logo.png')
        if os.path.exists(logo_path):
            with open(logo_path, 'rb') as logo_file:
                logo_image = MIMEImage(logo_file.read())
                logo_image.add_header('Content-ID', '<voxmill_logo>')
                logo_image.add_header('Content-Disposition', 'inline', filename='voxmill_logo.png')
                msg.attach(logo_image)
        else:
            print(f"   ⚠️  Logo not found at {logo_path}, email will use text-only header")
        
        # Attach PDF
        with open(PDF_FILE, 'rb') as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype='pdf')
            pdf_attachment.add_header('Content-Disposition', 'attachment', 
                                     filename=f'Voxmill_{city}_{area}_Intelligence.pdf')
            msg.attach(pdf_attachment)
        
        # Send via SMTP
        print(f"   → Connecting to {SMTP_HOST}...")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        print(f"   ✅ Elite email sent successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Email delivery failed: {str(e)}")
        raise

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE EMAIL SENDER — FORTUNE 500 EDITION")
    print("="*70)
    
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python email_sender.py <recipient_email> <recipient_name> <area> [city]")
        print("Example: python email_sender.py john@agency.com 'John Smith' Mayfair London")
        sys.exit(1)
    
    recipient_email = sys.argv[1]
    recipient_name = sys.argv[2]
    area = sys.argv[3]
    city = sys.argv[4] if len(sys.argv) > 4 else "London"
    
    try:
        send_email(recipient_email, recipient_name, area, city)
        print(f"\n✅ Elite email delivery complete")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

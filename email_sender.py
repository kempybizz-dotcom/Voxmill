"""
VOXMILL ELITE EMAIL SENDER
===========================
Professional HTML email delivery with PDF attachment
Voxmill branded layout

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
# HTML EMAIL TEMPLATE
# ============================================================================

def generate_html_email(recipient_name, area, city):
    """Generate professional HTML email"""
    
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
                    <table width="600" cellpadding="0" cellspacing="0" style="background-color: #1A1A1A; border: 1px solid #D4AF37;">
                        
                        <!-- Header -->
                        <tr>
                            <td style="background: linear-gradient(135deg, #1A1A1A 0%, #0A0A0A 100%); padding: 30px; border-bottom: 3px solid #D4AF37;">
                                <table cellpadding="0" cellspacing="0">
                                    <tr>
                                        <td style="padding-right: 15px; vertical-align: middle;">
                                            <img src="cid:voxmill_logo" alt="Voxmill" width="50" height="50" style="display: block;" />
                                        </td>
                                        <td style="vertical-align: middle;">
                                            <h1 style="margin: 0; color: #D4AF37; font-size: 24px; font-weight: bold; letter-spacing: 2px;">
                                                VOXMILL
                                            </h1>
                                            <p style="margin: 5px 0 0 0; color: #E8E8E8; font-size: 12px; letter-spacing: 1px;">
                                                MARKET INTELLIGENCE
                                            </p>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                        
                        <!-- Body -->
                        <tr>
                            <td style="padding: 40px 30px;">
                                <p style="color: #E8E8E8; font-size: 16px; line-height: 24px; margin: 0 0 20px 0;">
                                    {recipient_name},
                                </p>
                                
                                <p style="color: #E8E8E8; font-size: 14px; line-height: 22px; margin: 0 0 20px 0;">
                                    Following our conversation — I've attached this week's <b style="color: #D4AF37;">Voxmill Market Intelligence</b> report for <b style="color: #D4AF37;">{area}, {city}</b>.
                                </p>
                                
                                <div style="background-color: #0A0A0A; border-left: 4px solid #D4AF37; padding: 20px; margin: 25px 0;">
                                    <p style="color: #D4AF37; font-size: 13px; font-weight: bold; margin: 0 0 10px 0; letter-spacing: 1px;">
                                        📊 REPORT HIGHLIGHTS
                                    </p>
                                    <ul style="color: #E8E8E8; font-size: 13px; line-height: 20px; margin: 0; padding-left: 20px;">
                                        <li>40+ luxury properties analyzed with AI-powered deal scoring</li>
                                        <li>Competitor landscape analysis and market positioning</li>
                                        <li>Executive intelligence with actionable insights</li>
                                        <li>Pricing trends and anomaly detection</li>
                                    </ul>
                                </div>
                                
                                <p style="color: #E8E8E8; font-size: 14px; line-height: 22px; margin: 0 0 20px 0;">
                                    Have a look at the attached PDF. I'll follow up in 24-48 hours to discuss anything that stands out for your portfolio.
                                </p>
                                
                                <p style="color: #E8E8E8; font-size: 14px; line-height: 22px; margin: 30px 0 0 0;">
                                    Best,<br>
                                    <b style="color: #D4AF37;">Olly</b><br>
                                    <span style="color: #B8960C; font-size: 12px;">Voxmill Market Intelligence</span>
                                </p>
                            </td>
                        </tr>
                        
                        <!-- Footer -->
                        <tr>
                            <td style="background-color: #0A0A0A; padding: 20px 30px; border-top: 1px solid #4A4A4A;">
                                <p style="color: #6B5507; font-size: 11px; line-height: 16px; margin: 0; text-align: center;">
                                    <b style="color: #D4AF37;">VOXMILL AUTOMATIONS</b><br>
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
• 40+ luxury properties analyzed with AI-powered deal scoring
• Competitor landscape analysis and market positioning
• Executive intelligence with actionable insights
• Pricing trends and anomaly detection

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
    
    print(f"\n📧 SENDING EMAIL")
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
        
        print(f"   ✅ Email sent successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Email failed: {str(e)}")
        raise

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE EMAIL SENDER")
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
        print(f"\n✅ Email delivery complete")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

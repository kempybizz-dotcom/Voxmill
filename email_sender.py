"""
VOXMILL ABSOLUTE LUXURY EMAIL SENDER
====================================
Production-ready email sender with pixel-perfect LexCura-level design
Integrates with voxmill_master.py
Perfect iPhone + desktop rendering
"""

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def create_luxury_template(recipient_name, area, city):
    """Create absolute luxury email template"""
    
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
            background-color: #0A0A0A !important;
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
            max-width: 600px;
            margin: 0 auto;
            background-color: #0A0A0A;
            color: #FFFFFF;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Helvetica, Arial, sans-serif;
        }}
        
        /* Header */
        .header {{
            padding: 50px 40px 40px 40px;
            text-align: center;
            background-color: #0A0A0A;
            border-top: 4px solid #CBA135;
        }}
        
        .logo-diamond {{
            width: 64px;
            height: 64px;
            background: linear-gradient(135deg, #CBA135 0%, #E5C866 50%, #CBA135 100%);
            transform: rotate(45deg);
            margin: 0 auto 24px auto;
            position: relative;
            border-radius: 4px;
            box-shadow: 0 8px 32px rgba(203, 161, 53, 0.25);
        }}
        
        .logo-v {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%) rotate(-45deg);
            font-family: Georgia, 'Times New Roman', serif;
            font-size: 32px;
            font-weight: bold;
            color: #0A0A0A;
            line-height: 1;
        }}
        
        .brand-name {{
            font-size: 28px;
            font-weight: 700;
            color: #FFFFFF;
            letter-spacing: 4px;
            margin-bottom: 8px;
        }}
        
        .brand-tagline {{
            font-size: 12px;
            color: #CBA135;
            letter-spacing: 3px;
            text-transform: uppercase;
            font-weight: 500;
            opacity: 0.9;
        }}
        
        /* Main content */
        .main-content {{
            padding: 40px 40px 50px 40px;
            background-color: #0A0A0A;
        }}
        
        .greeting {{
            font-size: 18px;
            color: #FFFFFF;
            margin-bottom: 32px;
            font-weight: 400;
        }}
        
        .headline {{
            font-size: 32px;
            font-weight: 700;
            color: #FFFFFF;
            text-align: center;
            margin-bottom: 16px;
            line-height: 1.2;
            letter-spacing: -0.5px;
        }}
        
        .subheadline {{
            font-size: 14px;
            color: #CBA135;
            text-align: center;
            margin-bottom: 40px;
            letter-spacing: 2px;
            text-transform: uppercase;
            font-weight: 600;
            opacity: 0.9;
        }}
        
        .description {{
            font-size: 16px;
            color: #E8E8E8;
            line-height: 1.6;
            margin-bottom: 36px;
            text-align: center;
            font-weight: 400;
        }}
        
        /* Executive summary box */
        .executive-box {{
            background: linear-gradient(135deg, #111111 0%, #0F0F0F 100%);
            border-left: 4px solid #CBA135;
            border-radius: 0 8px 8px 0;
            padding: 32px 28px;
            margin: 36px 0;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.4);
        }}
        
        .executive-title {{
            font-size: 11px;
            color: #CBA135;
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 20px;
            font-weight: 700;
        }}
        
        .executive-item {{
            font-size: 15px;
            color: #FFFFFF;
            margin-bottom: 12px;
            padding-left: 24px;
            position: relative;
            line-height: 1.5;
            font-weight: 400;
        }}
        
        .executive-item:before {{
            content: "";
            position: absolute;
            left: 0;
            top: 10px;
            width: 6px;
            height: 6px;
            background: linear-gradient(45deg, #CBA135, #E5C866);
            border-radius: 50%;
        }}
        
        .executive-item:last-child {{
            margin-bottom: 0;
        }}
        
        /* CTA */
        .cta-section {{
            text-align: center;
            margin: 48px 0 40px 0;
        }}
        
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #CBA135 0%, #E5C866 50%, #CBA135 100%);
            color: #0A0A0A;
            text-decoration: none;
            padding: 18px 48px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 700;
            letter-spacing: 2px;
            text-transform: uppercase;
            box-shadow: 0 8px 24px rgba(203, 161, 53, 0.3);
            transition: all 0.3s ease;
        }}
        
        .follow-up {{
            font-size: 15px;
            color: #B8B8B8;
            text-align: center;
            margin-top: 32px;
            line-height: 1.6;
            font-weight: 400;
        }}
        
        /* Signature */
        .signature {{
            margin-top: 48px;
            padding-top: 32px;
            border-top: 1px solid #1A1A1A;
        }}
        
        .sig-name {{
            font-size: 18px;
            color: #FFFFFF;
            font-weight: 600;
            margin-bottom: 6px;
        }}
        
        .sig-title {{
            font-size: 14px;
            color: #CBA135;
            margin-bottom: 16px;
            letter-spacing: 1px;
            font-weight: 500;
        }}
        
        .sig-company {{
            font-size: 12px;
            color: #888888;
            letter-spacing: 2px;
            text-transform: uppercase;
            font-weight: 500;
        }}
        
        /* Footer */
        .footer {{
            background-color: #0A0A0A;
            padding: 40px 40px 50px 40px;
            text-align: center;
            border-top: 1px solid #1A1A1A;
        }}
        
        .footer-text {{
            font-size: 12px;
            color: #666666;
            line-height: 1.6;
            margin-bottom: 20px;
            font-weight: 400;
        }}
        
        .confidential {{
            font-size: 11px;
            color: #CBA135;
            text-transform: uppercase;
            letter-spacing: 2px;
            font-weight: 600;
            opacity: 0.8;
        }}
        
        /* Mobile responsiveness */
        @media screen and (max-width: 600px) {{
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
            
            .main-content {{
                padding-top: 32px !important;
                padding-bottom: 40px !important;
            }}
            
            .headline {{
                font-size: 26px !important;
                line-height: 1.2 !important;
                margin-bottom: 12px !important;
            }}
            
            .brand-name {{
                font-size: 24px !important;
                letter-spacing: 3px !important;
            }}
            
            .logo-diamond {{
                width: 56px !important;
                height: 56px !important;
            }}
            
            .logo-v {{
                font-size: 28px !important;
            }}
            
            .cta-button {{
                padding: 16px 36px !important;
                font-size: 13px !important;
                letter-spacing: 1.5px !important;
            }}
            
            .executive-box {{
                padding: 24px 20px !important;
                margin: 28px 0 !important;
            }}
            
            .description {{
                font-size: 15px !important;
            }}
            
            .greeting {{
                font-size: 16px !important;
                margin-bottom: 24px !important;
            }}
        }}
        
        /* iPhone specific */
        @media screen and (max-width: 414px) {{
            .headline {{
                font-size: 24px !important;
            }}
            
            .brand-name {{
                font-size: 22px !important;
            }}
        }}
        
        /* Dark mode support */
        @media (prefers-color-scheme: dark) {{
            .email-container,
            .header,
            .main-content,
            .footer {{
                background-color: #0A0A0A !important;
            }}
        }}
    </style>
</head>

<body style="margin: 0; padding: 0; background-color: #0A0A0A;">
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
            
            <div class="greeting">Dear {recipient_name},</div>
            
            <div class="headline">Market Intelligence Snapshot —<br>{area}</div>
            <div class="subheadline">Weekly Precision Report • Confidential Advisory</div>
            
            <div class="description">
                Following our conversation, I've prepared this week's market intelligence 
                briefing for <strong>{area}, {city}</strong>. This analysis provides executive-level 
                insights into current market positioning and competitive dynamics.
            </div>
            
            <!-- Executive Summary -->
            <div class="executive-box">
                <div class="executive-title">📊 Executive Summary</div>
                <div class="executive-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
                <div class="executive-item">Competitor landscape analysis and strategic positioning</div>
                <div class="executive-item">Executive intelligence briefing with actionable insights</div>
                <div class="executive-item">Pricing trend analysis and market anomaly detection</div>
            </div>
            
            <!-- Call to Action -->
            <div class="cta-section">
                <a href="#" class="cta-button">Access Full Report</a>
            </div>
            
            <div class="follow-up">
                I'll follow up within 24-48 hours to discuss strategic implications 
                and how this intelligence can enhance your competitive positioning.
            </div>
            
            <!-- Signature -->
            <div class="signature">
                <div class="sig-name">Olly</div>
                <div class="sig-title">Voxmill Market Intelligence</div>
                <div class="sig-company">Confidential Advisory Services</div>
            </div>
            
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <div class="footer-text">
                © {datetime.now().year} Voxmill Automations • Confidential Market Intelligence<br>
                This briefing contains proprietary analysis for authorized recipients only
            </div>
            <div class="confidential">Voxmill Automations</div>
        </div>
        
    </div>
</body>
</html>
"""

def send_luxury_email(recipient_email, recipient_name, area, city, pdf_path):
    """Send absolute luxury email with PDF attachment"""
    
    print(f"\n📧 SENDING LUXURY EMAIL")
    print(f"   To: {recipient_email}")
    print(f"   Subject: Market intelligence snapshot — {area}")
    
    try:
        # Get email credentials
        sender_email = os.getenv('VOXMILL_EMAIL')
        sender_password = os.getenv('VOXMILL_EMAIL_PASSWORD')
        
        if not sender_email or not sender_password:
            print(f"   ❌ Email credentials not configured")
            print(f"   Set VOXMILL_EMAIL and VOXMILL_EMAIL_PASSWORD in Render environment")
            return False
        
        # Create message
        msg = MIMEMultipart('related')
        msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
        msg['To'] = recipient_email
        msg['Subject'] = f"Market intelligence snapshot — {area}"
        
        # Create HTML content
        html_content = create_luxury_template(recipient_name, area, city)
        
        # Attach HTML
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        # Attach PDF if it exists
        if os.path.exists(pdf_path):
            with open(pdf_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                
                # Clean filename
                area_clean = area.replace(' ', '_').replace(',', '')
                city_clean = city.replace(' ', '_').replace(',', '')
                filename = f"Voxmill_{area_clean}_{city_clean}_Intelligence.pdf"
                
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {filename}'
                )
                msg.attach(part)
            
            print(f"   ✅ PDF attached: {filename}")
        else:
            print(f"   ⚠️  PDF not found: {pdf_path}")
        
        # Send email
        print(f"   → Connecting to smtp.gmail.com...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print(f"   ✅ Email sent successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Email sending failed: {str(e)}")
        return False

def main():
    """Test function"""
    
    # Test email creation
    html = create_luxury_template("Test Client", "Mayfair", "London")
    
    with open('/tmp/test_luxury_email.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("✅ Test luxury email created: /tmp/test_luxury_email.html")
    
    return True

if __name__ == "__main__":
    main()

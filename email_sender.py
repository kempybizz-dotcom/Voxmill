"""
VOXMILL CINEMA-GRADE EMAIL TEMPLATE
===================================
LexCura-inspired luxury email design
Rich black background, strategic gold accents, perfect responsive layout
"""

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def create_luxury_email_template(recipient_name, area, city, pdf_path):
    """Create LexCura-level luxury email template"""
    
    # Luxury email HTML with responsive design
    email_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Voxmill Market Intelligence</title>
    <style>
        /* Reset and base styles */
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            line-height: 1.6;
            background-color: #0A0A0A;
            color: #FFFFFF;
            margin: 0;
            padding: 0;
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
        }}
        
        /* Container */
        .email-container {{
            max-width: 600px;
            margin: 0 auto;
            background-color: #0A0A0A;
            padding: 0;
        }}
        
        /* Header */
        .header {{
            background-color: #0A0A0A;
            padding: 40px 40px 20px 40px;
            text-align: center;
            border-top: 3px solid #CBA135;
        }}
        
        .logo-section {{
            margin-bottom: 30px;
        }}
        
        .logo-diamond {{
            width: 60px;
            height: 60px;
            background: linear-gradient(135deg, #CBA135 0%, #D4B55A 100%);
            transform: rotate(45deg);
            margin: 0 auto 20px auto;
            position: relative;
            box-shadow: 0 4px 20px rgba(203, 161, 53, 0.3);
        }}
        
        .logo-v {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%) rotate(-45deg);
            font-family: Georgia, serif;
            font-size: 28px;
            font-weight: bold;
            color: #0A0A0A;
            line-height: 1;
        }}
        
        .brand-name {{
            font-size: 24px;
            font-weight: 700;
            color: #FFFFFF;
            letter-spacing: 3px;
            margin-bottom: 8px;
        }}
        
        .brand-tagline {{
            font-size: 11px;
            color: #CBA135;
            letter-spacing: 2px;
            text-transform: uppercase;
            font-weight: 500;
        }}
        
        /* Main content */
        .main-content {{
            padding: 30px 40px;
            background-color: #0A0A0A;
        }}
        
        .greeting {{
            font-size: 16px;
            color: #FFFFFF;
            margin-bottom: 25px;
            font-weight: 500;
        }}
        
        .headline {{
            font-size: 28px;
            font-weight: 700;
            color: #FFFFFF;
            text-align: center;
            margin-bottom: 15px;
            line-height: 1.3;
        }}
        
        .subheadline {{
            font-size: 14px;
            color: #CBA135;
            text-align: center;
            margin-bottom: 35px;
            letter-spacing: 1px;
            text-transform: uppercase;
            font-weight: 500;
        }}
        
        .description {{
            font-size: 15px;
            color: #E8E8E8;
            line-height: 1.7;
            margin-bottom: 30px;
            text-align: center;
        }}
        
        /* Highlights box */
        .highlights-box {{
            background-color: #111111;
            border-left: 4px solid #CBA135;
            padding: 25px;
            margin: 30px 0;
            border-radius: 0 6px 6px 0;
        }}
        
        .highlights-title {{
            font-size: 12px;
            color: #CBA135;
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 15px;
            font-weight: 600;
        }}
        
        .highlight-item {{
            font-size: 14px;
            color: #FFFFFF;
            margin-bottom: 8px;
            padding-left: 20px;
            position: relative;
        }}
        
        .highlight-item::before {{
            content: "•";
            color: #CBA135;
            font-size: 16px;
            position: absolute;
            left: 0;
            top: 0;
        }}
        
        /* CTA Button */
        .cta-section {{
            text-align: center;
            margin: 40px 0 30px 0;
        }}
        
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #CBA135 0%, #D4B55A 100%);
            color: #0A0A0A;
            text-decoration: none;
            padding: 16px 40px;
            border-radius: 6px;
            font-size: 14px;
            font-weight: 600;
            letter-spacing: 1px;
            text-transform: uppercase;
            box-shadow: 0 4px 15px rgba(203, 161, 53, 0.4);
            transition: all 0.3s ease;
        }}
        
        .cta-button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(203, 161, 53, 0.5);
        }}
        
        .follow-up {{
            font-size: 14px;
            color: #B8B8B8;
            text-align: center;
            margin-top: 25px;
            line-height: 1.6;
        }}
        
        /* Signature */
        .signature {{
            margin-top: 40px;
            padding-top: 25px;
            border-top: 1px solid #222222;
        }}
        
        .sig-name {{
            font-size: 16px;
            color: #FFFFFF;
            font-weight: 600;
            margin-bottom: 4px;
        }}
        
        .sig-title {{
            font-size: 13px;
            color: #CBA135;
            margin-bottom: 15px;
            letter-spacing: 0.5px;
        }}
        
        .sig-company {{
            font-size: 12px;
            color: #999999;
            letter-spacing: 1px;
        }}
        
        /* Footer */
        .footer {{
            background-color: #0A0A0A;
            padding: 30px 40px;
            text-align: center;
            border-top: 1px solid #1A1A1A;
        }}
        
        .footer-text {{
            font-size: 11px;
            color: #666666;
            line-height: 1.5;
            margin-bottom: 15px;
        }}
        
        .confidential {{
            font-size: 10px;
            color: #CBA135;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 500;
        }}
        
        /* Mobile responsiveness */
        @media only screen and (max-width: 600px) {{
            .email-container {{
                width: 100% !important;
                max-width: 100% !important;
            }}
            
            .header,
            .main-content,
            .footer {{
                padding-left: 20px !important;
                padding-right: 20px !important;
            }}
            
            .headline {{
                font-size: 24px !important;
                line-height: 1.2 !important;
            }}
            
            .brand-name {{
                font-size: 20px !important;
                letter-spacing: 2px !important;
            }}
            
            .logo-diamond {{
                width: 50px !important;
                height: 50px !important;
            }}
            
            .logo-v {{
                font-size: 24px !important;
            }}
            
            .cta-button {{
                padding: 14px 30px !important;
                font-size: 13px !important;
            }}
            
            .highlights-box {{
                padding: 20px !important;
            }}
        }}
        
        /* Dark mode support */
        @media (prefers-color-scheme: dark) {{
            .email-container {{
                background-color: #0A0A0A !important;
            }}
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <!-- Header -->
        <div class="header">
            <div class="logo-section">
                <div class="logo-diamond">
                    <div class="logo-v">V</div>
                </div>
                <div class="brand-name">VOXMILL</div>
                <div class="brand-tagline">MARKET INTELLIGENCE</div>
            </div>
        </div>
        
        <!-- Main Content -->
        <div class="main-content">
            <div class="greeting">Dear {recipient_name},</div>
            
            <div class="headline">Market Intelligence Snapshot —<br>{area}</div>
            <div class="subheadline">Weekly Precision Report • Confidential Analysis</div>
            
            <div class="description">
                Following our conversation, I've prepared this week's market intelligence 
                report for <strong>{area}, {city}</strong>. This analysis provides executive-level 
                insights into current market positioning and competitive dynamics.
            </div>
            
            <!-- Highlights -->
            <div class="highlights-box">
                <div class="highlights-title">📊 Report Highlights</div>
                <div class="highlight-item">40+ luxury properties analyzed with AI-powered deal scoring</div>
                <div class="highlight-item">Competitor landscape analysis and market positioning</div>
                <div class="highlight-item">Executive intelligence with actionable insights</div>
                <div class="highlight-item">Pricing trends and anomaly detection</div>
            </div>
            
            <!-- CTA -->
            <div class="cta-section">
                <a href="#" class="cta-button">Access Full Report</a>
            </div>
            
            <div class="follow-up">
                I'll follow up in 24-48 hours to discuss strategic implications 
                and how this intelligence can enhance your competitive positioning.
            </div>
            
            <!-- Signature -->
            <div class="signature">
                <div class="sig-name">Olly</div>
                <div class="sig-title">Voxmill Market Intelligence</div>
                <div class="sig-company">CONFIDENTIAL MARKET ANALYSIS</div>
            </div>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <div class="footer-text">
                © {datetime.now().year} Voxmill Automations • Confidential Market Intelligence<br>
                This report contains proprietary analysis for authorized recipients only
            </div>
            <div class="confidential">VOXMILL AUTOMATIONS</div>
        </div>
    </div>
</body>
</html>
"""
    
    return email_html

def send_luxury_email(recipient_email, recipient_name, area, city, pdf_path):
    """Send LexCura-level luxury email with PDF attachment"""
    
    print(f"\n📧 SENDING LUXURY EMAIL")
    print(f"   To: {recipient_email}")
    print(f"   Market: {area}, {city}")
    
    try:
        # Email credentials
        sender_email = os.getenv('VOXMILL_EMAIL')
        sender_password = os.getenv('VOXMILL_EMAIL_PASSWORD')
        
        if not sender_email or not sender_password:
            print(f"   ⚠️  Email credentials not set - generating template only")
            return False
        
        # Create message
        msg = MIMEMultipart('related')
        msg['From'] = f"Olly - Voxmill Intelligence <{sender_email}>"
        msg['To'] = recipient_email
        msg['Subject'] = f"Market intelligence snapshot — {area}"
        
        # Create HTML content
        html_content = create_luxury_email_template(recipient_name, area, city, pdf_path)
        
        # Attach HTML
        html_part = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(html_part)
        
        # Attach PDF
        if os.path.exists(pdf_path):
            with open(pdf_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                
                filename = f"Voxmill_{area}_{city}_Intelligence.pdf"
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {filename}'
                )
                msg.attach(part)
        
        # Send email
        print(f"   → Connecting to smtp.gmail.com...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print(f"   ✅ Luxury email sent successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Email failed: {str(e)}")
        return False

def main():
    """Test luxury email template"""
    
    # Create test email
    html = create_luxury_email_template("Lead Name", "Mayfair", "London", "/tmp/test.pdf")
    
    # Save for preview
    with open('/tmp/luxury_email_template.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    print("✅ Luxury email template created")
    print("   Preview: /tmp/luxury_email_template.html")
    
    return True

if __name__ == "__main__":
    main()

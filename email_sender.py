#!/usr/bin/env python3
"""
VOXMILL EMAIL SENDER V2.1 (PRODUCTION-READY)
=============================================
Executive email delivery with retry logic and updated messaging

✅ V2.1 PRODUCTION UPDATES:
   • FIXED: Environment variable loading for credentials
   • UPDATED: Professional messaging for trial phase
   • ADDED: 3x retry with exponential backoff
   • IMPROVED: Error handling and logging
"""

import os
import sys
import logging
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SMTP Configuration
SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587


def send_voxmill_email(
    recipient_email: str,
    recipient_name: str,
    area: str,
    city: str,
    pdf_path: str = None,
    logo_path: str = None,
    max_attempts: int = 3
) -> bool:
    """
    Send executive intelligence email with retry logic
    
    ✅ V2.1 UPDATES:
       - Fixed environment variable loading
       - Updated messaging for trial phase
       - Added 3x retry with exponential backoff
    
    Args:
        recipient_email: Client email address
        recipient_name: Client name
        area: Market area (e.g., "Mayfair")
        city: City name (e.g., "London")
        pdf_path: Path to PDF attachment
        logo_path: Path to logo image (optional)
        max_attempts: Maximum retry attempts (default: 3)
    
    Returns:
        True if email sent successfully
    """
    
    # ✅ V2.1 FIX: Load credentials from environment
    sender_email = os.getenv('VOXMILL_EMAIL')
    sender_password = os.getenv('VOXMILL_EMAIL_PASSWORD')
    
    # Validate credentials exist
    if not sender_email or not sender_password:
        logger.error("❌ Email credentials not configured")
        logger.error("   Set VOXMILL_EMAIL and VOXMILL_EMAIL_PASSWORD environment variables")
        raise ValueError("Email credentials not found in environment")
    
    logger.info(f"\n📧 Preparing email for {recipient_name} <{recipient_email}>")
    logger.info(f"   Area: {area}, {city}")
    logger.info(f"   Sender: {sender_email}")
    if pdf_path:
        logger.info(f"   Attachment: {pdf_path}")
    
    # Build email
    msg = MIMEMultipart('alternative')
    msg['From'] = f"Voxmill Intelligence <{sender_email}>"
    msg['To'] = recipient_email
    msg['Subject'] = f"{area} Market Intelligence — Voxmill Executive Brief"
    
    # ✅ V2.1 UPDATE: New professional messaging for trial phase
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
                line-height: 1.6;
                color: #1a1a1a;
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                background-color: #ffffff;
            }}
            .header {{
                border-bottom: 2px solid #000000;
                padding-bottom: 20px;
                margin-bottom: 30px;
            }}
            .logo {{
                font-size: 24px;
                font-weight: 600;
                color: #000000;
                letter-spacing: 1px;
            }}
            .content {{
                margin: 30px 0;
                font-size: 15px;
                line-height: 1.8;
            }}
            .greeting {{
                font-weight: 500;
                margin-bottom: 20px;
            }}
            .body-text {{
                margin-bottom: 20px;
                color: #2a2a2a;
            }}
            .cta {{
                margin: 35px 0;
                padding: 18px 32px;
                background-color: #000000;
                color: #ffffff;
                text-decoration: none;
                border-radius: 4px;
                display: inline-block;
                font-weight: 500;
                text-align: center;
            }}
            .cta:hover {{
                background-color: #2a2a2a;
            }}
            .signature {{
                margin-top: 40px;
                padding-top: 20px;
                border-top: 1px solid #e0e0e0;
            }}
            .signature-name {{
                font-weight: 600;
                color: #000000;
                margin-bottom: 4px;
            }}
            .signature-title {{
                color: #666666;
                font-size: 14px;
            }}
            .footer {{
                margin-top: 40px;
                padding-top: 20px;
                border-top: 1px solid #e0e0e0;
                font-size: 13px;
                color: #666666;
            }}
            .attachment-notice {{
                background-color: #f5f5f5;
                border-left: 3px solid #000000;
                padding: 15px;
                margin: 25px 0;
                font-size: 14px;
            }}
            .market-label {{
                display: inline-block;
                padding: 6px 12px;
                background-color: #000000;
                color: #ffffff;
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.5px;
                margin-bottom: 15px;
                border-radius: 3px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <div class="logo">VOXMILL</div>
        </div>
        
        <div class="content">
            <div class="market-label">{area.upper()} // {city.upper()}</div>
            
            <div class="greeting">
                {recipient_name},
            </div>
            
            <div class="body-text">
                Enclosed is the preliminary intelligence brief currently being trialled with a small group of firms ahead of our 2026 rollout. It reflects the presentation standard and analytical structure used within our private reporting line before client-specific calibration.
            </div>
            
            <div class="body-text">
                You're welcome to review the format at your discretion. Should you wish to see how this framework is adapted to your market and competitor set, I can provide a short walkthrough.
            </div>
            
            {f'''
            <div class="attachment-notice">
                <strong>📎 Attached:</strong> {area} Executive Intelligence Deck ({datetime.now().strftime("%B %Y")})
            </div>
            ''' if pdf_path else ''}
        </div>
        
        <div class="signature">
            <div class="signature-name">Best regards,<br>Oliver</div>
            <div class="signature-title">Director — Voxmill Intelligence</div>
        </div>
        
        <div class="footer">
            <p><strong>Voxmill Intelligence</strong><br>
            Institutional-Grade Market Analysis<br>
            <a href="https://voxmill.com" style="color: #666666;">voxmill.com</a></p>
            
            <p style="font-size: 12px; color: #999999; margin-top: 20px;">
                This communication contains proprietary market intelligence. If you received this in error, please notify the sender immediately.
            </p>
        </div>
    </body>
    </html>
    """
    
    # Attach HTML body
    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)
    
    # Attach PDF if provided
    if pdf_path and os.path.exists(pdf_path):
        try:
            with open(pdf_path, 'rb') as attachment:
                pdf_part = MIMEBase('application', 'pdf')
                pdf_part.set_payload(attachment.read())
            
            encoders.encode_base64(pdf_part)
            
            filename = f"Voxmill_{area.replace(' ', '_')}_Intelligence_Deck.pdf"
            pdf_part.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
            
            msg.attach(pdf_part)
            logger.info(f"   ✅ PDF attachment added: {filename}")
            
        except Exception as e:
            logger.warning(f"   ⚠️  Could not attach PDF: {e}")
    
    # ✅ V2.1 NEW: Retry logic with exponential backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"\n📧 Send Attempt {attempt}/{max_attempts}")
            
            # Connect to SMTP server
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                logger.info(f"   Connecting to {SMTP_HOST}:{SMTP_PORT}...")
                
                server.starttls()
                logger.info(f"   TLS enabled")
                
                server.login(sender_email, sender_password)
                logger.info(f"   Authenticated as {sender_email}")
                
                server.send_message(msg)
                logger.info(f"   Message sent")
            
            logger.info(f"\n✅ EMAIL SENT SUCCESSFULLY (attempt {attempt})")
            logger.info(f"   To: {recipient_email}")
            logger.info(f"   Subject: {area} Market Intelligence — Voxmill Executive Brief")
            
            return True  # SUCCESS - exit retry loop
            
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"❌ SMTP Authentication failed: {e}")
            logger.error("   Check VOXMILL_EMAIL and VOXMILL_EMAIL_PASSWORD")
            raise  # Don't retry auth errors
            
        except smtplib.SMTPException as e:
            logger.warning(f"⚠️  SMTP error on attempt {attempt}: {e}")
            
            if attempt < max_attempts:
                wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                logger.info(f"   Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"\n❌ All {max_attempts} email attempts failed")
                raise
        
        except Exception as e:
            logger.error(f"❌ Non-retryable error on attempt {attempt}: {e}")
            raise
    
    return False


def send_test_email():
    """
    Test email sending functionality
    """
    print("="*70)
    print("VOXMILL EMAIL SENDER - TEST MODE")
    print("="*70)
    
    # Check credentials
    sender_email = os.getenv('VOXMILL_EMAIL')
    sender_password = os.getenv('VOXMILL_EMAIL_PASSWORD')
    
    if not sender_email or not sender_password:
        print("❌ EMAIL CREDENTIALS NOT SET")
        print("\nSet environment variables:")
        print("  export VOXMILL_EMAIL='your-email@gmail.com'")
        print("  export VOXMILL_EMAIL_PASSWORD='your-app-password'")
        return False
    
    print(f"\n✅ Sender configured: {sender_email}")
    print(f"✅ Password configured: {'*' * len(sender_password)}")
    
    # Get test recipient
    test_email = input("\nEnter test recipient email: ").strip()
    if not test_email:
        print("❌ No email provided")
        return False
    
    # Send test
    try:
        success = send_voxmill_email(
            recipient_email=test_email,
            recipient_name="Test User",
            area="Mayfair",
            city="London",
            pdf_path=None,  # No attachment for test
            max_attempts=1
        )
        
        if success:
            print("\n" + "="*70)
            print("✅ TEST EMAIL SENT SUCCESSFULLY")
            print("="*70)
            print(f"Check inbox: {test_email}")
            return True
        else:
            print("\n❌ Test failed")
            return False
            
    except Exception as e:
        print(f"\n❌ Test error: {e}")
        return False


def main():
    """
    CLI entry point
    """
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Test mode
        return 0 if send_test_email() else 1
    
    # Normal mode - expect arguments
    if len(sys.argv) < 5:
        print("Usage: python email_sender.py <email> <name> <area> <city> [pdf_path]")
        print("\nOr for testing:")
        print("  python email_sender.py --test")
        return 1
    
    recipient_email = sys.argv[1]
    recipient_name = sys.argv[2]
    area = sys.argv[3]
    city = sys.argv[4]
    pdf_path = sys.argv[5] if len(sys.argv) > 5 else None
    
    try:
        success = send_voxmill_email(
            recipient_email=recipient_email,
            recipient_name=recipient_name,
            area=area,
            city=city,
            pdf_path=pdf_path,
            max_attempts=3
        )
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Email sending failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

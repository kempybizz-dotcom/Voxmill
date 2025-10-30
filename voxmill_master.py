"""
VOXMILL MASTER ORCHESTRATOR
============================
One command execution of the entire Voxmill intelligence pipeline

USAGE:
    python voxmill_master.py --vertical uk-real-estate --area Mayfair --city London --email john@agency.com --name "John Smith"

WHAT IT DOES:
    1. Collects real market data (Zoopla/Realty APIs)
    2. Analyzes with GPT-4o AI
    3. Generates elite PDF
    4. Sends professional HTML email

ONE COMMAND. FULL EXECUTION.
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPTS = {
    'data_collector': 'data_collector.py',
    'ai_analyzer': 'ai_analyzer.py',
    'pdf_generator': 'pdf_generator.py',
    'email_sender': 'email_sender.py'
}

# ============================================================================
# PIPELINE EXECUTION
# ============================================================================

def run_pipeline(vertical, area, city, recipient_email, recipient_name, skip_email=False):
    """Execute full Voxmill pipeline"""
    
    print("\n" + "="*70)
    print("VOXMILL MASTER ORCHESTRATOR")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Vertical: {vertical}")
    print(f"Area: {area}")
    print(f"City: {city}")
    print(f"Recipient: {recipient_name} <{recipient_email}>")
    print("="*70)
    
    try:
        # Step 1: Data Collection
        print(f"\n[STEP 1/4] DATA COLLECTION")
        print(f"   Collecting real market data...")
        
        result = subprocess.run(
            [sys.executable, SCRIPTS['data_collector'], vertical, area, city],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"\n❌ Data collection failed:")
            print(result.stderr)
            return False
        
        print(result.stdout)
        
        # Step 2: AI Analysis
        print(f"\n[STEP 2/4] AI ANALYSIS")
        print(f"   Analyzing data with GPT-4o...")
        
        result = subprocess.run(
            [sys.executable, SCRIPTS['ai_analyzer']],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"\n❌ AI analysis failed:")
            print(result.stderr)
            return False
        
        print(result.stdout)
        
        # Step 3: PDF Generation
        print(f"\n[STEP 3/4] PDF GENERATION")
        print(f"   Creating elite PDF report...")
        
        result = subprocess.run(
            [sys.executable, SCRIPTS['pdf_generator']],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"\n❌ PDF generation failed:")
            print(result.stderr)
            return False
        
        print(result.stdout)
        
        # Step 4: Email Delivery
        if not skip_email:
            print(f"\n[STEP 4/4] EMAIL DELIVERY")
            print(f"   Sending professional email...")
            
            result = subprocess.run(
                [sys.executable, SCRIPTS['email_sender'], 
                 recipient_email, recipient_name, area, city],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"\n⚠️ Email delivery failed:")
                print(result.stderr)
                print(f"\n   PDF saved locally: /tmp/Voxmill_Elite_Intelligence.pdf")
                return False
            
            print(result.stdout)
        else:
            print(f"\n[STEP 4/4] EMAIL DELIVERY")
            print(f"   ⚠️ Email skipped (--skip-email flag)")
            print(f"   PDF saved: /tmp/Voxmill_Elite_Intelligence.pdf")
        
        # Success
        print("\n" + "="*70)
        print("✅ VOXMILL PIPELINE COMPLETE")
        print("="*70)
        print(f"\n📊 REPORT GENERATED:")
        print(f"   • Vertical: {vertical}")
        print(f"   • Location: {area}, {city}")
        print(f"   • Recipient: {recipient_name}")
        
        if not skip_email:
            print(f"   • Email: SENT to {recipient_email}")
        else:
            print(f"   • Email: SKIPPED (manual send required)")
            print(f"   • PDF: /tmp/Voxmill_Elite_Intelligence.pdf")
        
        print(f"\n🎯 NEXT STEP:")
        if skip_email:
            print(f"   Manually attach PDF and send to {recipient_email}")
        else:
            print(f"   Follow up with {recipient_name} in 24-48 hours")
        
        return True
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    
    parser = argparse.ArgumentParser(
        description='Voxmill Master Orchestrator - Elite Market Intelligence Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # UK Real Estate
  python voxmill_master.py --vertical uk-real-estate --area Mayfair --city London \\
    --email john@agency.com --name "John Smith"
  
  # Miami Real Estate
  python voxmill_master.py --vertical miami-real-estate --area "Miami Beach" --city Miami \\
    --email sarah@realty.com --name "Sarah Johnson"
  
  # UK Luxury Car Rentals
  python voxmill_master.py --vertical uk-car-rentals --city London \\
    --email david@luxurycars.com --name "David Brown"
  
  # Skip email (generate PDF only)
  python voxmill_master.py --vertical uk-real-estate --area Chelsea --city London \\
    --email test@test.com --name "Test" --skip-email

SUPPORTED VERTICALS:
  • uk-real-estate      - UK luxury real estate
  • miami-real-estate   - Miami luxury real estate
  • uk-car-rentals      - UK luxury car rental companies
  • chartering          - Yacht/jet charter companies
        """
    )
    
    parser.add_argument('--vertical', type=str, required=True,
                       choices=['uk-real-estate', 'miami-real-estate', 'uk-car-rentals', 'chartering'],
                       help='Market vertical to analyze')
    
    parser.add_argument('--area', type=str, required=True,
                       help='Target area (e.g., Mayfair, Chelsea, Miami Beach)')
    
    parser.add_argument('--city', type=str, default='London',
                       help='Target city (default: London)')
    
    parser.add_argument('--email', type=str, required=True,
                       help='Recipient email address')
    
    parser.add_argument('--name', type=str, required=True,
                       help='Recipient name')
    
    parser.add_argument('--skip-email', action='store_true',
                       help='Skip email delivery (generate PDF only)')
    
    args = parser.parse_args()
    
    # Validate environment
    required_env = ['RAPIDAPI_KEY', 'OPENAI_API_KEY']
    missing = [e for e in required_env if not os.environ.get(e)]
    
    if missing:
        print(f"\n❌ ERROR: Missing required environment variables:")
        for var in missing:
            print(f"   • {var}")
        print(f"\nSet these in your Render environment or export locally.")
        sys.exit(1)
    
    if not args.skip_email:
        email_vars = ['VOXMILL_EMAIL', 'VOXMILL_EMAIL_PASSWORD']
        missing_email = [e for e in email_vars if not os.environ.get(e)]
        
        if missing_email:
            print(f"\n⚠️ WARNING: Email credentials not configured:")
            for var in missing_email:
                print(f"   • {var}")
            print(f"\n   Will skip email delivery (PDF only mode)")
            args.skip_email = True
    
    # Run pipeline
    success = run_pipeline(
        vertical=args.vertical,
        area=args.area,
        city=args.city,
        recipient_email=args.email,
        recipient_name=args.name,
        skip_email=args.skip_email
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

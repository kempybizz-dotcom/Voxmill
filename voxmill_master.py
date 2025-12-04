"""
VOXMILL MASTER ORCHESTRATOR
============================
One command execution of the entire Voxmill intelligence pipeline
FULLY UNIVERSAL - Supports all verticals with dynamic terminology

USAGE:
    python voxmill_master.py --vertical uk-real-estate --area Mayfair --city London --email john@agency.com --name "John Smith"

WHAT IT DOES:
    1. Collects real market data (Zoopla/Realty APIs)
    2. Analyzes with GPT-4o AI
    3. Generates elite PDF with vertical-specific terminology
    4. Sends professional HTML email
    5. Saves dataset to MongoDB for WhatsApp service

ONE COMMAND. FULL EXECUTION.
"""

import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timezone

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPTS = {
    'data_collector': 'data_collector.py',
    'ai_analyzer': 'ai_analyzer.py',
    'pdf_generator': 'pdf_generator.py',
    'email_sender': 'email_sender.py'
}

# Vertical configuration mapping
VERTICAL_CONFIG = {
    'uk-real-estate': {
        'type': 'real_estate',
        'name': 'Real Estate',
        'unit_metric': 'sqft',
        'inventory_label': 'Active Listings',
        'value_metric_label': 'Price',
        'velocity_metric_label': 'Absorption Rate',
        'market_signal_label': 'value signals',
        'acquisition_label': 'Acquisition',
        'forward_indicator_label': 'Price Momentum'
    },
    'miami-real-estate': {
        'type': 'real_estate',
        'name': 'Real Estate',
        'unit_metric': 'sqft',
        'inventory_label': 'Active Listings',
        'value_metric_label': 'Price',
        'velocity_metric_label': 'Absorption Rate',
        'market_signal_label': 'value signals',
        'acquisition_label': 'Acquisition',
        'forward_indicator_label': 'Price Momentum'
    },
    'uk-car-rentals': {
        'type': 'luxury_goods',
        'name': 'Luxury Vehicle Fleet',
        'unit_metric': 'unit',
        'inventory_label': 'Active Fleet',
        'value_metric_label': 'Daily Rate',
        'velocity_metric_label': 'Utilization Rate',
        'market_signal_label': 'opportunity signals',
        'acquisition_label': 'Fleet Expansion',
        'forward_indicator_label': 'Demand Pressure'
    },
    'chartering': {
        'type': 'luxury_goods',
        'name': 'Charter Services',
        'unit_metric': 'booking',
        'inventory_label': 'Available Assets',
        'value_metric_label': 'Charter Rate',
        'velocity_metric_label': 'Booking Velocity',
        'market_signal_label': 'market signals',
        'acquisition_label': 'Asset Acquisition',
        'forward_indicator_label': 'Seasonal Demand'
    }
}

# ============================================================================
# MONGODB SAVE FUNCTION
# ============================================================================

def save_to_mongodb(area, vertical, vertical_config):
    """Save generated dataset to MongoDB for WhatsApp service + price_history"""
    try:
        from pymongo import MongoClient
        
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            print("⚠️  MONGODB_URI not set, skipping MongoDB save")
            return False
        
        # Load the analysis data that was just generated
        analysis_file = '/tmp/voxmill_analysis.json'
        if not os.path.exists(analysis_file):
            print(f"⚠️  Analysis file not found: {analysis_file}")
            return False
        
        with open(analysis_file, 'r') as f:
            analysis_data = json.load(f)
        
        # Connect to MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client['Voxmill']
        
        # ==============================================================
        # EXISTING: Save full dataset to 'datasets' collection
        # ==============================================================
        datasets_collection = db['datasets']
        
        dataset_doc = {
            "metadata": {
                "area": area,
                "city": analysis_data.get('metadata', {}).get('city', 'London'),
                "vertical": vertical_config,
                "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
                "property_count": len(analysis_data.get('properties', []))
            },
            "properties": analysis_data.get('properties', []),
            "metrics": analysis_data.get('metrics', analysis_data.get('kpis', {})),
            "kpis": analysis_data.get('kpis', analysis_data.get('metrics', {})),
            "intelligence": analysis_data.get('intelligence', {}),
            "forecast": analysis_data.get('forecast', {})
        }
        
        result = datasets_collection.replace_one(
            {"metadata.area": area, "metadata.vertical.type": vertical_config.get('type')},
            dataset_doc,
            upsert=True
        )
        
        datasets_saved = result.upserted_id or result.modified_count > 0
        
        # ==============================================================
        # NEW: Save agent-level snapshots to 'price_history' collection
        # ==============================================================
        price_history_collection = db['price_history']
        
        properties = analysis_data.get('properties', [])
        agent_snapshots = {}
        
        # Group properties by agent
        for prop in properties:
            agent = prop.get('agent', prop.get('agency', 'Unknown'))
            
            # Skip Private and empty agents
            if not agent or agent.strip() == '' or agent == 'Private' or agent == 'Unknown':
                continue
            
            if agent not in agent_snapshots:
                agent_snapshots[agent] = {
                    'agent': agent,
                    'area': area,
                    'timestamp': datetime.now(timezone.utc),
                    'properties': [],
                    'avg_price': 0,
                    'total_inventory': 0
                }
            
            agent_snapshots[agent]['properties'].append({
                'address': prop.get('address', 'N/A'),
                'price': prop.get('price', 0),
                'type': prop.get('type', prop.get('property_type', 'Unknown')),
                'sqft': prop.get('sqft', 0),
                'days_listed': prop.get('days_listed', prop.get('days_on_market', 0))
            })
        
        # Calculate averages and store each agent snapshot
        price_history_count = 0
        for agent, snapshot in agent_snapshots.items():
            prices = [p['price'] for p in snapshot['properties'] if p['price'] > 0]
            
            if prices:
                snapshot['avg_price'] = sum(prices) / len(prices)
                snapshot['total_inventory'] = len(snapshot['properties'])
                
                # Insert snapshot into price_history
                price_history_collection.insert_one(snapshot)
                price_history_count += 1
        
        client.close()
        
        # Report results
        if datasets_saved:
            print(f"✅ Dataset saved to MongoDB 'datasets' collection for {area}")
        
        if price_history_count > 0:
            print(f"✅ Saved {price_history_count} agent snapshots to 'price_history' collection")
            print(f"   Agents tracked: {', '.join(agent_snapshots.keys())}")
        else:
            print(f"⚠️  No agent-level data to save (all properties Private/Unknown)")
        
        return datasets_saved or price_history_count > 0
        
    except Exception as e:
        print(f"⚠️  Error saving to MongoDB: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

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
    
    # Get vertical configuration
    if vertical not in VERTICAL_CONFIG:
        print(f"\n❌ ERROR: Unsupported vertical '{vertical}'")
        print(f"Supported verticals: {', '.join(VERTICAL_CONFIG.keys())}")
        return False
    
    vertical_config = VERTICAL_CONFIG[vertical]
    print(f"\n📋 Vertical Configuration:")
    print(f"   • Name: {vertical_config['name']}")
    print(f"   • Type: {vertical_config['type']}")
    print(f"   • Unit Metric: {vertical_config['unit_metric']}")
    print(f"   • Inventory Label: {vertical_config['inventory_label']}")
    print(f"   • Velocity Metric: {vertical_config['velocity_metric_label']}")
    
    try:
        # Step 1: Data Collection
        print(f"\n[STEP 1/5] DATA COLLECTION")
        print(f"   Collecting real market data...")
        print(f"   Vertical: {vertical_config['name']}")
        
        # Serialize vertical config as JSON
        vertical_config_json = json.dumps(vertical_config)
        
        result = subprocess.run(
            [sys.executable, SCRIPTS['data_collector'], 
             vertical, area, city, vertical_config_json],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"\n❌ Data collection failed:")
            print(result.stderr)
            return False
        
        print(result.stdout)
        
        # Step 2: AI Analysis
        print(f"\n[STEP 2/5] AI ANALYSIS")
        print(f"   Analyzing data with GPT-4o...")
        print(f"   Context: {vertical_config['name']} intelligence")
        
        # Pass vertical type to AI analyzer
        vertical_type = vertical_config['type']
        
        result = subprocess.run(
            [sys.executable, SCRIPTS['ai_analyzer'], vertical_type],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"\n❌ AI analysis failed:")
            print(result.stderr)
            return False
        
        print(result.stdout)
        
        # Step 3: PDF Generation
        print(f"\n[STEP 3/5] PDF GENERATION")
        print(f"   Creating elite PDF report...")
        print(f"   Terminology: {vertical_config['inventory_label']}, {vertical_config['velocity_metric_label']}")
        
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

         if not skip_email:
            print(f"\n[STEP 5/5] EMAIL DELIVERY")
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
                print(f"\n   PDF saved locally: /tmp/Voxmill_Executive_Intelligence_Deck.pdf")
                return False
            
            print(result.stdout)
        else:
            print(f"\n[STEP 5/5] EMAIL DELIVERY")
            print(f"   ⚠️ Email skipped (--skip-email flag)")
            print(f"   PDF saved: /tmp/Voxmill_Executive_Intelligence_Deck.pdf")
        
        # Success
        print("\n" + "="*70)
        print("✅ VOXMILL PIPELINE COMPLETE")
        print("="*70)
        print(f"\n📊 REPORT GENERATED:")
        print(f"   • Vertical: {vertical_config['name']} ({vertical})")
        print(f"   • Location: {area}, {city}")
        print(f"   • Recipient: {recipient_name}")
        print(f"   • Terminology: {vertical_config['inventory_label']}, {vertical_config['velocity_metric_label']}")
        print(f"   • MongoDB: {'✅ Saved' if mongodb_success else '⚠️ Skipped'}")
        
           if not skip_email:
            print(f"   • Email: SENT to {recipient_email}")
        else:
            print(f"   • Email: SKIPPED (manual send required)")
            print(f"   • PDF: /tmp/Voxmill_Executive_Intelligence_Deck.pdf")
        
        print(f"\n🎯 NEXT STEP:")
        if skip_email:
            print(f"   Manually attach PDF and send to {recipient_email}")
        else:
            print(f"   Follow up with {recipient_name} in 24-48 hours")
        
        return True
        
        # Step 4: MongoDB Save
        print(f"\n[STEP 4/5] MONGODB SAVE")
        print(f"   Saving dataset for WhatsApp service...")
        
        mongodb_success = save_to_mongodb(area, vertical, vertical_config)
        
        if mongodb_success:
             print(f"   • MongoDB: {'✅ Saved' if mongodb_success else '⚠️ Skipped'}")
        else:
            print(f"   ⚠️  MongoDB save failed (non-critical)")
        
        
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
  • uk-real-estate      - UK luxury real estate (sqft, absorption rate, price)
  • miami-real-estate   - Miami luxury real estate (sqft, absorption rate, price)
  • uk-car-rentals      - UK luxury vehicle fleet (daily rates, utilization, fleet)
  • chartering          - Yacht/jet charter services (bookings, booking velocity, charter rates)

VERTICAL-SPECIFIC TERMINOLOGY:
  Real Estate:
    - Unit: sqft
    - Velocity: Absorption Rate
    - Inventory: Active Listings
    - Value: Price
  
  Luxury Goods (Cars/Charter):
    - Unit: unit/booking
    - Velocity: Utilization/Booking Velocity
    - Inventory: Active Fleet/Available Assets
    - Value: Daily/Charter Rate

ENVIRONMENT VARIABLES REQUIRED:
  • RAPIDAPI_KEY        - For UK Real Estate (Rightmove) data
  • REALTY_US_API_KEY   - For Miami Real Estate data (optional)
  • OUTSCRAPER_API_KEY  - For fallback scraping (optional)
  • OPENAI_API_KEY      - For GPT-4o AI analysis
  • VOXMILL_EMAIL       - For email delivery (optional)
  • VOXMILL_EMAIL_PASSWORD - For email delivery (optional)
  • MONGODB_URI         - For WhatsApp service dataset storage (optional)
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

    args = parser.parse_args()
    
    # VALIDATE VERTICAL FIRST (before checking environment)
    if args.vertical not in VERTICAL_CONFIG:
        print(f"\n❌ ERROR: Unsupported vertical '{args.vertical}'")
        print(f"\n✅ Supported verticals:")
        for v, config in VERTICAL_CONFIG.items():
            print(f"   • {v:25s} - {config['name']}")
        print(f"\nExample usage:")
        print(f"   python voxmill_master.py --vertical uk-real-estate --area Mayfair --city London \\")
        print(f"      --email test@example.com --name 'Test User'")
        sys.exit(1)
    
    # Validate environment
    required_env = ['RAPIDAPI_KEY', 'OPENAI_API_KEY']
    
    # Validate environment
    required_env = ['RAPIDAPI_KEY', 'OPENAI_API_KEY']
    missing = [e for e in required_env if not os.environ.get(e)]
    
    if missing:
        print(f"\n❌ ERROR: Missing required environment variables:")
        for var in missing:
            print(f"   • {var}")
        print(f"\nSet these in your Render environment or export locally.")
        print(f"\nFor local testing, you can export them:")
        print(f"   export RAPIDAPI_KEY='your_key_here'")
        print(f"   export OPENAI_API_KEY='your_key_here'")
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
    
    # Validate vertical
    if args.vertical not in VERTICAL_CONFIG:
        print(f"\n❌ ERROR: Unsupported vertical '{args.vertical}'")
        print(f"\nSupported verticals:")
        for v, config in VERTICAL_CONFIG.items():
            print(f"   • {v:20s} - {config['name']}")
        sys.exit(1)
    
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

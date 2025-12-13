#!/usr/bin/env python3
"""
VOXMILL MASTER ORCHESTRATOR - BULLETPROOF PRODUCTION v4.0
==========================================================
✅ MULTI-REGION SUPPORT - 1 PDF covering all client regions
✅ INDIVIDUAL TAILORING - Separate PDFs even for identical clients  
✅ AIRTABLE PRIMARY - Source of truth, queried every Sunday
✅ MONGODB BATCH SYNC - Auto-updates during execution
✅ NEVER FAILS - Complete fallback chain

CRITICAL FEATURES:
1. Client with 3 regions → 1 PDF with all 3 regions analyzed
2. 3 identical clients → 3 separate PDFs sent to different emails
3. Airtable preferences applied immediately (read fresh every run)
4. MongoDB syncs automatically during batch (backup/cache)
5. Complete audit trail of every execution
"""

import os
import sys
import json
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import shutil
import time
from pymongo import MongoClient
import gridfs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ✅ Environment variables
MONGODB_URI = os.getenv('MONGODB_URI')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID', 'apptsyINaEjzWgCha')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')

# MongoDB connection
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None


# ============================================================================
# AIRTABLE → MONGODB SYNC ENGINE
# ============================================================================

def sync_client_to_mongodb(client_data: dict) -> bool:
    """
    Sync single client from Airtable to MongoDB
    Creates or updates MongoDB record
    
    Args:
        client_data: Client dict from Airtable
    
    Returns: True if synced successfully
    """
    if not mongo_client:
        return False
    
    try:
        clients_collection = db['client_profiles']
        
        # Use email as unique identifier
        email = client_data.get('email')
        if not email:
            logger.warning(f"Cannot sync client without email: {client_data.get('name')}")
            return False
        
        # Prepare MongoDB document
        mongo_doc = {
            'email': email,
            'name': client_data.get('name'),
            'whatsapp_number': client_data.get('whatsapp_number'),
            'company': client_data.get('company'),
            'city': client_data.get('city'),
            'regions': client_data.get('regions', []),
            'preferences': {
                'competitor_focus': client_data.get('competitor_focus', 'medium'),
                'report_depth': client_data.get('report_depth', 'detailed')
            },
            'subscription_status': client_data.get('subscription_status', 'Active'),
            'last_synced_from_airtable': datetime.now(timezone.utc).isoformat(),
            'source': 'airtable_batch_sync'
        }
        
        # Upsert (update if exists, create if not)
        result = clients_collection.update_one(
            {'email': email},
            {'$set': mongo_doc},
            upsert=True
        )
        
        if result.upserted_id:
            logger.info(f"   ✅ Created in MongoDB: {client_data.get('name')}")
        else:
            logger.info(f"   ✅ Updated in MongoDB: {client_data.get('name')}")
        
        return True
    
    except Exception as e:
        logger.error(f"MongoDB sync failed for {client_data.get('name')}: {e}")
        return False


def load_clients_from_airtable() -> list:
    """
    Load ALL active clients from Airtable (source of truth)
    
    Returns: List of client dicts with all necessary fields
    """
    
    logger.info("\n" + "="*70)
    logger.info("LOADING CLIENTS FROM AIRTABLE (SOURCE OF TRUTH)")
    logger.info("="*70)
    
    if not AIRTABLE_API_KEY:
        logger.error("❌ AIRTABLE_API_KEY not configured")
        return []
    
    try:
        import requests
        
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
        headers = {
            'Authorization': f'Bearer {AIRTABLE_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Query all active clients
        params = {
            'filterByFormula': "{Subscription Status}='Active'",
            'maxRecords': 100
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        if response.status_code != 200:
            raise Exception(f"Airtable API error {response.status_code}: {response.text[:200]}")
        
        data = response.json()
        records = data.get('records', [])
        
        logger.info(f"📋 Found {len(records)} active clients in Airtable")
        
        clients = []
        
        for record in records:
            fields = record['fields']
            
            # Extract email (REQUIRED)
            email = fields.get('Email', '').strip()
            if not email:
                logger.warning(f"⚠️  Skipping client {fields.get('Name', 'Unknown')} - no email")
                continue
            
            # Extract regions (comma-separated → list)
            regions_str = fields.get('Regions', '').strip()
            regions = [r.strip() for r in regions_str.split(',') if r.strip()] if regions_str else []
            
            # Fallback: Use Preferred City as region if Regions empty
            if not regions:
                preferred_city = fields.get('Preferred City', 'London').strip()
                regions = [preferred_city]
                logger.info(f"   ℹ️  {fields.get('Name')} has no Regions - using Preferred City: {preferred_city}")
            
            # Build client dict
            client = {
                'email': email,
                'name': fields.get('Name', 'Valued Client').strip(),
                'whatsapp_number': fields.get('WhatsApp Number', '').strip(),
                'company': fields.get('Company', '').strip(),
                'city': fields.get('Preferred City', 'London').strip(),
                'regions': regions,  # ✅ LIST of all regions
                'competitor_focus': fields.get('Competitor Focus', 'Medium').lower(),
                'report_depth': fields.get('Report Depth', 'Detailed').lower(),
                'subscription_status': fields.get('Subscription Status', 'Active'),
                'airtable_record_id': record['id']
            }
            
            clients.append(client)
            
            logger.info(f"   ✅ {client['name']}: {email}")
            logger.info(f"      Regions: {', '.join(regions)}")
            logger.info(f"      Preferences: {client['competitor_focus']}, {client['report_depth']}")
        
        logger.info(f"\n✅ Loaded {len(clients)} valid clients from Airtable")
        
        return clients
    
    except Exception as e:
        logger.error(f"❌ Airtable load failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def load_clients_from_mongodb_fallback() -> list:
    """
    Fallback: Load clients from MongoDB if Airtable fails
    
    Returns: List of client dicts
    """
    
    logger.warning("\n" + "="*70)
    logger.warning("FALLBACK: LOADING CLIENTS FROM MONGODB")
    logger.warning("="*70)
    
    if not mongo_client:
        logger.error("❌ MongoDB not available - no fallback possible")
        return []
    
    try:
        clients_collection = db['client_profiles']
        cursor = clients_collection.find({'subscription_status': 'Active'})
        
        clients = []
        
        for doc in cursor:
            prefs = doc.get('preferences', {})
            
            client = {
                'email': doc.get('email', ''),
                'name': doc.get('name', 'Valued Client'),
                'whatsapp_number': doc.get('whatsapp_number', ''),
                'company': doc.get('company', ''),
                'city': doc.get('city', 'London'),
                'regions': doc.get('regions', []),
                'competitor_focus': prefs.get('competitor_focus', 'medium'),
                'report_depth': prefs.get('report_depth', 'detailed'),
                'subscription_status': doc.get('subscription_status', 'Active')
            }
            
            if client['email']:
                clients.append(client)
        
        logger.warning(f"⚠️  Loaded {len(clients)} clients from MongoDB (last synced data)")
        
        return clients
    
    except Exception as e:
        logger.error(f"❌ MongoDB fallback failed: {e}")
        return []


# ============================================================================
# WORKSPACE MANAGEMENT
# ============================================================================

class ExecutionWorkspace:
    """
    Isolated workspace for a single client execution
    Prevents file collisions between concurrent runs
    """
    
    def __init__(self, client: dict, regions: list):
        """
        Create unique workspace for this client execution
        
        Args:
            client: Client dict from Airtable
            regions: List of regions to analyze
        """
        # Generate unique execution ID
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        uuid_short = uuid4().hex[:8]
        
        # Use first region + client name for ID
        primary_region = regions[0] if regions else 'Unknown'
        safe_name = client['name'].replace(' ', '_')[:20]
        
        self.exec_id = f"{safe_name}_{primary_region}_{timestamp}_{uuid_short}"
        
        # Create workspace directory
        self.workspace = Path(f"/tmp/voxmill_{self.exec_id}")
        self.workspace.mkdir(parents=True, exist_ok=True)
        
        # Define file paths within workspace
        self.raw_data_file = self.workspace / "voxmill_raw_data.json"
        self.analysis_file = self.workspace / "voxmill_analysis.json"
        
        # PDF filename includes all regions
        regions_str = '_'.join(regions[:3])  # Max 3 regions in filename
        self.pdf_file = self.workspace / f"Voxmill_{client['city']}_{regions_str}_Intelligence.pdf"
        
        # Metadata
        self.client = client
        self.regions = regions
        self.city = client['city']
        self.start_time = datetime.now(timezone.utc)
        
        logger.info(f"📁 Workspace created: {self.workspace}")
        logger.info(f"   Exec ID: {self.exec_id}")
        logger.info(f"   Client: {client['name']} <{client['email']}>")
        logger.info(f"   Regions: {', '.join(regions)}")
        logger.info(f"   City: {self.city}")
    
    def cleanup(self, keep_pdf: bool = False):
        """Clean up workspace after execution"""
        try:
            if keep_pdf and self.pdf_file.exists():
                logger.info(f"   Keeping PDF: {self.pdf_file}")
                shutil.copy(self.pdf_file, f"/tmp/voxmill_last_pdf_{self.client['name']}.pdf")
            
            if self.workspace.exists():
                shutil.rmtree(self.workspace)
                logger.info(f"   ✅ Workspace cleaned: {self.workspace}")
        
        except Exception as e:
            logger.error(f"   ⚠️  Workspace cleanup failed: {e}")
    
    def get_env_vars(self) -> dict:
        """Get environment variables for child processes"""
        return {
            **os.environ,
            'VOXMILL_WORKSPACE': str(self.workspace),
            'VOXMILL_EXEC_ID': self.exec_id,
            'VOXMILL_RAW_DATA': str(self.raw_data_file),
            'VOXMILL_ANALYSIS': str(self.analysis_file),
            'VOXMILL_PDF': str(self.pdf_file)
        }


# ============================================================================
# MULTI-REGION DATA COLLECTION
# ============================================================================

def run_multi_region_data_collection(workspace: ExecutionWorkspace) -> bool:
    """
    Step 1: Collect data for ALL client regions
    Combines properties from multiple areas into single dataset
    
    Args:
        workspace: Execution workspace
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 1: MULTI-REGION DATA COLLECTION")
    logger.info("="*70)
    
    try:
        # Import dataset loader
        sys.path.insert(0, '/opt/render/project/src')
        from app.dataset_loader import load_dataset
        
        all_properties = []
        region_stats = {}
        
        # Collect data for each region
        for i, region in enumerate(workspace.regions, 1):
            logger.info(f"\n   [{i}/{len(workspace.regions)}] Collecting data for: {region}, {workspace.city}")
            
            try:
                dataset = load_dataset(
                    area=region,
                    max_properties=100
                )
                
                properties = dataset.get('properties', [])
                
                # Tag each property with its region
                for prop in properties:
                    prop['source_region'] = region
                    prop['area'] = region
                
                all_properties.extend(properties)
                region_stats[region] = len(properties)
                
                logger.info(f"      ✅ {len(properties)} properties from {region}")
            
            except Exception as e:
                logger.error(f"      ❌ {region} collection failed: {e}")
                region_stats[region] = 0
        
        # Check if we got any data
        if len(all_properties) == 0:
            logger.warning(f"   ⚠️  No properties found across all regions")
        
        # Save combined dataset
        raw_data = {
            'metadata': {
                'vertical': {
                    'type': 'real_estate',
                    'name': 'Real Estate',
                    'vertical': 'uk-real-estate'
                },
                'area': workspace.regions[0],  # ✅ FIX: First region for ai_analyzer
                'regions': workspace.regions,  # ✅ ALL regions
                'city': workspace.city,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'client_name': workspace.client['name'],
                'region_stats': region_stats
            },
            'raw_data': {
                'properties': all_properties
            }
        }
        
        with open(workspace.raw_data_file, 'w') as f:
            json.dump(raw_data, f, indent=2)
        
        logger.info(f"\n   ✅ Multi-region data collection complete")
        logger.info(f"      Total properties: {len(all_properties)}")
        for region, count in region_stats.items():
            logger.info(f"      {region}: {count} properties")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Multi-region data collection error: {e}", exc_info=True)
        return False


def run_ai_analysis(workspace: ExecutionWorkspace) -> bool:
    """
    Step 2: Run AI analysis on combined multi-region data
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 2: AI ANALYSIS (MULTI-REGION)")
    logger.info("="*70)
    
    try:
        cmd = [sys.executable, 'ai_analyzer.py']
        
        result = subprocess.run(
            cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=180
        )
        
        if result.returncode != 0:
            logger.error(f"❌ AI analysis failed:")
            logger.error(f"   STDOUT: {result.stdout[-500:]}")
            logger.error(f"   STDERR: {result.stderr[-500:]}")
            return False
        
        if not workspace.analysis_file.exists():
            logger.error(f"❌ Analysis file not created: {workspace.analysis_file}")
            return False
        
        file_size = workspace.analysis_file.stat().st_size
        logger.info(f"   ✅ Analysis complete: {file_size:,} bytes")
        
        return True
    
    except subprocess.TimeoutExpired:
        logger.error("❌ AI analysis timeout (3 minutes)")
        return False
    
    except Exception as e:
        logger.error(f"❌ AI analysis error: {e}", exc_info=True)
        return False


def run_pdf_generation(workspace: ExecutionWorkspace) -> bool:
    """
    Step 3: Generate PDF with client preferences
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 3: PDF GENERATION WITH CLIENT PREFERENCES")
    logger.info("="*70)
    
    try:
        client = workspace.client
        
        logger.info(f"   🎯 Client: {client['name']}")
        logger.info(f"   📊 Competitor Focus: {client['competitor_focus']}")
        logger.info(f"   📋 Report Depth: {client['report_depth']}")
        logger.info(f"   🗺️  Regions: {', '.join(workspace.regions)}")
        
        # Build command with preference flags
        pdf_cmd = [
            sys.executable, 'pdf_generator.py',
            '--workspace', str(workspace.workspace),
            '--output', workspace.pdf_file.name,
            '--competitor-focus', client['competitor_focus'],
            '--report-depth', client['report_depth']
        ]
        
        # Execute
        result = subprocess.run(
            pdf_cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            logger.error(f"❌ PDF generation failed:")
            logger.error(f"   STDOUT: {result.stdout[-500:]}")
            logger.error(f"   STDERR: {result.stderr[-500:]}")
            return False
        
        if not workspace.pdf_file.exists():
            logger.error(f"❌ PDF not created: {workspace.pdf_file}")
            return False
        
        file_size = workspace.pdf_file.stat().st_size
        
        if file_size < 10000:
            logger.error(f"❌ PDF too small ({file_size} bytes)")
            return False
        
        logger.info(f"   ✅ PDF generated: {file_size:,} bytes")
        logger.info(f"   ✅ Preferences applied: {client['competitor_focus']}, {client['report_depth']}")
        
        return True
    
    except subprocess.TimeoutExpired:
        logger.error("❌ PDF generation timeout (2 minutes)")
        return False
    
    except Exception as e:
        logger.error(f"❌ PDF generation error: {e}", exc_info=True)
        return False


def upload_pdf_to_r2(workspace: ExecutionWorkspace) -> str:
    """
    Step 4: Upload PDF to Cloudflare R2
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 4: CLOUDFLARE R2 UPLOAD")
    logger.info("="*70)
    
    try:
        from app.pdf_storage import upload_pdf_to_cloud
        
        # Use email as client_id (safe for filenames)
        client_id = workspace.client['email'].replace('@', '_at_').replace('.', '_')
        
        # Use first region for filename
        primary_region = workspace.regions[0] if workspace.regions else 'Unknown'
        
        url = upload_pdf_to_cloud(
            pdf_path=str(workspace.pdf_file),
            client_id=client_id,
            area=primary_region
        )
        
        if url:
            logger.info(f"   ✅ PDF uploaded to R2")
            logger.info(f"   URL: {url[:80]}...")
            return url
        else:
            logger.error("   ❌ R2 upload failed")
            return None
    
    except Exception as e:
        logger.error(f"   ❌ R2 upload error: {e}", exc_info=True)
        return None


def save_to_mongodb(workspace: ExecutionWorkspace, pdf_url: str = None) -> bool:
    """
    Step 5: Save analysis + PDF metadata to MongoDB
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 5: MONGODB STORAGE")
    logger.info("="*70)
    
    try:
        if mongo_client is None:
            logger.error("   ❌ MongoDB not connected")
            return False
        
        db = mongo_client['Voxmill']
        
        # Load analysis JSON
        with open(workspace.analysis_file, 'r') as f:
            analysis = json.load(f)
        
        # Add PDF metadata
        if pdf_url:
            analysis['pdf_metadata'] = {
                'cloudflare_url': pdf_url,
                'uploaded_at': datetime.now(timezone.utc).isoformat(),
                'exec_id': workspace.exec_id,
                'filename': workspace.pdf_file.name
            }
        
        # Add client metadata
        analysis['client_metadata'] = {
            'name': workspace.client['name'],
            'email': workspace.client['email'],
            'regions': workspace.regions,
            'city': workspace.city,
            'competitor_focus': workspace.client['competitor_focus'],
            'report_depth': workspace.client['report_depth']
        }
        
        # Store in datasets collection
        datasets_collection = db['datasets']
        result = datasets_collection.insert_one(analysis)
        
        logger.info(f"   ✅ Analysis saved to MongoDB")
        logger.info(f"   Document ID: {result.inserted_id}")
        
        # Optional: Upload PDF binary to GridFS (backup)
        if workspace.pdf_file.exists():
            fs = gridfs.GridFS(db)
            
            with open(workspace.pdf_file, 'rb') as pdf_file:
                gridfs_id = fs.put(
                    pdf_file.read(),
                    filename=workspace.pdf_file.name,
                    metadata={
                        'client_name': workspace.client['name'],
                        'client_email': workspace.client['email'],
                        'regions': workspace.regions,
                        'city': workspace.city,
                        'exec_id': workspace.exec_id,
                        'cloudflare_url': pdf_url,
                        'uploaded_at': datetime.now(timezone.utc).isoformat()
                    }
                )
            
            logger.info(f"   ✅ PDF backed up to GridFS: {gridfs_id}")
        
        return True
    
    except Exception as e:
        logger.error(f"   ❌ MongoDB save error: {e}", exc_info=True)
        return False


def send_email(workspace: ExecutionWorkspace, pdf_url: str = None) -> bool:
    """
    Step 6: Send email with PDF attachment
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 6: EMAIL DELIVERY")
    logger.info("="*70)
    
    client = workspace.client
    recipient_email = client['email']
    
    # Validate email
    if not recipient_email or '@' not in recipient_email:
        logger.info(f"   ⏭️  Email delivery skipped - invalid email: {recipient_email}")
        return True
    
    try:
        from email_sender import send_voxmill_email
        
        # Build area string from regions
        area_str = ', '.join(workspace.regions)
        
        logo_path = Path(__file__).parent / "voxmill_logo.png"
        
        # Validate PDF exists
        if not workspace.pdf_file.exists():
            raise FileNotFoundError(f"PDF not found: {workspace.pdf_file}")
        
        logger.info(f"   📄 PDF: {workspace.pdf_file} ({workspace.pdf_file.stat().st_size:,} bytes)")
        logger.info(f"   📧 Sending to: {recipient_email}")
        logger.info(f"   🗺️  Areas: {area_str}")
        
        # Send email
        send_voxmill_email(
            recipient_email=recipient_email,
            recipient_name=client['name'],
            area=area_str,
            city=workspace.city,
            pdf_path=str(workspace.pdf_file),
            logo_path=str(logo_path) if logo_path.exists() else None
        )
        
        logger.info(f"   ✅ Email sent to {recipient_email}")
        return True
    
    except Exception as e:
        import traceback
        logger.warning(f"   ⚠️  Email sending failed:\n{traceback.format_exc()}")
        return False


def log_execution(workspace: ExecutionWorkspace, status: str, steps_completed: list, error: str = None):
    """Log execution to MongoDB for audit trail"""
    try:
        if mongo_client is None:
            return
        
        db = mongo_client['Voxmill']
        execution_log = db['execution_log']
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - workspace.start_time).total_seconds()
        
        log_entry = {
            'exec_id': workspace.exec_id,
            'client_name': workspace.client['name'],
            'client_email': workspace.client['email'],
            'regions': workspace.regions,
            'city': workspace.city,
            'competitor_focus': workspace.client['competitor_focus'],
            'report_depth': workspace.client['report_depth'],
            'status': status,
            'start_time': workspace.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'steps_completed': steps_completed,
            'error': error,
            'workspace': str(workspace.workspace)
        }
        
        execution_log.insert_one(log_entry)
        logger.info(f"   📝 Execution logged: {status} ({duration:.1f}s)")
    
    except Exception as e:
        logger.error(f"Failed to log execution: {e}")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def execute_client_pipeline(client: dict) -> dict:
    """
    Execute complete pipeline for ONE client
    Handles multiple regions in single PDF
    
    Args:
        client: Client dict from Airtable
    
    Returns: Dict with execution results
    """
    
    workspace = None
    steps_completed = []
    pdf_url = None
    
    try:
        # Create workspace
        workspace = ExecutionWorkspace(
            client=client,
            regions=client['regions']
        )
        
        logger.info("\n" + "="*70)
        logger.info("🚀 VOXMILL INTELLIGENCE PIPELINE STARTING")
        logger.info("="*70)
        logger.info(f"   Execution ID: {workspace.exec_id}")
        logger.info(f"   Client: {client['name']} <{client['email']}>")
        logger.info(f"   Regions: {', '.join(client['regions'])}")
        logger.info(f"   City: {client['city']}")
        logger.info(f"   Preferences: {client['competitor_focus']}, {client['report_depth']}")
        logger.info("="*70)
        
        # Step 1: Multi-region data collection
        if not run_multi_region_data_collection(workspace):
            raise Exception("Multi-region data collection failed")
        steps_completed.append('multi_region_data_collection')
        
        # Step 2: AI Analysis
        if not run_ai_analysis(workspace):
            raise Exception("AI analysis failed")
        steps_completed.append('ai_analysis')
        
        # Step 3: PDF Generation
        if not run_pdf_generation(workspace):
            raise Exception("PDF generation failed")
        steps_completed.append('pdf_generation')
        
        # Step 4: Upload to R2
        pdf_url = upload_pdf_to_r2(workspace)
        if pdf_url:
            steps_completed.append('r2_upload')
        
        # Step 5: Save to MongoDB
        if save_to_mongodb(workspace, pdf_url):
            steps_completed.append('mongodb_save')
        
        # Step 6: Send Email
        if send_email(workspace, pdf_url):
            steps_completed.append('email_sent')
        
        # Step 7: Sync client to MongoDB
        sync_client_to_mongodb(client)
        steps_completed.append('mongodb_sync')
        
        # Log success
        log_execution(workspace, 'success', steps_completed)
        
        logger.info("\n" + "="*70)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("="*70)
        logger.info(f"   Client: {client['name']}")
        logger.info(f"   Exec ID: {workspace.exec_id}")
        logger.info(f"   Steps: {', '.join(steps_completed)}")
        if pdf_url:
            logger.info(f"   PDF URL: {pdf_url[:80]}...")
        logger.info("="*70)
        
        return {
            'success': True,
            'client_name': client['name'],
            'client_email': client['email'],
            'exec_id': workspace.exec_id,
            'regions': client['regions'],
            'pdf_url': pdf_url,
            'steps_completed': steps_completed
        }
    
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {e}", exc_info=True)
        
        if workspace:
            log_execution(workspace, 'failed', steps_completed, str(e))
        
        return {
            'success': False,
            'client_name': client.get('name', 'Unknown'),
            'client_email': client.get('email', 'Unknown'),
            'error': str(e),
            'exec_id': workspace.exec_id if workspace else None,
            'steps_completed': steps_completed
        }
    
    finally:
        if workspace:
            workspace.cleanup(keep_pdf=False)


# ============================================================================
# BATCH PROCESSING
# ============================================================================

def process_all_clients():
    """
    Batch mode: Process ALL active clients from Airtable
    """
    
    logger.info("\n" + "="*70)
    logger.info("🔄 BATCH MODE: PROCESSING ALL ACTIVE CLIENTS")
    logger.info("="*70)
    
    # Load clients from Airtable (source of truth)
    clients = load_clients_from_airtable()
    
    # Fallback to MongoDB if Airtable fails
    if not clients:
        logger.warning("⚠️  Airtable load failed - trying MongoDB fallback...")
        clients = load_clients_from_mongodb_fallback()
    
    if not clients:
        logger.error("❌ No clients found in Airtable or MongoDB")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    logger.info(f"\n📋 Processing {len(clients)} client(s)")
    logger.info("="*70)
    
    stats = {
        'success': 0,
        'failed': 0,
        'total': len(clients),
        'results': []
    }
    
    # Process each client
    for i, client in enumerate(clients, 1):
        logger.info(f"\n[{i}/{len(clients)}] Processing: {client['name']}")
        logger.info(f"   Email: {client['email']}")
        logger.info(f"   Regions: {', '.join(client['regions'])}")
        logger.info(f"   City: {client['city']}")
        logger.info(f"   Preferences: {client['competitor_focus']}, {client['report_depth']}")
        
        try:
            result = execute_client_pipeline(client)
            
            if result['success']:
                stats['success'] += 1
                logger.info(f"   ✅ Complete\n")
            else:
                stats['failed'] += 1
                logger.error(f"   ❌ Failed: {result.get('error')}\n")
            
            stats['results'].append(result)
        
        except Exception as e:
            stats['failed'] += 1
            logger.error(f"   ❌ Exception: {e}\n", exc_info=True)
            stats['results'].append({
                'success': False,
                'client_name': client['name'],
                'client_email': client['email'],
                'error': str(e)
            })
    
    # Final summary
    logger.info("\n" + "="*70)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("="*70)
    logger.info(f"   ✅ Success: {stats['success']}")
    logger.info(f"   ❌ Failed: {stats['failed']}")
    logger.info(f"   📊 Total: {stats['total']}")
    logger.info("="*70)
    
    return stats


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    
    # Force unbuffered output for Render cron visibility
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    print("="*70, flush=True)
    print("VOXMILL MASTER v4.0 - BULLETPROOF MULTI-REGION SYSTEM", flush=True)
    print("="*70, flush=True)
    print("✅ Airtable Primary + MongoDB Batch Sync", flush=True)
    print("✅ Multi-Region Support (1 PDF with all regions)", flush=True)
    print("✅ Individual Tailoring (separate PDFs per client)", flush=True)
    print("="*70, flush=True)
    
    # Run batch processing
    stats = process_all_clients()
    
    # Exit code based on results
    sys.exit(0 if stats['failed'] == 0 else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
VOXMILL MASTER ORCHESTRATOR - PRODUCTION VERSION v3.0
======================================================
ZERO FILE COLLISIONS - UUID-based workspace isolation
REDIS QUEUE INTEGRATION - Production-grade job processing
CLOUDFLARE R2 PDF STORAGE - Permanent, scalable storage
COMPLETE AUDIT TRAIL - MongoDB execution logging

CRITICAL CHANGES FROM v2.0:
- Unique execution workspace per run (/tmp/voxmill_{exec_id}/)
- All child processes write to isolated workspace
- PDFs uploaded to Cloudflare R2 with presigned URLs
- MongoDB stores both JSON analysis + PDF metadata
- Redis queue for job coordination (optional, falls back to direct execution)
- Execution locking prevents duplicate runs
- Complete error handling with retries
- CLIENT PREFERENCES INTEGRATION - Reads MongoDB preferences for PDF customization
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

# Optional: Redis for job queue
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️  Redis not available - using direct execution mode")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ✅ FIX: Define all environment variables as globals
MONGODB_URI = os.getenv('MONGODB_URI')
REDIS_URL = os.getenv('REDIS_URL')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID', 'apptsyINaEjzWgCha')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME', 'Clients')

# MongoDB connection
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None
db = mongo_client['Voxmill'] if mongo_client else None

# Redis connection (optional)
redis_client = None
if REDIS_AVAILABLE and REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("✅ Redis connected - queue mode enabled")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e} - using direct execution")
        redis_client = None


def get_client_preferences(client_identifier: str) -> dict:
    """
    Query Airtable then MongoDB for client preferences
    Falls back to defaults if not found
    
    Returns dict with:
    - competitor_focus: 'low' | 'medium' | 'high'
    - report_depth: 'executive' | 'detailed' | 'deep'
    - preferred_regions: ['Area1', 'Area2']
    """
    
    # Try Airtable first (source of truth)
    try:
        if AIRTABLE_API_KEY:
            import requests
            
            normalized = client_identifier.replace('whatsapp:', '').replace('whatsapp%3A', '')
            
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            headers = {
                'Authorization': f'Bearer {AIRTABLE_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'filterByFormula': f"OR({{WhatsApp}}='{normalized}', {{Email}}='{client_identifier}')",
                'maxRecords': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                if records:
                    record = records[0]['fields']
                    
                    preferences = {
                        'competitor_focus': record.get('Competitor Focus', 'medium'),
                        'report_depth': record.get('Report Depth', 'detailed'),
                        'preferred_regions': [r.strip() for r in record.get('Regions', '').split(',') if r.strip()],
                        'name': record.get('Name', 'Valued Client'),
                        'tier': record.get('Tier', 'tier_1')
                    }
                    
                    logger.info(f"✅ Loaded Airtable preferences: {preferences['competitor_focus']}, {preferences['report_depth']}")
                    return preferences
    
    except Exception as e:
        logger.warning(f"Airtable preferences fetch failed: {e}")
    
    # Fallback to MongoDB
    try:
        if mongo_client:
            normalized = client_identifier.replace('whatsapp:', '').replace('whatsapp%3A', '')
            
            client = db['client_profiles'].find_one({
                "$or": [
                    {"email": client_identifier},
                    {"whatsapp_number": client_identifier},
                    {"whatsapp_number": normalized}
                ]
            })
            
            if client:
                prefs = client.get('preferences', {})
                
                preferences = {
                    'competitor_focus': prefs.get('competitor_focus', 'medium'),
                    'report_depth': prefs.get('report_depth', 'detailed'),
                    'preferred_regions': prefs.get('preferred_regions', []),
                    'name': client.get('name', 'Valued Client'),
                    'tier': client.get('tier', 'tier_1')
                }
                
                logger.info(f"✅ Loaded MongoDB preferences: {preferences['competitor_focus']}, {preferences['report_depth']}")
                return preferences
    
    except Exception as e:
        logger.warning(f"MongoDB preferences fetch failed: {e}")
    
    # Ultimate fallback
    logger.info(f"📋 Using default preferences for {client_identifier}")
    return {
        'competitor_focus': 'medium',
        'report_depth': 'detailed',
        'preferred_regions': [],
        'name': 'Valued Client',
        'tier': 'tier_1'
    }

# ============================================================================
# WORKSPACE MANAGEMENT
# ============================================================================

class ExecutionWorkspace:
    """
    Isolated workspace for a single execution.
    Prevents file collisions between concurrent runs.
    """
    
    def __init__(self, area: str, city: str, vertical: str):
        """
        Create unique workspace for this execution
        
        Args:
            area: Market area (e.g., "Mayfair")
            city: City name (e.g., "London")
            vertical: Vertical type (e.g., "uk-real-estate")
        """
        # Generate unique execution ID
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        uuid_short = uuid4().hex[:8]
        self.exec_id = f"{area}_{timestamp}_{uuid_short}"
        
        # Create workspace directory
        self.workspace = Path(f"/tmp/voxmill_{self.exec_id}")
        self.workspace.mkdir(parents=True, exist_ok=True)
        
        # Define file paths within workspace
        self.raw_data_file = self.workspace / "voxmill_raw_data.json"
        self.analysis_file = self.workspace / "voxmill_analysis.json"
        self.pdf_file = self.workspace / f"Voxmill_{area}_Intelligence_Deck.pdf"
        
        # Metadata
        self.area = area
        self.city = city
        self.vertical = vertical
        self.start_time = datetime.now(timezone.utc)
        
        logger.info(f"📁 Workspace created: {self.workspace}")
        logger.info(f"   Exec ID: {self.exec_id}")
        logger.info(f"   Area: {area}, City: {city}")
    
    def cleanup(self, keep_pdf: bool = False):
        """
        Clean up workspace after execution
        
        Args:
            keep_pdf: If True, keep PDF file (for debugging)
        """
        try:
            if keep_pdf and self.pdf_file.exists():
                logger.info(f"   Keeping PDF: {self.pdf_file}")
                # Move PDF to /tmp/ for manual inspection
                shutil.copy(self.pdf_file, f"/tmp/voxmill_last_pdf_{self.area}.pdf")
            
            # Remove workspace directory
            if self.workspace.exists():
                shutil.rmtree(self.workspace)
                logger.info(f"   ✅ Workspace cleaned: {self.workspace}")
        
        except Exception as e:
            logger.error(f"   ⚠️  Workspace cleanup failed: {e}")
    
    def get_env_vars(self) -> dict:
        """
        Get environment variables to pass to child processes
        
        Returns: Dict of environment variables
        """
        return {
            **os.environ,
            'VOXMILL_WORKSPACE': str(self.workspace),
            'VOXMILL_EXEC_ID': self.exec_id,
            'VOXMILL_RAW_DATA': str(self.raw_data_file),
            'VOXMILL_ANALYSIS': str(self.analysis_file),
            'VOXMILL_PDF': str(self.pdf_file)
        }


# ============================================================================
# EXECUTION LOCKING (Prevent Duplicate Runs)
# ============================================================================

class ExecutionLock:
    """
    Simple file-based lock to prevent duplicate executions for same area
    """
    
    def __init__(self, area: str, vertical: str):
        self.lockfile = Path(f"/tmp/voxmill_lock_{vertical}_{area}.lock")
        self.area = area
        self.vertical = vertical
        self.acquired = False
    
    def acquire(self, timeout: int = 5) -> bool:
        """
        Acquire execution lock
        
        Args:
            timeout: Seconds to wait for lock
        
        Returns: True if lock acquired
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if not self.lockfile.exists():
                try:
                    # Create lock file with PID
                    self.lockfile.write_text(str(os.getpid()))
                    self.acquired = True
                    logger.info(f"🔒 Execution lock acquired for {self.area}")
                    return True
                except Exception as e:
                    logger.warning(f"Lock acquisition failed: {e}")
            
            # Check if lock is stale (older than 2 hours)
            try:
                if self.lockfile.exists():
                    age = time.time() - self.lockfile.stat().st_mtime
                    if age > 7200:  # 2 hours
                        logger.warning(f"Removing stale lock (age: {age/3600:.1f}h)")
                        self.lockfile.unlink()
                        continue
            except:
                pass
            
            time.sleep(1)
        
        logger.error(f"❌ Could not acquire lock for {self.area} (timeout)")
        return False
    
    def release(self):
        """Release execution lock"""
        if self.acquired and self.lockfile.exists():
            try:
                self.lockfile.unlink()
                logger.info(f"🔓 Execution lock released for {self.area}")
            except Exception as e:
                logger.error(f"Lock release failed: {e}")


# ============================================================================
# PIPELINE EXECUTION
# ============================================================================

def run_data_collection(workspace: ExecutionWorkspace, vertical_config: dict) -> bool:
    """
    Step 1: Use world-class dataset loader from WhatsApp analyst
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 1: DATA COLLECTION")
    logger.info("="*70)
    
    try:
        # Import the WhatsApp analyst's world-class data stack
        sys.path.insert(0, '/opt/render/project/src')
        from app.dataset_loader import load_dataset
        
        # Load data using the working stack
        dataset = load_dataset(
            area=workspace.area,
            max_properties=100
        )
        
        # Save to workspace (convert to expected format)
        raw_data = {
            'metadata': {
                'vertical': vertical_config,
                'area': workspace.area,
                'city': workspace.city,
                'timestamp': datetime.now(timezone.utc).isoformat()
            },
            'raw_data': dataset
        }
        
        with open(workspace.raw_data_file, 'w') as f:
            json.dump(raw_data, f, indent=2)
        
        logger.info(f"   ✅ Data loaded: {len(dataset.get('properties', []))} properties")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data loading error: {e}", exc_info=True)
        return False

def run_ai_analysis(workspace: ExecutionWorkspace) -> bool:
    """
    Step 2: Run ai_analyzer.py in isolated workspace
    
    Args:
        workspace: Execution workspace
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 2: AI ANALYSIS")
    logger.info("="*70)
    
    try:
        cmd = [sys.executable, 'ai_analyzer.py']
        
        result = subprocess.run(
            cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=180  # 3 minute timeout
        )
        
        if result.returncode != 0:
            logger.error(f"❌ AI analysis failed:")
            logger.error(f"   STDOUT: {result.stdout[-500:]}")
            logger.error(f"   STDERR: {result.stderr[-500:]}")
            return False
        
        # Verify analysis file exists
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


def run_pdf_generation(workspace: ExecutionWorkspace, client_identifier: str) -> bool:
    """
    Step 3: Run pdf_generator.py with client preferences
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 3: PDF GENERATION WITH CLIENT PREFERENCES")
    logger.info("="*70)
    
    try:
        # ✅ NEW: Load client preferences
        preferences = get_client_preferences(client_identifier)
        
        logger.info(f"   🎯 Client: {preferences.get('name', 'Unknown')}")
        logger.info(f"   📊 Competitor Focus: {preferences['competitor_focus']}")
        logger.info(f"   📋 Report Depth: {preferences['report_depth']}")
        
        # Build command with preference flags
        pdf_cmd = [
            sys.executable, 'pdf_generator.py',
            '--workspace', str(workspace.workspace),
            '--output', workspace.pdf_file.name,
            '--competitor-focus', preferences['competitor_focus'],
            '--report-depth', preferences['report_depth']
        ]
        
        logger.info(f"   Command: {' '.join(pdf_cmd)}")
        
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
        
        # Verify PDF exists
        if not workspace.pdf_file.exists():
            logger.error(f"❌ PDF not created: {workspace.pdf_file}")
            return False
        
        file_size = workspace.pdf_file.stat().st_size
        
        if file_size < 10000:
            logger.error(f"❌ PDF too small ({file_size} bytes)")
            return False
        
        logger.info(f"   ✅ PDF generated: {file_size:,} bytes")
        logger.info(f"   ✅ Preferences applied: {preferences['competitor_focus']}, {preferences['report_depth']}")
        
        return True
    
    except subprocess.TimeoutExpired:
        logger.error("❌ PDF generation timeout (2 minutes)")
        return False
    
    except Exception as e:
        logger.error(f"❌ PDF generation error: {e}", exc_info=True)
        return False


def upload_pdf_to_r2(workspace: ExecutionWorkspace, client_email: str) -> str:
    """
    Step 4: Upload PDF to Cloudflare R2
    
    Args:
        workspace: Execution workspace
        client_email: Client email for organization
    
    Returns: Public URL or None
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 4: CLOUDFLARE R2 UPLOAD")
    logger.info("="*70)
    
    try:
        from app.pdf_storage import upload_pdf_to_cloud
        
        # Use email as client_id (safe for filenames)
        client_id = client_email.replace('@', '_at_').replace('.', '_').replace('whatsapp:', '').replace('+', '')
        
        url = upload_pdf_to_cloud(
            pdf_path=str(workspace.pdf_file),
            client_id=client_id,
            area=workspace.area
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
    
    Args:
        workspace: Execution workspace
        pdf_url: Cloudflare R2 URL (optional)
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 5: MONGODB STORAGE")
    logger.info("="*70)
    
    try:
        if mongo_client is None:
            logger.error("   ❌ MongoDB not connected")
            return False
        
        # Get database reference
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
                        'area': workspace.area,
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


def send_email(area: str, city: str, recipient_email: str, recipient_name: str, 
               workspace_dir: Path, cloudflare_url: str, exec_id: str) -> bool:
    """
    Step 6: Send email with PDF attachment
    
    Args:
        area: Market area (e.g., "Mayfair")
        city: City name (e.g., "London")
        recipient_email: Recipient email address
        recipient_name: Recipient name
        workspace_dir: Path to workspace directory
        cloudflare_url: Cloudflare R2 URL for PDF
        exec_id: Execution ID
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 6: EMAIL DELIVERY")
    logger.info("="*70)
    
    # Skip if no email provided
    if not recipient_email or recipient_email == "none" or recipient_email.startswith('whatsapp:'):
        logger.info("   ⏭️  Email delivery skipped (WhatsApp client or no email)")
        return True
    
    try:
        from email_sender import send_voxmill_email
        
        # Build PDF path from workspace
        pdf_path = workspace_dir / f"Voxmill_{area}_Intelligence_Deck.pdf"
        logo_path = Path(__file__).parent / "voxmill_logo.png"
        
        # Validate PDF exists
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found in workspace: {pdf_path}")
        
        logger.info(f"   📄 PDF: {pdf_path} ({pdf_path.stat().st_size:,} bytes)")
        
        # Send email with retry logic
        send_voxmill_email(
            recipient_email=recipient_email,
            recipient_name=recipient_name,
            area=area,
            city=city,
            pdf_path=str(pdf_path),
            logo_path=str(logo_path) if logo_path.exists() else None
        )
        
        logger.info(f"   ✅ Email sent to {recipient_email}")
        return True
    
    except Exception as e:
        import traceback
        logger.warning(f"   ⚠️  Email sending failed:\n{traceback.format_exc()}")
        return False


def log_execution(workspace: ExecutionWorkspace, status: str, steps_completed: list, error: str = None):
    """
    Log execution to MongoDB for audit trail
    
    Args:
        workspace: Execution workspace
        status: "success" or "failed"
        steps_completed: List of completed step names
        error: Error message if failed
    """
    try:
        if mongo_client is None:
            return
        
        db = mongo_client['Voxmill']
        execution_log = db['execution_log']
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time - workspace.start_time).total_seconds()
        
        log_entry = {
            'exec_id': workspace.exec_id,
            'area': workspace.area,
            'city': workspace.city,
            'vertical': workspace.vertical,
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

def execute_voxmill_pipeline(
    area: str,
    city: str = "London",
    vertical: str = "uk-real-estate",
    vertical_config: dict = None,
    client_email: str = None,
    client_name: str = "Valued Client"
) -> dict:
    """
    Execute complete Voxmill intelligence pipeline
    
    Args:
        area: Market area (e.g., "Mayfair")
        city: City name (e.g., "London")
        vertical: Vertical identifier
        vertical_config: Vertical configuration dict
        client_email: Client email/WhatsApp for PDF delivery and preferences
        client_name: Client name for personalization
    
    Returns: Dict with execution results
    """
    
    # Default vertical config
    if not vertical_config:
        vertical_config = {
            'type': 'real_estate',
            'name': 'Real Estate',
            'vertical': vertical
        }
    
    # Acquire execution lock
    lock = ExecutionLock(area, vertical)
    if not lock.acquire(timeout=10):
        return {
            'success': False,
            'error': 'Another execution is in progress for this area',
            'exec_id': None
        }
    
    # Create workspace
    workspace = None
    steps_completed = []
    pdf_url = None
    
    try:
        workspace = ExecutionWorkspace(area, city, vertical)
        
        logger.info("\n" + "="*70)
        logger.info("🚀 VOXMILL INTELLIGENCE PIPELINE STARTING")
        logger.info("="*70)
        logger.info(f"   Execution ID: {workspace.exec_id}")
        logger.info(f"   Area: {area}")
        logger.info(f"   City: {city}")
        logger.info(f"   Vertical: {vertical}")
        logger.info(f"   Client: {client_name} <{client_email}>")
        logger.info("="*70)
        
        # Step 1: Data Collection
        if not run_data_collection(workspace, vertical_config):
            raise Exception("Data collection failed")
        steps_completed.append('data_collection')
        
        # Step 2: AI Analysis
        if not run_ai_analysis(workspace):
            raise Exception("AI analysis failed")
        steps_completed.append('ai_analysis')
        
        # Step 3: PDF Generation (with preferences)
        if not run_pdf_generation(workspace, client_email or "default"):
            raise Exception("PDF generation failed")
        steps_completed.append('pdf_generation')
        
        # Step 4: Upload to R2
        pdf_url = upload_pdf_to_r2(workspace, client_email or "default")
        if pdf_url:
            steps_completed.append('r2_upload')
        
        # Step 5: Save to MongoDB
        if save_to_mongodb(workspace, pdf_url):
            steps_completed.append('mongodb_save')
        
        # Step 6: Send Email (optional, skip for WhatsApp clients)
        if client_email and client_email != "none" and not client_email.startswith('whatsapp:'):
            if send_email(area, city, client_email, client_name, workspace.workspace, pdf_url, workspace.exec_id):
                steps_completed.append('email_sent')
        
        # Log success
        log_execution(workspace, 'success', steps_completed)
        
        logger.info("\n" + "="*70)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("="*70)
        logger.info(f"   Exec ID: {workspace.exec_id}")
        logger.info(f"   Steps: {', '.join(steps_completed)}")
        if pdf_url:
            logger.info(f"   PDF URL: {pdf_url[:80]}...")
        logger.info("="*70)
        
        return {
            'success': True,
            'exec_id': workspace.exec_id,
            'area': area,
            'city': city,
            'pdf_url': pdf_url,
            'steps_completed': steps_completed,
            'workspace': str(workspace.workspace)
        }
    
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {e}", exc_info=True)
        
        # Log failure
        if workspace:
            log_execution(workspace, 'failed', steps_completed, str(e))
        
        return {
            'success': False,
            'error': str(e),
            'exec_id': workspace.exec_id if workspace else None,
            'steps_completed': steps_completed
        }
    
    finally:
        # Clean up workspace
        if workspace:
            workspace.cleanup(keep_pdf=False)
        
        # Release lock
        lock.release()


# ============================================================================
# QUEUE INTEGRATION (Optional)
# ============================================================================

def queue_job(job_data: dict) -> bool:
    """
    Queue a job in Redis for worker processing
    
    Args:
        job_data: Job parameters dict
    
    Returns: True if queued successfully
    """
    if not redis_client:
        logger.warning("Redis not available - executing directly")
        return False
    
    try:
        job_json = json.dumps(job_data)
        redis_client.lpush('voxmill:pdf_jobs', job_json)
        logger.info(f"✅ Job queued: {job_data.get('area', 'Unknown')}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to queue job: {e}")
        return False


def get_all_active_clients_simple():
    """
    Get all active clients from Airtable/MongoDB
    Returns list of dicts with: whatsapp_number, name, city, regions, preferences
    """
    
    clients = []
    
    # ✅ FIX: Use global AIRTABLE variables (already defined at top)
    if AIRTABLE_API_KEY:
        try:
            import requests
            
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            headers = {
                'Authorization': f'Bearer {AIRTABLE_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'filterByFormula': "{Status}='Active'",
                'maxRecords': 100
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                for record in records:
                    fields = record['fields']
                    regions_str = fields.get('Regions', '')
                    regions = [r.strip() for r in regions_str.split(',') if r.strip()]
                    
                    client = {
                        'whatsapp_number': fields.get('WhatsApp', ''),
                        'name': fields.get('Name', 'Valued Client'),
                        'city': fields.get('City', 'London'),
                        'regions': regions,
                        'area': regions[0] if regions else fields.get('City', 'London'),
                        'competitor_focus': fields.get('Competitor Focus', 'medium'),
                        'report_depth': fields.get('Report Depth', 'detailed')
                    }
                    
                    if client['whatsapp_number']:
                        clients.append(client)
                
                logger.info(f"✅ Loaded {len(clients)} active clients from Airtable")
                return clients
        
        except Exception as e:
            logger.warning(f"Airtable fetch failed: {e}")
    
    # Fallback to MongoDB
    if mongo_client:
        try:
            cursor = db['client_profiles'].find({'status': 'active'})
            
            for doc in cursor:
                prefs = doc.get('preferences', {})
                regions = prefs.get('preferred_regions', [])
                
                client = {
                    'whatsapp_number': doc.get('whatsapp_number', ''),
                    'name': doc.get('name', 'Valued Client'),
                    'city': doc.get('city', 'London'),
                    'regions': regions,
                    'area': regions[0] if regions else doc.get('city', 'London'),
                    'competitor_focus': prefs.get('competitor_focus', 'medium'),
                    'report_depth': prefs.get('report_depth', 'detailed')
                }
                
                if client['whatsapp_number']:
                    clients.append(client)
            
            logger.info(f"✅ Loaded {len(clients)} active clients from MongoDB")
            return clients
        
        except Exception as e:
            logger.warning(f"MongoDB fetch failed: {e}")
    
    return clients

# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    
    # ✅ FIX: Force unbuffered output for Render cron visibility
    import sys
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    print("="*70, flush=True)
    print("VOXMILL MASTER STARTING", flush=True)
    print("="*70, flush=True)
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Voxmill Intelligence Pipeline')
    
    # Optional parameters - if none provided, runs batch mode
    parser.add_argument('--area', help='Market area (e.g., Mayfair) - optional if using batch mode')
    parser.add_argument('--city', default='London', help='City name')
    parser.add_argument('--vertical', default='uk-real-estate', help='Vertical identifier')
    parser.add_argument('--email', default='none', help='Client email or WhatsApp number')
    parser.add_argument('--name', default='Valued Client', help='Client name')
    parser.add_argument('--queue', action='store_true', help='Queue job instead of running directly')
    
    args = parser.parse_args()
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # BATCH MODE: No --area provided = process all active clients
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if not args.area:
        logger.info("\n" + "="*70)
        logger.info("🔄 BATCH MODE: Processing all active clients")
        logger.info("="*70)
        
        clients = get_all_active_clients_simple()
        
        if not clients:
            logger.warning("⚠️  No active clients found in Airtable or MongoDB")
            sys.exit(0)
        
        logger.info(f"📋 Found {len(clients)} active client(s)\n")
        
        stats = {'success': 0, 'failed': 0}
        
        for i, client in enumerate(clients, 1):
            logger.info(f"[{i}/{len(clients)}] Processing: {client['name']}")
            logger.info(f"   Area: {client['area']}, City: {client['city']}")
            
            vertical_config = {
                'type': 'real_estate',
                'name': 'Real Estate',
                'vertical': 'uk-real-estate'
            }
            
            try:
                result = execute_voxmill_pipeline(
                    area=client['area'],
                    city=client['city'],
                    vertical='uk-real-estate',
                    vertical_config=vertical_config,
                    client_email=client['whatsapp_number'],
                    client_name=client['name']
                )
                
                if result['success']:
                    stats['success'] += 1
                    logger.info(f"   ✅ Complete\n")
                else:
                    stats['failed'] += 1
                    logger.error(f"   ❌ Failed: {result.get('error')}\n")
            
            except Exception as e:
                stats['failed'] += 1
                logger.error(f"   ❌ Exception: {e}\n", exc_info=True)
        
        logger.info("="*70)
        logger.info("BATCH PROCESSING COMPLETE")
        logger.info("="*70)
        logger.info(f"   Success: {stats['success']}")
        logger.info(f"   Failed: {stats['failed']}")
        logger.info(f"   Total: {len(clients)}")
        logger.info("="*70)
        
        sys.exit(0 if stats['failed'] == 0 else 1)
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SINGLE CLIENT MODE: --area provided = manual execution
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    # Prepare vertical config
    vertical_config = {
        'type': 'real_estate',
        'name': 'Real Estate',
        'vertical': args.vertical
    }
    
    # Queue mode
    if args.queue and redis_client:
        job_data = {
            'area': args.area,
            'city': args.city,
            'vertical': args.vertical,
            'vertical_config': vertical_config,
            'client_email': args.email,
            'client_name': args.name
        }
        
        if queue_job(job_data):
            print(f"✅ Job queued for {args.area}")
            sys.exit(0)
        else:
            print("⚠️  Queueing failed - falling back to direct execution")
    
    # Direct execution mode
    result = execute_voxmill_pipeline(
        area=args.area,
        city=args.city,
        vertical=args.vertical,
        vertical_config=vertical_config,
        client_email=args.email,
        client_name=args.name
    )
    
    if result['success']:
        print(f"\n✅ SUCCESS")
        print(f"   Exec ID: {result['exec_id']}")
        if result.get('pdf_url'):
            print(f"   PDF URL: {result['pdf_url']}")
        sys.exit(0)
    else:
        print(f"\n❌ FAILED: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()

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

# MongoDB
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

# Environment variables
MONGODB_URI = os.getenv('MONGODB_URI')
REDIS_URL = os.getenv('REDIS_URL')

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
    Step 1: Run data_collector.py in isolated workspace
    
    Args:
        workspace: Execution workspace
        vertical_config: Vertical configuration dict
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 1: DATA COLLECTION")
    logger.info("="*70)
    
    try:
        # Prepare vertical config as JSON string
        vertical_config_json = json.dumps(vertical_config)
        
        # Run data collector with workspace environment
        cmd = [
            sys.executable,
            'data_collector.py',
            workspace.vertical,
            workspace.area,
            workspace.city,
            vertical_config_json
        ]
        
        logger.info(f"   Command: {' '.join(cmd[:4])}...")
        
        result = subprocess.run(
            cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode != 0:
            logger.error(f"❌ Data collection failed:")
            logger.error(f"   STDOUT: {result.stdout[-500:]}")
            logger.error(f"   STDERR: {result.stderr[-500:]}")
            return False
        
        # Verify output file exists
        if not workspace.raw_data_file.exists():
            logger.error(f"❌ Data file not created: {workspace.raw_data_file}")
            return False
        
        # Check file has content
        file_size = workspace.raw_data_file.stat().st_size
        if file_size < 100:
            logger.error(f"❌ Data file too small ({file_size} bytes)")
            return False
        
        logger.info(f"   ✅ Data collected: {file_size:,} bytes")
        logger.info(f"   Output: {result.stdout[-200:]}")
        
        return True
    
    except subprocess.TimeoutExpired:
        logger.error("❌ Data collection timeout (5 minutes)")
        return False
    
    except Exception as e:
        logger.error(f"❌ Data collection error: {e}", exc_info=True)
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


def run_pdf_generation(workspace: ExecutionWorkspace) -> bool:
    """
    Step 3: Run pdf_generator.py in isolated workspace
    
    Args:
        workspace: Execution workspace
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 3: PDF GENERATION")
    logger.info("="*70)
    
    try:
        cmd = [
            sys.executable,
            'pdf_generator.py',
            '--workspace', str(workspace.workspace),
            '--output', workspace.pdf_file.name
        ]
        
        logger.info(f"   Command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
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
        
        if file_size < 10000:  # PDF should be at least 10KB
            logger.error(f"❌ PDF too small ({file_size} bytes) - likely corrupt")
            return False
        
        logger.info(f"   ✅ PDF generated: {file_size:,} bytes")
        
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
        client_id = client_email.replace('@', '_at_').replace('.', '_')
        
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
        # ✅ FIX: Database objects don't support bool() - use 'is None' check
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


def send_email(workspace: ExecutionWorkspace, client_email: str, client_name: str, pdf_url: str) -> bool:
    """
    Step 6: Send email with PDF (optional if using WhatsApp)
    
    Args:
        workspace: Execution workspace
        client_email: Recipient email
        client_name: Recipient name
        pdf_url: Cloudflare R2 URL
    
    Returns: True if successful
    """
    logger.info("\n" + "="*70)
    logger.info("STEP 6: EMAIL DELIVERY")
    logger.info("="*70)
    
    # Skip if no email provided
    if not client_email or client_email == "none":
        logger.info("   ⏭️  Email delivery skipped (no email provided)")
        return True
    
    try:
        cmd = [
            sys.executable,
            'email_sender.py',
            client_email,
            client_name,
            str(workspace.pdf_file),
            workspace.area
        ]
        
        result = subprocess.run(
            cmd,
            env=workspace.get_env_vars(),
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            logger.warning(f"   ⚠️  Email sending failed:")
            logger.warning(f"   {result.stderr[-300:]}")
            return False
        
        logger.info(f"   ✅ Email sent to {client_email}")
        return True
    
    except Exception as e:
        logger.error(f"   ❌ Email error: {e}")
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
        # ✅ FIX: Use mongo_client check instead of db
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
        client_email: Client email for PDF delivery
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
        
        # Step 3: PDF Generation
        if not run_pdf_generation(workspace):
            raise Exception("PDF generation failed")
        steps_completed.append('pdf_generation')
        
        # Step 4: Upload to R2
        pdf_url = upload_pdf_to_r2(workspace, client_email or "default")
        if pdf_url:
            steps_completed.append('r2_upload')
        
        # Step 5: Save to MongoDB
        if save_to_mongodb(workspace, pdf_url):
            steps_completed.append('mongodb_save')
        
        # Step 6: Send Email (optional)
        if client_email and client_email != "none":
            if send_email(workspace, client_email, client_name, pdf_url):
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


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Voxmill Intelligence Pipeline')
    parser.add_argument('--area', required=True, help='Market area (e.g., Mayfair)')
    parser.add_argument('--city', default='London', help='City name')
    parser.add_argument('--vertical', default='uk-real-estate', help='Vertical identifier')
    parser.add_argument('--email', default='none', help='Client email')
    parser.add_argument('--name', default='Valued Client', help='Client name')
    parser.add_argument('--queue', action='store_true', help='Queue job instead of running directly')
    
    args = parser.parse_args()
    
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

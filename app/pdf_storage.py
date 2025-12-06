#!/usr/bin/env python3
"""
VOXMILL PDF STORAGE MODULE V1.0
================================
MongoDB GridFS + Cloudflare R2 integration for permanent PDF storage
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None

# Cloudflare R2 Configuration
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "voxmill-reports")


def upload_pdf_to_cloud(pdf_path: str, client_id: str, area: str) -> Optional[str]:
    """
    Upload PDF to Cloudflare R2 and generate presigned URL
    
    ✅ MATCHES voxmill_master.py import exactly
    
    Args:
        pdf_path: Path to PDF file
        client_id: Client identifier (email-based)
        area: Market area
    
    Returns:
        Presigned URL (7-day expiration) or None
    """
    try:
        # Check R2 configuration
        if not all([R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY]):
            logger.warning("⚠️ Cloudflare R2 not configured - skipping cloud upload")
            return None
        
        import boto3
        
        # Initialize S3-compatible client for R2
        s3_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto'
        )
        
        # Read PDF
        with open(pdf_path, 'rb') as f:
            pdf_binary = f.read()
        
        # Generate unique key
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"voxmill_{area.lower().replace(' ', '_')}_{client_id}_{timestamp}.pdf"
        
        # Upload to R2
        s3_client.put_object(
            Bucket=R2_BUCKET,
            Key=filename,
            Body=pdf_binary,
            ContentType='application/pdf',
            Metadata={
                'area': area,
                'client_id': client_id,
                'generated_at': datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Generate presigned URL (7-day expiration)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': R2_BUCKET,
                'Key': filename
            },
            ExpiresIn=604800  # 7 days in seconds
        )
        
        logger.info(f"✅ PDF uploaded to Cloudflare R2: {filename}")
        
        return presigned_url
        
    except Exception as e:
        logger.error(f"❌ R2 upload failed: {e}", exc_info=True)
        return None


def upload_pdf_to_gridfs(
    pdf_path: str,
    area: str,
    city: str,
    exec_id: str,
    client_email: str,
    cloudflare_url: Optional[str] = None
) -> Optional[str]:
    """
    Upload PDF to MongoDB GridFS with metadata
    
    Args:
        pdf_path: Path to PDF file
        area: Market area
        city: City name
        exec_id: Unique execution ID
        client_email: Client email address
        cloudflare_url: Optional R2 URL to store in metadata
    
    Returns:
        GridFS file_id as string, or None on failure
    """
    try:
        if not mongo_client:
            logger.error("MongoDB not configured - cannot upload to GridFS")
            return None
        
        db = mongo_client['Voxmill']
        fs = gridfs.GridFS(db)
        
        # Read PDF binary
        with open(pdf_path, 'rb') as f:
            pdf_binary = f.read()
        
        # Prepare metadata
        metadata = {
            'area': area,
            'city': city,
            'exec_id': exec_id,
            'client_email': client_email,
            'generated_at': datetime.now(timezone.utc),
            'file_size': len(pdf_binary),
            'cloudflare_url': cloudflare_url
        }
        
        # Upload to GridFS
        filename = f"voxmill_{area.lower().replace(' ', '_')}_{exec_id}.pdf"
        file_id = fs.put(
            pdf_binary,
            filename=filename,
            metadata=metadata,
            content_type='application/pdf'
        )
        
        logger.info(f"✅ PDF uploaded to GridFS: {file_id}")
        
        return str(file_id)
        
    except Exception as e:
        logger.error(f"❌ GridFS upload failed: {e}", exc_info=True)
        return None


def get_latest_pdf_for_client(whatsapp_number: str, area: str) -> Optional[str]:
    """
    Retrieve latest PDF URL for client's requested area
    
    Args:
        whatsapp_number: Client's WhatsApp number (for logging)
        area: Market area requested
    
    Returns:
        PDF URL (R2 or fallback MongoDB endpoint) or None
    """
    try:
        if not mongo_client:
            logger.error("MongoDB not configured")
            return None
        
        db = mongo_client['Voxmill']
        
        # Find latest PDF for this area (within last 7 days)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
        
        file_doc = db['fs.files'].find_one(
            {
                'metadata.area': area,
                'uploadDate': {'$gte': cutoff_date}
            },
            sort=[('uploadDate', -1)]
        )
        
        if not file_doc:
            logger.info(f"No recent PDF found for {area}")
            return None
        
        # Check for Cloudflare R2 URL first
        if 'metadata' in file_doc and file_doc['metadata'].get('cloudflare_url'):
            cloudflare_url = file_doc['metadata']['cloudflare_url']
            logger.info(f"✅ Returning R2 URL for {area}")
            return cloudflare_url
        
        # Fallback: Generate MongoDB-served URL
        file_id = file_doc['_id']
        
        # This requires /pdf/{file_id} endpoint in main.py
        base_url = os.getenv('APP_BASE_URL', 'https://voxmill.onrender.com')
        mongodb_url = f"{base_url}/pdf/{file_id}"
        
        logger.info(f"⚠️ Returning MongoDB fallback URL for {area}")
        return mongodb_url
        
    except Exception as e:
        logger.error(f"Error retrieving PDF: {e}", exc_info=True)
        return None


def get_pdf_by_id(file_id: str) -> Optional[bytes]:
    """
    Retrieve PDF binary from GridFS by file_id
    
    Args:
        file_id: MongoDB GridFS file ID
    
    Returns:
        PDF binary data or None
    """
    try:
        if not mongo_client:
            logger.error("MongoDB not configured")
            return None
        
        db = mongo_client['Voxmill']
        fs = gridfs.GridFS(db)
        
        # Get file
        grid_file = fs.get(ObjectId(file_id))
        pdf_binary = grid_file.read()
        
        logger.info(f"✅ Retrieved PDF from GridFS: {file_id}")
        return pdf_binary
        
    except Exception as e:
        logger.error(f"Error retrieving PDF from GridFS: {e}")
        return None


if __name__ == '__main__':
    # Health check
    print("="*70)
    print("VOXMILL PDF STORAGE - HEALTH CHECK")
    print("="*70)
    
    print(f"MongoDB:  {'✅' if mongo_client else '❌'}")
    print(f"R2 Config: {'✅' if all([R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY]) else '❌'}")
    
    if mongo_client:
        try:
            db = mongo_client['Voxmill']
            pdf_count = db['fs.files'].count_documents({})
            print(f"PDFs stored: {pdf_count}")
        except Exception as e:
            print(f"Error checking PDFs: {e}")
    
    print("="*70)

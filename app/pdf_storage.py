"""
Voxmill PDF Storage Module
Handles GridFS storage and retrieval for WhatsApp service
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import gridfs
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def get_latest_pdf_for_client(whatsapp_number: str, area: str) -> str:
    """
    Get latest PDF URL for client's preferred area
    Returns: Cloudflare R2 URL or MongoDB URL
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
        
        file_id = file_doc['_id']
        
        # Check if we have Cloudflare R2 URL cached
        if 'metadata' in file_doc and 'cloudflare_url' in file_doc['metadata']:
            return file_doc['metadata']['cloudflare_url']
        
        # Fallback: Generate MongoDB-served URL
        # Requires /pdf/ endpoint in main.py
        return f"https://your-app.onrender.com/pdf/{file_id}"
        
    except Exception as e:
        logger.error(f"Error getting PDF: {e}", exc_info=True)
        return None


def upload_to_cloudflare_r2(pdf_binary: bytes, filename: str) -> str:
    """
    Upload PDF to Cloudflare R2 storage
    Returns: Public URL or None
    """
    try:
        r2_account_id = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
        r2_access_key = os.getenv("CLOUDFLARE_R2_ACCESS_KEY")
        r2_secret_key = os.getenv("CLOUDFLARE_R2_SECRET_KEY")
        r2_bucket = os.getenv("CLOUDFLARE_R2_BUCKET", "voxmill-pdfs")
        
        if not all([r2_account_id, r2_access_key, r2_secret_key]):
            logger.warning("Cloudflare R2 not configured")
            return None
        
        import boto3
        
        # R2 uses S3-compatible API
        s3 = boto3.client(
            's3',
            endpoint_url=f'https://{r2_account_id}.r2.cloudflarestorage.com',
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key,
            region_name='auto'
        )
        
        # Upload
        s3.put_object(
            Bucket=r2_bucket,
            Key=filename,
            Body=pdf_binary,
            ContentType='application/pdf'
        )
        
        # Generate presigned URL (7-day expiration)
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': r2_bucket, 'Key': filename},
            ExpiresIn=604800  # 7 days
        )
        
        logger.info(f"PDF uploaded to R2: {filename}")
        return url
        
    except Exception as e:
        logger.error(f"R2 upload failed: {e}", exc_info=True)
        return None

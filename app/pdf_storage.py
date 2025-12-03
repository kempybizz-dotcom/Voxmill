import os
import boto3
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Cloudflare R2 configuration (S3-compatible)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")  # e.g., https://xxxxx.r2.cloudflarestorage.com
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "voxmill-reports")

# Initialize S3 client (works with R2)
s3_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name='auto'
) if R2_ENDPOINT and R2_ACCESS_KEY else None


def upload_pdf_to_cloud(pdf_path: str, client_id: str, area: str) -> str:
    """
    Upload PDF to cloud storage and return public URL
    
    Args:
        pdf_path: Local path to PDF file
        client_id: Client identifier (phone number or name)
        area: Market area (Mayfair, Chelsea, etc.)
    
    Returns: Public URL to PDF
    """
    try:
        if not s3_client:
            logger.error("Cloud storage not configured")
            return None
        
        # Generate unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_client_id = client_id.replace('whatsapp:', '').replace('+', '').replace(':', '')
        filename = f"reports/{safe_client_id}/{area}_{timestamp}.pdf"
        
        # Upload to cloud
        with open(pdf_path, 'rb') as pdf_file:
            s3_client.upload_fileobj(
                pdf_file,
                R2_BUCKET,
                filename,
                ExtraArgs={
                    'ContentType': 'application/pdf',
                    'ContentDisposition': f'inline; filename="{area}_Intelligence_Report.pdf"'
                }
            )
        
        # Generate public URL (valid for 7 days)
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': R2_BUCKET, 'Key': filename},
            ExpiresIn=604800  # 7 days in seconds
        )
        
        logger.info(f"PDF uploaded successfully: {filename}")
        return url
        
    except Exception as e:
        logger.error(f"Error uploading PDF: {str(e)}", exc_info=True)
        return None


def get_latest_pdf_for_client(client_id: str, area: str) -> str:
    """
    Get URL for client's most recent PDF report
    
    Args:
        client_id: Client identifier
        area: Market area
    
    Returns: Public URL or None
    """
    try:
        if not s3_client:
            return None
        
        safe_client_id = client_id.replace('whatsapp:', '').replace('+', '').replace(':', '')
        prefix = f"reports/{safe_client_id}/"
        
        # List objects with this prefix
        response = s3_client.list_objects_v2(
            Bucket=R2_BUCKET,
            Prefix=prefix
        )
        
        if 'Contents' not in response or not response['Contents']:
            return None
        
        # Get most recent file
        objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_key = objects[0]['Key']
        
        # Generate presigned URL
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': R2_BUCKET, 'Key': latest_key},
            ExpiresIn=604800
        )
        
        return url
        
    except Exception as e:
        logger.error(f"Error retrieving PDF: {str(e)}", exc_info=True)
        return None

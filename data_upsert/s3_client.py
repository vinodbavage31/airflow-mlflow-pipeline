import os
import boto3
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_s3_client(aws_config: dict = None):
    if aws_config is None:
        aws_config = {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'region': os.getenv('AWS_REGION', 'us-east-1'),
        }
    
    if aws_config.get('access_key_id'):
        return boto3.client(
            's3',
            aws_access_key_id=aws_config['access_key_id'],
            aws_secret_access_key=aws_config['secret_access_key'],
            region_name=aws_config.get('region', 'us-east-1')
        )
    else:
        return boto3.client('s3', region_name=aws_config.get('region', 'us-east-1'))


def upload_file_to_s3(s3_client, local_file_path: str, bucket: str, s3_key: str) -> bool:
    import os
    
    try:
        logger.info(f"Uploading {local_file_path} to s3://{bucket}/{s3_key}...")
        
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        
        file_size = os.path.getsize(local_file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        s3_client.upload_file(local_file_path, bucket, s3_key)
        
        logger.info(f"Successfully uploaded {os.path.basename(local_file_path)} "
              f"({file_size_mb:.2f} MB) to s3://{bucket}/{s3_key}")
        return True
        
    except FileNotFoundError as e:
        logger.error(f"Error: {str(e)}")
        return False
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure AWS credentials.")
        return False
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket '{bucket}' does not exist.")
        elif error_code == 'AccessDenied':
            logger.error("Access denied. Check your AWS credentials and bucket permissions.")
        else:
            logger.error(f"Error uploading file: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return False

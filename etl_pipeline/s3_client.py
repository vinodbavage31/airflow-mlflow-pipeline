import os
import boto3
import logging
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

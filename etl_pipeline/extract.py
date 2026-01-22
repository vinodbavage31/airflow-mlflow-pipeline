import pandas as pd
import logging
from io import StringIO, BytesIO
from etl_pipeline.s3_client import create_s3_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_from_s3(s3_client, bucket: str, key: str, file_format: str = 'auto') -> pd.DataFrame:
    if file_format == 'auto':
        if key.endswith('.parquet'):
            file_format = 'parquet'
        elif key.endswith('.csv'):
            file_format = 'csv'
        elif key.endswith('.json'):
            file_format = 'json'
        else:
            file_format = 'parquet'
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        if file_format == 'parquet':
            df = pd.read_parquet(BytesIO(response['Body'].read()))
        elif file_format == 'csv':
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
        elif file_format == 'json':
            json_content = response['Body'].read().decode('utf-8')
            df = pd.read_json(StringIO(json_content), orient='records')
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        return df
    except Exception as e:
        raise Exception(f"Error reading {bucket}/{key}: {str(e)}")


def extract_source1(aws_config: dict, bucket: str, source1_path: str) -> pd.DataFrame:
    logger.info(f"Extracting Source 1 from: s3://{bucket}/{source1_path}")
    
    s3_client = create_s3_client(aws_config)
    df = read_from_s3(s3_client, bucket, source1_path)
    
    logger.info(f"Source 1 extracted: {len(df)} records")
    return df


def extract_source2(aws_config: dict, bucket: str, source2_path: str) -> pd.DataFrame:
    logger.info(f"Extracting Source 2 from: s3://{bucket}/{source2_path}")
    
    s3_client = create_s3_client(aws_config)
    df = read_from_s3(s3_client, bucket, source2_path)
    
    logger.info(f"Source 2 extracted: {len(df)} records")
    return df

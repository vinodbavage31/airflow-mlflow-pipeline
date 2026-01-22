import os
import json
import logging
from pathlib import Path
from airflow.models import Variable
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()


def get_airflow_config():
    try:
        config_json = Variable.get("etl_pipeline_config", deserialize_json=True)
        return config_json
    except:
        pass
    
    def get_var(key, default=''):
        try:
            return Variable.get(key, default_var=default)
        except:
            return os.getenv(key, default)
    
    config = {
        's3': {
            'input_bucket': get_var('S3_INPUT_BUCKET'),
            'output_bucket': get_var('S3_OUTPUT_BUCKET'),
            'source1_path': get_var('S3_SOURCE1_PATH', 'source1_supply_chain.parquet'),
            'source2_path': get_var('S3_SOURCE2_PATH', 'source2_financial.parquet'),
            'output_path': get_var('S3_OUTPUT_PATH', 'processed/'),
        },
        'aws': {
            'access_key_id': get_var('AWS_ACCESS_KEY_ID'),
            'secret_access_key': get_var('AWS_SECRET_ACCESS_KEY'),
            'region': get_var('AWS_REGION', 'us-east-1'),
        },
        'output': {
            'format': get_var('ETL_OUTPUT_FORMAT', 'parquet'),
            'save_intermediate': get_var('SAVE_INTERMEDIATE', 'false').lower() == 'true',
        },
        'iceberg': {
            'enabled': get_var('ICEBERG_ENABLED', 'false').lower() == 'true',
            'table_path': get_var('ICEBERG_TABLE_PATH', ''),
            's3_location': get_var('ICEBERG_S3_LOCATION', ''),
            'partition_key': get_var('ICEBERG_PARTITION_KEY', ''),
            'aws_account_id': get_var('AWS_ACCOUNT_ID', ''),
        }
    }
    return config

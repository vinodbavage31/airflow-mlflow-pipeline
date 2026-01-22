#!/usr/bin/env python3

import argparse
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from data_generator import generate_and_save_data
from s3_uploader import upload_files_to_s3
from s3_client import create_s3_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()


def load_config(config_path: str = None) -> dict:
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    
    config = {
        'generation': {
            'num_rows': int(os.getenv('NUM_ROWS', '100')),
            'output_format': os.getenv('OUTPUT_FORMAT', 'parquet'),
            'output_dir': os.getenv('OUTPUT_DIR', './data'),
        },
        's3': {
            'bucket': os.getenv('S3_BUCKET', ''),
            'source1_path': os.getenv('S3_SOURCE1_PATH', 'source1_supply_chain.parquet'),
            'source2_path': os.getenv('S3_SOURCE2_PATH', 'source2_financial.parquet'),
        },
        'aws': {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'region': os.getenv('AWS_REGION', 'us-east-1'),
        },
        'upload': {
            'enabled': os.getenv('UPLOAD_TO_S3', 'true').lower() == 'true',
            'skip_source1': os.getenv('SKIP_SOURCE1', 'false').lower() == 'true',
            'skip_source2': os.getenv('SKIP_SOURCE2', 'false').lower() == 'true',
        }
    }
    return config


def main():
    parser = argparse.ArgumentParser(
        description='Main pipeline: Generate dummy data and upload to S3'
    )
    parser.add_argument(
        'num_rows',
        type=int,
        nargs='?',
        help='Number of rows to generate for each source (overrides config)'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration JSON file'
    )
    parser.add_argument(
        '--format',
        choices=['json', 'csv', 'parquet', 'both'],
        help='Output format (overrides config)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory (overrides config)'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        help='S3 bucket name (overrides config)'
    )
    parser.add_argument(
        '--no-upload',
        action='store_true',
        help='Skip S3 upload step'
    )
    parser.add_argument(
        '--skip-source1',
        action='store_true',
        help='Skip uploading source1 file'
    )
    parser.add_argument(
        '--skip-source2',
        action='store_true',
        help='Skip uploading source2 file'
    )
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    num_rows = args.num_rows or config['generation']['num_rows']
    output_format = args.format or config['generation']['output_format']
    output_dir = args.output_dir or config['generation']['output_dir']
    bucket = args.bucket or config['s3']['bucket']
    upload_enabled = not args.no_upload and config['upload']['enabled']
    skip_source1 = args.skip_source1 or config['upload']['skip_source1']
    skip_source2 = args.skip_source2 or config['upload']['skip_source2']
    
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("Data Generation and Upload Pipeline")
    logger.info("=" * 60)
    logger.info(f"Configuration:")
    logger.info(f"  - Rows per source: {num_rows}")
    logger.info(f"  - Output format: {output_format}")
    logger.info(f"  - Output directory: {output_dir}")
    if upload_enabled:
        logger.info(f"  - S3 bucket: {bucket}")
        logger.info(f"  - Upload enabled: Yes")
    else:
        logger.info(f"  - Upload enabled: No")
    logger.info("=" * 60)
    
    try:
        generated_files = generate_and_save_data(
            num_rows=num_rows,
            output_dir=output_dir,
            output_format=output_format
        )
    except Exception as e:
        logger.error(f"Error generating data: {str(e)}", exc_info=True)
        return 1
    
    if upload_enabled:
        if not bucket:
            logger.warning("S3 bucket not specified. Skipping upload.")
            logger.warning("  Set --bucket or configure in config file.")
        else:
            logger.info("=" * 60)
            logger.info("Step 2: Uploading to S3")
            logger.info("=" * 60)
            
            source1_file = None
            source2_file = None
            
            if output_format == 'parquet' or output_format == 'both':
                source1_file = generated_files.get('source1_parquet')
                source2_file = generated_files.get('source2_parquet')
            elif output_format == 'csv':
                source1_file = generated_files.get('source1_csv')
                source2_file = generated_files.get('source2_csv')
            elif output_format == 'json':
                source1_file = generated_files.get('source1_json')
                source2_file = generated_files.get('source2_json')
            
            upload_results = upload_files_to_s3(
                aws_config=config['aws'],
                bucket=bucket,
                source1_path=source1_file,
                source2_path=source2_file,
                s3_source1_key=config['s3']['source1_path'],
                s3_source2_key=config['s3']['source2_path'],
                data_dir=output_dir,
                skip_source1=skip_source1,
                skip_source2=skip_source2
            )
            
            if not all(v for v in upload_results.values() if v is not None):
                logger.warning("Some files failed to upload. Check errors above.")
                return 1
    
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)
    
    return 0


if __name__ == '__main__':
    exit(main())

import os
import logging
from pathlib import Path
from typing import Optional, Dict
from s3_client import create_s3_client, upload_file_to_s3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_file_in_directory(directory: str, filename: str) -> Optional[str]:
    dir_path = Path(directory)
    
    exact_path = dir_path / filename
    if exact_path.exists():
        return str(exact_path)
    
    base_name = Path(filename).stem
    extensions = ['.parquet', '.csv', '.json']
    
    for ext in extensions:
        file_path = dir_path / f"{base_name}{ext}"
        if file_path.exists():
            return str(file_path)
    
    return None


def upload_files_to_s3(
    aws_config: Dict,
    bucket: str,
    source1_path: Optional[str] = None,
    source2_path: Optional[str] = None,
    s3_source1_key: Optional[str] = None,
    s3_source2_key: Optional[str] = None,
    data_dir: str = '.',
    skip_source1: bool = False,
    skip_source2: bool = False
) -> Dict[str, bool]:
    try:
        s3_client = create_s3_client(aws_config)
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}", exc_info=True)
        return {'source1': False, 'source2': False}
    
    if not bucket:
        logger.error("S3 bucket not specified.")
        return {'source1': False, 'source2': False}
    
    logger.info("=" * 60)
    logger.info("Uploading files to S3")
    logger.info("=" * 60)
    logger.info(f"S3 Bucket: {bucket}")
    logger.info(f"Local Data Directory: {data_dir}")
    
    results = {}
    
    if not skip_source1:
        if source1_path:
            local_source1 = source1_path
        else:
            local_source1 = find_file_in_directory(data_dir, 'source1_supply_chain.parquet')
            if not local_source1:
                local_source1 = find_file_in_directory(data_dir, 'source1_supply_chain')
        
        if local_source1 and os.path.exists(local_source1):
            s3_key = s3_source1_key or 'source1_supply_chain.parquet'
            results['source1'] = upload_file_to_s3(s3_client, local_source1, bucket, s3_key)
        else:
            logger.warning(f"Source 1 file not found. Skipping...")
            results['source1'] = False
    else:
        results['source1'] = None
    
    if not skip_source2:
        if source2_path:
            local_source2 = source2_path
        else:
            local_source2 = find_file_in_directory(data_dir, 'source2_financial.parquet')
            if not local_source2:
                local_source2 = find_file_in_directory(data_dir, 'source2_financial')
        
        if local_source2 and os.path.exists(local_source2):
            s3_key = s3_source2_key or 'source2_financial.parquet'
            results['source2'] = upload_file_to_s3(s3_client, local_source2, bucket, s3_key)
        else:
            logger.warning(f"Source 2 file not found. Skipping...")
            results['source2'] = False
    else:
        results['source2'] = None
    
    logger.info("=" * 60)
    success_count = sum(1 for v in results.values() if v is True)
    total_count = sum(1 for v in results.values() if v is not None)
    
    if success_count == total_count and total_count > 0:
        logger.info(f"Upload complete! {success_count}/{total_count} files uploaded successfully.")
    elif total_count > 0:
        logger.warning(f"Upload completed with errors. {success_count}/{total_count} files uploaded successfully.")
    logger.info("=" * 60)
    
    return results

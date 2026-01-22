#!/usr/bin/env python3
"""
Main ETL Pipeline - Orchestrates Extract, Transform, and Load operations
"""

import os
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from etl_pipeline.extract import extract_source1, extract_source2
from etl_pipeline.transform import clean_data_source1, clean_data_source2, resolve_entities, harmonize_schema
from etl_pipeline.load import load_corporate_registry
from etl_pipeline.load import iceberg_upsert


# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    # Try loading from current directory
    load_dotenv()


def load_config(config_path: str = None) -> dict:
    """Load configuration from file or environment variables"""
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    
    # Default configuration from environment variables
    config = {
        's3': {
            'input_bucket': os.getenv('S3_INPUT_BUCKET', ''),
            'output_bucket': os.getenv('S3_OUTPUT_BUCKET', ''),
            'source1_path': os.getenv('S3_SOURCE1_PATH', 'source1_supply_chain.parquet'),
            'source2_path': os.getenv('S3_SOURCE2_PATH', 'source2_financial.parquet'),
            'output_path': os.getenv('S3_OUTPUT_PATH', 'processed/'),
        },
        'aws': {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'region': os.getenv('AWS_REGION', 'us-east-1'),
        },
        'output': {
            'format': os.getenv('ETL_OUTPUT_FORMAT', os.getenv('OUTPUT_FORMAT', 'parquet')),
            'save_intermediate': os.getenv('SAVE_INTERMEDIATE', 'false').lower() == 'true',
        },
        'iceberg': {
            'enabled': os.getenv('ICEBERG_ENABLED', 'false').lower() == 'true',
            'table_path': os.getenv('ICEBERG_TABLE_PATH', ''),
            's3_location': os.getenv('ICEBERG_S3_LOCATION', ''),
            'partition_key': os.getenv('ICEBERG_PARTITION_KEY', ''),
            'aws_account_id': os.getenv('AWS_ACCOUNT_ID', ''),
        }
    }
    return config


def main():
    parser = argparse.ArgumentParser(
        description='ETL Pipeline: Extract, Transform, and Load Supply Chain and Financial Data'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration JSON file'
    )
    parser.add_argument(
        '--format',
        choices=['parquet', 'csv', 'json'],
        help='Output format (overrides config)'
    )
    parser.add_argument(
        '--save-intermediate',
        action='store_true',
        help='Save intermediate transformed files'
    )
    parser.add_argument(
        '--no-iceberg',
        action='store_true',
        help='Skip Iceberg upsert (if enabled in config)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override with command line arguments
    output_format = args.format or config['output']['format']
    save_intermediate = args.save_intermediate or config['output']['save_intermediate']
    iceberg_enabled = not args.no_iceberg and config['iceberg']['enabled']
    
    print("=" * 60)
    print("ETL Pipeline - Extract, Transform, Load")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  - Input bucket: {config['s3']['input_bucket']}")
    print(f"  - Output bucket: {config['s3']['output_bucket']}")
    print(f"  - Output format: {output_format}")
    print(f"  - Save intermediate: {save_intermediate}")
    print(f"  - Iceberg enabled: {iceberg_enabled}")
    print("=" * 60)
    print()
    
    try:
        # ============================================================
        # EXTRACT
        # ============================================================
        print("=" * 60)
        print("STEP 1: EXTRACT")
        print("=" * 60)
        
        df_source1 = extract_source1(
            aws_config=config['aws'],
            bucket=config['s3']['input_bucket'],
            source1_path=config['s3']['source1_path']
        )
        
        df_source2 = extract_source2(
            aws_config=config['aws'],
            bucket=config['s3']['input_bucket'],
            source2_path=config['s3']['source2_path']
        )
        
        # ============================================================
        # TRANSFORM
        # ============================================================
        print("\n" + "=" * 60)
        print("STEP 2: TRANSFORM")
        print("=" * 60)
        
        # 2.1 Data Cleaning
        print("\n--- Data Cleaning ---")
        df_s1_cleaned = clean_data_source1(df_source1)
        df_s2_cleaned = clean_data_source2(df_source2)
        
        # Save intermediate files if requested
        if save_intermediate:
            from load import load_to_s3
            load_to_s3(
                aws_config=config['aws'],
                df=df_s1_cleaned,
                bucket=config['s3']['output_bucket'],
                output_path=config['s3']['output_path'],
                output_name="source1_cleaned",
                output_format=output_format
            )
            load_to_s3(
                aws_config=config['aws'],
                df=df_s2_cleaned,
                bucket=config['s3']['output_bucket'],
                output_path=config['s3']['output_path'],
                output_name="source2_cleaned",
                output_format=output_format
            )
        
        # 2.2 Entity Resolution
        print("\n--- Entity Resolution ---")
        df_merged = resolve_entities(df_s1_cleaned, df_s2_cleaned)
        
        if save_intermediate:
            load_to_s3(
                aws_config=config['aws'],
                df=df_merged,
                bucket=config['s3']['output_bucket'],
                output_path=config['s3']['output_path'],
                output_name="entities_resolved",
                output_format=output_format
            )
        
        # 2.3 Harmonization
        print("\n--- Schema Harmonization ---")
        corporate_registry = harmonize_schema(df_merged)
        
        # ============================================================
        # LOAD
        # ============================================================
        print("\n" + "=" * 60)
        print("STEP 3: LOAD")
        print("=" * 60)
        
        # Load to S3
        s3_key = load_corporate_registry(
            aws_config=config['aws'],
            df=corporate_registry,
            bucket=config['s3']['output_bucket'],
            output_path=config['s3']['output_path'],
            output_format=output_format
        )
        
        # Optional: Load to Iceberg
        if iceberg_enabled:
            print("\n--- Iceberg Upsert ---")
            
            
            # Parse table path (format: database.table)
            table_path = config['iceberg']['table_path']
            if '.' in table_path:
                database_name, table_name = table_path.split('.', 1)
            else:
                database_name = 'default'
                table_name = table_path
            
            # Get S3 location for Iceberg table
            s3_location = config['iceberg'].get('s3_location', 
                f"s3://{config['s3']['output_bucket']}/iceberg/{database_name}/{table_name}/")
            
            iceberg_upsert(
                df=corporate_registry,
                aws_config=config['aws'],
                database_name=database_name,
                table_name=table_name,
                s3_location=s3_location,
                s3_bucket=config['s3']['output_bucket'],
                partition_key=config['iceberg'].get('partition_key')
            )
        
        # ============================================================
        # SUMMARY
        # ============================================================
        print("\n" + "=" * 60)
        print("✓ ETL Pipeline completed successfully!")
        print("=" * 60)
        print(f"Summary:")
        print(f"  - Source 1 records: {len(df_source1)}")
        print(f"  - Source 2 records: {len(df_source2)}")
        print(f"  - Resolved entities: {len(df_merged)}")
        print(f"  - Corporate registry: {len(corporate_registry)}")
        print(f"  - Output location: s3://{config['s3']['output_bucket']}/{s3_key}")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ ERROR: ETL Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit(main())

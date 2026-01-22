import pandas as pd
import boto3
from io import StringIO, BytesIO
from datetime import datetime
from typing import Optional
from etl_pipeline.s3_client import create_s3_client
import pyarrow as pa
import json
from pyiceberg.catalog import load_catalog

def write_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str, format: str = 'parquet'):
    try:
        if format.lower() == 'parquet':
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
            buffer.seek(0)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue()
            )
        elif format.lower() == 'csv':
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue().encode('utf-8')
            )
        elif format.lower() == 'json':
            buffer = StringIO()
            df.to_json(buffer, orient='records', indent=2)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue().encode('utf-8')
            )
        else:
            raise ValueError(f"Unsupported format: {format}")
    except Exception as e:
        raise Exception(f"Error writing to {bucket}/{key}: {str(e)}")


def prepare_for_export(df: pd.DataFrame) -> pd.DataFrame:
    df_export = df.copy()
    
    for col in df_export.columns:
        if df_export[col].dtype == 'object':
            sample = df_export[col].dropna().iloc[0] if len(df_export[col].dropna()) > 0 else None
            if isinstance(sample, list):
                df_export[col] = df_export[col].apply(
                    lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x
                )
    
    return df_export


def load_to_s3(
    aws_config: dict,
    df: pd.DataFrame,
    bucket: str,
    output_path: str,
    output_name: str,
    output_format: str = 'parquet'
) -> str:   
    print(f"Loading data to S3: {output_name}")
    
    s3_client = create_s3_client(aws_config)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{output_path}{output_name}_{timestamp}.{output_format}"
    
    if output_format in ['csv', 'json']:
        df_export = prepare_for_export(df)
    else:
        df_export = df
    
    write_to_s3(s3_client, df_export, bucket, s3_key, output_format)
    
    print(f"✓ Successfully loaded to s3://{bucket}/{s3_key}")
    return s3_key


def load_corporate_registry(
    aws_config: dict,
    df: pd.DataFrame,
    bucket: str,
    output_path: str,
    output_format: str = 'parquet'
) -> str:
    return load_to_s3(
        aws_config=aws_config,
        df=df,
        bucket=bucket,
        output_path=output_path,
        output_name="corporate_registry",
        output_format=output_format
    )


def create_glue_client(aws_config: dict):
    if aws_config.get('access_key_id'):
        return boto3.client(
            'glue',
            aws_access_key_id=aws_config['access_key_id'],
            aws_secret_access_key=aws_config['secret_access_key'],
            region_name=aws_config.get('region', 'us-east-1')
        )
    else:
        return boto3.client('glue', region_name=aws_config.get('region', 'us-east-1'))

def iceberg_upsert(
    df: pd.DataFrame,
    aws_config: dict,
    database_name: str,
    table_name: str,
    s3_location: str,
    s3_bucket: str,
    partition_key: Optional[str] = None
):
    print(f"🔄 Writing to Iceberg: {database_name}.{table_name}")
    
    list_columns = ['activity_places', 'top_suppliers', 'main_customers']
    for col in list_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else str(x))
    
    if partition_key and partition_key not in df.columns:
        df[partition_key] = pd.Timestamp.now().strftime('%Y-%m-%d')
    
    try:
        
        catalog = load_catalog(
            "glue_catalog",
            **{
                "type": "glue",
                "glue.region": aws_config.get('region', 'us-east-1'),
                "glue.id": "197633664132"
            }
        )

        
        table = catalog.load_table(f"{database_name}.{table_name}")
        
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        table.append(arrow_table)
        
        print(f"✅ SUCCESS: {len(df)} rows → {database_name}.{table_name}")
        
    except Exception as e:
        print(f"✗ Iceberg failed: {str(e)}")
        print("   Fallback to S3...")
        return load_to_s3(aws_config, df, s3_bucket, "iceberg_fallback/", f"{table_name}_backup")
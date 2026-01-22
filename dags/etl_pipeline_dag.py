from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'etl_pipeline'))

from etl_pipeline.extract import extract_source1, extract_source2, read_from_s3
from etl_pipeline.transform import clean_data_source1, clean_data_source2, resolve_entities, harmonize_schema
from etl_pipeline.load import load_corporate_registry, load_to_s3, iceberg_upsert
from etl_pipeline.s3_client import create_s3_client
from airflow_utils import get_airflow_config

logger = logging.getLogger(__name__)


def extract_task(**context):
    logger.info("Starting Extract Task")
    config = get_airflow_config()
    
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
    
    s1_key = load_to_s3(
        aws_config=config['aws'],
        df=df_source1,
        bucket=config['s3']['output_bucket'],
        output_path=f"{config['s3']['output_path']}intermediate/",
        output_name=f"extracted_source1_{context['ds_nodash']}",
        output_format='parquet'
    )
    
    s2_key = load_to_s3(
        aws_config=config['aws'],
        df=df_source2,
        bucket=config['s3']['output_bucket'],
        output_path=f"{config['s3']['output_path']}intermediate/",
        output_name=f"extracted_source2_{context['ds_nodash']}",
        output_format='parquet'
    )
    
    context['ti'].xcom_push(key='source1_s3_key', value=s1_key)
    context['ti'].xcom_push(key='source2_s3_key', value=s2_key)
    context['ti'].xcom_push(key='source1_count', value=len(df_source1))
    context['ti'].xcom_push(key='source2_count', value=len(df_source2))
    
    logger.info(f"Extract completed: Source1={len(df_source1)} records, Source2={len(df_source2)} records")
    return {'source1_s3_key': s1_key, 'source2_s3_key': s2_key}


def transform_task(**context):
    logger.info("Starting Transform Task")
    config = get_airflow_config()
    
    ti = context['ti']
    source1_key = ti.xcom_pull(key='source1_s3_key')
    source2_key = ti.xcom_pull(key='source2_s3_key')
    
    s3_client = create_s3_client(config['aws'])
    
    df_source1 = read_from_s3(
        s3_client,
        bucket=config['s3']['output_bucket'],
        key=source1_key,
        file_format='parquet'
    )
    
    df_source2 = read_from_s3(
        s3_client,
        bucket=config['s3']['output_bucket'],
        key=source2_key,
        file_format='parquet'
    )
    
    logger.info("Data Cleaning")
    df_s1_cleaned = clean_data_source1(df_source1)
    df_s2_cleaned = clean_data_source2(df_source2)
    
    if config['output']['save_intermediate']:
        load_to_s3(
            aws_config=config['aws'],
            df=df_s1_cleaned,
            bucket=config['s3']['output_bucket'],
            output_path=f"{config['s3']['output_path']}intermediate/",
            output_name=f"source1_cleaned_{context['ds_nodash']}",
            output_format=config['output']['format']
        )
        load_to_s3(
            aws_config=config['aws'],
            df=df_s2_cleaned,
            bucket=config['s3']['output_bucket'],
            output_path=f"{config['s3']['output_path']}intermediate/",
            output_name=f"source2_cleaned_{context['ds_nodash']}",
            output_format=config['output']['format']
        )
    
    logger.info("Entity Resolution")
    df_merged = resolve_entities(df_s1_cleaned, df_s2_cleaned)
    
    if config['output']['save_intermediate']:
        merged_key = load_to_s3(
            aws_config=config['aws'],
            df=df_merged,
            bucket=config['s3']['output_bucket'],
            output_path=f"{config['s3']['output_path']}intermediate/",
            output_name=f"entities_resolved_{context['ds_nodash']}",
            output_format=config['output']['format']
        )
        ti.xcom_push(key='merged_s3_key', value=merged_key)
    
    logger.info("Schema Harmonization")
    corporate_registry = harmonize_schema(df_merged)
    
    registry_key = load_to_s3(
        aws_config=config['aws'],
        df=corporate_registry,
        bucket=config['s3']['output_bucket'],
        output_path=f"{config['s3']['output_path']}intermediate/",
        output_name=f"corporate_registry_{context['ds_nodash']}",
        output_format='parquet'
    )
    
    ti.xcom_push(key='registry_s3_key', value=registry_key)
    ti.xcom_push(key='registry_count', value=len(corporate_registry))
    ti.xcom_push(key='merged_count', value=len(df_merged))
    
    logger.info(f"Transform completed: Registry={len(corporate_registry)} records")
    return {'registry_s3_key': registry_key}


def load_task(**context):
    logger.info("Starting Load Task")
    config = get_airflow_config()
    
    ti = context['ti']
    registry_key = ti.xcom_pull(key='registry_s3_key')
    
    s3_client = create_s3_client(config['aws'])
    
    corporate_registry = read_from_s3(
        s3_client,
        bucket=config['s3']['output_bucket'],
        key=registry_key,
        file_format='parquet'
    )
    
    s3_key = load_corporate_registry(
        aws_config=config['aws'],
        df=corporate_registry,
        bucket=config['s3']['output_bucket'],
        output_path=config['s3']['output_path'],
        output_format=config['output']['format']
    )
    
    ti.xcom_push(key='final_s3_key', value=s3_key)
    
    logger.info(f"Load completed: Output at s3://{config['s3']['output_bucket']}/{s3_key}")
    
    if config['iceberg']['enabled']:
        logger.info("Iceberg Upsert")
        
        table_path = config['iceberg']['table_path']
        if '.' in table_path:
            database_name, table_name = table_path.split('.', 1)
        else:
            database_name = 'default'
            table_name = table_path
        
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
    
    return {'final_s3_key': s3_key}


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'etl_corporate_registry_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Corporate Registry: Extract, Transform, Load',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'corporate_registry', 's3'],
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

extract >> transform >> load

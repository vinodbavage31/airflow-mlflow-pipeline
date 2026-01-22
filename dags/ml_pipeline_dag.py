from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ml_pipeline.pipeline import main as ml_pipeline_main
from airflow_utils import get_airflow_config

logger = logging.getLogger(__name__)


def ml_training_task(**context):
    logger.info("Starting ML Training Task")
    
    try:
        result = ml_pipeline_main()
        context['ti'].xcom_push(key='ml_training_status', value='success')
        context['ti'].xcom_push(key='ml_training_result', value=result)
        return {'status': 'success', 'result': result}
    except Exception as e:
        logger.error(f"ML Training failed: {str(e)}", exc_info=True)
        context['ti'].xcom_push(key='ml_training_status', value='failed')
        context['ti'].xcom_push(key='ml_training_error', value=str(e))
        raise


default_args = {
    'owner': 'ml_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
}

dag = DAG(
    'ml_corporate_profit_prediction',
    default_args=default_args,
    description='ML Pipeline: Train model to predict corporate profit threshold from Iceberg table',
    schedule_interval=timedelta(days=7),
    catchup=False,
    tags=['ml', 'training', 'profit_prediction', 'iceberg'],
)

ml_training = PythonOperator(
    task_id='train_model',
    python_callable=ml_training_task,
    dag=dag,
)

ml_training

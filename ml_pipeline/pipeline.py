import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from data_reader import create_spark_session, read_from_iceberg_pyiceberg
from feature_engineering import engineer_features, prepare_features_for_training
from model_training import train_logistic_regression, split_data
from model_registry import setup_mlflow, register_model, save_model_metadata

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


def load_config() -> dict:
    config = {
        'iceberg': {
            'database_name': os.getenv('ICEBERG_DATABASE_NAME', 'corporate_db'),
            'table_name': os.getenv('ICEBERG_TABLE_NAME', 'corporate_registry'),
            'aws_account_id': os.getenv('AWS_ACCOUNT_ID', ''),
        },
        'aws': {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'region': os.getenv('AWS_REGION', 'us-east-1'),
        },
        'ml': {
            'profit_threshold': float(os.getenv('PROFIT_THRESHOLD', '100.0')),
            'train_ratio': float(os.getenv('TRAIN_RATIO', '0.8')),
            'max_iter': int(os.getenv('MAX_ITER', '100')),
            'reg_param': float(os.getenv('REG_PARAM', '0.01')),
            'feature_columns': os.getenv('FEATURE_COLUMNS', 'revenue,num_suppliers,profit_margin_normalized').split(','),
            'model_name': os.getenv('MODEL_NAME', 'corporate_profit_predictor'),
        },
        'mlflow': {
            'tracking_uri': os.getenv('MLFLOW_TRACKING_URI', 'file:./mlruns'),
            'experiment_name': os.getenv('MLFLOW_EXPERIMENT_NAME', 'corporate_profit_prediction'),
        }
    }
    return config


def main():
    logger.info("=" * 60)
    logger.info("ML Pipeline - Corporate Profit Prediction")
    logger.info("=" * 60)
    
    config = load_config()
    
    logger.info("Configuration:")
    logger.info(f"  - Iceberg Table: {config['iceberg']['database_name']}.{config['iceberg']['table_name']}")
    logger.info(f"  - Profit Threshold: {config['ml']['profit_threshold']}")
    logger.info(f"  - Model Name: {config['ml']['model_name']}")
    logger.info("=" * 60)
    
    spark = create_spark_session("CorporateProfitMLPipeline")
    
    try:
        logger.info("Step 1: Reading data from Iceberg table")
        df = read_from_iceberg_pyiceberg(
            database_name=config['iceberg']['database_name'],
            table_name=config['iceberg']['table_name'],
            aws_config=config['aws'],
            spark=spark
        )
        
        logger.info("Step 2: Feature Engineering")
        df_features = engineer_features(df, config['ml']['profit_threshold'])
        
        logger.info("Step 3: Preparing features for training")
        df_prepared = prepare_features_for_training(
            df_features,
            feature_columns=config['ml']['feature_columns']
        )
        
        logger.info("Step 4: Splitting data")
        train_df, test_df = split_data(df_prepared, config['ml']['train_ratio'])
        
        logger.info("Step 5: Training model")
        model, predictions, metrics = train_logistic_regression(
            train_df,
            test_df,
            max_iter=config['ml']['max_iter'],
            reg_param=config['ml']['reg_param']
        )
        
        logger.info("Step 6: Setting up MLflow")
        setup_mlflow(
            tracking_uri=config['mlflow']['tracking_uri'],
            experiment_name=config['mlflow']['experiment_name']
        )
        
        logger.info("Step 7: Registering model")
        run_id = register_model(
            model=model,
            model_name=config['ml']['model_name'],
            metrics=metrics,
            feature_columns=config['ml']['feature_columns'],
            tags={
                "profit_threshold": str(config['ml']['profit_threshold']),
                "iceberg_table": f"{config['iceberg']['database_name']}.{config['iceberg']['table_name']}"
            }
        )
        
        logger.info("Step 8: Saving model metadata")
        save_model_metadata(
            model_name=config['ml']['model_name'],
            metrics=metrics,
            feature_columns=config['ml']['feature_columns'],
            model_path=f"mlruns/{run_id}/artifacts/model"
        )
        
        logger.info("=" * 60)
        logger.info("ML Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Model Metrics:")
        for metric_name, metric_value in metrics.items():
            logger.info(f"  - {metric_name}: {metric_value:.4f}")
        logger.info(f"Model registered: {config['ml']['model_name']}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ML Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()
    
    return 0


if __name__ == '__main__':
    exit(main())

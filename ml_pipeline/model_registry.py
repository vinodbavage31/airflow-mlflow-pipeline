import mlflow
import mlflow.spark
import mlflow.pyfunc
from pyspark.ml import Model
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_mlflow(
    tracking_uri: str = None,
    experiment_name: str = "corporate_profit_prediction"
):
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    else:
        default_uri = os.getenv("MLFLOW_TRACKING_URI", "file:./mlruns")
        mlflow.set_tracking_uri(default_uri)
    
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(experiment_name)
            logger.info(f"Created new MLflow experiment: {experiment_name}")
        else:
            experiment_id = experiment.experiment_id
            logger.info(f"Using existing MLflow experiment: {experiment_name}")
    except Exception as e:
        logger.warning(f"Could not set up experiment: {str(e)}")
        experiment_id = "0"
    
    mlflow.set_experiment(experiment_name)
    return experiment_id


def register_model(
    model: Model,
    model_name: str,
    metrics: dict,
    feature_columns: list,
    model_version: str = None,
    tags: dict = None
) -> str:
    logger.info(f"Registering model: {model_name}")
    
    if model_version is None:
        model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    with mlflow.start_run(run_name=f"{model_name}_{model_version}") as run:
        run_id = run.info.run_id
        
        mlflow.log_params({
            "model_type": "LogisticRegression",
            "feature_columns": ",".join(feature_columns),
            "model_version": model_version
        })
        
        mlflow.log_metrics(metrics)
        
        if tags:
            mlflow.set_tags(tags)
        
        mlflow.spark.log_model(
            model,
            artifact_path="model",
            registered_model_name=model_name
        )
        
        logger.info(f"Model registered successfully")
        logger.info(f"  - Run ID: {run_id}")
        logger.info(f"  - Model Name: {model_name}")
        logger.info(f"  - Version: {model_version}")
        logger.info(f"  - Metrics: {metrics}")
        
        return run_id


def load_model(model_name: str, version: int = None):
    logger.info(f"Loading model: {model_name} (version: {version})")
    
    if version:
        model_uri = f"models:/{model_name}/{version}"
    else:
        model_uri = f"models:/{model_name}/latest"
    
    model = mlflow.spark.load_model(model_uri)
    
    logger.info(f"Model loaded successfully from: {model_uri}")
    return model


def save_model_metadata(
    model_name: str,
    metrics: dict,
    feature_columns: list,
    model_path: str,
    metadata_path: str = None
):
    import json
    
    if metadata_path is None:
        metadata_path = f"./model_metadata/{model_name}_metadata.json"
    
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    
    metadata = {
        "model_name": model_name,
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics,
        "feature_columns": feature_columns,
        "model_path": model_path,
        "model_type": "LogisticRegression"
    }
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Model metadata saved to: {metadata_path}")

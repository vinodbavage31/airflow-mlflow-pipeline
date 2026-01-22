import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from ml_pipeline.data_reader import create_spark_session
from ml_pipeline.feature_engineering import engineer_features, prepare_features_for_training
from ml_pipeline.model_training import train_logistic_regression, split_data
from ml_pipeline.model_registry import setup_mlflow, register_model

if __name__ == "__main__":
    spark = create_spark_session("MLPipelineExample")
    
    print("Example: Creating sample data for testing")
    
    sample_data = [
        ("corp1", 1000.0, 150.0, 15.0, 5, 3, 2, "Large"),
        ("corp2", 500.0, 80.0, 16.0, 3, 2, 1, "Medium"),
        ("corp3", 2000.0, 250.0, 12.5, 8, 5, 3, "Enterprise"),
        ("corp4", 50.0, 30.0, 60.0, 2, 1, 1, "Small"),
        ("corp5", 1500.0, 120.0, 8.0, 6, 4, 2, "Large"),
    ]
    
    df = spark.createDataFrame(sample_data, [
        "corporate_id", "revenue", "profit", "profit_margin",
        "num_suppliers", "num_customers", "num_activity_places", "revenue_category"
    ])
    
    print("Sample data created")
    df.show()
    
    df_features = engineer_features(df, profit_threshold=100.0)
    print("\nFeatures engineered")
    df_features.show()
    
    df_prepared = prepare_features_for_training(df_features)
    print("\nFeatures prepared for training")
    df_prepared.select("corporate_id", "high_profit", "features").show(truncate=False)
    
    train_df, test_df = split_data(df_prepared, train_ratio=0.6)
    
    print("\nTraining model...")
    model, predictions, metrics = train_logistic_regression(train_df, test_df)
    
    print(f"\nModel Metrics: {metrics}")
    
    print("\nPredictions:")
    predictions.select("corporate_id", "high_profit", "prediction", "probability").show(truncate=False)
    
    setup_mlflow(experiment_name="example_experiment")
    
    run_id = register_model(
        model=model,
        model_name="example_model",
        metrics=metrics,
        feature_columns=["revenue", "num_suppliers", "profit_margin_normalized"]
    )
    
    print(f"\nModel registered with run_id: {run_id}")
    
    spark.stop()

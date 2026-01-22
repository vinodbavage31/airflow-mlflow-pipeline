from pyspark.sql import DataFrame
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def train_logistic_regression(
    train_df: DataFrame,
    test_df: DataFrame = None,
    max_iter: int = 100,
    reg_param: float = 0.01
) -> tuple:
    logger.info("Training Logistic Regression model")
    logger.info(f"Training set size: {train_df.count()}")
    
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="high_profit",
        maxIter=max_iter,
        regParam=reg_param
    )
    
    model = lr.fit(train_df)
    
    logger.info("Model training completed")
    
    if test_df is not None:
        logger.info(f"Evaluating on test set: {test_df.count()} records")
        predictions = model.transform(test_df)
        
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="high_profit",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        evaluator_accuracy = MulticlassClassificationEvaluator(
            labelCol="high_profit",
            predictionCol="prediction",
            metricName="accuracy"
        )
        
        auc = evaluator_auc.evaluate(predictions)
        accuracy = evaluator_accuracy.evaluate(predictions)
        
        logger.info(f"Model Metrics:")
        logger.info(f"  - AUC-ROC: {auc:.4f}")
        logger.info(f"  - Accuracy: {accuracy:.4f}")
        
        return model, predictions, {"auc": auc, "accuracy": accuracy}
    
    return model, None, {}


def split_data(df: DataFrame, train_ratio: float = 0.8, seed: int = 42) -> tuple:
    logger.info(f"Splitting data: train={train_ratio*100:.0f}%, test={(1-train_ratio)*100:.0f}%")
    
    train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=seed)
    
    logger.info(f"Train set: {train_df.count()} records")
    logger.info(f"Test set: {test_df.count()} records")
    
    return train_df, test_df

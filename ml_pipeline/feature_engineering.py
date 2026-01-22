from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def calculate_profit_threshold(df: DataFrame, profit_threshold: float = 100.0) -> DataFrame:
    logger.info(f"Calculating profit threshold label (threshold: {profit_threshold})")
    
    df_with_label = df.withColumn(
        "high_profit",
        F.when(F.col("profit") >= profit_threshold, 1.0).otherwise(0.0)
    )
    
    high_profit_count = df_with_label.filter(F.col("high_profit") == 1.0).count()
    total_count = df_with_label.count()
    
    logger.info(f"High profit corporations: {high_profit_count}/{total_count} ({high_profit_count/total_count*100:.2f}%)")
    
    return df_with_label


def parse_list_column(df: DataFrame, column_name: str) -> DataFrame:
    logger.info(f"Parsing list column: {column_name}")
    
    def count_items(value):
        if value is None:
            return 0
        if isinstance(value, str):
            if not value or value.lower() in {"nan", "none", "null"}:
                return 0
            if ";" in value:
                return len([p.strip() for p in value.split(";") if p.strip()])
            return 1
        return 0
    
    df_parsed = df.withColumn(
        f"{column_name}_count",
        F.udf(count_items, IntegerType())(F.col(column_name))
    )
    
    return df_parsed


def engineer_features(df: DataFrame, profit_threshold: float = 100.0) -> DataFrame:
    logger.info("Starting feature engineering")
    
    df_processed = df.select(
        F.col("corporate_id"),
        F.col("corporate_name"),
        F.col("revenue").cast(DoubleType()).alias("revenue"),
        F.col("profit").cast(DoubleType()).alias("profit"),
        F.col("profit_margin").cast(DoubleType()).alias("profit_margin"),
        F.col("num_suppliers").cast(IntegerType()).alias("num_suppliers"),
        F.col("num_customers").cast(IntegerType()).alias("num_customers"),
        F.col("num_activity_places").cast(IntegerType()).alias("num_activity_places"),
        F.col("revenue_category")
    )
    
    df_processed = df_processed.filter(
        F.col("revenue").isNotNull() &
        F.col("profit").isNotNull() &
        F.col("num_suppliers").isNotNull()
    )
    
    df_with_label = calculate_profit_threshold(df_processed, profit_threshold)
    
    df_features = df_with_label.withColumn(
        "revenue_log",
        F.log(F.col("revenue") + 1.0)
    ).withColumn(
        "profit_margin_normalized",
        F.when(F.col("profit_margin").isNull(), 0.0).otherwise(F.col("profit_margin") / 100.0)
    ).withColumn(
        "supplier_customer_ratio",
        F.when(F.col("num_customers") > 0, F.col("num_suppliers") / F.col("num_customers")).otherwise(0.0)
    )
    
    logger.info(f"Feature engineering completed. Features: revenue, revenue_log, num_suppliers, profit_margin_normalized, supplier_customer_ratio")
    logger.info(f"Total records after feature engineering: {df_features.count()}")
    
    return df_features


def prepare_features_for_training(df: DataFrame, feature_columns: list = None) -> DataFrame:
    logger.info("Preparing features for model training")
    
    if feature_columns is None:
        feature_columns = ["revenue", "num_suppliers", "profit_margin_normalized"]
    
    df_clean = df.select(
        ["corporate_id", "high_profit"] + feature_columns
    ).filter(
        F.col("high_profit").isNotNull()
    )
    
    for col_name in feature_columns:
        df_clean = df_clean.filter(F.col(col_name).isNotNull())
    
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_vectorized = assembler.transform(df_clean)
    
    logger.info(f"Features prepared. Records: {df_vectorized.count()}")
    
    return df_vectorized

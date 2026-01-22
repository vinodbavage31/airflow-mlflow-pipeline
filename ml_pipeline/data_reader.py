from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyiceberg.catalog import load_catalog
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "MLPipeline") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-warehouse-path/") \
        .getOrCreate()
    return spark


def read_from_iceberg(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    aws_config: dict = None
) -> DataFrame:
    logger.info(f"Reading from Iceberg table: {database_name}.{table_name}")
    
    if aws_config:
        spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_config.get('access_key_id', ''))
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_config.get('secret_access_key', ''))
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_config.get('region', 'us-east-1')}.amazonaws.com")
    
    try:
        df = spark.read \
            .format("iceberg") \
            .option("catalog", "glue_catalog") \
            .load(f"{database_name}.{table_name}")
        
        logger.info(f"Successfully read {df.count()} records from Iceberg table")
        return df
    except Exception as e:
        logger.error(f"Error reading from Iceberg table: {str(e)}")
        raise


def read_from_iceberg_pyiceberg(
    database_name: str,
    table_name: str,
    aws_config: dict = None,
    spark: SparkSession = None
) -> DataFrame:
    logger.info(f"Reading from Iceberg table using pyiceberg: {database_name}.{table_name}")
    
    if spark is None:
        spark = create_spark_session()
    
    try:
        if aws_config is None:
            aws_config = {
                'access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
                'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                'region': os.getenv('AWS_REGION', 'us-east-1'),
            }
        
        catalog = load_catalog(
            "glue_catalog",
            **{
                "type": "glue",
                "glue.region": aws_config.get('region', 'us-east-1'),
                "glue.id": aws_config.get('aws_account_id', os.getenv('AWS_ACCOUNT_ID', ''))
            }
        )
        
        table = catalog.load_table(f"{database_name}.{table_name}")
        
        df = table.scan().to_arrow().to_pandas()
        
        spark_df = spark.createDataFrame(df)
        
        logger.info(f"Successfully read {spark_df.count()} records from Iceberg table")
        return spark_df
    except Exception as e:
        logger.error(f"Error reading from Iceberg table: {str(e)}")
        raise

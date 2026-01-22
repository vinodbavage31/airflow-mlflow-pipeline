# End-to-End ML Pipeline with Airflow, PySpark & MLflow

A production-grade, containerized machine learning pipeline demonstrating modern MLOps practices. This project orchestrates data generation, ETL processing, model training, and experiment tracking using industry-standard tools.

**Author**: Vinod Bavage  
**Repository**: [github.com/vinodbavage31/ML-pipeline](https://github.com/vinodbavage31/ML-pipeline)

---

## 📋 Overview

This project implements a complete ML workflow from data generation to model deployment, designed as a learning resource for MLOps fundamentals:

1. **Data Generation**: Automated creation of synthetic supply chain and financial datasets
2. **Data Storage**: Upload to S3 with support for multiple formats (Parquet, CSV, JSON)
3. **ETL Pipeline**: Data ingestion, transformation, and feature engineering using PySpark
4. **ML Training**: Model training and evaluation with experiment tracking
5. **Orchestration**: Airflow DAGs managing end-to-end workflow automation
6. **Experiment Tracking**: MLflow for logging parameters, metrics, and model artifacts

---

## 🏗️ Architecture

```
┌─────────────────┐
│ Data Generation │ → Faker generates synthetic data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   S3 Storage    │ → Parquet/CSV/JSON files
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Airflow DAGs    │ → Orchestration layer
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PySpark ETL    │ → Data transformation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Model Training  │ → ML pipeline with PySpark MLlib
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ MLflow Tracking │ → Experiment management
└─────────────────┘
```

**Key Components**:
- **Apache Airflow**: Workflow orchestration and scheduling
- **PySpark**: Distributed data processing and ML training
- **MLflow**: Experiment tracking and model registry
- **Docker**: Containerized deployment for reproducibility
- **Apache Iceberg**: Modern table format for data lake (optional)

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Git
- Python 3.8+
- AWS credentials (for S3 upload)

### Installation

1. **Clone the repository**:
```bash
git clone https://github.com/vinodbavage31/ML-pipeline.git
cd ML-pipeline
```

2. **Configure environment variables**:
```bash
cp .env.example .env
# Edit .env with your AWS credentials and configuration
```

3. **Build and start services**:
```bash
docker-compose build
docker-compose up -d
```

4. **Access the interfaces**:
- **Airflow UI**: http://localhost:8080
- **MLflow UI**: http://localhost:5000

---

## 📊 Data Generation Pipeline

Generate synthetic supply chain and financial data for the ML pipeline.

### Quick Usage

```bash
# Generate 100 rows and upload to S3
python main_data_generate_pipeline.py 100 --config pipeline_config.json

# Generate without uploading
python main_data_generate_pipeline.py 100 --no-upload

# Custom configuration
python main_data_generate_pipeline.py 200 --bucket my-bucket --format parquet
```

### Configuration File

Create `pipeline_config.json`:

```json
{
  "generation": {
    "num_rows": 100,
    "output_format": "parquet",
    "output_dir": "./data"
  },
  "s3": {
    "bucket": "your-bucket-name",
    "source1_path": "source1_supply_chain.parquet",
    "source2_path": "source2_financial.parquet"
  },
  "aws": {
    "access_key_id": "your-access-key-id",
    "secret_access_key": "your-secret-access-key",
    "region": "us-east-1"
  },
  "upload": {
    "enabled": true,
    "skip_source1": false,
    "skip_source2": false
  }
}
```

### Data Sources

**Source 1 (Supply Chain Data)**:
- `corporate_name_S1`: Company name
- `address`: Full address
- `activity_places`: List of activity locations (2-5 items)
- `top_suppliers`: List of supplier names (3-8 items)

**Source 2 (Financial Data)**:
- `corporate_name_S2`: Company name
- `main_customers`: List of customer names (2-6 items)
- `revenue`: Revenue in millions (10M - 5000M)
- `profit`: Profit in millions (5-20% of revenue)

### Command Line Options

```bash
python main_data_generate_pipeline.py [NUM_ROWS] [OPTIONS]

Positional:
  NUM_ROWS              Number of rows to generate

Options:
  --config PATH         Configuration JSON file
  --format FORMAT       Output format: json, csv, parquet, or both
  --output-dir DIR      Output directory (default: ./data)
  --bucket BUCKET       S3 bucket name
  --no-upload           Skip S3 upload
  --skip-source1        Skip source1 upload
  --skip-source2        Skip source2 upload
```

---

## 🔄 Running the Pipeline

### 1. Start Services

```bash
docker-compose up -d
```

### 2. Access Airflow

Navigate to http://localhost:8080 and enable the DAGs:
- `etl_pipeline_dag`: Data ingestion and transformation
- `ml_pipeline_dag`: Model training and evaluation

### 3. Trigger Pipeline

Click the "Trigger DAG" button to start the workflow. Monitor progress in real-time through the Airflow UI.

### 4. View Experiments

Open http://localhost:5000 to view:
- Training parameters
- Model metrics (accuracy, precision, recall, F1)
- Logged artifacts and models

---

## 📁 Project Structure

```
.
├── dags/
│   ├── etl_pipeline_dag.py          # ETL orchestration
│   ├── ml_pipeline_dag.py           # ML training orchestration
│   ├── airflow_utils.py             # Shared utilities
│   └── __init__.py
│
├── etl_pipeline/
│   └── # PySpark ETL transformations
│
├── ml_pipeline/
│   └── # Model training and evaluation
│
├── data_generator.py                 # Synthetic data generation
├── s3_client.py                      # S3 utilities
├── s3_uploader.py                    # S3 upload functions
├── main_data_generate_pipeline.py   # Data pipeline entry point
│
├── mlruns/                           # MLflow experiment storage
├── logs/                             # Airflow task logs
├── plugins/                          # Custom Airflow plugins
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── pipeline_config.json
├── .env
└── README.md
```

---

## 🔧 Configuration

### Environment Variables

Key configuration variables in `.env`:

```bash
# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_EXPERIMENT_NAME=ml-pipeline-experiment

# Model Parameters
TRAIN_RATIO=0.8
REG_PARAM=0.1
FEATURE_COLUMNS=feature1,feature2,feature3

# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET=your-bucket-name

# Data Generation
NUM_ROWS=100
OUTPUT_FORMAT=parquet
OUTPUT_DIR=./data
```

### Docker Services

| Service        | Description                    | Port |
|----------------|--------------------------------|------|
| Airflow Web    | DAG monitoring & orchestration | 8080 |
| MLflow Server  | Experiment tracking UI         | 5000 |
| Airflow Scheduler | Task scheduling            | -    |

---

## 📦 Dependencies

Core libraries (see `requirements.txt`):

```
apache-airflow>=2.7.0
mlflow>=2.8.0
pyspark>=3.3.0
boto3>=1.26.0
pandas>=1.5.0
pyarrow>=10.0.0
faker>=19.0.0
python-dotenv>=1.0.0
pyiceberg>=0.5.0
```

---

## 📈 Features

- ✅ Fully containerized environment with Docker Compose
- ✅ Automated synthetic data generation with realistic distributions
- ✅ Multi-format support (Parquet, CSV, JSON)
- ✅ S3 integration for cloud storage
- ✅ Distributed data processing with PySpark
- ✅ Modular ETL and ML pipelines
- ✅ Comprehensive experiment tracking
- ✅ Reproducible local development setup
- ✅ Clean separation of concerns (data → ETL → ML → tracking)

---

## 📊 Format Comparison

| Format      | Pros                                        | Cons                      | Best For                  |
|-------------|---------------------------------------------|---------------------------|---------------------------|
| **Parquet** | Efficient, preserves types, compressed      | Requires pandas/pyarrow   | Production, ETL pipelines |
| **CSV**     | Human-readable, universal                   | Loses list structure      | Manual inspection, Excel  |
| **JSON**    | Preserves structure, readable               | Larger files, slower      | APIs, web applications    |

**Recommendation**: Use Parquet for production workflows to maintain data integrity and optimize performance.

---

## 🎓 Learning Objectives

This project demonstrates:

1. **MLOps Best Practices**: End-to-end ML workflow automation
2. **Containerization**: Docker-based reproducible environments
3. **Data Engineering**: PySpark for distributed data processing
4. **Workflow Orchestration**: Airflow DAG design and task dependencies
5. **Experiment Management**: MLflow tracking and model versioning
6. **Cloud Integration**: S3 for scalable data storage
7. **Configuration Management**: Environment-based settings

---

## 🔍 Notes

- **Database**: SQLite is used for simplicity in local development
- **Executor**: SequentialExecutor configured for Airflow
- **Purpose**: Educational and demonstration purposes
- **Extensibility**: Can be extended with PostgreSQL, CeleryExecutor, or cloud deployment (AWS/GCP/Azure)

---

## 🛠️ Troubleshooting

**Services not starting?**
```bash
docker-compose down -v
docker-compose up --build
```

**Airflow DAG not appearing?**
- Check `dags/` folder permissions
- Verify DAG syntax: `docker-compose exec webserver airflow dags list`

**MLflow not logging experiments?**
- Verify `MLFLOW_TRACKING_URI` in environment
- Check MLflow server logs: `docker-compose logs mlflow`

---

## 📝 License

This project is open source and available for educational purposes.

---

## 🤝 Contributing

This is a learning project. Feel free to fork, experiment, and suggest improvements!

---

## 📧 Contact

**Vinod Bavage**  
GitHub: [@vinodbavage31](https://github.com/vinodbavage31)

---

⭐ If you found this project helpful, please consider giving it a star!
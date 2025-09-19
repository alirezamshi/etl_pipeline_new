# Data Engineering Pipeline

A robust, scalable ETL and data workflow pipeline supporting multiple platforms including local CSVs, S3, Databricks, and Neo4j. Built with modularity, maintainability, and production resilience in mind.

## 🏗️ Architecture

This pipeline follows modern data engineering best practices inspired by:
- **Airflow**: DAG orchestration and task dependencies
- **dbt**: Modular transformations and SQL-first approach
- **Prefect/Dagster**: Resilient flows with retries and monitoring
- **Spark**: Distributed processing capabilities

## 📁 Project Structure

```
├── src/                    # Core application code
│   ├── extractors/         # Data ingestion modules
│   ├── transformers/       # Data transformation logic
│   ├── loaders/           # Data persistence modules
│   ├── analytics/         # Data inspection and quality checks
│   ├── utils/             # Utility functions and helpers
│   └── main.py            # Main pipeline orchestrator
├── config/                # Environment configurations
│   ├── dev.yaml          # Development environment
│   └── prod.yaml         # Production environment
├── tests/                 # Test suite
├── input_data/           # Input data files
├── output_data/          # Processed output data
├── intermediate_data/    # Temporary intermediate files
└── docs/                 # Documentation
```

## 🚀 Features

### Multi-Platform Support
- **Local Files**: CSV, Parquet, JSON
- **Cloud Storage**: AWS S3 with full credential management
- **Databases**: Neo4j graph database support
- **Future**: Databricks, PostgreSQL, BigQuery ready

### Data Processing
- **Extraction**: Reliable data ingestion with retry logic
- **Transformation**: Clean, aggregate, and transform data
- **Loading**: Idempotent data persistence with conflict resolution
- **Analytics**: Comprehensive data quality checks and profiling

### Production Features
- **Configuration Management**: Environment-specific YAML configs
- **Logging**: Structured JSON logging with Splunk compatibility
- **Monitoring**: Data quality metrics and pipeline health checks
- **Idempotency**: Skip unchanged pipeline runs
- **Error Handling**: Comprehensive exception handling and retries

## 🛠️ Installation

### Prerequisites
- Python 3.10+
- Poetry (recommended) or pip

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd data-engineering-pipeline

# Install dependencies with Poetry
poetry install

# Or with pip
pip install -r requirements.txt

# Activate virtual environment (if using Poetry)
poetry shell
```

## 📖 Usage

### Basic Pipeline Execution
```bash
# Run with development configuration
python src/main.py --config config/dev.yaml

# Run with production configuration
python src/main.py --config config/prod.yaml --log-level INFO --json-logs

# Reset idempotency check
python src/main.py --config config/dev.yaml --reset-idempotency
```

### Configuration

#### Development Example (`config/dev.yaml`)
```yaml
source:
  type: csv
  path: sample_sales_data.csv

transform:
  type: cleaner
  remove_duplicates: true
  missing_strategy: fill

load:
  type: csv
  path: cleaned_data.csv
  overwrite: true

analytics:
  enabled: true
  quality:
    quality_rules:
      completeness:
        max_null_percentage: 5.0
```

#### Production Example (`config/prod.yaml`)
```yaml
source:
  type: s3
  bucket: ${S3_BUCKET}
  key: data/input.parquet
  access_key: ${AWS_ACCESS_KEY_ID}
  secret_key: ${AWS_SECRET_ACCESS_KEY}

transform:
  type: aggregator
  group_by: ["category"]
  aggregations:
    amount: ["sum", "mean"]

load:
  type: s3
  bucket: ${S3_OUTPUT_BUCKET}
  key: reports/aggregated.parquet
```

### Environment Variables
```bash
# AWS Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export S3_BUCKET=your-input-bucket
export S3_OUTPUT_BUCKET=your-output-bucket

# Neo4j Credentials
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=your_password
```

## 🔧 Components

### Extractors
- **CSVExtractor**: Read local CSV files with chunking support
- **S3Extractor**: Extract data from AWS S3 buckets
- **Neo4jExtractor**: Query Neo4j graph databases

### Transformers
- **DataCleaner**: Remove duplicates, handle missing values, filter data
- **DataAggregator**: Group and aggregate data with multiple functions
- **SQLTransformer**: Apply SQL transformations using in-memory SQLite

### Loaders
- **CSVLoader**: Write data to local CSV files
- **S3Loader**: Upload data to AWS S3 buckets
- **Neo4jLoader**: Load data into Neo4j graph database

### Analytics
- **DataInspector**: Profile data characteristics and statistics
- **DataQualityChecker**: Validate data against quality rules
- **DataReporter**: Generate comprehensive data reports

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_extractors.py -v

# Run tests with different verbosity
pytest -v --tb=short
```

## 📊 Data Quality

The pipeline includes comprehensive data quality checking:

- **Completeness**: Null value analysis and thresholds
- **Uniqueness**: Duplicate detection and unique constraints
- **Validity**: Data type validation and range checks
- **Consistency**: Cross-column relationship validation
- **Accuracy**: Business rule validation

Quality reports include:
- Overall quality score
- Detailed issue breakdown
- Actionable recommendations
- Executive summary

## 🔄 Pipeline Flow

1. **Configuration Loading**: Parse YAML config with environment variable substitution
2. **Idempotency Check**: Skip if configuration unchanged
3. **Data Extraction**: Pull data from configured source
4. **Data Transformation**: Apply cleaning, aggregation, or custom transforms
5. **Data Analysis**: Optional quality checks and profiling
6. **Data Loading**: Persist to configured destination
7. **Cleanup**: Remove intermediate files if configured

## 🚀 Deployment

### Local Development
```bash
# Run pipeline
python src/main.py --config config/dev.yaml

# Monitor logs
tail -f pipeline.log
```

### Production Deployment
```bash
# Set environment variables
export ENVIRONMENT=prod
export LOG_LEVEL=INFO

# Run with production config
python src/main.py --config config/prod.yaml --json-logs

# Or use with cron/scheduler
0 2 * * * cd /path/to/pipeline && python src/main.py --config config/prod.yaml
```

### Docker Deployment
```dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "src/main.py", "--config", "config/prod.yaml", "--json-logs"]
```

## 🔍 Monitoring

### Logging
- Structured JSON logging for production
- Configurable log levels
- Pipeline metrics and timing
- Error tracking with stack traces

### Metrics
- Data volume processed
- Pipeline execution time
- Quality scores
- Error rates

### Alerting
- Failed pipeline runs
- Data quality threshold breaches
- Missing data sources
- Performance degradation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add tests for new functionality
- Update documentation
- Use type hints
- Write descriptive commit messages

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the documentation in the `docs/` directory
- Review the test files for usage examples

## 🗺️ Roadmap

- [ ] Databricks integration
- [ ] PostgreSQL support
- [ ] BigQuery connector
- [ ] Real-time streaming support
- [ ] Web UI for pipeline monitoring
- [ ] Advanced data lineage tracking
- [ ] Machine learning model integration
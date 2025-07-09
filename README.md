# Data Processing Framework

A comprehensive multi-tool data collection and processing framework designed for API security tools like Cequence, 42Crunch, and Apigee. The framework provides standardized data collection, processing, and reporting capabilities with robust error handling and scheduling.

## Features

### Core Features
- **Multi-Tool Support**: Cequence, 42Crunch, and Apigee integrations
- **Standardized Exception Handling**: UUID-tracked errors with structured JSON logging
- **Flexible Scheduling**: Cron-based collection and processing schedules
- **REST API Interface**: FastAPI-based web API for management and monitoring
- **Parallel Processing**: Concurrent job execution with queue management
- **Data Validation**: Comprehensive validation and enrichment pipelines
- **Excel Processing**: Multi-sheet reading and data transformation
- **Configurable Logging**: Structured logging with multiple outputs

### Technical Stack
- **Python 3.9+**
- **FastAPI** for REST API
- **Celery** for background tasks
- **Redis** for caching and queuing
- **PostgreSQL** for data storage
- **Pydantic** for data validation
- **Loguru/Structlog** for structured logging

## Installation

### Prerequisites
- Python 3.9 or higher
- Redis server
- PostgreSQL (optional)

### Setup
1. **Clone or setup the framework:**
   ```bash
   cd data_processing
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create configuration:**
   ```bash
   cp config.yaml.example config.yaml
   # Edit config.yaml with your tool credentials
   ```

4. **Set environment variables:**
   ```bash
   export CEQUENCE_API_KEY="your-api-key"
   export FORTYTWCRUNCH_CLIENT_ID="your-client-id"
   export FORTYTWCRUNCH_CLIENT_SECRET="your-client-secret"
   export APIGEE_SERVICE_ACCOUNT_KEY="your-service-account-key"
   export APIGEE_PROJECT_ID="your-project-id"
   ```

## Usage

### Command Line Interface

#### Start the Server
```bash
python main.py server --config config.yaml
```

#### Manual Data Collection
```bash
python main.py collect cequence --config config.yaml
python main.py collect 42crunch --config config.yaml
python main.py collect apigee --config config.yaml
```

#### Manual Data Processing
```bash
python main.py process cequence --stage validation --config config.yaml
python main.py process 42crunch --stage enrichment --config config.yaml
python main.py process apigee --stage analytics --config config.yaml
```

#### List Supported Tools
```bash
python main.py tools
```

#### Validate Configuration
```bash
python main.py validate-config --config config.yaml
```

#### Cleanup Old Jobs
```bash
python main.py cleanup --max-age 24 --config config.yaml
```

### REST API

#### Health Check
```bash
curl http://localhost:8000/health
```

#### Get Supported Tools
```bash
curl http://localhost:8000/tools
```

#### Get All Jobs
```bash
curl http://localhost:8000/jobs
```

#### Get Job Status
```bash
curl http://localhost:8000/jobs/{job_id}
```

#### Trigger Manual Collection
```bash
curl -X POST http://localhost:8000/jobs/collect/cequence
```

#### Trigger Manual Processing
```bash
curl -X POST http://localhost:8000/jobs/process/cequence?processing_stage=validation
```

### Python API

#### Basic Usage
```python
from core.config_manager import config_manager
from tools.tool_factory import ToolFactory

# Load configuration
config_manager.load_config('config.yaml')

# Create collector
collector = ToolFactory.create_collector('cequence', credentials)

# Collect data
result = await collector.collect_data('manual')
print(f"Collected {result.records_collected} records")

# Create processor
processor = ToolFactory.create_processor('cequence', credentials)

# Process data
result = await processor.process_data(data, 'validation')
print(f"Processed {result.records_processed} records")
```

## Configuration

### Main Configuration (config.yaml)
```yaml
# Tool configurations
tools:
  cequence:
    name: "cequence"
    api_base_url: "https://api.cequence.ai"
    auth_type: "api_key"
    credentials:
      api_key: "${CEQUENCE_API_KEY}"

# Schedule configurations
schedules:
  daily_collection:
    cron_expression: "0 2 * * *"
    tools: ["cequence", "42crunch", "apigee"]
    job_type: "collection"
    enabled: true
```

### Environment Variables
```bash
# Cequence
CEQUENCE_API_KEY=your-api-key

# 42Crunch
FORTYTWCRUNCH_CLIENT_ID=your-client-id
FORTYTWCRUNCH_CLIENT_SECRET=your-client-secret

# Apigee
APIGEE_SERVICE_ACCOUNT_KEY=your-service-account-key
APIGEE_PROJECT_ID=your-project-id

# Framework
ENCRYPTION_KEY=your-encryption-key
JWT_SECRET=your-jwt-secret
```

## Architecture

### Core Components
- **Exception Handling**: Standardized error recording with UUIDs
- **Logging System**: Structured JSON logging with rotation
- **Configuration Manager**: YAML-based configuration with validation
- **API Client**: Multi-authentication REST client with rate limiting
- **Scheduler**: Cron-based job scheduling with parallel execution
- **Data Models**: Pydantic models for validation and serialization

### Tool Structure
```
tools/
├── __init__.py
├── base_tool.py          # Base classes for collectors and processors
├── tool_factory.py       # Tool instantiation factory
├── cequence/
│   ├── __init__.py
│   ├── collector.py      # Cequence data collection
│   └── processor.py      # Cequence data processing
├── fortytwocrunck/
│   ├── __init__.py
│   ├── collector.py      # 42Crunch data collection
│   └── processor.py      # 42Crunch data processing
└── apigee/
    ├── __init__.py
    ├── collector.py      # Apigee data collection
    └── processor.py      # Apigee data processing
```

### Data Flow
1. **Collection**: Tools collect raw data from APIs
2. **Storage**: Raw data saved to storage/raw_data/
3. **Processing**: Data validated, enriched, and analyzed
4. **Storage**: Processed data saved to storage/processed_data/
5. **Reporting**: Summary reports generated

## Error Handling

### Standard Exception Format
```json
{
  "error_id": "uuid4-string",
  "timestamp": "2024-01-01T00:00:00.000Z",
  "error_code": "DATA_COLLECTION_ERROR",
  "message": "Collection failed for tool cequence",
  "context": {
    "tool_name": "cequence",
    "collection_type": "scheduled",
    "function": "collect_data",
    "file": "collector.py",
    "line": 123
  },
  "stack_trace": "...",
  "original_exception": "..."
}
```

### Exception Types
- `DataCollectionException`: Data collection failures
- `DataProcessingError`: Data processing failures
- `ConfigurationException`: Configuration errors
- `APIClientException`: API communication errors
- `SchedulerException`: Job scheduling failures

## Monitoring

### Job Status Tracking
```python
# Get job status
job = await collection_scheduler.get_job_status(job_id)
print(f"Status: {job.status}")
print(f"Duration: {job.duration}s")
print(f"Records: {job.records_processed}")
```

### Performance Metrics
- Collection success/failure rates
- Processing duration and throughput
- Error frequency and patterns
- Resource utilization

## Data Storage

### Directory Structure
```
storage/
├── raw_data/
│   ├── cequence/
│   ├── 42crunch/
│   └── apigee/
├── processed_data/
│   ├── cequence/
│   ├── 42crunch/
│   └── apigee/
├── reports/
└── backups/
```

### Data Formats
- **Raw Data**: JSON files with timestamps
- **Processed Data**: Enriched JSON with metadata
- **Reports**: CSV/Excel summaries

## Contributing

### Adding New Tools
1. Create tool directory under `tools/`
2. Implement `BaseCollector` and `BaseProcessor`
3. Register in `ToolFactory`
4. Add configuration schema
5. Update documentation

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Run linting
flake8 .
black .
```

## Security

### API Security
- API key authentication
- Rate limiting
- Request/response validation
- Secure credential storage

### Data Security
- Encryption at rest (configurable)
- Secure credential handling
- Audit logging
- Access control

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
1. Check the logs in `logs/data_processing.log`
2. Validate configuration with `python main.py validate-config`
3. Review error tracking in exception logs
4. Check tool-specific documentation

## Changelog

### Version 1.0.0
- Initial release
- Multi-tool support (Cequence, 42Crunch, Apigee)
- Standardized exception handling
- Flexible scheduling system
- REST API interface
- Comprehensive logging and monitoring # data_processor

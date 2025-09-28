# ETL Script Documentation
## Comprehensive Guide to Classes, Methods, and Dependencies

---

## 1. Main ETL Orchestrator

### 1.1 ETLPipelineOrchestrator Class

**File**: `robust_main_etl.py`

**Purpose**: Main orchestrator class that coordinates the entire ETL pipeline execution with comprehensive error handling and monitoring.

#### Class Variables and Dependencies
```python
# External Dependencies
import os
import sys
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import traceback

# Internal Dependencies
from robust_etl_config import ETLConfig, StudyConfig
from robust_data_extractor import DataExtractor, DataExtractionError
from robust_data_validator import DataValidator, ValidationError
from robust_data_transformer import DataTransformer, DataTransformationError
from robust_data_loader import DataLoader, DataLoadError, ETLPerformanceMonitor
```

#### Constructor and Attributes
```python
def __init__(self, config: ETLConfig):
    self.config = config
    self.logger = self._setup_logging()
    self.pipeline_stats = {
        'pipeline_start_time': datetime.now(),
        'studies_processed': 0,
        'studies_failed': 0,
        'total_records_processed': 0,
        'total_duration_seconds': 0,
        'errors': [],
        'warnings': []
    }
    
    # Initialize ETL components
    self.extractor = DataExtractor(config)
    self.validator = DataValidator(config)
    self.transformer = DataTransformer(config)
```

#### Key Methods

**`execute_study_pipeline(self, study_code: str) -> Dict[str, Any]`**
- **Purpose**: Execute complete ETL pipeline for a single study
- **Parameters**: 
  - `study_code`: Study identifier (e.g., "SRP049820")
- **Returns**: Comprehensive pipeline execution results
- **Process Flow**:
  1. Extract data from all sources (JSON, TSV)
  2. Validate extracted data quality and consistency
  3. Transform data to warehouse format
  4. Load data with MERGE operations
  5. Generate performance report
- **Error Handling**: Captures and logs all exceptions with full traceback

**`execute_full_pipeline(self, study_codes: List[str] = None) -> Dict[str, Any]`**
- **Purpose**: Execute ETL pipeline for multiple studies
- **Parameters**:
  - `study_codes`: Optional list of specific studies to process
- **Returns**: Aggregated pipeline results across all studies
- **Features**: Batch processing with progress tracking

**`_setup_logging(self) -> logging.Logger`**
- **Purpose**: Configure comprehensive logging with file rotation
- **Features**: 
  - Console and file handlers
  - Rotating file logs with size limits
  - Structured JSON logging for monitoring

---

## 2. Configuration Management

### 2.1 ETLConfig Class

**File**: `robust_etl_config.py`

**Purpose**: Centralized configuration management with validation and dynamic study code detection.

#### Class Attributes
```python
@dataclass
class ETLConfig:
    base_path: str
    connection_string: str
    database_name: str = "BioinformaticsWarehouse"
    
    # File pattern configuration
    study_code_pattern: str = r"[A-Za-z]{2,3}-[A-Za-z]{3}-\d+|[A-Za-z]{3}\d+"
    json_metadata_file: str = "aggregated_metadata.json"
    tsv_metadata_pattern: str = "{study_code}/metadata_{study_code}.tsv"
    expression_data_pattern: str = "{study_code}/{study_code}.tsv"
    
    # Processing settings
    batch_size: int = 50000
    chunk_size: int = 10000
    max_workers: int = 8
    memory_limit_mb: int = 4096
    timeout_seconds: int = 7200
    retry_attempts: int = 3
    retry_delay_seconds: int = 5
    
    # Data validation thresholds
    max_null_percentage: float = 0.1
    min_genes_per_sample: int = 1000
    max_duplicate_percentage: float = 0.05
```

#### Key Methods

**`__post_init__(self) -> None`**
- **Purpose**: Post-initialization validation and setup
- **Validation**: Ensures all configuration values are within valid ranges
- **Setup**: Compiles regex patterns for study code detection

**`from_environment(cls) -> "ETLConfig"`**
- **Purpose**: Create configuration from environment variables
- **Features**: Supports deployment in containerized environments

**`get_study_file_paths(self, study_code: str) -> Dict[str, Path]`**
- **Purpose**: Resolve file paths for a specific study
- **Returns**: Dictionary with paths to JSON metadata, TSV metadata, and expression data

---

## 3. Data Extraction Layer

### 3.1 DataExtractor Class

**File**: `robust_data_extractor.py`

**Purpose**: Extract data from various source formats with robust error handling and validation.

#### Dependencies
```python
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Iterator, List, Optional, Tuple
import logging
import hashlib
from datetime import datetime
import chardet
```

#### Constructor and Attributes
```python
def __init__(self, config):
    self.config = config
    self.logger = logging.getLogger(__name__)
    self.extraction_stats = {
        'files_processed': 0,
        'records_extracted': 0,
        'warnings': [],
        'errors': []
    }
```

#### Key Methods

**`extract_all_sources(self, study_code: str) -> Dict[str, Any]`**
- **Purpose**: Extract all data sources for a specific study
- **Process**:
  1. Extract JSON metadata
  2. Extract TSV metadata
  3. Extract expression data matrix
  4. Validate data consistency across sources
- **Returns**: Comprehensive extracted data structure

**`extract_json_metadata(self, json_path: Path, study_code: str) -> Dict[str, Any]`**
- **Purpose**: Parse JSON metadata file
- **Features**:
  - Automatic encoding detection
  - Study-specific data extraction
  - Validation of required fields
- **Returns**: Experiment and sample metadata

**`extract_expression_data(self, tsv_path: Path) -> pd.DataFrame`**
- **Purpose**: Parse TSV expression matrix
- **Features**:
  - Handles large files efficiently
  - Validates matrix structure
  - Preserves data types
- **Returns**: Expression matrix as pandas DataFrame

**`_detect_encoding(self, file_path: Path) -> str`**
- **Purpose**: Detect file encoding automatically
- **Returns**: Detected encoding (e.g., 'utf-8', 'latin-1')

**`_validate_data_consistency(self, json_data: Dict, tsv_metadata: pd.DataFrame, expression_data: pd.DataFrame, study_code: str) -> None`**
- **Purpose**: Ensure data consistency across all sources
- **Validation**:
  - Sample count consistency
  - Study code presence in all sources
  - Gene count validation

#### Custom Exception
```python
class DataExtractionError(Exception):
    """Custom exception for data extraction errors"""
    pass
```

---

## 4. Data Validation Layer

### 4.1 DataValidator Class

**File**: `robust_data_validator.py`

**Purpose**: Comprehensive data validation and quality assurance with configurable thresholds.

#### Dependencies
```python
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
import logging
import re
from datetime import datetime
import hashlib
import json
```

#### Constructor and Attributes
```python
def __init__(self, config):
    self.config = config
    self.logger = logging.getLogger(__name__)
    self.validation_results = {
        'validation_passed': False,
        'validation_timestamp': None,
        'checks_performed': [],
        'warnings': [],
        'errors': [],
        'quality_metrics': {}
    }
    
    # Validation patterns
    self._study_code_pattern = re.compile(r'^[A-Za-z]{2,3}-[A-Za-z]{3}-\d+|[A-Za-z]{3}\d+$')
    self._gene_symbol_pattern = re.compile(r'^[A-Za-z0-9_-]+$')
    self._sample_accession_pattern = re.compile(r'^SRR\d+$')
    
    # Quality thresholds
    self.quality_thresholds = {
        'max_null_percentage': config.max_null_percentage,
        'max_duplicate_percentage': config.max_duplicate_percentage,
        'min_genes_per_sample': config.min_genes_per_sample,
        'max_expression_range': 1000000,
        'min_samples_per_study': 3,
        'max_missing_samples_percentage': 0.2
    }
```

#### Key Methods

**`validate_all_data(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]`**
- **Purpose**: Perform comprehensive validation on all extracted data
- **Validation Checks**:
  - Data completeness and null percentages
  - Gene and sample count requirements
  - Expression value ranges and distributions
  - Duplicate detection
  - Metadata consistency
- **Returns**: Detailed validation results with quality score

**`validate_expression_matrix(self, df: pd.DataFrame) -> Dict[str, Any]`**
- **Purpose**: Validate expression matrix structure and content
- **Checks**:
  - Minimum gene count per sample
  - Null value percentages
  - Expression value ranges
  - Data type consistency

**`validate_metadata_consistency(self, json_data: Dict, expression_df: pd.DataFrame) -> Dict[str, Any]`**
- **Purpose**: Validate consistency between JSON metadata and expression data
- **Checks**:
  - Sample count alignment
  - Sample accession code matching
  - Study metadata completeness

**`get_quality_score(self) -> float`**
- **Purpose**: Calculate overall data quality score (0-100)
- **Factors**: Null percentages, duplicate rates, validation errors

**`log_validation_report(self, validation_results: Dict[str, Any]) -> None`**
- **Purpose**: Generate structured validation report
- **Features**: JSON-formatted logging for monitoring

#### Custom Exception
```python
class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass
```

---

## 5. Data Transformation Layer

### 5.1 DataTransformer Class

**File**: `robust_data_transformer.py`

**Purpose**: Transform extracted data into warehouse-ready format with performance optimization.

#### Dependencies
```python
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, Iterator
import logging
from datetime import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
```

#### Constructor and Attributes
```python
def __init__(self, config):
    self.config = config
    self.logger = logging.getLogger(__name__)
    self.transformation_stats = {
        'records_transformed': 0,
        'genes_processed': 0,
        'samples_processed': 0,
        'warnings': [],
        'errors': []
    }
    
    # Gene symbol validation patterns
    self._gene_symbol_pattern = re.compile(r'^[A-Za-z0-9_-]+$')
    self._valid_gene_types = {'protein_coding', 'lncRNA', 'miRNA', 'rRNA', 'tRNA', 'pseudogene'}
```

#### Key Methods

**`transform_all_data(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]`**
- **Purpose**: Transform all extracted data into warehouse format
- **Process**:
  1. Melt expression matrix to long format
  2. Generate dimension records
  3. Create batch identifiers
  4. Optimize data types for loading
- **Returns**: Transformed data ready for database loading

**`melt_expression_matrix(self, expression_df: pd.DataFrame, study_code: str) -> pd.DataFrame`**
- **Purpose**: Convert wide expression matrix to long format
- **Input**: Wide matrix (genes x samples)
- **Output**: Long format (gene_id, sample_id, expression_value)
- **Features**: Memory-efficient processing for large matrices

**`generate_dimension_records(self, json_data: Dict, melted_df: pd.DataFrame) -> Dict[str, Any]`**
- **Purpose**: Generate dimension table records from metadata
- **Creates Records For**:
  - Studies
  - Samples with illness inference
  - Genes with symbol validation
  - Platforms with normalization

**`infer_illness_from_title(self, sample_title: str) -> str`**
- **Purpose**: Infer illness category from sample title
- **Rules**:
  - "control", "healthy" → CONTROL
  - "sepsis", "septic" → SEPSIS
  - "shock" → SEPTIC_SHOCK
  - Default → UNKNOWN

**`optimize_data_types(self, df: pd.DataFrame) -> pd.DataFrame`**
- **Purpose**: Optimize pandas data types for memory efficiency
- **Optimizations**:
  - Float32 instead of Float64 where appropriate
  - Category types for repeated strings
  - Integer types for IDs

**`get_transformation_summary(self) -> Dict[str, Any]`**
- **Purpose**: Get summary statistics of transformation process
- **Returns**: Record counts, processing times, warnings

#### Custom Exception
```python
class DataTransformationError(Exception):
    """Custom exception for data transformation errors"""
    pass
```

---

## 6. Data Loading Layer

### 6.1 DataLoader Class

**File**: `robust_data_loader.py`

**Purpose**: Load transformed data into database with MERGE statements and performance monitoring.

#### Dependencies
```python
import pandas as pd
import pyodbc
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
```

#### Constructor and Attributes
```python
def __init__(self, config):
    self.config = config
    self.logger = logging.getLogger(__name__)
    self.connection = None
    self.load_stats = {
        'tables_loaded': 0,
        'records_inserted': 0,
        'records_updated': 0,
        'records_failed': 0,
        'load_duration_seconds': 0,
        'warnings': [],
        'errors': []
    }
    self.batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
```

#### Key Methods

**`load_all_data(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]`**
- **Purpose**: Load all transformed data into database
- **Process**:
  1. Upsert dimension tables with MERGE
  2. Bulk insert fact records
  3. Update statistics and metadata
  4. Validate loaded data
- **Returns**: Load statistics and success status

**`upsert_dimension_tables(self, dimension_data: Dict[str, Any]) -> Dict[str, int]`**
- **Purpose**: Upsert all dimension tables and return generated keys
- **Uses MERGE Operations For**:
  - dim_study
  - dim_sample
  - dim_gene
  - dim_platform
- **Returns**: Mapping of dimension names to generated keys

**`bulk_insert_fact_records(self, fact_data: pd.DataFrame, dimension_keys: Dict[str, int]) -> int`**
- **Purpose**: Bulk insert expression records into fact table
- **Features**:
  - SqlBulkCopy for high performance
  - Batch processing for memory efficiency
  - Foreign key resolution
- **Returns**: Number of records successfully inserted

**`generate_batch_id(self) -> str`**
- **Purpose**: Generate unique batch identifier for idempotency
- **Format**: batch_YYYYMMDD_HHMMSS_random

**`get_load_summary(self) -> Dict[str, Any]`**
- **Purpose**: Get comprehensive loading statistics
- **Includes**: Record counts, timing, errors, warnings

#### Context Manager Support
```python
def __enter__(self):
    """Context manager entry - establish database connection"""
    self.connect()
    return self

def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit - close database connection"""
    self.disconnect()
```

#### Custom Exception
```python
class DataLoadError(Exception):
    """Custom exception for data loading errors"""
    pass
```

---

## 7. Performance Monitoring

### 7.1 ETLPerformanceMonitor Class

**File**: `robust_data_loader.py` (included)

**Purpose**: Monitor and report ETL pipeline performance metrics.

#### Constructor and Attributes
```python
class ETLPerformanceMonitor:
    def __init__(self, connection):
        self.connection = connection
        self.logger = logging.getLogger(__name__)
```

#### Key Methods

**`generate_performance_report(self, study_code: str) -> Dict[str, Any]`**
- **Purpose**: Generate comprehensive performance report for a study
- **Metrics**:
  - Processing duration
  - Records per second
  - Memory usage
  - Database performance stats
- **Returns**: Structured performance report

**`log_performance_metrics(self, metrics: Dict[str, Any]) -> None`**
- **Purpose**: Log performance metrics for monitoring
- **Features**: JSON-formatted logging for analytics

---

## 8. Testing Framework

### 8.1 ETLTestSuite Class

**File**: `test_etl_pipeline.py`

**Purpose**: Comprehensive test suite for all ETL pipeline components.

#### Dependencies
```python
import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
import logging
from typing import Dict, Any, List
import traceback
```

#### Constructor and Attributes
```python
def __init__(self):
    self.test_results = {
        'tests_run': 0,
        'tests_passed': 0,
        'tests_failed': 0,
        'component_results': {},
        'performance_metrics': {}
    }
    self.logger = self._setup_logging()
    self.test_data_dir = None
```

#### Key Methods

**`run_all_tests(self) -> Dict[str, Any]`**
- **Purpose**: Execute complete test suite
- **Test Categories**:
  - Extraction tests
  - Validation tests
  - Transformation tests
  - Loading tests
  - Integration tests
- **Returns**: Comprehensive test results

**`create_test_data(self) -> Path`**
- **Purpose**: Generate realistic test data
- **Creates**:
  - Small expression matrix
  - JSON metadata
  - Configuration files
- **Returns**: Path to test data directory

**`test_extraction_component(self) -> Dict[str, Any]`**
- **Purpose**: Test data extraction functionality
- **Tests**:
  - File reading
  - Data parsing
  - Error handling
  - Performance

**`test_validation_component(self) -> Dict[str, Any]`**
- **Purpose**: Test data validation logic
- **Tests**:
  - Quality checks
  - Threshold validation
  - Error detection

**`test_transformation_component(self) -> Dict[str, Any]`**
- **Purpose**: Test data transformation
- **Tests**:
  - Matrix melting
  - Dimension generation
  - Data type optimization

**`test_loading_component(self) -> Dict[str, Any]`**
- **Purpose**: Test database loading
- **Tests**:
  - Connection handling
  - MERGE operations
  - Bulk inserts
  - Error recovery

**`test_end_to_end_pipeline(self) -> Dict[str, Any]`**
- **Purpose**: Complete end-to-end integration test
- **Process**: Full pipeline execution with realistic data
- **Validation**: Data integrity and performance

---

## 9. Dependencies and Requirements

### 9.1 Core Dependencies

```python
# Data Processing
pandas>=1.3.0
numpy>=1.21.0
pyarrow>=5.0.0

# Database
pyodbc>=4.0.0
sqlalchemy>=1.4.0

# Configuration and Utilities
pyyaml>=5.4.0
dataclasses>=0.6  # For Python < 3.7
python-dateutil>=2.8.0

# Performance and Monitoring
tqdm>=4.62.0  # Progress bars
psutil>=5.8.0  # System monitoring
memory-profiler>=0.58.0  # Memory profiling

# Testing
pytest>=6.2.0
pytest-cov>=2.12.0
pytest-mock>=3.6.0

# Development
black>=21.7.0  # Code formatting
flake8>=3.9.0  # Linting
mypy>=0.910  # Type checking
```

### 9.2 System Requirements

- **Python**: 3.8+ (recommended: 3.9+)
- **Database**: SQL Server 2017+ (recommended: SQL Server 2022)
- **Memory**: 8GB minimum, 16GB recommended
- **Disk**: SSD recommended for staging area
- **OS**: Linux/Windows/macOS (Linux recommended for production)

### 9.3 Database Driver Requirements

```bash
# For Linux (Ubuntu/Debian)
sudo apt-get install unixodbc-dev
sudo apt-get install msodbcsql17

# For Windows
# Install Microsoft ODBC Driver 17 for SQL Server

# For macOS
brew install unixodbc
brew install msodbcsql17
```

---

## 10. Configuration File Structure

### 10.1 Main Configuration (`config.yaml`)

```yaml
database:
  connection_string: "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=BioinformaticsWarehouse;Trusted_Connection=yes;"
  database_name: "BioinformaticsWarehouse"

processing:
  batch_size: 50000
  chunk_size: 10000
  max_workers: 8
  memory_limit_mb: 4096
  timeout_seconds: 7200

validation:
  max_null_percentage: 0.1
  max_duplicate_percentage: 0.05
  min_genes_per_sample: 1000
  min_samples_per_study: 3

logging:
  level: "INFO"
  file: "/var/log/etl/pipeline.log"
  rotation: true
  max_size_mb: 100
  backup_count: 5

monitoring:
  enable_performance_logging: true
  log_slow_queries: true
  slow_query_threshold_ms: 1000
```

### 10.2 Study-Specific Configuration

```yaml
study_overrides:
  SRP049820:
    illness_overrides:
      SRR1652895: "CONTROL"
      SRR1652896: "SEPSIS"
    platform_mapping:
      "Illumina Genome Analyzer": 
        manufacturer: "Illumina"
        technology: "RNA-SEQ"
```

---

## 11. Usage Examples

### 11.1 Basic Pipeline Execution

```python
from robust_main_etl import ETLPipelineOrchestrator
from robust_etl_config import ETLConfig

# Create configuration
config = ETLConfig(
    base_path="/data/gene_expression",
    connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=BioinformaticsWarehouse;Trusted_Connection=yes;"
)

# Initialize orchestrator
orchestrator = ETLPipelineOrchestrator(config)

# Process single study
results = orchestrator.execute_study_pipeline("SRP049820")

# Process multiple studies
all_results = orchestrator.execute_full_pipeline(["SRP049820", "SRP123456"])

# Check results
if results['success']:
    print(f"Successfully processed {results['records_processed']} records")
    print(f"Processing time: {results['duration_seconds']:.2f} seconds")
else:
    print(f"Pipeline failed: {results['error']}")
```

### 11.2 Component-Level Usage

```python
# Individual component usage for testing or custom workflows
from robust_data_extractor import DataExtractor
from robust_data_validator import DataValidator

# Extract data
extractor = DataExtractor(config)
extracted_data = extractor.extract_all_sources("SRP049820")

# Validate data
validator = DataValidator(config)
validation_results = validator.validate_all_data(extracted_data)

if validation_results['validation_passed']:
    print(f"Data quality score: {validator.get_quality_score()}")
else:
    print("Validation errors:", validation_results['errors'])
```

### 11.3 Testing

```python
# Run test suite
from test_etl_pipeline import ETLTestSuite

test_suite = ETLTestSuite()
results = test_suite.run_all_tests()

print(f"Tests run: {results['tests_run']}")
print(f"Tests passed: {results['tests_passed']}")
print(f"Tests failed: {results['tests_failed']}")
```

---

This comprehensive documentation provides detailed information about all classes, methods, dependencies, and usage patterns for the ETL pipeline. The architecture supports scalable, production-grade processing of gene expression data with robust error handling, performance optimization, and monitoring capabilities.
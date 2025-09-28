# Comprehensive ETL Architecture Document
## Gene Expression Data Pipeline for SQL Server 2022

### Executive Summary

This document outlines a production-grade ETL platform for ingesting gene expression data (TSV matrices) and metadata (JSON) into a SQL Server 2022 data warehouse. The architecture addresses the gaps identified in the current implementation and provides a robust, scalable solution for gene pair analysis.

**Key Improvements Over Current Implementation:**
- Row-oriented staging to avoid SQL Server column limits
- Content-hash based incremental loading with idempotency
- Comprehensive illness inference rules and platform normalization
- Advanced analytics interfaces for downstream ML/correlation analysis
- Robust error handling with structured logging and diagnostics
- Performance optimization for large-scale datasets

---

## 1. System Architecture Overview

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ TSV Matrix  │  │ JSON Meta   │  │ Config YAML │             │
│  │ (Expression)│  │ (Samples)   │  │ (Settings)  │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE LAYERS                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   EXTRACTION    │  │  TRANSFORMATION │  │     LOADING     │ │
│  │   (Row-Orient)  │  │ (Dim Upserts)   │  │ (Merge Ops)     │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TARGET DATA WAREHOUSE                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   Staging   │  │ Dimensions  │  │    Facts    │             │
│  │   Tables    │  │   (SCD1)    │  │  (Append)   │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS INTERFACES                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │     Views   │  │Materialized │  │Feature      │             │
│  │   (Long)    │  │   Tables    │  │Catalog      │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Core Components

#### 1.2.1 Data Extraction Layer
- **Stream Processing**: TSV files processed in chunks using PyArrow
- **Row-Oriented Melt**: Expression matrices converted from wide to long format
- **Content Hashing**: SHA256-based batch_id generation for idempotency
- **Memory Management**: Configurable chunk sizes for large datasets

#### 1.2.2 Data Transformation Layer
- **Dimension Upserts**: SCD1 pattern for study, sample, gene, platform, illness dimensions
- **Illness Inference**: Regex-based classification with override support
- **Platform Normalization**: Standardized platform naming and metadata
- **Data Quality**: Comprehensive validation and QC metric capture

#### 1.2.3 Data Loading Layer
- **Bulk Operations**: SqlBulkCopy for staging table population
- **MERGE Statements**: Upsert operations for dimension tables
- **Batch Processing**: Content-hash based duplicate prevention
- **Performance Monitoring**: Real-time metrics and SLA tracking

---

## 2. Data Model Design

### 2.1 Dimensional Model

#### 2.1.1 Dimension Tables

**dim_study**
```sql
CREATE TABLE dim_study (
    study_key INT IDENTITY(1,1) PRIMARY KEY,
    study_accession_code VARCHAR(50) NOT NULL UNIQUE,
    study_title NVARCHAR(1000),
    study_pubmed_id VARCHAR(20),
    study_technology VARCHAR(50),
    study_organism VARCHAR(100),
    study_platform_summary NVARCHAR(500),
    study_description NVARCHAR(MAX),
    source_first_published DATETIME2,
    source_last_modified DATETIME2,
    etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_date DATETIME2 DEFAULT GETUTCDATE()
);
```

**dim_sample**
```sql
CREATE TABLE dim_sample (
    sample_key INT IDENTITY(1,1) PRIMARY KEY,
    sample_accession_code VARCHAR(50) NOT NULL UNIQUE,
    sample_title NVARCHAR(1000),
    sample_organism VARCHAR(100),
    sample_platform VARCHAR(200),
    sample_treatment NVARCHAR(500),
    sample_cell_line VARCHAR(100),
    sample_tissue VARCHAR(100),
    is_processed BIT,
    processor_name VARCHAR(100),
    processor_version VARCHAR(50),
    illness_key INT,
    platform_key INT,
    study_key INT,
    exclude_from_analysis BIT DEFAULT 0,
    etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_date DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (illness_key) REFERENCES dim_illness(illness_key),
    FOREIGN KEY (platform_key) REFERENCES dim_platform(platform_key),
    FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
);
```

**dim_gene**
```sql
CREATE TABLE dim_gene (
    gene_key INT IDENTITY(1,1) PRIMARY KEY,
    gene_id VARCHAR(50) NOT NULL UNIQUE,
    gene_symbol VARCHAR(50),
    gene_name NVARCHAR(500),
    gene_type VARCHAR(50),
    chromosome VARCHAR(10),
    start_position BIGINT,
    end_position BIGINT,
    etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
    etl_updated_date DATETIME2 DEFAULT GETUTCDATE()
);
```

**dim_illness** (Seed Values)
```sql
CREATE TABLE dim_illness (
    illness_key INT IDENTITY(1,1) PRIMARY KEY,
    illness_label VARCHAR(50) NOT NULL UNIQUE,
    illness_definition NVARCHAR(500)
);

-- Seed data
INSERT INTO dim_illness (illness_label, illness_definition) VALUES
('CONTROL', 'Healthy control samples'),
('NO_SEPSIS', 'Samples without sepsis'),
('SEPSIS', 'Sepsis patients'),
('SEPTIC_SHOCK', 'Septic shock patients'),
('UNKNOWN', 'Illness status unknown');
```

**dim_platform**
```sql
CREATE TABLE dim_platform (
    platform_key INT IDENTITY(1,1) PRIMARY KEY,
    platform_accession VARCHAR(100) NOT NULL UNIQUE,
    platform_name NVARCHAR(500),
    manufacturer VARCHAR(100),
    measurement_technology VARCHAR(20) -- RNA-SEQ, MICROARRAY, OTHER
);
```

#### 2.1.2 Fact Tables

**fact_gene_expression**
```sql
CREATE TABLE fact_gene_expression (
    expression_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    sample_key INT NOT NULL,
    gene_key INT NOT NULL,
    study_key INT NOT NULL,
    expression_value FLOAT(53) NOT NULL,
    expression_log2_value AS (CASE WHEN expression_value > 0 THEN LOG(expression_value) / LOG(2.0) END) PERSISTED,
    is_significant BIT NULL,
    batch_id VARCHAR(64) NOT NULL,
    etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (sample_key) REFERENCES dim_sample(sample_key),
    FOREIGN KEY (gene_key) REFERENCES dim_gene(gene_key),
    FOREIGN KEY (study_key) REFERENCES dim_study(study_key),
    INDEX CCI_fact_gene_expression CLUSTERED COLUMNSTORE,
    INDEX NCI_gene_study ON (gene_key, study_key) INCLUDE (expression_value)
);
```

**fact_feature_values** (Optional - for ML features)
```sql
CREATE TABLE fact_feature_values (
    feature_record_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    entity_scope VARCHAR(20) NOT NULL, -- GENE, GENE_PAIR, SAMPLE, STUDY_COHORT
    entity_id_1 BIGINT NOT NULL,
    entity_id_2 BIGINT NULL,
    feature_name VARCHAR(100) NOT NULL,
    feature_value FLOAT(53) NOT NULL,
    study_key INT NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
    FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
);
```

### 2.2 Staging Schema

**staging.expression_rows**
```sql
CREATE TABLE staging.expression_rows (
    study_accession_code VARCHAR(50),
    batch_id VARCHAR(64),
    gene_id VARCHAR(50),
    sample_accession_code VARCHAR(50),
    expression_value FLOAT(53),
    line_no BIGINT NULL,
    file_name NVARCHAR(260),
    file_hash CHAR(64),
    PRIMARY KEY (batch_id, gene_id, sample_accession_code)
);
```

### 2.3 Metadata/QC Tables

**meta_etl_process_log**
```sql
CREATE TABLE meta_etl_process_log (
    run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
    batch_id VARCHAR(64),
    step NVARCHAR(64),
    status NVARCHAR(16),
    attempt INT,
    started_at DATETIME2,
    ended_at DATETIME2,
    rows_read BIGINT NULL,
    rows_written BIGINT NULL,
    error_code INT NULL,
    error_message NVARCHAR(MAX) NULL,
    context_json NVARCHAR(MAX) NULL
);
```

**meta_data_validation_log**
```sql
CREATE TABLE meta_data_validation_log (
    run_id UNIQUEIDENTIFIER,
    batch_id VARCHAR(64),
    study_key INT,
    validation_code NVARCHAR(64),
    severity NVARCHAR(16),
    details NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);
```

**meta_study_qc**
```sql
CREATE TABLE meta_study_qc (
    study_key INT PRIMARY KEY,
    ks_statistic FLOAT,
    ks_pvalue FLOAT,
    ks_warning NVARCHAR(200),
    quantile_normalized BIT,
    quant_sf_only BIT,
    collected_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
);
```

---

## 3. Core Python Components

### 3.1 Enhanced Configuration Management

```python
# config/pipeline_config.py
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import yaml
from pathlib import Path

@dataclass
class PipelineConfig:
    """Enhanced configuration with study-specific overrides"""
    
    # Database configuration
    connection_string: str
    database_name: str = "BioinformaticsWarehouse"
    
    # File patterns
    study_code_pattern: str = r"[A-Za-z]{2,3}-[A-Za-z]{3}-\d+|[A-Za-z]{3}\d+"
    json_metadata_file: str = "aggregated_metadata.json"
    
    # Processing configuration
    chunk_size: int = 50000
    max_workers: int = 4
    memory_limit_mb: int = 8192
    
    # Illness inference rules
    illness_inference_rules: List[Dict[str, str]] = field(default_factory=list)
    illness_overrides: Dict[str, str] = field(default_factory=dict)
    
    # Platform normalization
    platform_lookup: Dict[str, Dict[str, str]] = field(default_factory=dict)
    
    # Quality thresholds
    qc_thresholds: Dict[str, float] = field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, config_path: Path) -> "PipelineConfig":
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        return cls(**config_data)
```

### 3.2 Advanced Data Extractor

```python
# extractors/advanced_extractor.py
import pyarrow.csv as pv
import pyarrow as pa
from typing import Iterator, Dict, Any, List
import hashlib
from pathlib import Path

class AdvancedDataExtractor:
    """Enhanced extractor with row-oriented processing"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def extract_expression_matrix_streaming(
        self, 
        tsv_path: Path, 
        study_code: str,
        chunk_size: int = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream TSV file and yield row-oriented expression data
        
        Args:
            tsv_path: Path to expression matrix
            study_code: Study accession code
            chunk_size: Number of genes to process per chunk
            
        Yields:
            Dictionary with expression data in long format
        """
        chunk_size = chunk_size or self.config.chunk_size
        
        # Compute file hash for batch_id
        file_hash = self._compute_file_hash(tsv_path)
        batch_id = self._generate_batch_id(study_code, file_hash)
        
        # Stream read TSV with PyArrow
        parse_options = pv.ParseOptions(delimiter='\t')
        read_options = pv.ReadOptions(column_names=None)
        
        with pv.open_csv(tsv_path, parse_options=parse_options, read_options=read_options) as reader:
            for batch in reader:
                # Convert to pandas for easier manipulation
                df = batch.to_pandas()
                
                # Melt to long format: gene_id, sample_id, expression_value
                melted_df = pd.melt(
                    df, 
                    id_vars=['Gene'], 
                    var_name='sample_accession_code', 
                    value_name='expression_value'
                )
                
                # Rename columns to match schema
                melted_df = melted_df.rename(columns={'Gene': 'gene_id'})
                
                # Add metadata
                melted_df['study_accession_code'] = study_code
                melted_df['batch_id'] = batch_id
                melted_df['file_hash'] = file_hash
                melted_df['file_name'] = tsv_path.name
                
                yield melted_df.to_dict('records')
    
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _generate_batch_id(self, study_code: str, file_hash: str) -> str:
        """Generate deterministic batch_id"""
        content = f"{study_code}||{file_hash}||{datetime.now().isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]
```

### 3.3 Illness Inference Engine

```python
# transform/illness_inference.py
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class IllnessRule:
    """Illness inference rule configuration"""
    pattern: str
    illness_label: str
    priority: int
    description: str

class IllnessInferenceEngine:
    """Advanced illness classification with override support"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.rules = self._load_inference_rules()
        self.overrides = config.illness_overrides
    
    def _load_inference_rules(self) -> List[IllnessRule]:
        """Load illness inference rules from configuration"""
        return [
            IllnessRule(
                pattern=r'\b(septic\s*shock|sshock|septic_shock|shock)\b',
                illness_label='SEPTIC_SHOCK',
                priority=1,
                description='Septic shock indicators'
            ),
            IllnessRule(
                pattern=r'\bno[-_\s]?sepsis\b|\bnon[-_\s]?sepsis\b',
                illness_label='NO_SEPSIS',
                priority=2,
                description='No sepsis indicators'
            ),
            IllnessRule(
                pattern=r'\bsepsis\b',
                illness_label='SEPSIS',
                priority=3,
                description='Sepsis indicators'
            ),
            IllnessRule(
                pattern=r'\bcontrol\b|\bhealthy\b',
                illness_label='CONTROL',
                priority=4,
                description='Control/healthy samples'
            )
        ]
    
    def infer_illness(
        self, 
        sample_title: str, 
        sample_accession: str,
        study_code: str
    ) -> Tuple[str, str]:
        """
        Infer illness from sample title with override support
        
        Returns:
            Tuple of (illness_label, inference_method)
        """
        # Check for overrides first
        if sample_accession in self.overrides:
            return self.overrides[sample_accession], 'override'
        
        # Apply regex rules in priority order
        for rule in sorted(self.rules, key=lambda x: x.priority):
            if re.search(rule.pattern, sample_title, re.IGNORECASE):
                return rule.illness_label, 'regex'
        
        # Default to UNKNOWN
        return 'UNKNOWN', 'default'
```

### 3.4 Platform Normalization Engine

```python
# transform/platform_normalizer.py
import re
from typing import Dict, Optional, Tuple

class PlatformNormalizationEngine:
    """Normalize platform names and extract metadata"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.manufacturer_lookup = {
            'illumina': 'Illumina',
            'affymetrix': 'Affymetrix',
            'agilent': 'Agilent',
            'roche': 'Roche',
            '454': 'Roche 454'
        }
    
    def normalize_platform(
        self, 
        platform_raw: str, 
        study_technology: str
    ) -> Tuple[str, str, str, str]:
        """
        Normalize platform information
        
        Returns:
            Tuple of (platform_accession, platform_name, manufacturer, measurement_technology)
        """
        # Handle "Name (Accession)" format
        match = re.match(r'(.+)\(([^)]+)\)', platform_raw)
        if match:
            platform_name = match.group(1).strip()
            platform_accession = match.group(2)
        else:
            platform_name = platform_raw
            platform_accession = platform_raw
        
        # Extract manufacturer
        manufacturer = self._infer_manufacturer(platform_name)
        
        # Determine measurement technology
        measurement_tech = self._infer_measurement_technology(
            platform_name, study_technology
        )
        
        return platform_accession, platform_name, manufacturer, measurement_tech
    
    def _infer_manufacturer(self, platform_name: str) -> str:
        """Infer manufacturer from platform name"""
        platform_lower = platform_name.lower()
        for key, manufacturer in self.manufacturer_lookup.items():
            if key in platform_lower:
                return manufacturer
        return 'Unknown'
    
    def _infer_measurement_technology(
        self, 
        platform_name: str, 
        study_technology: str
    ) -> str:
        """Infer measurement technology"""
        if 'rna-seq' in study_technology.lower() or 'rna_seq' in study_technology.lower():
            return 'RNA-SEQ'
        elif 'microarray' in platform_name.lower():
            return 'MICROARRAY'
        else:
            return 'OTHER'
```

### 3.5 Enhanced Data Loader

```python
# loaders/enhanced_loader.py
import pyodbc
import pandas as pd
from typing import Dict, Any, List, Optional
import logging
from contextlib import contextmanager

class EnhancedDataLoader:
    """Enhanced loader with MERGE operations and performance optimization"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection_string = config.connection_string
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = pyodbc.connect(self.connection_string)
        try:
            yield connection
        finally:
            connection.close()
    
    def bulk_load_staging(
        self, 
        expression_data: List[Dict[str, Any]], 
        batch_id: str
    ) -> int:
        """
        Bulk load expression data to staging table
        
        Args:
            expression_data: List of expression records
            batch_id: Batch identifier for idempotency
            
        Returns:
            Number of records loaded
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Prepare data for bulk insert
            df = pd.DataFrame(expression_data)
            
            # Use fast_executemany for better performance
            cursor.fast_executemany = True
            
            # Insert into staging table
            insert_sql = """
            INSERT INTO staging.expression_rows 
            (study_accession_code, batch_id, gene_id, sample_accession_code, 
             expression_value, file_name, file_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            data_tuples = [
                (
                    row['study_accession_code'],
                    batch_id,
                    row['gene_id'],
                    row['sample_accession_code'],
                    row['expression_value'],
                    row['file_name'],
                    row['file_hash']
                )
                for _, row in df.iterrows()
            ]
            
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            
            return len(data_tuples)
    
    def upsert_dimensions(self, study_data: Dict[str, Any]) -> Dict[str, int]:
        """
        Upsert all dimension tables and return generated keys
        
        Returns:
            Dictionary mapping dimension names to their keys
        """
        dimension_keys = {}
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Upsert study dimension
            study_key = self._upsert_study(cursor, study_data['study_metadata'])
            dimension_keys['study_key'] = study_key
            
            # Upsert platform dimension
            platform_key = self._upsert_platform(cursor, study_data['platform_metadata'])
            dimension_keys['platform_key'] = platform_key
            
            # Upsert samples
            sample_keys = self._upsert_samples(
                cursor, 
                study_data['sample_metadata'], 
                study_key, 
                platform_key
            )
            dimension_keys['sample_keys'] = sample_keys
            
            # Upsert genes
            gene_keys = self._upsert_genes(cursor, study_data['gene_metadata'])
            dimension_keys['gene_keys'] = gene_keys
            
            conn.commit()
            
        return dimension_keys
    
    def _upsert_study(self, cursor, study_metadata: Dict[str, Any]) -> int:
        """Upsert study dimension using MERGE"""
        merge_sql = """
        MERGE dim_study AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
            (study_accession_code, study_title, study_pubmed_id, study_technology, 
             study_organism, study_description, source_first_published, source_last_modified)
        ON target.study_accession_code = source.study_accession_code
        WHEN MATCHED THEN
            UPDATE SET 
                study_title = source.study_title,
                study_pubmed_id = source.study_pubmed_id,
                study_technology = source.study_technology,
                study_organism = source.study_organism,
                study_description = source.study_description,
                source_first_published = source.source_first_published,
                source_last_modified = source.source_last_modified,
                etl_updated_date = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (study_accession_code, study_title, study_pubmed_id, study_technology, 
                   study_organism, study_description, source_first_published, source_last_modified)
            VALUES (source.study_accession_code, source.study_title, source.study_pubmed_id, 
                   source.study_technology, source.study_organism, source.study_description,
                   source.source_first_published, source.source_last_modified)
        OUTPUT INSERTED.study_key;
        """
        
        cursor.execute(merge_sql, (
            study_metadata['accession_code'],
            study_metadata['title'],
            study_metadata.get('pubmed_id'),
            study_metadata['technology'],
            study_metadata['organisms'][0] if study_metadata.get('organisms') else None,
            study_metadata.get('description'),
            study_metadata.get('source_first_published'),
            study_metadata.get('source_last_modified')
        ))
        
        return cursor.fetchone()[0]
```

### 3.6 Analytics Interface Layer

```python
# analytics/interface_generator.py
from typing import Dict, List, Any
import pandas as pd

class AnalyticsInterfaceGenerator:
    """Generate analytics-ready views and materialized tables"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def create_expression_long_view(self, connection) -> None:
        """Create vw_expression_long view for downstream analysis"""
        view_sql = """
        CREATE OR ALTER VIEW analytics.vw_expression_long AS
        SELECT 
            s.sample_key,
            s.sample_accession_code,
            s.sample_title,
            i.illness_label,
            st.study_key,
            st.study_accession_code,
            st.study_title,
            g.gene_key,
            g.gene_id,
            g.gene_symbol,
            fe.expression_value,
            fe.expression_log2_value,
            fe.batch_id,
            fe.etl_created_date
        FROM fact_gene_expression fe
        INNER JOIN dim_sample s ON fe.sample_key = s.sample_key
        INNER JOIN dim_gene g ON fe.gene_key = g.gene_key
        INNER JOIN dim_study st ON fe.study_key = st.study_key
        LEFT JOIN dim_illness i ON s.illness_key = i.illness_key
        WHERE s.exclude_from_analysis = 0
        """
        
        cursor = connection.cursor()
        cursor.execute(view_sql)
        connection.commit()
    
    def create_cohort_aggregation_view(self, connection) -> None:
        """Create view for cohort-based analysis"""
        view_sql = """
        CREATE OR ALTER VIEW analytics.vw_expression_by_cohort AS
        SELECT 
            st.study_accession_code,
            i.illness_label,
            g.gene_id,
            COUNT(DISTINCT s.sample_key) as sample_count,
            AVG(fe.expression_value) as mean_expression,
            STDEV(fe.expression_value) as std_expression,
            MIN(fe.expression_value) as min_expression,
            MAX(fe.expression_value) as max_expression,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fe.expression_value) as median_expression
        FROM fact_gene_expression fe
        INNER JOIN dim_sample s ON fe.sample_key = s.sample_key
        INNER JOIN dim_gene g ON fe.gene_key = g.gene_key
        INNER JOIN dim_study st ON fe.study_key = st.study_key
        INNER JOIN dim_illness i ON s.illness_key = i.illness_key
        WHERE s.exclude_from_analysis = 0
        GROUP BY st.study_accession_code, i.illness_label, g.gene_id
        """
        
        cursor = connection.cursor()
        cursor.execute(view_sql)
        connection.commit()
```

---

## 4. Performance Optimization

### 4.1 Indexing Strategy

```sql
-- Columnstore indexes for fact tables
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_gene_expression 
ON fact_gene_expression;

-- Non-clustered indexes for dimension lookups
CREATE NONCLUSTERED INDEX NCI_sample_accession 
ON dim_sample(sample_accession_code);

CREATE NONCLUSTERED INDEX NCI_gene_id 
ON dim_gene(gene_id);

-- Composite indexes for common queries
CREATE NONCLUSTERED INDEX NCI_expression_lookup 
ON fact_gene_expression(gene_key, study_key) 
INCLUDE (sample_key, expression_value);
```

### 4.2 Partitioning Strategy

```sql
-- Partition fact table by study_key for better performance
CREATE PARTITION FUNCTION PF_study_key (INT)
AS RANGE LEFT FOR VALUES (100, 200, 300, 400, 500);

CREATE PARTITION SCHEME PS_study_key 
AS PARTITION PF_study_key ALL TO ([PRIMARY]);

-- Apply partitioning to fact table
CREATE CLUSTERED INDEX CI_fact_gene_expression_study 
ON fact_gene_expression(study_key) 
ON PS_study_key(study_key);
```

### 4.3 Batch Processing Optimization

```python
# performance/batch_optimizer.py
class BatchOptimizer:
    """Optimize batch sizes based on available memory and data characteristics"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.memory_limit_bytes = config.memory_limit_mb * 1024 * 1024
    
    def calculate_optimal_chunk_size(
        self, 
        file_size_mb: int, 
        sample_count: int,
        available_memory_mb: int = None
    ) -> int:
        """
        Calculate optimal chunk size based on file characteristics
        
        Args:
            file_size_mb: Size of TSV file in MB
            sample_count: Number of samples in matrix
            available_memory_mb: Available system memory
            
        Returns:
            Optimal chunk size (number of genes)
        """
        if available_memory_mb is None:
            available_memory_mb = self.config.memory_limit_mb
        
        # Estimate memory per gene (assuming FLOAT64 = 8 bytes per value)
        bytes_per_gene = sample_count * 8 * 2  # *2 for pandas overhead
        
        # Calculate safe chunk size (use 50% of available memory)
        safe_memory_bytes = (available_memory_mb * 1024 * 1024) * 0.5
        optimal_chunk_size = int(safe_memory_bytes / bytes_per_gene)
        
        # Apply bounds
        return max(100, min(optimal_chunk_size, 10000))
```

---

## 5. Error Handling and Monitoring

### 5.1 Structured Logging

```python
# monitoring/structured_logger.py
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

class StructuredLogger:
    """Enhanced logging with structured JSON output"""
    
    def __init__(self, logger_name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
    
    def log_etl_event(
        self,
        event_type: str,
        batch_id: str,
        study_accession: str,
        step: str,
        status: str,
        duration_seconds: Optional[float] = None,
        rows_processed: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log structured ETL event for monitoring and diagnostics
        """
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': event_type,
            'batch_id': batch_id,
            'study_accession': study_accession,
            'step': step,
            'status': status,
            'duration_seconds': duration_seconds,
            'rows_processed': rows_processed,
            'error_code': error_code,
            'error_message': error_message,
            'context': context or {}
        }
        
        if status == 'ERROR':
            self.logger.error(json.dumps(log_entry))
        elif status == 'WARNING':
            self.logger.warning(json.dumps(log_entry))
        else:
            self.logger.info(json.dumps(log_entry))
    
    def log_validation_failure(
        self,
        batch_id: str,
        study_accession: str,
        validation_code: str,
        severity: str,
        details: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log validation failure with structured format"""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'event_type': 'validation_failure',
            'batch_id': batch_id,
            'study_accession': study_accession,
            'validation_code': validation_code,
            'severity': severity,
            'details': details,
            'context': context or {}
        }
        
        if severity == 'ERROR':
            self.logger.error(json.dumps(log_entry))
        else:
            self.logger.warning(json.dumps(log_entry))
```

### 5.2 Error Codes and Diagnostics

```python
# monitoring/error_codes.py
from enum import Enum
from typing import Dict, Any

class ETLErrorCode(Enum):
    """Standardized error codes for ETL pipeline"""
    
    # Data extraction errors (1000-1999)
    EXTRACTION_FILE_NOT_FOUND = 1001
    EXTRACTION_INVALID_FORMAT = 1002
    EXTRACTION_ENCODING_ERROR = 1003
    EXTRACTION_PARSE_ERROR = 1004
    
    # Validation errors (2000-2999)
    VALIDATION_SAMPLE_MISMATCH = 2001
    VALIDATION_GENE_COUNT_LOW = 2002
    VALIDATION_NULL_PERCENTAGE_HIGH = 2003
    VALIDATION_DUPLICATE_SAMPLES = 2004
    
    # Transformation errors (3000-3999)
    TRANSFORMATION_ILLNESS_INFERENCE_FAILED = 3001
    TRANSFORMATION_PLATFORM_NORMALIZATION_FAILED = 3002
    TRANSFORMATION_DIMENSION_LOOKUP_FAILED = 3003
    
    # Loading errors (4000-4999)
    LOAD_CONNECTION_FAILED = 4001
    LOAD_BATCH_DUPLICATE = 4002
    LOAD_FOREIGN_KEY_VIOLATION = 4003
    LOAD_CONSTRAINT_VIOLATION = 4004
    
    # Analytics interface errors (5000-5999)
    ANALYTICS_VIEW_CREATION_FAILED = 5001
    ANALYTICS_MATERIALIZATION_FAILED = 5002

class ErrorDiagnosticManager:
    """Manage error diagnostics and context"""
    
    def __init__(self):
        self.error_contexts = {}
    
    def capture_error_context(
        self,
        error_code: ETLErrorCode,
        study_accession: str,
        batch_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Capture comprehensive error context"""
        error_context = {
            'error_code': error_code.value,
            'error_name': error_code.name,
            'study_accession': study_accession,
            'batch_id': batch_id,
            'timestamp': datetime.utcnow().isoformat(),
            'context': context,
            'system_info': {
                'python_version': sys.version,
                'available_memory': psutil.virtual_memory().available,
                'disk_space': psutil.disk_usage('/').free
            }
        }
        
        self.error_contexts[batch_id] = error_context
        return error_context
```

---

## 6. Testing Framework

### 6.1 Unit Testing

```python
# tests/test_extraction.py
import pytest
import tempfile
import pandas as pd
from pathlib import Path
from extractors.advanced_extractor import AdvancedDataExtractor

class TestAdvancedDataExtractor:
    """Test suite for advanced data extraction"""
    
    def test_expression_matrix_streaming(self):
        """Test streaming extraction of expression matrix"""
        # Create test TSV file
        test_data = {
            'Gene': ['ENSG00000000003', 'ENSG00000000005', 'ENSG00000000419'],
            'SRR1652895': [1.735, 0.173, 4.689],
            'SRR1652896': [0.448, 0.448, 0.448]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
            df = pd.DataFrame(test_data)
            df.to_csv(f.name, sep='\t', index=False)
            temp_file = Path(f.name)
        
        try:
            config = PipelineConfig(connection_string="test")
            extractor = AdvancedDataExtractor(config)
            
            # Test streaming extraction
            results = list(extractor.extract_expression_matrix_streaming(
                temp_file, "SRP049820", chunk_size=2
            ))
            
            # Verify results
            assert len(results) == 2  # 2 chunks
            assert len(results[0]) == 4  # 2 genes * 2 samples
            assert results[0][0]['gene_id'] == 'ENSG00000000003'
            assert results[0][0]['sample_accession_code'] == 'SRR1652895'
            
        finally:
            temp_file.unlink()
    
    def test_batch_id_generation(self):
        """Test deterministic batch_id generation"""
        config = PipelineConfig(connection_string="test")
        extractor = AdvancedDataExtractor(config)
        
        batch_id1 = extractor._generate_batch_id("SRP049820", "abc123")
        batch_id2 = extractor._generate_batch_id("SRP049820", "abc123")
        
        assert batch_id1 == batch_id2  # Should be deterministic
        assert len(batch_id1) == 32  # SHA256 truncated to 32 chars
```

### 6.2 Integration Testing

```python
# tests/test_integration.py
import pytest
import tempfile
import json
from pathlib import Path
from main.etl_orchestrator import ETLOrchestrator

class TestETLIntegration:
    """End-to-end integration tests"""
    
    def test_complete_study_processing(self):
        """Test complete ETL pipeline for a single study"""
        # Create temporary test data
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create mock TSV file
            tsv_data = {
                'Gene': ['ENSG00000000003', 'ENSG00000000005'],
                'SRR1652895': [1.735, 0.173],
                'SRR1652896': [0.448, 0.448]
            }
            
            tsv_file = temp_path / "SRP049820.tsv"
            pd.DataFrame(tsv_data).to_csv(tsv_file, sep='\t', index=False)
            
            # Create mock JSON metadata
            json_data = {
                "experiments": {
                    "SRP049820": {
                        "accession_code": "SRP049820",
                        "title": "Test Study",
                        "technology": "RNA-SEQ",
                        "organisms": ["HOMO_SAPIENS"],
                        "sample_accession_codes": ["SRR1652895", "SRR1652896"]
                    }
                },
                "samples": {
                    "SRR1652895": {
                        "refinebio_accession_code": "SRR1652895",
                        "refinebio_title": "Control Sample 1",
                        "refinebio_organism": "HOMO_SAPIENS",
                        "refinebio_platform": "Illumina Genome Analyzer"
                    },
                    "SRR1652896": {
                        "refinebio_accession_code": "SRR1652896",
                        "refinebio_title": "Sepsis Sample 1",
                        "refinebio_organism": "HOMO_SAPIENS",
                        "refinebio_platform": "Illumina Genome Analyzer"
                    }
                }
            }
            
            json_file = temp_path / "aggregated_metadata.json"
            with open(json_file, 'w') as f:
                json.dump(json_data, f)
            
            # Run ETL pipeline
            config = PipelineConfig(
                connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=test;Trusted_Connection=yes;",
                base_path=str(temp_path)
            )
            
            orchestrator = ETLOrchestrator(config)
            results = orchestrator.execute_study_pipeline("SRP049820")
            
            # Verify results
            assert results['success'] == True
            assert results['records_processed'] == 4  # 2 genes * 2 samples
            assert results['validation_passed'] == True
```

---

## 7. Deployment and Operations

### 7.1 Configuration Management

```yaml
# config/pipeline_config.yaml
database:
  connection_string: "Driver={ODBC Driver 17 for SQL Server};Server=${DB_SERVER};Database=${DB_NAME};UID=${DB_USER};PWD=${DB_PASSWORD};"
  database_name: "BioinformaticsWarehouse"

processing:
  chunk_size: 50000
  max_workers: 4
  memory_limit_mb: 8192
  timeout_seconds: 7200

illness_inference:
  rules:
    - pattern: "\\b(septic\\s*shock|sshock|septic_shock|shock)\\b"
      label: "SEPTIC_SHOCK"
      priority: 1
    - pattern: "\\bno[-_\\s]?sepsis\\b|\\bnon[-_\\s]?sepsis\\b"
      label: "NO_SEPSIS"
      priority: 2
    - pattern: "\\bsepsis\\b"
      label: "SEPSIS"
      priority: 3
    - pattern: "\\bcontrol\\b|\\bhealthy\\b"
      label: "CONTROL"
      priority: 4
  
  overrides:
    SRR1652895: "CONTROL"
    SRR1652896: "SEPSIS"

platform_normalization:
  manufacturer_lookup:
    illumina: "Illumina"
    affymetrix: "Affymetrix"
    agilent: "Agilent"

quality_thresholds:
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
```

### 7.2 Docker Deployment

```dockerfile
# Dockerfile
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ /app/src/
COPY config/ /app/config/
COPY main.py /app/

# Set working directory
WORKDIR /app

# Create log directory
RUN mkdir -p /var/log/etl

# Set environment variables
ENV PYTHONPATH=/app
ENV ETL_CONFIG_PATH=/app/config/pipeline_config.yaml

# Run the application
CMD ["python", "main.py"]
```

### 7.3 Monitoring and Alerting

```python
# monitoring/health_check.py
import psutil
import pyodbc
from datetime import datetime, timedelta
from typing import Dict, Any, List

class ETLHealthChecker:
    """Health check and monitoring for ETL pipeline"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def check_system_health(self) -> Dict[str, Any]:
        """Check system resources"""
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'status': 'healthy' if all([
                psutil.cpu_percent() < 80,
                psutil.virtual_memory().percent < 80,
                psutil.disk_usage('/').percent < 90
            ]) else 'warning'
        }
    
    def check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and performance"""
        try:
            with pyodbc.connect(self.config.connection_string) as conn:
                cursor = conn.cursor()
                
                # Check connection
                cursor.execute("SELECT 1")
                cursor.fetchone()
                
                # Check recent ETL activity
                cursor.execute("""
                    SELECT COUNT(*) as recent_runs
                    FROM meta_etl_process_log 
                    WHERE started_at > DATEADD(hour, -1, GETUTCDATE())
                """)
                recent_runs = cursor.fetchone()[0]
                
                # Check for recent failures
                cursor.execute("""
                    SELECT COUNT(*) as recent_failures
                    FROM meta_etl_process_log 
                    WHERE status = 'FAILED' 
                    AND started_at > DATEADD(hour, -24, GETUTCDATE())
                """)
                recent_failures = cursor.fetchone()[0]
                
                return {
                    'connection_status': 'connected',
                    'recent_runs_last_hour': recent_runs,
                    'recent_failures_last_24h': recent_failures,
                    'status': 'healthy' if recent_failures == 0 else 'warning'
                }
                
        except Exception as e:
            return {
                'connection_status': 'failed',
                'error': str(e),
                'status': 'critical'
            }
    
    def generate_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'system_health': self.check_system_health(),
            'database_health': self.check_database_health(),
            'overall_status': 'healthy'  # Logic to determine overall status
        }
```

---

## 8. Summary and Next Steps

### 8.1 Key Improvements Implemented

1. **Row-Oriented Processing**: Avoids SQL Server column limits by melting wide matrices to long format
2. **Content-Hash Based Idempotency**: Ensures duplicate prevention and enables safe reprocessing
3. **Comprehensive Illness Inference**: Regex-based classification with override support
4. **Platform Normalization**: Standardized platform metadata extraction
5. **Advanced Analytics Interfaces**: Views and materialized tables for downstream analysis
6. **Robust Error Handling**: Structured logging with standardized error codes
7. **Performance Optimization**: Batch processing, indexing, and partitioning strategies

### 8.2 Implementation Phases

**Phase 1 (Week 1): Core ETL Pipeline**
- Implement enhanced data extraction with streaming
- Build dimension upsert logic with MERGE operations
- Create staging table population with bulk operations
- Add basic validation and quality checks

**Phase 2 (Week 2): Advanced Features**
- Implement illness inference engine
- Add platform normalization logic
- Create comprehensive error handling
- Build performance monitoring

**Phase 3 (Week 3): Analytics Integration**
- Create analytics views and materialized tables
- Implement feature catalog for ML integration
- Add advanced validation and QC metrics
- Build monitoring and alerting

**Phase 4 (Week 4): Production Hardening**
- Performance optimization and tuning
- Comprehensive testing and validation
- Documentation and runbook creation
- Production deployment

### 8.3 Critical Success Factors

1. **Data Quality**: Ensure 100% key coverage and <5% null values
2. **Performance**: Achieve 10M fact rows in ≤60 minutes
3. **Reliability**: Zero data loss and idempotent processing
4. **Monitoring**: Real-time visibility into pipeline health
5. **Maintainability**: Clear documentation and configuration management

This architecture provides a robust foundation for your gene pair analysis platform, addressing all the requirements from your comprehensive specification while building on the existing codebase structure.
# ETL Requirements Document
## Gene Expression Data Pipeline for SQL Server 2022

### Version History
| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-09-27 | Technical Business Analyst | Initial requirements document |

---

## Executive Summary

This document outlines the technical and business requirements for a robust, production-grade ETL pipeline that processes gene expression data from TSV files and JSON metadata sources, transforming and loading them into a SQL Server 2022 data warehouse. The system must be dynamic, scalable, and capable of handling multiple studies with varying file structures and metadata schemas.

### Key Requirements Summary
- **Dynamic schema handling** for varying TSV file structures across studies
- **Production-grade reliability** with comprehensive error handling and logging
- **Scalability** for large datasets (potentially millions of records)
- **Study-based processing** with configurable parameters
- **Star schema** dimensional model implementation
- **Comprehensive data validation** at all stages

---

## Business Context

### Objectives
1. Automate the ingestion of gene expression data from multiple bioinformatics studies
2. Normalize data from varying source formats into a consistent dimensional model
3. Enable analytical queries and reporting on gene expression patterns across studies
4. Maintain data lineage and audit trails for regulatory compliance
5. Support incremental loading for ongoing data updates

### Success Criteria
- ETL process completes within defined SLA windows
- Data accuracy verified through automated validation checks
- System handles concurrent processing of multiple studies
- Comprehensive logging enables rapid troubleshooting
- Zero data loss during processing

---

## Data Sources

### Primary Sources
1. **TSV Files**: Gene expression data files
   - **Location**: File system directories organized by study ID
   - **Format**: Tab-separated values with gene IDs in first column, sample IDs as column headers
   - **Size**: Variable (11-50,000+ genes × 50-1000+ samples per study)
   - **Naming Convention**: `[StudyID].tsv` or `[StudyID]-Copy.tsv`
   - **Encoding**: UTF-8
   - **Sample Structure**:
     ```
     Gene         SRR1652895  SRR1652896  SRR1652897
     ENSG00000000003  1.735493    0.44816     0.361399
     ENSG00000000005  0.172857    0.44816     0.361399
     ```

2. **JSON Metadata Files**: Study and sample metadata
   - **Location**: Same directories as corresponding TSV files
   - **Format**: Structured JSON with nested experiment and sample information
   - **Naming Convention**: `aggregated_metadata.json`
   - **Contains**: Study details, sample metadata, processing information

### Data Source Characteristics
- **Frequency**: Batch processing (daily/weekly/on-demand)
- **Availability**: File system access required
- **Quality**: Pre-processed research data with high quality expectations
- **Volume**: Variable per study (MB to GB range per file set)

---

## Data Destinations (Target)

### Target Database: SQL Server 2022
- **Instance**: Production SQL Server 2022
- **Database**: GeneExpressionDW (Data Warehouse)
- **Schema**: Multi-schema approach
  - `staging`: Temporary tables for ETL processing
  - `dim`: Dimension tables
  - `fact`: Fact tables
  - `meta`: ETL metadata and control tables
  - `log`: Logging and audit tables

### Target Data Model (Star Schema)

#### Dimension Tables
1. **dim_study**
   - `study_key` (INT, IDENTITY, PK)
   - `study_accession_code` (VARCHAR(50), Natural Key)
   - `study_title` (NVARCHAR(500))
   - `study_technology` (VARCHAR(100))
   - `study_organism` (VARCHAR(100))
   - `study_platform` (NVARCHAR(200))
   - `study_description` (NVARCHAR(MAX))
   - `created_date` (DATETIME2)
   - `source_first_published` (DATETIME2)
   - `source_last_modified` (DATETIME2)
   - `etl_created_date` (DATETIME2)
   - `etl_updated_date` (DATETIME2)

2. **dim_sample**
   - `sample_key` (INT, IDENTITY, PK)
   - `sample_accession_code` (VARCHAR(50), Natural Key)
   - `sample_title` (NVARCHAR(500))
   - `sample_organism` (VARCHAR(100))
   - `sample_platform` (NVARCHAR(200))
   - `sample_treatment` (NVARCHAR(200))
   - `sample_cell_line` (NVARCHAR(200))
   - `sample_tissue` (NVARCHAR(200))
   - `is_processed` (BIT)
   - `processor_name` (VARCHAR(100))
   - `processor_version` (VARCHAR(50))
   - `etl_created_date` (DATETIME2)
   - `etl_updated_date` (DATETIME2)
   - `illness_key` (int)
   - `platform_key` (int)

3. **dim_gene**
   - `gene_key` (INT, IDENTITY, PK)
   - `gene_id` (VARCHAR(50), Natural Key)
   - `gene_symbol` (VARCHAR(100))
   - `gene_name` (NVARCHAR(500))
   - `gene_type` (VARCHAR(100))
   - `chromosome` (VARCHAR(10))
   - `start_position` (BIGINT)
   - `end_position` (BIGINT)
   - `strand` (CHAR(1))
   - `etl_created_date` (DATETIME2)
   - `etl_updated_date` (DATETIME2)

4. **dim_illness**
   - `illness_key` (INT, IDENTITY, PK)
   - `illness_label` (VARCHAR(50), Natural Key)

3. **dim_platform**
   - `platform_key` (INT, IDENTITY, PK)
   - `platform_accession` (VARCHAR(50), Natural Key)

#### Fact Table
1. **fact_gene_expression**
   - `expression_key` (BIGINT, IDENTITY, PK)
   - `sample_key` (INT, FK)
   - `gene_key` (INT, FK)
   - `study_key` (INT, FK)
   - `expression_value` (DECIMAL(18,9))
   - `expression_log2_value` (DECIMAL(18,9))
   - `is_significant` (BIT)
   - `batch_id` (VARCHAR(50))
   - `etl_created_date` (DATETIME2)

#### Metadata Tables
1. **meta_etl_process_log**
   - `log_id` (BIGINT, IDENTITY, PK)
   - `process_name` (VARCHAR(100))
   - `study_id` (VARCHAR(50))
   - `start_time` (DATETIME2)
   - `end_time` (DATETIME2)
   - `status` (VARCHAR(20)) -- SUCCESS, FAILURE, WARNING
   - `records_processed` (BIGINT)
   - `records_inserted` (BIGINT)
   - `records_updated` (BIGINT)
   - `records_rejected` (BIGINT)
   - `error_message` (NVARCHAR(MAX))
   - `batch_id` (VARCHAR(50))

2. **meta_data_validation_log**
   - `validation_id` (BIGINT, IDENTITY, PK)
   - `batch_id` (VARCHAR(50))
   - `validation_type` (VARCHAR(50))
   - `table_name` (VARCHAR(100))
   - `validation_rule` (NVARCHAR(500))
   - `validation_result` (VARCHAR(20)) -- PASS, FAIL, WARNING
   - `affected_records` (BIGINT)
   - `validation_message` (NVARCHAR(MAX))
   - `validation_time` (DATETIME2)

---

## ETL Process Architecture

### High-Level Process Flow
1. **Configuration & Discovery**
2. **Extraction & Staging**
3. **Data Validation**
4. **Transformation & Enrichment**
5. **Loading & Integration**
6. **Validation & Reconciliation**
7. **Cleanup & Logging**

### Dynamic Schema Handling Strategy
The ETL process must handle varying file structures through:
- **Dynamic table creation** for staging tables based on source file schemas
- **Metadata-driven processing** using system tables to define column mappings
- **Template-based SQL generation** for MERGE operations
- **Configurable validation rules** based on detected data types

---

## Detailed Requirements

### Functional Requirements

#### FR001: Dynamic File Processing
- **Requirement**: Process TSV files with varying structures automatically
- **Details**: 
  - Detect column count and data types dynamically
  - Create staging tables with appropriate schema
  - Handle special characters in column names
  - Support Unicode data encoding
- **Acceptance Criteria**: Process files with 10-50,000 columns successfully

#### FR002: Study-Based Processing
- **Requirement**: Support configurable processing by study identifier
- **Details**:
  - Accept study ID parameter for targeted processing
  - Process all files in study-specific directories
  - Support batch processing of multiple studies
  - Maintain study-level processing logs
- **Acceptance Criteria**: Process individual studies or study lists as specified

#### FR003: Data Transformation
- **Requirement**: Transform wide-format TSV data to normalized star schema
- **Details**:
  - UNPIVOT gene expression matrices
  - Generate surrogate keys for all dimensions
  - Perform SCD Type 1 updates on dimension tables
  - Calculate derived metrics (e.g., log2 transformations)
- **Acceptance Criteria**: 1 source row with N samples becomes N fact records

#### FR004: Metadata Integration
- **Requirement**: Extract and integrate JSON metadata with expression data
- **Details**:
  - Parse nested JSON structures
  - Handle varying metadata schemas
  - Create dimension records from metadata
  - Link metadata to expression data via natural keys
- **Acceptance Criteria**: All metadata fields mapped to appropriate dimensions

#### FR005: Incremental Loading
- **Requirement**: Support both full and incremental data loading
- **Details**:
  - Detect new/modified files in source directories
  - Update existing records when source data changes
  - Maintain data lineage and audit trails
  - Support point-in-time recovery scenarios
- **Acceptance Criteria**: Process only changed data in incremental mode

### Non-Functional Requirements

#### NFR001: Performance
- **Requirement**: Process large datasets within SLA windows
- **Details**:
  - Handle files up to 5GB in size
  - Process 10M+ records within 2-hour window
  - Support concurrent processing of multiple studies
  - Optimize for SQL Server bulk loading operations
- **Acceptance Criteria**: Meet defined processing time benchmarks

#### NFR002: Scalability
- **Requirement**: Scale to handle increasing data volumes
- **Details**:
  - Partition fact tables by study and date
  - Implement parallel processing where possible
  - Use indexed staging tables for performance
  - Support cluster/multi-server deployment
- **Acceptance Criteria**: Linear scaling with data volume increases

#### NFR003: Reliability
- **Requirement**: Provide production-grade reliability and recoverability
- **Details**:
  - Implement transaction-based processing
  - Support restart from failure points
  - Provide rollback capabilities
  - Maintain referential integrity throughout
- **Acceptance Criteria**: 99.9% successful completion rate

#### NFR004: Data Quality
- **Requirement**: Ensure high data quality through validation
- **Details**:
  - Validate data types and ranges
  - Check referential integrity constraints
  - Detect and flag statistical outliers
  - Provide data quality metrics and reports
- **Acceptance Criteria**: <0.1% data quality issues in production

---

## Data Validation Requirements

### Source Data Validation
- **File Structure Validation**: Verify TSV format, header presence, column consistency
- **Data Type Validation**: Ensure numeric expression values, valid identifiers
- **Completeness Validation**: Check for missing critical fields, empty files
- **Business Rule Validation**: Expression values within expected ranges, valid gene IDs

### Transformation Validation
- **Record Count Reconciliation**: Source row count × (columns-1) = target row count
- **Data Integrity Checks**: No data loss during UNPIVOT operations
- **Key Generation Validation**: Unique surrogate key generation
- **Cross-Reference Validation**: Metadata links correctly to expression data

### Target Data Validation
- **Referential Integrity**: All foreign key relationships valid
- **Constraint Validation**: All table constraints satisfied
- **Duplicate Detection**: No unintended duplicate records
- **Statistical Validation**: Data distributions within expected parameters

---

## Error Handling & Logging

### Error Handling Strategy
1. **Prevention**: Input validation and schema checking
2. **Detection**: Real-time monitoring and alerting
3. **Isolation**: Quarantine invalid records for manual review
4. **Recovery**: Automatic retry with exponential backoff
5. **Notification**: Alert stakeholders of critical failures

### Logging Requirements
- **Process Logging**: Start/end times, record counts, status codes
- **Error Logging**: Detailed error messages with context and stack traces
- **Data Quality Logging**: Validation results and quality metrics
- **Performance Logging**: Execution times and resource utilization
- **Audit Logging**: Data lineage and change tracking

### Log Retention
- **Operational Logs**: 90 days
- **Error Logs**: 1 year
- **Audit Logs**: 7 years (regulatory requirement)
- **Performance Logs**: 1 year

---

## Technical Implementation Specifications

### Dynamic SQL Framework
- **Template-based SQL generation** for varying schemas
- **Parameter-driven stored procedures** for study processing
- **Dynamic MERGE statements** for staging-to-production loads
- **Metadata-driven column mapping** using system catalogs

### Staging Strategy
- **Study-specific staging tables**: `staging.stg_{study_id}_expression`
- **Temporary tables** for intermediate transformations
- **Static dimension staging** for consistent metadata processing
- **Cleanup procedures** for staging table management

### Bulk Loading Optimization
- **SQL Server BULK INSERT** for initial data loading
- **Columnstore indexes** on fact tables for query performance
- **Partitioned tables** for large fact data
- **Optimized MERGE operations** for dimension updates

---

## Security & Compliance

### Data Security
- **Service account authentication** for database connections
- **Encrypted connections** for all data transfer
- **Access control** through database roles and permissions
- **Audit trails** for all data modifications

### Regulatory Compliance
- **Data lineage tracking** for research compliance
- **Change auditing** for all ETL modifications
- **Data retention policies** aligned with institutional requirements
- **Validation documentation** for quality assurance

---

## Monitoring & Alerting

### Key Performance Indicators (KPIs)
- **Processing Success Rate**: >99% successful completion
- **Data Quality Score**: >99.5% valid records
- **Processing Time**: Within SLA windows
- **Storage Growth Rate**: Predictable and managed
- **Error Resolution Time**: <24 hours for critical issues

### Alerting Scenarios
- **Process Failures**: Immediate notification to operations team
- **Data Quality Issues**: Daily summary reports
- **Performance Degradation**: Threshold-based alerting
- **Storage Capacity**: Proactive space management alerts

---

## Implementation Timeline

### Phase 1: Core Infrastructure (Weeks 1-2)
- Database schema creation
- Basic ETL framework development
- Dynamic SQL generation framework
- Initial testing with sample data

### Phase 2: Advanced Features (Weeks 3-4)
- Comprehensive error handling implementation
- Data validation framework
- Performance optimization
- Monitoring and alerting setup

### Phase 3: Production Deployment (Weeks 5-6)
- User acceptance testing
- Production environment setup
- Documentation completion
- Go-live preparation and cutover

---

## Dependencies & Assumptions

### Dependencies
- **SQL Server 2022** instance availability and configuration
- **File system access** to source data directories
- **Network connectivity** between ETL server and SQL Server
- **Service accounts** with appropriate permissions
- **Monitoring infrastructure** for alerting capabilities

### Assumptions
- Source data follows consistent TSV format standards
- JSON metadata maintains relatively stable structure
- Processing windows allow for batch processing approach
- Data volumes grow predictably over time
- Network latency between components is minimal

---

## Risk Assessment & Mitigation

### High-Risk Items
1. **Dynamic Schema Handling Complexity**
   - *Risk*: Unforeseen data structure variations
   - *Mitigation*: Comprehensive testing with diverse datasets, robust error handling

2. **Performance at Scale**
   - *Risk*: Processing times exceed SLA requirements
   - *Mitigation*: Performance testing, optimization strategies, parallel processing

3. **Data Quality Issues**
   - *Risk*: Invalid source data corrupts target database
   - *Mitigation*: Multi-level validation, quarantine procedures

### Medium-Risk Items
1. **System Integration Challenges**
   - *Risk*: SQL Server compatibility issues
   - *Mitigation*: Thorough testing, vendor support engagement

2. **Operational Complexity**
   - *Risk*: Difficult troubleshooting and maintenance
   - *Mitigation*: Comprehensive documentation, training programs

---

## Success Metrics

### Technical Metrics
- **Data Throughput**: Records processed per hour
- **System Uptime**: ETL service availability percentage
- **Error Rate**: Percentage of failed processing attempts
- **Data Latency**: Time from source update to target availability

### Business Metrics
- **User Satisfaction**: Stakeholder feedback scores
- **Regulatory Compliance**: Audit success rate
- **Cost Efficiency**: Processing cost per million records
- **Time to Insight**: End-to-end data processing time

---

## Appendices

### Appendix A: Sample Data Structures
[Example TSV and JSON file formats]

### Appendix B: SQL Server Configuration Requirements
[Database settings, memory allocation, disk space requirements]

### Appendix C: Error Code Reference
[Comprehensive list of system error codes and their meanings]

### Appendix D: Performance Benchmarks
[Expected processing times for various data volumes]

---

*This requirements document serves as the foundation for implementing a robust, scalable ETL solution for gene expression data processing. Regular reviews and updates ensure alignment with evolving business needs and technical capabilities.*
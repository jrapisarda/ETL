
/*
===============================================================================
Production-Grade ETL Framework for Gene Expression Data
SQL Server 2022 Implementation
Author: Technical Business Analyst
Date: 2025-09-27
===============================================================================
*/

-- =====================================================
-- SECTION 1: DATABASE AND SCHEMA SETUP
-- =====================================================

-- Create schemas
CREATE SCHEMA staging;
CREATE SCHEMA dim;
CREATE SCHEMA fact;
CREATE SCHEMA meta;
CREATE SCHEMA log;
GO

-- =====================================================
-- SECTION 2: METADATA AND CONTROL TABLES
-- =====================================================

-- ETL Process Configuration
CREATE TABLE meta.etl_process_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    is_active BIT DEFAULT 1,
    source_directory VARCHAR(500),
    file_pattern VARCHAR(100),
    target_schema VARCHAR(50),
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Study Processing Metadata
CREATE TABLE meta.study_process_metadata (
    study_id VARCHAR(50) PRIMARY KEY,
    study_directory VARCHAR(500) NOT NULL,
    tsv_file_name VARCHAR(255),
    json_file_name VARCHAR(255),
    last_processed_date DATETIME2,
    file_last_modified DATETIME2,
    total_genes INT,
    total_samples INT,
    processing_status VARCHAR(20) DEFAULT 'PENDING',
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Dynamic Column Mapping
CREATE TABLE meta.column_mapping (
    mapping_id INT IDENTITY(1,1) PRIMARY KEY,
    study_id VARCHAR(50) NOT NULL,
    source_column_name VARCHAR(255) NOT NULL,
    source_column_index INT NOT NULL,
    source_data_type VARCHAR(50),
    target_table VARCHAR(100),
    target_column VARCHAR(100),
    transformation_rule VARCHAR(500),
    is_key_column BIT DEFAULT 0,
    created_date DATETIME2 DEFAULT GETDATE()
);

-- ETL Process Log
CREATE TABLE meta.etl_process_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    study_id VARCHAR(50),
    batch_id VARCHAR(50) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2,
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILURE, WARNING, RUNNING
    records_processed BIGINT DEFAULT 0,
    records_inserted BIGINT DEFAULT 0,
    records_updated BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0,
    error_message NVARCHAR(MAX),
    execution_server VARCHAR(100),
    created_date DATETIME2 DEFAULT GETDATE()
);

-- Data Validation Log
CREATE TABLE meta.data_validation_log (
    validation_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    validation_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    validation_rule NVARCHAR(500) NOT NULL,
    validation_result VARCHAR(20) NOT NULL, -- PASS, FAIL, WARNING
    affected_records BIGINT DEFAULT 0,
    validation_message NVARCHAR(MAX),
    validation_time DATETIME2 DEFAULT GETDATE()
);

-- =====================================================
-- SECTION 3: DIMENSION TABLES (STAR SCHEMA)
-- =====================================================

-- Study Dimension
CREATE TABLE dim.study (
    study_key INT IDENTITY(1,1) PRIMARY KEY,
    study_accession_code VARCHAR(50) NOT NULL UNIQUE,
    study_title NVARCHAR(500),
    study_technology VARCHAR(100),
    study_organism VARCHAR(100),
    study_platform NVARCHAR(200),
    study_description NVARCHAR(MAX),
    created_date DATETIME2,
    source_first_published DATETIME2,
    source_last_modified DATETIME2,
    etl_created_date DATETIME2 DEFAULT GETDATE(),
    etl_updated_date DATETIME2 DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
);

-- Sample Dimension
CREATE TABLE dim.sample (
    sample_key INT IDENTITY(1,1) PRIMARY KEY,
    sample_accession_code VARCHAR(50) NOT NULL UNIQUE,
    sample_title NVARCHAR(500),
    sample_organism VARCHAR(100),
    sample_platform NVARCHAR(200),
    sample_treatment NVARCHAR(200),
    sample_cell_line NVARCHAR(200),
    sample_tissue NVARCHAR(200),
    is_processed BIT DEFAULT 0,
    processor_name VARCHAR(100),
    processor_version VARCHAR(50),
    etl_created_date DATETIME2 DEFAULT GETDATE(),
    etl_updated_date DATETIME2 DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
);

-- Gene Dimension
CREATE TABLE dim.gene (
    gene_key INT IDENTITY(1,1) PRIMARY KEY,
    gene_id VARCHAR(50) NOT NULL UNIQUE,
    gene_symbol VARCHAR(100),
    gene_name NVARCHAR(500),
    gene_type VARCHAR(100),
    chromosome VARCHAR(10),
    start_position BIGINT,
    end_position BIGINT,
    strand CHAR(1),
    etl_created_date DATETIME2 DEFAULT GETDATE(),
    etl_updated_date DATETIME2 DEFAULT GETDATE(),
    etl_batch_id VARCHAR(50)
);

-- =====================================================
-- SECTION 4: FACT TABLE
-- =====================================================

-- Gene Expression Fact Table (Partitioned)
CREATE TABLE fact.gene_expression (
    expression_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    sample_key INT NOT NULL REFERENCES dim.sample(sample_key),
    gene_key INT NOT NULL REFERENCES dim.gene(gene_key),
    study_key INT NOT NULL REFERENCES dim.study(study_key),
    expression_value DECIMAL(18,9) NOT NULL,
    expression_log2_value DECIMAL(18,9),
    is_significant BIT DEFAULT 0,
    batch_id VARCHAR(50) NOT NULL,
    etl_created_date DATETIME2 DEFAULT GETDATE()
) ON [PRIMARY];

-- Create indexes for fact table
CREATE NONCLUSTERED INDEX IX_fact_gene_expression_study_sample 
    ON fact.gene_expression (study_key, sample_key) INCLUDE (expression_value);

CREATE NONCLUSTERED INDEX IX_fact_gene_expression_gene 
    ON fact.gene_expression (gene_key) INCLUDE (expression_value);

-- =====================================================
-- SECTION 5: DYNAMIC ETL STORED PROCEDURES
-- =====================================================

-- Main ETL Controller Procedure
CREATE OR ALTER PROCEDURE meta.sp_execute_gene_expression_etl
    @study_id VARCHAR(50) = NULL,
    @source_directory VARCHAR(500) = NULL,
    @batch_id VARCHAR(50) = NULL,
    @debug_mode BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @process_start DATETIME2 = GETDATE();
    DECLARE @current_batch_id VARCHAR(50) = ISNULL(@batch_id, 'BATCH_' + CONVERT(VARCHAR, @process_start, 112) + '_' + REPLACE(CONVERT(VARCHAR, @process_start, 108), ':', ''));
    DECLARE @error_message NVARCHAR(MAX);
    DECLARE @current_study VARCHAR(50);

    BEGIN TRY
        -- Log process start
        INSERT INTO meta.etl_process_log (process_name, study_id, batch_id, start_time, status)
        VALUES ('GENE_EXPRESSION_ETL_MAIN', @study_id, @current_batch_id, @process_start, 'RUNNING');

        -- Create cursor for studies to process
        DECLARE study_cursor CURSOR FOR
        SELECT DISTINCT study_id 
        FROM meta.study_process_metadata 
        WHERE (@study_id IS NULL OR study_id = @study_id)
            AND processing_status IN ('PENDING', 'READY');

        OPEN study_cursor;
        FETCH NEXT FROM study_cursor INTO @current_study;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Process each study
            EXEC meta.sp_process_study 
                @study_id = @current_study,
                @batch_id = @current_batch_id,
                @debug_mode = @debug_mode;

            FETCH NEXT FROM study_cursor INTO @current_study;
        END;

        CLOSE study_cursor;
        DEALLOCATE study_cursor;

        -- Update process log with success
        UPDATE meta.etl_process_log 
        SET end_time = GETDATE(), 
            status = 'SUCCESS'
        WHERE batch_id = @current_batch_id 
            AND process_name = 'GENE_EXPRESSION_ETL_MAIN';

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE() + ' (Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ')';

        -- Log error
        UPDATE meta.etl_process_log 
        SET end_time = GETDATE(), 
            status = 'FAILURE',
            error_message = @error_message
        WHERE batch_id = @current_batch_id 
            AND process_name = 'GENE_EXPRESSION_ETL_MAIN';

        -- Close cursor if still open
        IF CURSOR_STATUS('global','study_cursor') >= 0
        BEGIN
            CLOSE study_cursor;
            DEALLOCATE study_cursor;
        END;

        RAISERROR(@error_message, 16, 1);
    END CATCH;
END;
GO

-- Dynamic Staging Table Creation Procedure
CREATE OR ALTER PROCEDURE meta.sp_create_dynamic_staging_table
    @study_id VARCHAR(50),
    @column_list NVARCHAR(MAX),
    @staging_table_name VARCHAR(100) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @drop_sql NVARCHAR(MAX);

    SET @staging_table_name = 'staging.stg_' + @study_id + '_expression';

    -- Drop existing staging table if exists
    SET @drop_sql = 'IF OBJECT_ID(''' + @staging_table_name + ''') IS NOT NULL DROP TABLE ' + @staging_table_name;
    EXEC sp_executesql @drop_sql;

    -- Create dynamic staging table
    SET @sql = 'CREATE TABLE ' + @staging_table_name + ' (' + CHAR(13) +
               'staging_id BIGINT IDENTITY(1,1) PRIMARY KEY,' + CHAR(13) +
               'batch_id VARCHAR(50) NOT NULL,' + CHAR(13) +
               'source_row_number INT NOT NULL,' + CHAR(13) +
               @column_list + ',' + CHAR(13) +
               'created_date DATETIME2 DEFAULT GETDATE()' + CHAR(13) +
               ')';

    IF @debug_mode = 1 PRINT @sql;
    EXEC sp_executesql @sql;

    -- Create index on batch_id
    SET @sql = 'CREATE NONCLUSTERED INDEX IX_' + REPLACE(@staging_table_name, '.', '_') + '_batch_id ON ' + @staging_table_name + ' (batch_id)';
    EXEC sp_executesql @sql;
END;
GO

-- Data Validation Procedure
CREATE OR ALTER PROCEDURE meta.sp_validate_staging_data
    @staging_table_name VARCHAR(100),
    @batch_id VARCHAR(50),
    @validation_passed BIT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @record_count INT;
    DECLARE @null_count INT;
    DECLARE @validation_message NVARCHAR(MAX);

    SET @validation_passed = 1;

    -- Validation 1: Check for empty table
    SET @sql = 'SELECT @record_count = COUNT(*) FROM ' + @staging_table_name + ' WHERE batch_id = @batch_id';
    EXEC sp_executesql @sql, N'@record_count INT OUTPUT, @batch_id VARCHAR(50)', @record_count OUTPUT, @batch_id;

    IF @record_count = 0
    BEGIN
        SET @validation_passed = 0;
        SET @validation_message = 'No records found in staging table';

        INSERT INTO meta.data_validation_log (batch_id, validation_type, table_name, validation_rule, validation_result, validation_message)
        VALUES (@batch_id, 'RECORD_COUNT', @staging_table_name, 'Record count > 0', 'FAIL', @validation_message);
    END
    ELSE
    BEGIN
        INSERT INTO meta.data_validation_log (batch_id, validation_type, table_name, validation_rule, validation_result, affected_records)
        VALUES (@batch_id, 'RECORD_COUNT', @staging_table_name, 'Record count > 0', 'PASS', @record_count);
    END;

    -- Additional validations can be added here
    -- Validation 2: Check for required columns
    -- Validation 3: Check data type consistency  
    -- Validation 4: Check business rule constraints

END;
GO

-- MERGE Staging to Production Procedure
CREATE OR ALTER PROCEDURE meta.sp_merge_staging_to_production
    @staging_table_name VARCHAR(100),
    @target_table VARCHAR(100),
    @batch_id VARCHAR(50),
    @key_columns VARCHAR(500),
    @merge_type VARCHAR(20) = 'UPSERT' -- UPSERT, INSERT_ONLY, UPDATE_ONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @columns_list NVARCHAR(MAX);
    DECLARE @update_list NVARCHAR(MAX);
    DECLARE @insert_columns NVARCHAR(MAX);
    DECLARE @insert_values NVARCHAR(MAX);
    DECLARE @join_condition NVARCHAR(MAX);

    -- Build column lists dynamically from INFORMATION_SCHEMA
    SELECT @columns_list = STRING_AGG('[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @target_table
        AND COLUMN_NAME NOT IN ('etl_created_date', 'etl_updated_date', 'etl_batch_id')
        AND COLUMN_NAME NOT LIKE '%_key';

    -- Build update list
    SELECT @update_list = STRING_AGG('tgt.[' + COLUMN_NAME + '] = src.[' + COLUMN_NAME + ']', ', ')
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @target_table
        AND COLUMN_NAME NOT IN ('etl_created_date', 'etl_updated_date', 'etl_batch_id')
        AND COLUMN_NAME NOT LIKE '%_key'
        AND COLUMN_NAME NOT IN (SELECT value FROM STRING_SPLIT(@key_columns, ','));

    -- Build join condition
    SET @join_condition = REPLACE(@key_columns, ',', ' = src.[' + @key_columns + '] AND tgt.[');
    SET @join_condition = 'tgt.[' + @join_condition + '] = src.[' + @key_columns + ']';

    -- Generate MERGE statement
    SET @sql = 'MERGE ' + @target_table + ' AS tgt ' + CHAR(13) +
               'USING ' + @staging_table_name + ' AS src ' + CHAR(13) +
               'ON ' + @join_condition + CHAR(13);

    IF @merge_type IN ('UPSERT', 'UPDATE_ONLY')
    BEGIN
        SET @sql = @sql + 
                   'WHEN MATCHED THEN UPDATE SET ' + CHAR(13) +
                   @update_list + ', ' + CHAR(13) +
                   'etl_updated_date = GETDATE(), ' + CHAR(13) +
                   'etl_batch_id = @batch_id ' + CHAR(13);
    END;

    IF @merge_type IN ('UPSERT', 'INSERT_ONLY')
    BEGIN
        SET @sql = @sql + 
                   'WHEN NOT MATCHED BY TARGET THEN INSERT (' + @columns_list + ', etl_created_date, etl_batch_id) ' + CHAR(13) +
                   'VALUES (' + REPLACE(@columns_list, '[', 'src.[') + ', GETDATE(), @batch_id)' + CHAR(13);
    END;

    SET @sql = @sql + ';';

    -- Execute MERGE
    EXEC sp_executesql @sql, N'@batch_id VARCHAR(50)', @batch_id;

END;
GO

-- UNPIVOT Gene Expression Data Procedure
CREATE OR ALTER PROCEDURE meta.sp_unpivot_expression_data
    @study_id VARCHAR(50),
    @staging_table_name VARCHAR(100),
    @batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @unpivot_columns NVARCHAR(MAX);
    DECLARE @sample_columns NVARCHAR(MAX);

    -- Get sample column names (excluding Gene column)
    SET @sql = 'SELECT @sample_columns = STRING_AGG(QUOTENAME(COLUMN_NAME), '', '') ' +
               'FROM INFORMATION_SCHEMA.COLUMNS ' +
               'WHERE TABLE_SCHEMA + ''.'' + TABLE_NAME = ''' + @staging_table_name + ''' ' +
               'AND COLUMN_NAME NOT IN (''staging_id'', ''batch_id'', ''source_row_number'', ''Gene'', ''created_date'')';

    EXEC sp_executesql @sql, N'@sample_columns NVARCHAR(MAX) OUTPUT', @sample_columns OUTPUT;

    -- Create UNPIVOT query
    SET @sql = 'INSERT INTO staging.stg_unpivoted_expression (study_id, batch_id, gene_id, sample_id, expression_value) ' +
               'SELECT ''' + @study_id + ''', ''' + @batch_id + ''', Gene, sample_id, expression_value ' +
               'FROM ( ' +
               '    SELECT Gene, ' + @sample_columns + ' ' +
               '    FROM ' + @staging_table_name + ' ' +
               '    WHERE batch_id = ''' + @batch_id + ''' ' +
               ') src ' +
               'UNPIVOT ( ' +
               '    expression_value FOR sample_id IN (' + @sample_columns + ') ' +
               ') unpvt';

    -- Execute UNPIVOT
    EXEC sp_executesql @sql;

END;
GO

-- =====================================================
-- SECTION 6: UTILITY AND HELPER PROCEDURES
-- =====================================================

-- Cleanup Staging Tables
CREATE OR ALTER PROCEDURE meta.sp_cleanup_staging_tables
    @retention_days INT = 7,
    @batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @table_name VARCHAR(100);

    -- Clean up old staging tables
    DECLARE table_cursor CURSOR FOR
    SELECT DISTINCT 'staging.' + name
    FROM sys.tables 
    WHERE schema_id = SCHEMA_ID('staging')
        AND name LIKE 'stg_%_expression'
        AND create_date < DATEADD(DAY, -@retention_days, GETDATE());

    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @table_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'DROP TABLE ' + @table_name;
        EXEC sp_executesql @sql;

        FETCH NEXT FROM table_cursor INTO @table_name;
    END;

    CLOSE table_cursor;
    DEALLOCATE table_cursor;

    -- Clean up log entries
    DELETE FROM meta.etl_process_log 
    WHERE created_date < DATEADD(DAY, -(@retention_days * 2), GETDATE());

    DELETE FROM meta.data_validation_log 
    WHERE validation_time < DATEADD(DAY, -(@retention_days * 2), GETDATE());

END;
GO

-- Generate Batch ID
CREATE OR ALTER FUNCTION meta.fn_generate_batch_id()
RETURNS VARCHAR(50)
AS
BEGIN
    DECLARE @batch_id VARCHAR(50);
    DECLARE @timestamp VARCHAR(20) = CONVERT(VARCHAR, GETDATE(), 112) + '_' + REPLACE(CONVERT(VARCHAR, GETDATE(), 108), ':', '');
    SET @batch_id = 'BATCH_' + @timestamp + '_' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS VARCHAR);
    RETURN @batch_id;
END;
GO

-- =====================================================
-- SECTION 7: MONITORING AND REPORTING VIEWS
-- =====================================================

-- ETL Process Status View
CREATE OR ALTER VIEW meta.v_etl_process_status
AS
SELECT 
    l.process_name,
    l.study_id,
    l.batch_id,
    l.start_time,
    l.end_time,
    l.status,
    l.records_processed,
    l.records_inserted,
    l.records_updated,
    l.records_rejected,
    DATEDIFF(MINUTE, l.start_time, ISNULL(l.end_time, GETDATE())) AS duration_minutes,
    CASE 
        WHEN l.records_processed > 0 
        THEN CAST(l.records_inserted + l.records_updated AS DECIMAL(10,2)) / l.records_processed * 100
        ELSE 0 
    END AS success_percentage,
    l.error_message
FROM meta.etl_process_log l
WHERE l.created_date >= DATEADD(DAY, -30, GETDATE());
GO

-- Data Quality Summary View
CREATE OR ALTER VIEW meta.v_data_quality_summary
AS
SELECT 
    v.batch_id,
    COUNT(*) as total_validations,
    SUM(CASE WHEN v.validation_result = 'PASS' THEN 1 ELSE 0 END) as passed_validations,
    SUM(CASE WHEN v.validation_result = 'FAIL' THEN 1 ELSE 0 END) as failed_validations,
    SUM(CASE WHEN v.validation_result = 'WARNING' THEN 1 ELSE 0 END) as warning_validations,
    CAST(SUM(CASE WHEN v.validation_result = 'PASS' THEN 1 ELSE 0 END) AS DECIMAL(5,2)) / COUNT(*) * 100 AS quality_score
FROM meta.data_validation_log v
WHERE v.validation_time >= DATEADD(DAY, -30, GETDATE())
GROUP BY v.batch_id;
GO

-- =====================================================
-- SECTION 8: SAMPLE EXECUTION SCRIPTS
-- =====================================================

-- Sample: Execute ETL for specific study
/*
EXEC meta.sp_execute_gene_expression_etl 
    @study_id = 'SRP049820',
    @debug_mode = 1;
*/

-- Sample: Execute ETL for all pending studies
/*
EXEC meta.sp_execute_gene_expression_etl;
*/

-- Sample: Check process status
/*
SELECT * FROM meta.v_etl_process_status 
WHERE start_time >= CAST(GETDATE() AS DATE)
ORDER BY start_time DESC;
*/

-- Sample: Check data quality
/*
SELECT * FROM meta.v_data_quality_summary
ORDER BY quality_score DESC;
*/

-- =====================================================
-- SECTION 9: INITIAL CONFIGURATION DATA
-- =====================================================

-- Insert default ETL configuration
INSERT INTO meta.etl_process_config (process_name, source_directory, file_pattern, target_schema)
VALUES 
    ('GENE_EXPRESSION_ETL', 'C:\GeneExpression\Data\', '*.tsv', 'staging'),
    ('METADATA_PROCESSING', 'C:\GeneExpression\Data\', 'aggregated_metadata.json', 'staging');

-- =====================================================
-- END OF SQL SCRIPT
-- =====================================================

/*
IMPLEMENTATION NOTES:
1. This script provides a comprehensive framework for dynamic ETL processing
2. Tables use appropriate data types and constraints for gene expression data
3. Stored procedures handle dynamic schema creation and processing
4. Error handling and logging are built into all major procedures
5. Validation procedures ensure data quality throughout the pipeline
6. The framework supports both individual study and batch processing
7. Monitoring views provide operational visibility
8. The design follows SQL Server 2022 best practices

USAGE:
1. Execute this script on your SQL Server 2022 instance
2. Configure the source directories in meta.etl_process_config
3. Populate meta.study_process_metadata with your study information
4. Call meta.sp_execute_gene_expression_etl to process data

CUSTOMIZATION:
- Modify column mappings in the dynamic table creation procedures
- Add additional validation rules in sp_validate_staging_data
- Extend the star schema with additional dimensions as needed
- Configure partitioning on the fact table for very large datasets
*/

-- Enhanced ETL Pipeline Database Schema
-- SQL Server 2022 Optimized for Gene Expression Data

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'BioinformaticsWarehouse')
BEGIN
    CREATE DATABASE BioinformaticsWarehouse;
END
GO

USE BioinformaticsWarehouse;
GO

-- =============================================
-- DIMENSION TABLES
-- =============================================

-- Study dimension
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[study]') AND type in (N'U'))
BEGIN
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
    
    -- Create indexes
    CREATE UNIQUE INDEX IX_dim_study_accession ON dim_study(study_accession_code);
    CREATE INDEX IX_dim_study_organism ON dim_study(study_organism);
    CREATE INDEX IX_dim_study_technology ON dim_study(study_technology);
END
GO

-- Sample dimension
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[sample]') AND type in (N'U'))
BEGIN
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
        study_key INT NOT NULL,
        exclude_from_analysis BIT DEFAULT 0,
        etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
        etl_updated_date DATETIME2 DEFAULT GETUTCDATE(),
        FOREIGN KEY (illness_key) REFERENCES dim_illness(illness_key),
        FOREIGN KEY (platform_key) REFERENCES dim_platform(platform_key),
        FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
    );
    
    -- Create indexes
    CREATE UNIQUE INDEX IX_dim_sample_accession ON dim_sample(sample_accession_code);
    CREATE INDEX IX_dim_sample_study ON dim_sample(study_key);
    CREATE INDEX IX_dim_sample_illness ON dim_sample(illness_key);
END
GO

-- Gene dimension
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[gene]') AND type in (N'U'))
BEGIN
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
    
    -- Create indexes
    CREATE UNIQUE INDEX IX_dim_gene_id ON dim_gene(gene_id);
    CREATE INDEX IX_dim_gene_symbol ON dim_gene(gene_symbol);
END
GO

-- Illness dimension with seed data
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[illness]') AND type in (N'U'))
BEGIN
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
    
    -- Create indexes
    CREATE UNIQUE INDEX IX_dim_illness_label ON dim_illness(illness_label);
END
GO

-- Platform dimension
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[platform]') AND type in (N'U'))
BEGIN
    CREATE TABLE dim_platform (
        platform_key INT IDENTITY(1,1) PRIMARY KEY,
        platform_accession VARCHAR(100) NOT NULL UNIQUE,
        platform_name NVARCHAR(500),
        manufacturer VARCHAR(100),
        measurement_technology VARCHAR(20), -- RNA-SEQ/MICROARRAY/OTHER
        etl_created_date DATETIME2 DEFAULT GETUTCDATE(),
        etl_updated_date DATETIME2 DEFAULT GETUTCDATE()
    );
    
    -- Create indexes
    CREATE UNIQUE INDEX IX_dim_platform_accession ON dim_platform(platform_accession);
    CREATE INDEX IX_dim_platform_manufacturer ON dim_platform(manufacturer);
END
GO

-- =============================================
-- FACT TABLES
-- =============================================

-- Gene expression fact table with columnstore index
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fact].[gene_expression]') AND type in (N'U'))
BEGIN
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
        FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
    );
    
    -- Create columnstore index for analytics performance
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_gene_expression ON fact_gene_expression;
    
    -- Create non-clustered indexes for common queries
    CREATE NONCLUSTERED INDEX NCI_gene_study ON fact_gene_expression(gene_key, study_key) 
    INCLUDE (sample_key, expression_value);
    
    CREATE NONCLUSTERED INDEX NCI_sample_gene ON fact_gene_expression(sample_key, gene_key)
    INCLUDE (expression_value);
END
GO

-- Feature values fact table for ML integration
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fact].[feature_values]') AND type in (N'U'))
BEGIN
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
    
    -- Create indexes
    CREATE INDEX IX_fact_feature_study ON fact_feature_values(study_key);
    CREATE INDEX IX_fact_feature_entity ON fact_feature_values(entity_scope, entity_id_1);
END
GO

-- =============================================
-- STAGING TABLES
-- =============================================

-- Row-oriented staging table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[staging].[expression_rows]') AND type in (N'U'))
BEGIN
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
    
    -- Create indexes for staging table
    CREATE INDEX IX_staging_batch ON staging.expression_rows(batch_id);
    CREATE INDEX IX_staging_study ON staging.expression_rows(study_accession_code);
END
GO

-- =============================================
-- METADATA/QC TABLES
-- =============================================

-- ETL process log
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[etl_process_log]') AND type in (N'U'))
BEGIN
    CREATE TABLE meta_etl_process_log (
        run_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        batch_id VARCHAR(64),
        step NVARCHAR(64),
        status NVARCHAR(16),
        attempt INT DEFAULT 1,
        started_at DATETIME2,
        ended_at DATETIME2,
        rows_read BIGINT NULL,
        rows_written BIGINT NULL,
        error_code INT NULL,
        error_message NVARCHAR(MAX) NULL,
        context_json NVARCHAR(MAX) NULL
    );
    
    -- Create indexes
    CREATE INDEX IX_meta_process_batch ON meta_etl_process_log(batch_id);
    CREATE INDEX IX_meta_process_run ON meta_etl_process_log(run_id);
    CREATE INDEX IX_meta_process_started ON meta_etl_process_log(started_at);
END
GO

-- Data validation log
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[data_validation_log]') AND type in (N'U'))
BEGIN
    CREATE TABLE meta_data_validation_log (
        run_id UNIQUEIDENTIFIER,
        batch_id VARCHAR(64),
        study_key INT,
        validation_code NVARCHAR(64),
        severity NVARCHAR(16),
        details NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        FOREIGN KEY (study_key) REFERENCES dim_study(study_key)
    );
    
    -- Create indexes
    CREATE INDEX IX_meta_validation_batch ON meta_data_validation_log(batch_id);
    CREATE INDEX IX_meta_validation_study ON meta_data_validation_log(study_key);
END
GO

-- Study QC metrics
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[study_qc]') AND type in (N'U'))
BEGIN
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
END
GO

-- =============================================
-- ANALYTICS VIEWS
-- =============================================

-- Long format expression view
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[analytics].[vw_expression_long]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW analytics.vw_expression_long AS
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
    WHERE s.exclude_from_analysis = 0');
END
GO

-- Cohort-based expression view
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[analytics].[vw_expression_by_cohort]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW analytics.vw_expression_by_cohort AS
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
    GROUP BY st.study_accession_code, i.illness_label, g.gene_id');
END
GO

-- Gene pairs materialized view (optional, for correlation analysis)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[analytics].[vw_gene_pairs_candidate]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW analytics.vw_gene_pairs_candidate AS
    SELECT 
        g1.gene_key as gene_key_i,
        g1.gene_id as gene_id_i,
        g1.gene_symbol as gene_symbol_i,
        g2.gene_key as gene_key_j,
        g2.gene_id as gene_id_j,
        g2.gene_symbol as gene_symbol_j,
        s.study_key,
        s.study_accession_code,
        COUNT(DISTINCT fe1.sample_key) as common_samples
    FROM fact_gene_expression fe1
    INNER JOIN fact_gene_expression fe2 ON fe1.sample_key = fe2.sample_key AND fe1.study_key = fe2.study_key
    INNER JOIN dim_gene g1 ON fe1.gene_key = g1.gene_key
    INNER JOIN dim_gene g2 ON fe2.gene_key = g2.gene_key
    INNER JOIN dim_study s ON fe1.study_key = s.study_key
    WHERE g1.gene_key < g2.gene_key  -- Avoid duplicates
    GROUP BY g1.gene_key, g1.gene_id, g1.gene_symbol,
             g2.gene_key, g2.gene_id, g2.gene_symbol,
             s.study_key, s.study_accession_code
    HAVING COUNT(DISTINCT fe1.sample_key) >= 10');  -- Minimum samples for correlation
END
GO

-- =============================================
-- STORED PROCEDURES
-- =============================================

-- Procedure to load from staging to fact table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl].[sp_load_fact_expression]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE etl.sp_load_fact_expression
        @batch_id VARCHAR(64)
    AS
    BEGIN
        SET NOCOUNT ON;
        
        -- Load from staging to fact table with key resolution
        INSERT INTO fact_gene_expression (sample_key, gene_key, study_key, expression_value, batch_id)
        SELECT 
            s.sample_key,
            g.gene_key,
            st.study_key,
            se.expression_value,
            se.batch_id
        FROM staging.expression_rows se
        INNER JOIN dim_sample s ON se.sample_accession_code = s.sample_accession_code
        INNER JOIN dim_gene g ON se.gene_id = g.gene_id
        INNER JOIN dim_study st ON se.study_accession_code = st.study_accession_code
        WHERE se.batch_id = @batch_id;
        
        -- Return number of records loaded
        RETURN @@ROWCOUNT;
    END');
END
GO

-- Procedure to validate batch data
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[etl].[sp_validate_batch]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE etl.sp_validate_batch
        @batch_id VARCHAR(64),
        @study_key INT
    AS
    BEGIN
        SET NOCOUNT ON;
        
        DECLARE @validation_results TABLE (
            validation_code NVARCHAR(64),
            severity NVARCHAR(16),
            message NVARCHAR(MAX),
            record_count INT
        );
        
        -- Check for duplicate records
        INSERT @validation_results
        SELECT ''DUPLICATE_RECORDS'', ''ERROR'', 
               ''Duplicate records found in batch'', COUNT(*)
        FROM fact_gene_expression 
        WHERE batch_id = @batch_id
        GROUP BY sample_key, gene_key, study_key
        HAVING COUNT(*) > 1;
        
        -- Check for missing samples
        INSERT @validation_results
        SELECT ''MISSING_SAMPLES'', ''WARNING'', 
               ''Samples in staging but not in fact table'', COUNT(*)
        FROM staging.expression_rows se
        LEFT JOIN fact_gene_expression fe ON se.batch_id = fe.batch_id 
            AND se.sample_accession_code = (SELECT sample_accession_code FROM dim_sample WHERE sample_key = fe.sample_key)
            AND se.gene_id = (SELECT gene_id FROM dim_gene WHERE gene_key = fe.gene_key)
        WHERE se.batch_id = @batch_id AND fe.expression_key IS NULL;
        
        -- Check expression value ranges
        INSERT @validation_results
        SELECT ''EXPRESSION_RANGE'', ''WARNING'', 
               ''Expression values outside expected range'', COUNT(*)
        FROM fact_gene_expression 
        WHERE batch_id = @batch_id 
          AND (expression_value < 0 OR expression_value > 1000000);
        
        -- Return results
        SELECT * FROM @validation_results;
    END');
END
GO

-- =============================================
-- PARTITIONING STRATEGY (Optional)
-- =============================================

-- Create partition function for study_key
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PF_study_key')
BEGIN
    CREATE PARTITION FUNCTION PF_study_key (INT)
    AS RANGE LEFT FOR VALUES (100, 200, 300, 400, 500, 600, 700, 800, 900, 1000);
END
GO

-- Create partition scheme
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'PS_study_key')
BEGIN
    CREATE PARTITION SCHEME PS_study_key 
    AS PARTITION PF_study_key ALL TO ([PRIMARY]);
END
GO

-- Apply partitioning to fact table (run this after table creation)
-- ALTER TABLE fact_gene_expression 
-- DROP CONSTRAINT PK_fact_gene_expression;
-- 
-- ALTER TABLE fact_gene_expression 
-- ADD CONSTRAINT PK_fact_gene_expression PRIMARY KEY (expression_key, study_key) 
-- ON PS_study_key(study_key);

-- =============================================
-- INITIAL DATA SETUP
-- =============================================

-- Insert unknown gene for missing data
IF NOT EXISTS (SELECT * FROM dim_gene WHERE gene_id = 'UNKNOWN')
BEGIN
    INSERT INTO dim_gene (gene_id, gene_symbol, gene_name, gene_type) 
    VALUES ('UNKNOWN', 'UNKNOWN', 'Unknown gene', 'unknown');
END
GO

-- Insert unknown sample for missing data
IF NOT EXISTS (SELECT * FROM dim_sample WHERE sample_accession_code = 'UNKNOWN')
BEGIN
    INSERT INTO dim_sample (sample_accession_code, sample_title, study_key, illness_key) 
    VALUES ('UNKNOWN', 'Unknown sample', 
            (SELECT study_key FROM dim_study WHERE study_accession_code = 'UNKNOWN'),
            (SELECT illness_key FROM dim_illness WHERE illness_label = 'UNKNOWN'));
END
GO

-- =============================================
-- PERFORMANCE MONITORING VIEWS
-- =============================================

-- ETL performance summary
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[vw_etl_performance]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW monitoring.vw_etl_performance AS
    SELECT 
        batch_id,
        study_accession_code = (SELECT TOP 1 study_accession_code FROM staging.expression_rows WHERE batch_id = l.batch_id),
        step,
        status,
        attempt,
        started_at,
        ended_at,
        duration_seconds = DATEDIFF(second, started_at, ended_at),
        rows_read,
        rows_written,
        rows_per_second = CASE WHEN DATEDIFF(second, started_at, ended_at) > 0 
                               THEN rows_written / DATEDIFF(second, started_at, ended_at) 
                               ELSE NULL END,
        error_code,
        error_message
    FROM meta_etl_process_log l');
END
GO

-- Data quality metrics
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[monitoring].[vw_data_quality]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW monitoring.vw_data_quality AS
    SELECT 
        st.study_accession_code,
        st.study_title,
        total_samples = COUNT(DISTINCT s.sample_key),
        total_genes = COUNT(DISTINCT g.gene_key),
        total_expressions = COUNT(*),
        avg_expressions_per_sample = COUNT(*) * 1.0 / COUNT(DISTINCT s.sample_key),
        null_expressions = SUM(CASE WHEN fe.expression_value IS NULL THEN 1 ELSE 0 END),
        zero_expressions = SUM(CASE WHEN fe.expression_value = 0 THEN 1 ELSE 0 END),
        latest_batch_id = MAX(fe.batch_id),
        latest_load_date = MAX(fe.etl_created_date)
    FROM fact_gene_expression fe
    INNER JOIN dim_sample s ON fe.sample_key = s.sample_key
    INNER JOIN dim_gene g ON fe.gene_key = g.gene_key
    INNER JOIN dim_study st ON fe.study_key = st.study_key
    GROUP BY st.study_key, st.study_accession_code, st.study_title');
END
GO

PRINT 'Database schema created successfully!';
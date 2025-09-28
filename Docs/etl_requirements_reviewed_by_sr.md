# ETL Requirements Document — **Reviewed & Revised**
## Gene Expression Data Pipeline for SQL Server 2022

### Version History
| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.1 | 2025-09-27 | Sr. ETL Architect (Review) | Major review: added JSON→Dim mappings, illness inference rules, idempotency, incremental strategy, staging/unpivot strategy, QC capture, ML/analytics interfaces, test harness, SLAs, and ops runbook. |
| 1.0 | 2025-09-27 | Technical Business Analyst | Initial requirements document |

---

## Executive Summary (Tightened)
This pipeline ingests heterogeneous gene‑expression matrices (TSV) and study/sample metadata (JSON), normalizes to a star schema in SQL Server 2022, and exposes curated views/tables for downstream statistics (e.g., Spearman) and ML features. The design emphasizes metadata‑driven processing, **row‑oriented staging (to avoid SQL Server column‑limits)**, idempotent reprocessing, and reproducible batch lineage.

**What changed:**
- Replaced wide‑table staging with **row‑oriented melt/unpivot at extract time**.
- Added **strong JSON→dimension mapping** and **illness inference rules**.
- Defined **QC capture** (KS, normalization flags) and **materialized analytics interfaces**.
- Specified **content‑hash based incremental** and **deterministic batch_id**.

---

## Business Context (Clarified)
### Objectives (additions bolded)
1. Automate multi‑study ingestion.
2. Normalize to consistent dimensional model.
3. Enable analytical queries across studies **and stratified cohorts (e.g., Control vs Sepsis vs Septic Shock)**.
4. Maintain lineage/audit for compliance and reproducibility.
5. Support incremental loads **using content hashing**.
6. **Expose curated views/tables optimized for correlation and ML feature extraction.**

### Success Criteria (expanded)
- SLA met for study loads (see NFR001/SLAs).
- Automated validations (row‑count reconciliation, referential integrity, QC thresholds) pass.
- Concurrent study processing without lock contention.
- **Batch‑idempotent** runs (no duplicates on retries).
- **Analytics interfaces** fuel downstream Spearman/ML without ad‑hoc transformations.

---

## Data Sources (Augmented)
### TSV Expression Matrices
- Structure: first column = gene_id; remaining columns = sample_accession_codes; values = expression (TPM/CPM/Counts — **MUST be specified per study in config**).
- Constraint: Matrices may exceed ~1k samples; **do not stage as a wide table** (SQL Server hard column limit). Use row‑oriented ingest.

### JSON Metadata — `aggregated_metadata.json`
- Contains study‑level and sample‑level fields.
- **Minimum required fields (per sample):** `refinebio_accession_code`, `refinebio_platform`, `refinebio_title`, `refinebio_organism`, processing flags (`refinebio_processed`, `refinebio_processor_name`, `refinebio_processor_version`).
- **Study‑level fields:** accession, title, organisms, technology, description, publication identifiers, source timestamps, and QC flags (e.g., `ks_statistic`, `ks_pvalue`, `quantile_normalized`).

---

## Target Data Model (Revised)
### Dimensions (edits in **bold**)
1. **dim_study**
   - study_key (PK), study_accession_code (NK), study_title, **study_pubmed_id**, study_technology, study_organism, **study_platform_summary**, study_description, created_date, source_first_published, source_last_modified, etl_created_date, etl_updated_date.

2. **dim_sample**
   - sample_key (PK), sample_accession_code (NK), sample_title, sample_organism, sample_platform, sample_treatment, sample_cell_line, sample_tissue, is_processed, processor_name, processor_version, etl_created_date, etl_updated_date,
   - **illness_key (FK)**, **platform_key (FK)**, **study_key (FK)**, **exclude_from_analysis BIT DEFAULT 0**.

3. **dim_gene** (unchanged core) + **gene_symbol** optional enrichment.

4. **dim_illness**
   - illness_key (PK), illness_label (NK), **illness_definition NVARCHAR(500) NULL**.
   - **Seed values:** CONTROL, NO_SEPSIS, SEPSIS, SEPTIC_SHOCK, UNKNOWN.

5. **dim_platform** (expanded)
   - platform_key (PK), platform_accession (NK), **platform_name**, **manufacturer**, **measurement_technology** (RNA‑SEQ/MICROARRAY/OTHER).

### Fact Tables
1. **fact_gene_expression** (revised types/columns)
   - expression_key (BIGINT, PK IDENTITY)
   - sample_key (FK), gene_key (FK), study_key (FK)
   - **expression_value FLOAT(53)**
   - **expression_log2_value AS (CASE WHEN expression_value > 0 THEN LOG(expression_value) / LOG(2.0) END) PERSISTED**
   - is_significant (BIT) NULL
   - batch_id (VARCHAR(64))  -- **content-hash based**
   - etl_created_date (DATETIME2)
   - **INDEXES:** CCI on (sample_key, gene_key, study_key, expression_value); NCI on (gene_key, study_key) INCLUDE(expression_value).

2. **fact_feature_values** (**new; optional but recommended**)
   - feature_record_key (BIGINT, PK)
   - **entity_scope** (ENUM: GENE, GENE_PAIR, SAMPLE, STUDY_COORT)
   - **entity_id_1** (e.g., gene_key or pair composite id), **entity_id_2** (nullable)
   - feature_name (VARCHAR(100))
   - feature_value FLOAT(53)
   - study_key, batch_id, etl_created_date.

### Metadata/QC Tables (expanded)
- **meta_etl_process_log** (as-written) + add **attempt INTEGER**, **run_id UNIQUEIDENTIFIER**.
- **meta_data_validation_log** (as-written).
- **meta_study_qc** (**new**): study_key, ks_statistic FLOAT, ks_pvalue FLOAT, ks_warning NVARCHAR(200), quantile_normalized BIT, quant_sf_only BIT, collected_at DATETIME2.

---

## JSON → Dimension Mapping (Canonical)
**Study-level**
- accession_code → dim_study.study_accession_code
- title → dim_study.study_title
- organisms[0] → dim_study.study_organism
- technology → dim_study.study_technology
- description → dim_study.study_description
- pubmed_id → dim_study.study_pubmed_id
- source_first_published → dim_study.source_first_published
- source_last_modified → dim_study.source_last_modified
- **QC:** ks_statistic, ks_pvalue, ks_warning, quantile_normalized, quant_sf_only → meta_study_qc

**Sample-level**
- refinebio_accession_code → dim_sample.sample_accession_code
- refinebio_platform → dim_sample.sample_platform; also **normalize to dim_platform (see rules below)**
- refinebio_title → dim_sample.sample_title; also **used for illness inference**
- refinebio_organism → dim_sample.sample_organism
- refinebio_processed → dim_sample.is_processed
- refinebio_processor_name/version → dim_sample.processor_name/version
- **Join to dim_study via container experiment accession**

**Platform normalization rules**
- If value matches `Name (Accession)` keep `Accession` as platform_accession; `Name` as platform_name.
- If only one token, use it for both.
- Manufacturer inference (lookup table; extendable). Measurement_technology inferred from study.

---

## Illness Inference Rules (Deterministic)
Derive **dim_illness** from `sample_title` and/or explicit metadata when available. Use the following **ordered** regex rules (case‑insensitive):
1. `\b(septic\s*shock|sshock|septic_shock|shock)\b` → SEPTIC_SHOCK
2. `\bno[-_\s]?sepsis\b|\bnon[-_\s]?sepsis\b` → NO_SEPSIS
3. `\bsepsis\b` → SEPSIS
4. `\bcontrol\b|\bhealthy\b` → CONTROL
5. otherwise → UNKNOWN

**Overrides:** Support per‑study `illness_overrides.yaml` where sample_accession_code maps directly to an illness label. **Conflicts** are logged (warning) and resolved in favor of the override.

---

## ETL Process Architecture (Refined)
### High‑Level Flow (data‑driven)
1. **Discovery & Config** (YAML): study ids, file paths/globs, expression units, scaling, illness overrides, sample include/exclude, chunk size, parallelism.
2. **Extract (Python + PyArrow)**: stream TSV in chunks; **melt** to rows `(gene_id, sample_accession_code, expression_value)`.
3. **Stage (SQL Server)**: bulk insert rows into `staging.expression_rows` (schema below).
4. **Validate**: counts/types/range; gene id format; sample set vs JSON; QC capture.
5. **Transform**: dimension upserts (SCD1); resolve keys; enrich platform; infer illness.
6. **Load**: insert into `fact_gene_expression` with keys and computed log2.
7. **Post‑Load Validations**: reconciliation; RI checks; dedupe; stats sanity.
8. **Analytics Interfaces**: refresh materialized tables/views for Spearman/ML features.
9. **Cleanup & Logging**: staging purge by batch; durable logs.

### Staging Schema (row‑oriented)
- `staging.expression_rows`
  - study_accession_code VARCHAR(50)
  - batch_id VARCHAR(64)
  - gene_id VARCHAR(50)
  - sample_accession_code VARCHAR(50)
  - expression_value FLOAT(53)
  - **line_no BIGINT NULL** (source row)
  - **file_name NVARCHAR(260)**, **file_hash CHAR(64)**
  - **PRIMARY KEY** (batch_id, gene_id, sample_accession_code)

**Rationale:** Avoid wide‑table limits; supports >1024 samples safely; enables idempotent dedupe by (batch_id, gene_id, sample).

### Incremental & Idempotency (strong)
- **batch_id := SHA256(file_hash || study_accession || created_at || file_size)**.
- Before load, **anti‑join** on (batch_id, gene_id, sample_accession_code) to skip duplicates.
- **Upsert** dims by natural keys; facts are **append‑only per batch**.

### Parallelism Guidelines
- Partition extraction by TSV blocks (e.g., N genes per chunk) or **sample‑column stripes**.
- Use **MAXDOP** for big MERGE phases conservatively; prefer batch‑insert + key‑lookup patterns.

---

## Data Validation Requirements (Extended)
### Source Validation
- **TSV**: header present; first column gene id non‑null; N≥2 columns.
- **JSON/TSV consistency**: set equality of sample ids across sources.
- **Expression domain**: values ≥0 unless units are log‑scale (then document and **do not re‑log2**).

### Transformation Validation
- **Reconciliation**: `#genes_in_TSV * #samples_in_JSON == #rows_loaded_to_fact` (post‑dedupe).
- **Key coverage**: 100% of samples & genes resolved to keys; else quarantine.
- **Illness label coverage**: non‑UNKNOWN ≥95% (threshold configurable); emit warning if lower.

### Target Validation
- **RI/constraints**: FKs valid; no duplicate (sample_key, gene_key, study_key, batch_id).
- **Outlier screening**: Z‑score or MAD on per‑gene distribution; flag to `meta_data_validation_log`.

---

## Bulk Loading & Performance (Sharpened)
- Use **BULK INSERT** into a heap or use **SqlBulkCopy** from Python for `staging.expression_rows`.
- Create **CCI** on `fact_gene_expression` after initial load; for steady‑state, keep CCI and load in **batch groups (e.g., 100k rows)** to maintain segment quality.
- Fact **partitioning** on `study_key` and optionally **batch_id**; align with columnstore partitioning.
- Memory: aim for **chunk_size ≈ 1–5 million rows** per bulk copy, tuned by memory/IO.

**SLA Targets:** (tune per environment)
- 10M fact rows in ≤ 60 minutes on mid‑tier VM with SSD and CCI, assuming parallel extract (×4) and batch bulk‑copy.

---

## Security & Compliance (No change + clarifications)
- Service principals; least‑privileged roles (etl_reader, etl_loader, etl_admin).
- Encrypted connections (TLS); at‑rest encryption per DBA policy.
- Row‑level data is non‑PHI; if PHI emerges, enable **Always Encrypted** for sensitive columns.

---

## Monitoring & Alerting (Operationalized)
- Emit **structured logs** (JSON) with `run_id`, `batch_id`, `study_accession`, `study_key`, `disease_key`, counts, timings.
- **No external alerting required** (per your preference). A **failed job** is sufficient signal, but we must include rich diagnostics in DB logs.

### Failure Diagnostics (Detailed)
**Tables**
- `meta.meta_etl_process_log(run_id UNIQUEIDENTIFIER, batch_id VARCHAR(64), step NVARCHAR(64), status NVARCHAR(16), attempt INT, started_at DATETIME2, ended_at DATETIME2, rows_read BIGINT NULL, rows_written BIGINT NULL, error_code INT NULL, error_message NVARCHAR(MAX) NULL, context_json NVARCHAR(MAX) NULL)`
- `meta.meta_data_validation_log(run_id, batch_id, study_key, validation_code NVARCHAR(64), severity NVARCHAR(16), details NVARCHAR(MAX), created_at DATETIME2 DEFAULT SYSUTCDATETIME())`

**Standard Error Codes**
- `54001` AGG_MISSING_DISEASE_MAP — No active mapping in `dim.study_disease_map` for `study_key`.
- `54002` AGG_MULTI_DISEASE_MAP — Multiple active mappings for the study.
- `54010` SOURCE_MISMATCH_SAMPLES — TSV/JSON sample sets differ.
- `54020` UNIT_UNDETECTED — Could not infer units; transformation policy not applied.
- `54100` HETEROGENEITY_EXCESS — Pair excluded due to `i2_star > @i2_max`.

**Proc pattern (example)**
```sql
BEGIN TRY
  EXEC analytics.sp_update_meta_pair_metrics @study_key=@k, @feature_run_id=@fr OUT;
  INSERT meta.meta_etl_process_log(run_id,batch_id,step,status,attempt,started_at,ended_at,rows_written,context_json)
  VALUES(@run, @batch, N'update_meta_pair_metrics', N'SUCCESS', @attempt, @t0, SYSUTCDATETIME(), @rows, JSON_OBJECT(''study_key'':@k,''feature_run_id'':@fr));
END TRY
BEGIN CATCH
  DECLARE @err INT = ERROR_NUMBER(), @msg NVARCHAR(MAX)=ERROR_MESSAGE();
  INSERT meta.meta_etl_process_log(run_id,batch_id,step,status,attempt,started_at,ended_at,error_code,error_message,context_json)
  VALUES(@run, @batch, N'update_meta_pair_metrics', N'FAILED', @attempt, @t0, SYSUTCDATETIME(), @err, @msg,
         JSON_OBJECT(''study_key'':@k,''proc'':''analytics.sp_update_meta_pair_metrics''));
  IF @err IN (54001,54002) INSERT meta.meta_data_validation_log(run_id,batch_id,study_key,validation_code,severity,details)
     VALUES(@run,@batch,@k, CASE WHEN @err=54001 THEN N'AGG_MISSING_DISEASE_MAP' ELSE N'AGG_MULTI_DISEASE_MAP' END, N'ERROR', @msg);
  THROW; -- propagate to fail the job
END CATCH;
```

**Context JSON (minimum fields):** `study_accession`, `study_key`, `disease_key` (if resolved), `k_min`, `q`, `i2_max`, row counts, timer breakdown, file names+hashes, **`sql_text`** (last executed statement/proc name + parameters), **`config_snapshot`** (SHA‑256 + inlined YAML for `Config/settings.yaml` and active study YAMLs), and environment info (Python version, package versions).

**Retry policy:** up to 3 attempts for transient DB errors (e.g., deadlocks) with backoff; **do not retry** deterministic validation failures (codes 54xxx).

**SLA probes:** optional `sp_healthcheck()` returns last successful `run_id`, rows/sec, %UNKNOWN illness, recent error histogram.

## Analytics & ML Interfaces (New)
### Purpose
Provide **stable, query‑friendly** objects for downstream Spearman correlation and ML feature generation without reshaping data ad‑hoc.

### Objects
1. **vw_expression_long**: joins fact to dims (study, sample, gene, illness, platform).
2. **vw_expression_by_cohort**: aggregates per‑illness/study as needed.
3. **tbl_gene_pairs_materialized** (**optional**): periodic materialization of `(gene_key_i, gene_key_j, study_key)` candidate pairs for faster correlation jobs.
4. **fact_feature_values**: repository for your **134 features** per entity (gene or gene‑pair or sample). Feature catalog is defined in `meta_feature_catalog` (feature_name, description, expected_scale, null_policy).

### Contract for Spearman & ML jobs
- **Export contract**: Provide a SQL that returns `(sample_key, gene_key, expression_value, illness_label, study_key)` filtered by quality (e.g., exclude `exclude_from_analysis=1`). Downstream jobs compute Spearman and write results back to `fact_feature_values` or separate correlation tables.
- **Idempotency**: Feature writes must include (study_key, feature_name, entity ids, batch_id or feature_run_id).

---

## Technical Implementation Specs (Concrete for Agent)
### Extractor (Python)
- Libraries: `pyarrow.csv` or `pandas` with `pyarrow` engine; `tqdm`; `xxhash`/`hashlib` for content hash; `orjson` for JSON parse.
- Behavior: stream TSV in chunks; melt into rows; compute `file_hash`; emit `batch_id`.
- JSON parse: load study block; map samples; validate set equality with TSV headers; generate `illness_label`.

### Loader
- Use `SqlBulkCopy` (recommended) or staged `BULK INSERT` with a format file. Retry with exponential backoff on transient failures. Upsert dims via stored procs.

### Stored Procedures (minimal set)
1. `meta.sp_register_batch(@study_accession, @file_hash, @batch_id, @file_name, @created_at, @num_rows OUT)`
2. `dim.sp_upsert_study(...)` / `dim.sp_upsert_platform(...)` / `dim.sp_upsert_gene(...)` / `dim.sp_upsert_sample(...)`
3. `fact.sp_load_gene_expression(@batch_id)` — resolves keys and inserts into fact.
4. `qa.sp_validate_batch(@batch_id)` — runs validations and logs.
5. `analytics.sp_refresh_views(@study_key)` — refreshes materialized tables if used.

### Configuration Files
- `pipeline.yaml`: per study — paths, units, scaling, chunk_size, parallelism, skip_samples, **illness_overrides**.
- `platform_lookup.yaml`: accession→manufacturer/name/tech mapping.
- `feature_catalog.yaml`: declares the 134 feature names, descriptions, and scopes.

---

## Test Harness & Fixtures (Essential)
- **Fixture TSV**: 4 genes × 5 samples; **Fixture JSON**: 5 samples with titles covering CONTROL/NO_SEPSIS/SEPSIS/SEPTIC_SHOCK.
- **Unit tests**: extraction melt correctness; reconciliation math; illness mapping precedence (override beats regex); platform parsing.
- **Integration test**: end‑to‑end study load produces exact expected row count and key coverage = 100%.

---

## Implementation Timeline (Recast)
- **Phase 1 (Week 1)**: Extractor, row‑staging, basic dims, fact load, reconciliation.
- **Phase 2 (Week 2)**: Illness rules, platform normalization, QC capture, validations, indexing/partitioning.
- **Phase 3 (Week 3)**: Analytics views/tables, feature catalog wiring, ops monitoring/alerts.
- **Phase 4 (Week 4)**: Scale/perf hardening, UAT, cutover.

## AS AN AI AGENT YOU ARE EXPECTED TO HANDLE THE ENTIRE SPEC IN A SINGLE JOB. IF YOU NEED TO BREAK IT INTO PIECES PROVIDE SUMMARY OF EACH DELIVERABLE ## 

---

## Risks & Mitigations (Updated)
- **Wide matrices exceed SQL column limits** → Row‑staging melt avoids limit; early in pipeline.
- **Illness inference ambiguity** → Regex + per‑study overrides + warning thresholds.
- **TSV/JSON mismatch** → Hard validation fail; quarantine batch; config to skip missing samples.
- **Performance drift** → Track rows/sec; segment quality; tune chunk size and parallelism.

---

## Ops Runbook (New)
- **Reprocess batch**: rerun with same file; idempotent due to (batch_id, gene_id, sample).
- **Add override**: update `illness_overrides.yaml`; rerun `dim.sp_upsert_sample` and `analytics.sp_refresh_views`.
- **Hotfix gene dictionary**: run `dim.sp_upsert_gene` with corrections; facts remain intact.
- **Purge staging** older than 7 days by batch_id.

---

## Retention Policy (FINAL)
- **Artifacts/**: **retain indefinitely** (clinician-decision support). Treat as immutable deliverables.
  - Store with **content hash** (SHA‑256) in filename and metadata.
  - **Versioning**: append `v{major}.{minor}` to filenames; never overwrite in-place.
  - Optional **WORM** (write‑once, read‑many) on the folder via storage policy; if unavailable, enforce via ACLs and job policies (no deletes).
- **Archive/**: retain for **180 days**; then delete study/batch folders older than retention. (Config keys below.)
- **Logs/**: retain structured JSON and step logs for **180 days**; rotate by date subfolders; purge older trees.
- **Staging/**: purge rows older than **7 days** by `batch_id` (already in Ops Runbook).
- **DB logs** (`meta_etl_process_log`, `meta_data_validation_log`): retain for **180 days** via nightly purge proc.

**Config (add to `Config/settings.yaml`):**
```yaml
retention:
  artifacts_days: null   # null/None = indefinite retention
  artifacts_worm: true   # enforce write-once semantics via job policy/ACLs
  archive_days: 180
  logs_days: 180
  db_logs_days: 180
  staging_days: 7
```

**Scheduled jobs (names only; agent specifics optional):**
- `Housekeeping_Purge_Archive_Logs` — daily at 03:30; deletes `Archive\*` and `Logs\*` older than configured days.
- `Housekeeping_Purge_DB_Logs` — daily at 03:45; deletes from `meta_*_log` older than `@db_logs_days`.
- **No purge job for `Artifacts/`** (guardrail in housekeeping to skip this path).

### J. Final API/View Defaults (CONFIRMED)
- **Views include symbols:** Ranked/Significant views join `dim_gene_pair → dim_gene` to expose `geneA_symbol`, `geneB_symbol` alongside Ensembl IDs.
- **Aggregator precedence:** If both aggregator and META CSV produce a row for the same pair+disease, **aggregator wins** unless the CSV has `csv_override=1` (then loader logs a warning and upserts).
- **Study inclusion:** Meta aggregates include **all mapped studies** for the disease by default.
- **API limit:** `limit` parameter allowed range `1..1000`; default 100.

## Appendices
### A. Staging vs Fact Row Count Formula
`rows_fact = rows_TSV_lines_without_header * (num_samples_from_JSON)`

### B. Example Queries (sketch)
- **Row count reconciliation**
```sql
SELECT COUNT(*) AS fact_rows
FROM fact.fact_gene_expression WITH (NOLOCK)
WHERE study_key = @study_key AND batch_id = @batch_id;
```
- **Export for Spearman**
```sql
SELECT s.sample_key, g.gene_key, f.expression_value, i.illness_label, f.study_key
FROM fact.fact_gene_expression f
JOIN dim.dim_sample s ON f.sample_key = s.sample_key
JOIN dim.dim_gene g ON f.gene_key = g.gene_key
JOIN dim.dim_illness i ON s.illness_key = i.illness_key
WHERE f.study_key = @study_key AND s.exclude_from_analysis = 0;
```

---

## Gene‑Pair Feature Schema (from `pair_metrics_agent_scored.csv`)
**Scope you confirmed:** MVP persists **meta‑analytic aggregates across studies only** (Option B). We will not persist per‑study rows in the public metrics table; those are computed transiently during the aggregation step.

### A. Dimensions
- **dim_gene_pair** *(new)*
  - `pair_key` (PK, BIGINT IDENTITY)
  - `geneA_key` (FK → dim_gene), `geneB_key` (FK → dim_gene)
  - `pair_natural_key` AS `CONCAT(LEAST(geneA_key,geneB_key),'_',GREATEST(geneA_key,geneB_key))` UNIQUE
  - `geneA_symbol`, `geneB_symbol` (denorm convenience)
  - **Rule:** Canonical ordering (A < B) at insert to prevent duplicates.

### B. Facts (MVP = META only)
**fact_gene_pair_metrics** *(one row per pair; meta‑aggregate across all included studies)*
- **Keys**: `pair_key`, `metrics_scope='META_STUDIES'`, `study_key NULL`.
- **Uniqueness**: `UX_fact_pair_meta` UNIQUE(`pair_key`,`metrics_scope`).
- **Columns** *(1:1 with your file; examples)*:
  - **SS (Shock vs Sepsis)**: `dz_ss_mean`, `dz_ss_se`, `dz_ss_ci_low`, `dz_ss_ci_high`, `dz_ss_Q`, `dz_ss_I2`, `dz_ss_z`, `p_ss`, `kappa_ss`, `abs_dz_ss`, `n_studies_ss`.
  - **SOTH (Shock vs Others)**: analogous `dz_soth_*`, `p_soth`, `kappa_soth`, `abs_dz_soth`, `n_studies_soth`.
  - **Cohort correlations** per class (control/sepsis/septic_shock): weighted `r`, `se_r`, `I2`, `z_stat`, `p_value`, `n_samples`.
  - **Deltas/combined**: `delta_r_*`, `z_diff_*`, `p_diff_*`, `combined_p_value`, `combined_effect_size`, `combined_z_score`.
  - **Quality/consistency**: `power_score`, `consistency_score`, `direction_consistency`, `magnitude_consistency`, `total_samples`.
  - **Model/agent**: `neglog_q_ss`, `neglog_q_soth`, `unsupervised_score`, `metrics_score`, `rank_score`, `new_rank_score`, `agent_result_json` (NVARCHAR(MAX)).
  - **Audit**: `included_study_count`, `feature_run_id UNIQUEIDENTIFIER`, `batch_id`, `etl_created_date`.
- **Indexes**: NCI(`new_rank_score` DESC) INCLUDE(`combined_p_value`,`power_score`); NCI(`p_ss`,`p_soth`,`rank_score`).

### C. Meta‑Analysis Aggregator (Option B)
**Goal:** After each study load, recompute **meta‑aggregate** metrics per gene‑pair across all included studies.

**Pipeline step:** `analytics.sp_update_meta_pair_metrics(@study_key, @feature_run_id OUT)`
1. **Derive per‑study components (transient):**
   - From `vw_expression_long`, compute per‑study per‑cohort statistics needed by your formulas (correlations, effect sizes `dz`, SEs, and z‑scores). These can be staged in a temp table `#pair_components` (TEMP tables only; **do not persist per-study components**)
2. **Random‑effects meta (recommended):**
   - For each target metric θ (e.g., `dz_ss`), with study‑level estimates θᵢ and standard errors SEᵢ:
     - **Fixed weights**: wᵢ = 1/SEᵢ².
     - Q = Σ wᵢ (θᵢ − θ̄_F)² where θ̄_F = Σ wᵢθᵢ / Σ wᵢ.
     - C = Σ wᵢ − (Σ wᵢ² / Σ wᵢ).
     - **Tau² (DerSimonian–Laird):** τ² = max{0, (Q − (k−1))/C}.
     - **Random‑effects weights:** wᵢ* = 1/(SEᵢ² + τ²).
     - **Pooled estimate:** θ̄_RE = Σ wᵢ*θᵢ / Σ wᵢ*; SE_RE = sqrt(1/Σ wᵢ*). 
     - **Heterogeneity:** I² = max{0, (Q − (k−1))/Q} × 100.
     - **Z & p:** z = θ̄_RE / SE_RE; p = 2·(1 − Φ(|z|)).
3. **Combine p‑values when only z/p are available:** use **Stouffer’s method** with weights proportional to √n or equal weights if n unknown: Z = (Σ wᵢ zᵢ)/√(Σ wᵢ²).
4. **Kappa/consistency:** compute weighted means or rules you’ve encoded (direction & magnitude agreement across studies).
5. **Upsert** one row per `pair_key` into **fact_gene_pair_metrics** (scope META), updating audit fields and `included_study_count`.

**Incremental shortcut:** maintain running aggregates (Σw, Σwθ, Σwθ², k, C) per pair so a new study only updates these sums; recompute τ² and downstream metrics without full scan.

### D. Views for Analytics — **Finalized Ranking (FDR‑first, Heterogeneity‑filtered)**
- **vw_pair_metrics_ranked** *(FDR‑first, exclude high heterogeneity)*: computes `q_star = LEAST(COALESCE(q_combined,1), COALESCE(q_ss,1), COALESCE(q_soth,1))` and `i2_star = LEAST(COALESCE(dz_ss_I2,100), COALESCE(dz_soth_I2,100))`. Returns META rows meeting `included_study_count ≥ @k_min`, `q_star ≤ @q`, **and** `i2_star ≤ @i2_max`.
  - **Default params:** `@q = 0.05`, `@k_min = 3`, `@i2_max = 75`.
  - **ORDER BY:** `q_star ASC`, `new_rank_score DESC`, `power_score DESC`, `i2_star ASC`, `kappa_ss DESC`, `ABS(combined_effect_size) DESC`.
  - **Sketch:**
```sql
CREATE OR ALTER VIEW analytics.vw_pair_metrics_ranked AS
SELECT p.*, 
       LEAST(COALESCE(q_combined,1.0), COALESCE(q_ss,1.0), COALESCE(q_soth,1.0)) AS q_star,
       LEAST(COALESCE(dz_ss_I2,100.0), COALESCE(dz_soth_I2,100.0)) AS i2_star
FROM fact.fact_gene_pair_metrics p
WHERE metrics_scope = 'META_STUDIES'
  AND included_study_count >= COALESCE(CONVERT(int, SESSION_CONTEXT(N'k_min')), 3)
  AND LEAST(COALESCE(q_combined,1.0), COALESCE(q_ss,1.0), COALESCE(q_soth,1.0))
      <= COALESCE(TRY_CONVERT(float, SESSION_CONTEXT(N'q')), 0.05)
  AND LEAST(COALESCE(dz_ss_I2,100.0), COALESCE(dz_soth_I2,100.0))
      <= COALESCE(TRY_CONVERT(float, SESSION_CONTEXT(N'i2_max')), 75.0);
-- Consumer query should apply the ORDER BY listed above.
```
- **vw_pair_metrics_significant**: same filters as above; intended for downstream exports/tests; typically `ORDER BY q_star ASC`.

### E. Normalization & Cohort Harmonization Policy (NEW)
**Goal:** Make per‑study statistics comparable while keeping the pipeline robust across sequencing/microarray studies.

1) **Autodetect units per study (metadata + heuristics)**
   - If JSON indicates `quantile_normalized=true` (microarray) → treat as **log2‑like**; set `unit_tag='ARRAY_LOG2'`.
   - If technology=RNA‑seq and values look like counts (many zeros; max ≫ 1e4) → set `unit_tag='COUNTS'`.
   - If labeled TPM/FPKM/CPM or post‑quant: `unit_tag='TPM'` or `'CPM'`.

2) **Within‑study transform (applied at extract)**
   - `COUNTS` → **CPM** per sample then **log1p**: `log2(CPM + 1)` (scale‑invariant; avoids zeros).
   - `TPM|CPM` (not log) → **log1p**: `log2(value + 1)`.
   - `ARRAY_LOG2` → **pass‑through** (no re‑log).
   - Persist original value as `expression_value_raw`, transformed as `expression_value` (used downstream). Flag transform in `dim_study.study_unit_tag`.

3) **Why this works**
   - You’re using **Spearman** for correlations (rank‑based), so monotone transforms don’t change r.
   - Meta‑analysis operates on **per‑study** estimates, so unit heterogeneity is absorbed at the study level; DSL handles residual heterogeneity.

4) **Technology stratification (guardrail)**
   - **Do not mix technologies** in the same meta aggregate by default. Compute meta per `(disease, technology)` and optionally second‑stage combine if needed.

5) **QC checks**
   - Per‑study gene‑wise library‑size sanity (total counts), % zeroes, and KS normality flags recorded in `meta_study_qc`.

### F. Multi‑Disease Modeling (FINAL)
- **Dimension:** `dim_disease`
```sql
CREATE TABLE dim.dim_disease (
  disease_key     INT IDENTITY(1,1) PRIMARY KEY,
  disease_label   VARCHAR(64) NOT NULL UNIQUE,
  disease_ontology VARCHAR(64) NULL, -- optional: DOID/SNOMED/UMLS
  is_active       BIT NOT NULL DEFAULT 1,
  etl_created_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  etl_updated_date DATETIME2 NULL
);
GO

-- Seed (v1 scope)
MERGE dim.dim_disease AS t
USING (VALUES 
  ('SEPSIS'),
  ('SEPTIC_SHOCK'),
  ('CONTROL')
) AS s(disease_label)
ON t.disease_label = s.disease_label
WHEN NOT MATCHED THEN INSERT (disease_label) VALUES (s.disease_label);
GO
```
- **Fact column (META only):** add `disease_key` (NOT NULL) and FK.
```sql
ALTER TABLE fact.fact_gene_pair_metrics
  ADD disease_key INT NULL; -- backfill then enforce NOT NULL
GO
-- Backfill policy for existing rows (choose one disease if preexisting data) — default to SEPSIS
UPDATE f
SET    disease_key = d.disease_key
FROM fact.fact_gene_pair_metrics f
CROSS APPLY (SELECT disease_key FROM dim.dim_disease WHERE disease_label = 'SEPSIS') d
WHERE f.disease_key IS NULL;
GO

ALTER TABLE fact.fact_gene_pair_metrics
  ALTER COLUMN disease_key INT NOT NULL;
GO
ALTER TABLE fact.fact_gene_pair_metrics
  ADD CONSTRAINT FK_fact_pairmetrics_disease
      FOREIGN KEY (disease_key) REFERENCES dim.dim_disease(disease_key);
GO
```
- **Aggregation scope (MVP):** one META row per **(pair_key, disease_key, technology)**.
- **Views:** `vw_pair_metrics_ranked` and `vw_pair_metrics_significant` **must filter by `disease_key`** (via session context or parameterized wrapper proc).
- **Source of truth:** `dim.study_disease_map` (DB table; analyst‑editable). The aggregator **does not** read disease from files.

#### Study→Disease Mapping (DB SoT)
```sql
CREATE TABLE dim.study_disease_map (
  study_disease_map_key INT IDENTITY(1,1) PRIMARY KEY,
  study_key        INT NOT NULL,
  disease_key      INT NOT NULL,
  is_active        BIT NOT NULL DEFAULT 1,
  effective_from   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  effective_to     DATETIME2 NULL,
  created_by       SYSNAME NULL,
  created_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_by       SYSNAME NULL,
  updated_at       DATETIME2 NULL,
  CONSTRAINT UQ_study_disease UNIQUE (study_key, is_active),
  CONSTRAINT FK_sdm_study   FOREIGN KEY (study_key)   REFERENCES dim.dim_study(study_key),
  CONSTRAINT FK_sdm_disease FOREIGN KEY (disease_key) REFERENCES dim.dim_disease(disease_key)
);
GO
CREATE INDEX IX_sdm_study ON dim.study_disease_map(study_key) INCLUDE(disease_key, is_active);
```

**Upsert proc (sketch):**
```sql
CREATE OR ALTER PROCEDURE dim.sp_upsert_study_disease_map
  @study_accession_code VARCHAR(50),
  @disease_label        VARCHAR(64),
  @actor                SYSNAME = SUSER_SNAME()
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @study_key INT = (SELECT study_key FROM dim.dim_study WHERE study_accession_code=@study_accession_code);
  DECLARE @disease_key INT = (SELECT disease_key FROM dim.dim_disease WHERE disease_label=@disease_label);
  IF @study_key IS NULL OR @disease_key IS NULL THROW 50001, 'Unknown study or disease', 1;

  -- Close out existing active mapping (if any)
  UPDATE dim.study_disease_map
     SET is_active=0, effective_to=SYSUTCDATETIME(), updated_by=@actor, updated_at=SYSUTCDATETIME()
   WHERE study_key=@study_key AND is_active=1;

  INSERT dim.study_disease_map(study_key,disease_key,created_by)
  VALUES(@study_key,@disease_key,@actor);
END
```

**Aggregator change:** `analytics.sp_update_meta_pair_metrics` resolves `@disease_key` via:
```sql
SELECT TOP(1) @disease_key = disease_key
FROM dim.study_disease_map
WHERE study_key = @study_key AND is_active = 1
ORDER BY effective_from DESC;
```
**Cardinality & policy:** Many studies → 1 disease. Enforce **at most one active mapping per study** via `UQ_study_disease (study_key, is_active)`. Aggregator precheck requires **exactly one active mapping**; if none, **FAIL** and log to `meta_data_validation_log`; if >1 (should not occur), **FAIL** hard and alert Ops.

### G. Unsupervised Ranking Architecture (NEW)
 Unsupervised Ranking Architecture (NEW)
- **Feature space**: your 134 metrics + derived features (e.g., `-log10(q_star)`, `abs(combined_effect_size)`, `consistency_score`, `power_score`, `i2_star` negated).
- **Scaling**: robust z‑score (median/MAD) per feature at **(disease, technology)** slice.
- **Models**: combine **LOF**, **IsolationForest**, and a **rank‑SVM** trained on weak positives (MS4A4A, CD86 pairs) vs a random negative sample; stack into `unsupervised_score`.
- **Final `new_rank_score`**: weighted blend of `-log10(q_star)`, model ensemble score, and consistency/power terms (weights configurable in YAML).
- Persist per run into `feature_run_id`; clear provenance for reproducibility.

### H. LLM Review API Contract (NEW)
- REST `GET /analytics/pairs/top`: params `{disease, technology, q=0.05, k_min=3, i2_max=75, limit=K}` → returns JSON with fields from `vw_pair_metrics_ranked` + gene symbols and a short rationale stub.
- POST `POST /analytics/pairs/{pair_key}/review`: body includes LLM verdict/rationale; persist to `agent_result_json` and audit table `analytics.llm_reviews`.

### I. Loader for `pair_metrics_agent_scored.csv`
- **Scope:** META-only rows (no per-study persistence). Loader writes to `fact_gene_pair_metrics` with `metrics_scope='META_STUDIES'` and a required `@disease_key` parameter (resolved via `dim.study_disease_map` when invoked from the aggregator; when loading a CSV directly, pass `@disease_key`).
- **`pair_id` format (per your data):** `"<geneA_key>_<geneB_key>"` where each component is **`dim_gene.gene_key` (identity)**. The loader **canonicalizes** to `min(geneA_key,geneB_key)_max(...)` and resolves `pair_key` via `dim_gene_pair`.
- **Validation:** bounds check (p in [0,1], I² in [0,100], kappa in [0,1]); ensure both `gene*_key` exist in `dim_gene`; enforce canonical pair; dedupe on (`pair_key`,`metrics_scope`,`disease_key`).
- **Error policy:** if either gene_key is missing → **FAIL with code `54200 PAIR_GENEKEY_NOT_FOUND`**; if `pair_id` not parseable as `INT_INT` → **`54201 PAIR_ID_FORMAT_INVALID`**.

### F. Feature Store (134 features)
- **Option 1 (simple):** Keep features as columns in **fact_gene_pair_metrics**.
- **Option 2 (general):** Also write to `fact_feature_values` with `entity_scope='GENE_PAIR_META'`, `entity_id_1=pair_key`, `study_key=NULL` for tidy ML ingestion.
- **Option 1 (simple):** Keep features as columns in **fact_gene_pair_metrics**.
- **Option 2 (general):** Also write to `fact_feature_values` with `entity_scope='GENE_PAIR_META'`, `entity_id_1=pair_key`, `study_key=NULL` for tidy ML ingestion.

---

## Action for Agent (MVP Option B)
1) Build **dim_gene_pair** and **fact_gene_pair_metrics** (META‑only schema above).
2) Implement `analytics.sp_update_meta_pair_metrics` to recompute aggregates whenever a new study is loaded.
3) Wire monitoring to alert on low `included_study_count`, high heterogeneity (e.g., I² > 75%), or missing inputs.
4) Expose **vw_pair_metrics_ranked** and **vw_pair_metrics_significant**.

---

## τ² Estimation Method — **DerSimonian–Laird (Selected)**
Use **DSL** for random‑effects pooling. Implement both **effect‑size pooling** (e.g., `dz_ss`, `dz_soth`) and **correlation pooling** via **Fisher z‑transform**.

### A. Formulas (per gene‑pair, per metric θ)
Given study‑level estimates θᵢ with standard errors SEᵢ and k studies:
1. Fixed weights: `w_i = 1 / SE_i^2`
2. Fixed‑effects pooled mean: `theta_F = (Σ w_i θ_i) / (Σ w_i)`
3. Q‑statistic: `Q = Σ w_i (θ_i − theta_F)^2`
4. `C = Σ w_i − ( (Σ w_i^2) / (Σ w_i) )`
5. **DSL tau‑squared:** `tau2 = MAX(0, (Q − (k − 1)) / C)`
6. Random‑effects weights: `w_i_star = 1 / (SE_i^2 + tau2)`
7. Pooled estimate: `theta_RE = (Σ w_i_star θ_i) / (Σ w_i_star)`
8. Pooled SE: `SE_RE = SQRT( 1 / Σ w_i_star )`
9. z‑score & p‑value: `z = theta_RE / SE_RE`, `p = 2 * (1 − Φ(|z|))`
10. Heterogeneity: `I2 = MAX(0, (Q − (k − 1)) / Q) * 100`

**Correlations r:**
- Transform: `z_i = atanh(r_i)`; `SE_i = 1 / sqrt(n_i − 3)`; pool `z` via steps above; back‑transform: `r_meta = tanh(theta_RE)`.

### B. Incremental Update Sufficient Statistics
Maintain per pair & metric:
- `S1 = Σ w_i`, `S2 = Σ w_i^2`, `Sθ = Σ w_i θ_i`, `Sθ2 = Σ w_i θ_i^2`, and `k`.
- On new study j, update these sums and recompute: `theta_F`, `Q = Sθ2 − (Sθ^2 / S1)`, `C = S1 − (S2 / S1)`, `tau2`, `w_i_star` (requires SE_j), then pooled values.

### C. Stored Proc Skeleton (`analytics.sp_update_meta_pair_metrics`)
```sql
CREATE OR ALTER PROCEDURE analytics.sp_update_meta_pair_metrics
    @study_key INT,
    @feature_run_id UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
  SET NOCOUNT ON;
  SET @feature_run_id = NEWID();

  -- 1) Build per-study components (temp or persisted)
  -- Example for correlations (control cohort): compute r_i and n_i per pair
  -- Assumes vw_expression_long provides (sample_key, pair_key, cohort_label, expressionA, expressionB)

  -- 2) Transform correlations to Fisher z and compute SE_i
  -- 3) Upsert sufficient statistics into analytics.meta_pair_suffstats
  -- 4) Recompute DSL aggregates per pair for each metric (dz_ss, dz_soth, r_control, r_sepsis, r_shock, deltas)
  -- 5) Upsert into fact.fact_gene_pair_metrics (scope = META_STUDIES).

**Persistence policy (MVP):**
- Do **not** persist per-study components or sufficient statistics.
- The proc computes study-level components in **temp tables** and discards them after upsert.
- Only the **META outputs** (and existing fact/dim tables) are persisted.


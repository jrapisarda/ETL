#!/usr/bin/env python3
"""
Enhanced ETL Pipeline Test Suite
Comprehensive testing for all enhanced ETL components
"""

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
import pytest
import yaml
import hashlib
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import enhanced ETL components
from enhanced_main_etl import (
    EnhancedETLOrchestrator, 
    EnhancedDataExtractor, 
    EnhancedDataTransformer, 
    EnhancedDataLoader,
    EnhancedETLConfig,
    IllnessInferenceEngine,
    PlatformNormalizationEngine,
    DataExtractionError,
    DataTransformationError,
    DataLoadError
)

class TestEnhancedETLConfig:
    """Test configuration management"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = EnhancedETLConfig(
            connection_string="test_connection"
        )
        
        assert config.database_name == "BioinformaticsWarehouse"
        assert config.chunk_size == 50000
        assert config.max_workers == 4
        assert config.memory_limit_mb == 8192
        assert len(config.illness_inference_rules) == 4
        assert config.illness_inference_rules[0]['label'] == "SEPTIC_SHOCK"
    
    def test_yaml_config_loading(self):
        """Test loading configuration from YAML"""
        config_data = {
            'connection_string': 'test_connection',
            'database_name': 'TestDB',
            'chunk_size': 10000,
            'illness_inference_rules': [
                {
                    'pattern': 'test_pattern',
                    'label': 'TEST_LABEL',
                    'priority': 1
                }
            ],
            'illness_overrides': {
                'SRR123': 'CONTROL'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = f.name
        
        try:
            config = EnhancedETLConfig.from_yaml(Path(config_file))
            assert config.database_name == 'TestDB'
            assert config.chunk_size == 10000
            assert len(config.illness_inference_rules) == 1
            assert config.illness_inference_rules[0]['label'] == 'TEST_LABEL'
            assert config.illness_overrides['SRR123'] == 'CONTROL'
        finally:
            os.unlink(config_file)
    
    def test_study_file_paths(self):
        """Test study file path resolution"""
        config = EnhancedETLConfig(
            connection_string="test",
            base_path="/data"
        )
        
        paths = config.get_study_file_paths("SRP049820")
        
        assert paths['json_metadata'] == Path("/data/aggregated_metadata.json")
        assert paths['expression_data'] == Path("/data/SRP049820/SRP049820.tsv")

class TestIllnessInferenceEngine:
    """Test illness inference functionality"""
    
    def test_regex_rules(self):
        """Test regex-based illness inference"""
        config = EnhancedETLConfig(connection_string="test")
        engine = IllnessInferenceEngine(config)
        
        # Test septic shock
        illness, method = engine.infer_illness("Patient with septic shock", "SRR123")
        assert illness == "SEPTIC_SHOCK"
        assert method == "regex"
        
        # Test sepsis
        illness, method = engine.infer_illness("Sepsis patient", "SRR456")
        assert illness == "SEPSIS"
        assert method == "regex"
        
        # Test control
        illness, method = engine.infer_illness("Healthy control", "SRR789")
        assert illness == "CONTROL"
        assert method == "regex"
        
        # Test no sepsis
        illness, method = engine.infer_illness("No sepsis sample", "SRR101")
        assert illness == "NO_SEPSIS"
        assert method == "regex"
        
        # Test unknown
        illness, method = engine.infer_illness("Unknown sample", "SRR202")
        assert illness == "UNKNOWN"
        assert method == "default"
    
    def test_override_rules(self):
        """Test illness override functionality"""
        config = EnhancedETLConfig(
            connection_string="test",
            illness_overrides={
                "SRR123": "CONTROL",
                "SRR456": "SEPSIS"
            }
        )
        engine = IllnessInferenceEngine(config)
        
        # Test override takes precedence
        illness, method = engine.infer_illness("Septic shock patient", "SRR123")
        assert illness == "CONTROL"
        assert method == "override"
        
        illness, method = engine.infer_illness("Healthy control", "SRR456")
        assert illness == "SEPSIS"
        assert method == "override"
    
    def test_case_insensitive(self):
        """Test case insensitive matching"""
        config = EnhancedETLConfig(connection_string="test")
        engine = IllnessInferenceEngine(config)
        
        # Test various case combinations
        test_cases = [
            ("SEPTIC SHOCK", "SEPTIC_SHOCK"),
            ("septic shock", "SEPTIC_SHOCK"),
            ("Septic Shock", "SEPTIC_SHOCK"),
            ("SePsIs", "SEPSIS"),
            ("CONTROL", "CONTROL"),
            ("control", "CONTROL")
        ]
        
        for title, expected in test_cases:
            illness, method = engine.infer_illness(title, "SRR123")
            assert illness == expected, f"Failed for '{title}'"

class TestPlatformNormalizationEngine:
    """Test platform normalization functionality"""
    
    def test_platform_name_format_parsing(self):
        """Test parsing of "Name (Accession)" format"""
        config = EnhancedETLConfig(connection_string="test")
        engine = PlatformNormalizationEngine(config)
        
        # Test standard format
        accession, name, manufacturer, tech = engine.normalize_platform(
            "Illumina Genome Analyzer (GPL1111)", "RNA-SEQ"
        )
        assert accession == "GPL1111"
        assert name == "Illumina Genome Analyzer"
        assert manufacturer == "Illumina"
        assert tech == "RNA-SEQ"
        
        # Test without accession
        accession, name, manufacturer, tech = engine.normalize_platform(
            "Illumina HiSeq", "RNA-SEQ"
        )
        assert accession == "Illumina HiSeq"
        assert name == "Illumina HiSeq"
        assert manufacturer == "Illumina"
        assert tech == "RNA-SEQ"
    
    def test_manufacturer_inference(self):
        """Test manufacturer inference from platform names"""
        config = EnhancedETLConfig(connection_string="test")
        engine = PlatformNormalizationEngine(config)
        
        test_cases = [
            ("Illumina Genome Analyzer", "Illumina"),
            ("Affymetrix HG-U133", "Affymetrix"),
            ("Agilent SurePrint", "Agilent"),
            ("Unknown Platform", "Unknown")
        ]
        
        for platform_name, expected_manufacturer in test_cases:
            _, _, manufacturer, _ = engine.normalize_platform(platform_name, "RNA-SEQ")
            assert manufacturer == expected_manufacturer, f"Failed for '{platform_name}'"
    
    def test_technology_inference(self):
        """Test measurement technology inference"""
        config = EnhancedETLConfig(connection_string="test")
        engine = PlatformNormalizationEngine(config)
        
        test_cases = [
            ("Platform", "RNA-SEQ", "RNA-SEQ"),
            ("Platform", "microarray", "MICROARRAY"),
            ("Platform", "RNA_SEQ", "RNA-SEQ"),
            ("Platform", "other", "OTHER")
        ]
        
        for platform_name, study_tech, expected_tech in test_cases:
            _, _, _, tech = engine.normalize_platform(platform_name, study_tech)
            assert tech == expected_tech, f"Failed for '{study_tech}'"

class TestEnhancedDataExtractor:
    """Test enhanced data extraction"""
    
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
            config = EnhancedETLConfig(connection_string="test")
            extractor = EnhancedDataExtractor(config)
            
            # Test streaming extraction
            results = list(extractor.extract_expression_matrix_streaming(
                temp_file, "SRP049820"
            ))
            
            # Verify results
            assert len(results) == 1  # Single chunk
            chunk_df = results[0]
            assert len(chunk_df) == 6  # 3 genes * 2 samples
            
            # Check structure
            expected_columns = ['gene_id', 'sample_accession_code', 'expression_value', 
                              'study_accession_code', 'file_hash', 'file_name']
            assert all(col in chunk_df.columns for col in expected_columns)
            
            # Check data integrity
            assert chunk_df['study_accession_code'].iloc[0] == "SRP049820"
            assert chunk_df['gene_id'].iloc[0] == "ENSG00000000003"
            assert chunk_df['sample_accession_code'].iloc[0] == "SRR1652895"
            assert chunk_df['expression_value'].iloc[0] == 1.735
            
        finally:
            temp_file.unlink()
    
    def test_json_metadata_extraction(self):
        """Test JSON metadata extraction"""
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
            },
            "ks_statistic": 0.438,
            "ks_pvalue": 0.0,
            "quantile_normalized": True 
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(json_data, f)
            temp_file = f.name
        
        try:
            config = EnhancedETLConfig(connection_string="test")
            extractor = EnhancedDataExtractor(config)
            
            result = extractor.extract_json_metadata(Path(temp_file), "SRP049820")
            
            assert result['experiment']['accession_code'] == "SRP049820"
            assert result['experiment']['title'] == "Test Study"
            assert len(result['samples']) == 2
            assert result['qc_metrics']['ks_statistic'] == 0.438
            assert result['qc_metrics']['quantile_normalized'] == True
            
        finally:
            os.unlink(temp_file)
    
    def test_file_hash_computation(self):
        """Test file hash computation"""
        test_content = b"Test content for hashing"
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(test_content)
            temp_file = Path(f.name)
        
        try:
            config = EnhancedETLConfig(connection_string="test")
            extractor = EnhancedDataExtractor(config)
            
            hash1 = extractor._compute_file_hash(temp_file)
            hash2 = extractor._compute_file_hash(temp_file)
            
            assert hash1 == hash2  # Should be deterministic
            assert len(hash1) == 64  # SHA256 produces 64 character hex string
            assert hash1 != hashlib.sha256(b"different content").hexdigest()
            
        finally:
            temp_file.unlink()

class TestEnhancedDataTransformer:
    """Test enhanced data transformation"""
    
    def test_sample_transformation(self):
        """Test sample record transformation with illness inference"""
        config = EnhancedETLConfig(connection_string="test")
        transformer = EnhancedDataTransformer(config)
        
        samples = {
            "SRR1652895": {
                "refinebio_accession_code": "SRR1652895",
                "refinebio_title": "Control Sample 1",
                "refinebio_organism": "HOMO_SAPIENS",
                "refinebio_platform": "Illumina Genome Analyzer",
                "refinebio_processed": True,
                "refinebio_processor_name": "Tximport"
            },
            "SRR1652896": {
                "refinebio_accession_code": "SRR1652896",
                "refinebio_title": "Sepsis Patient Sample",
                "refinebio_organism": "HOMO_SAPIENS",
                "refinebio_platform": "Illumina Genome Analyzer",
                "refinebio_processed": True,
                "refinebio_processor_name": "Tximport"
            }
        }
        
        result = transformer._transform_samples(samples, "SRP049820", {})
        
        assert len(result) == 2
        assert result[0]['sample_accession_code'] == "SRR1652895"
        assert result[0]['illness_inferred'] == "CONTROL"
        assert result[0]['illness_inference_method'] == "regex"
        assert result[1]['sample_accession_code'] == "SRR1652896"
        assert result[1]['illness_inferred'] == "SEPSIS"
        assert result[1]['illness_inference_method'] == "regex"
    
    def test_platform_transformation(self):
        """Test platform transformation with normalization"""
        config = EnhancedETLConfig(connection_string="test")
        transformer = EnhancedDataTransformer(config)
        
        experiment = {
            "technology": "RNA-SEQ",
            "platform": "Illumina Genome Analyzer (GPL1111)"
        }
        
        result = transformer._transform_platform(experiment, "SRP049820")
        
        assert result['platform_accession'] == "GPL1111"
        assert result['platform_name'] == "Illumina Genome Analyzer"
        assert result['manufacturer'] == "Illumina"
        assert result['measurement_technology'] == "RNA-SEQ"
        assert result['study_accession_code'] == "SRP049820"
    
    def test_study_transformation(self):
        """Test study record transformation"""
        config = EnhancedETLConfig(connection_string="test")
        transformer = EnhancedDataTransformer(config)
        
        experiment = {
            "accession_code": "SRP049820",
            "title": "Test Study",
            "technology": "RNA-SEQ",
            "organisms": ["HOMO_SAPIENS"],
            "description": "Test description",
            "source_first_published": "2020-01-01T00:00:00",
            "source_last_modified": "2020-12-31T23:59:59"
        }
        
        result = transformer._transform_study(experiment, "SRP049820")
        
        assert result['study_accession_code'] == "SRP049820"
        assert result['study_title'] == "Test Study"
        assert result['study_technology'] == "RNA-SEQ"
        assert result['study_organism'] == "HOMO_SAPIENS"
        assert result['study_description'] == "Test description"

class TestEnhancedETLOrchestrator:
    """Test enhanced ETL orchestrator"""
    
    def test_logging_setup(self):
        """Test logging configuration"""
        config = EnhancedETLConfig(
            connection_string="test",
            log_level="DEBUG"
        )
        orchestrator = EnhancedETLOrchestrator(config)
        
        assert orchestrator.logger.level == logging.DEBUG
        assert orchestrator.logger.name == "Enhanced_ETL_Pipeline"
    
    def test_study_discovery(self):
        """Test automatic study discovery"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test study directories
            study1_dir = Path(temp_dir) / "SRP049820"
            study2_dir = Path(temp_dir) / "SRP123456"
            non_study_dir = Path(temp_dir) / "not_a_study"
            
            study1_dir.mkdir()
            study2_dir.mkdir()
            non_study_dir.mkdir()
            
            config = EnhancedETLConfig(
                connection_string="test",
                base_path=temp_dir
            )
            orchestrator = EnhancedETLOrchestrator(config)
            
            studies = orchestrator._discover_studies()
            
            assert "SRP049820" in studies
            assert "SRP123456" in studies
            assert "not_a_study" not in studies
            assert len(studies) == 2

class TestIntegration:
    """Integration tests for complete pipeline"""
    
    def test_end_to_end_pipeline(self):
        """Test complete ETL pipeline with realistic data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test study directory
            study_dir = temp_path / "SRP049820"
            study_dir.mkdir()
            
            # Create test expression matrix
            expression_data = {
                'Gene': ['ENSG00000000003', 'ENSG00000000005', 'ENSG00000000419'],
                'SRR1652895': [1.735, 0.173, 4.689],
                'SRR1652896': [0.448, 0.448, 0.448]
            }
            
            expression_file = study_dir / "SRP049820.tsv"
            pd.DataFrame(expression_data).to_csv(expression_file, sep='\t', index=False)
            
            # Create test JSON metadata
            json_data = {
                "experiments": {
                    "SRP049820": {
                        "accession_code": "SRP049820",
                        "title": "Test Endotoxin Tolerance Study",
                        "technology": "RNA-SEQ",
                        "organisms": ["HOMO_SAPIENS"],
                        "sample_accession_codes": ["SRR1652895", "SRR1652896"],
                        "platform": "Illumina Genome Analyzer (GPL1111)"
                    }
                },
                "samples": {
                    "SRR1652895": {
                        "refinebio_accession_code": "SRR1652895",
                        "refinebio_title": "Control Sample 1",
                        "refinebio_organism": "HOMO_SAPIENS",
                        "refinebio_platform": "Illumina Genome Analyzer (GPL1111)",
                        "refinebio_processed": True,
                        "refinebio_processor_name": "Tximport"
                    },
                    "SRR1652896": {
                        "refinebio_accession_code": "SRR1652896",
                        "refinebio_title": "Sepsis Patient Sample",
                        "refinebio_organism": "HOMO_SAPIENS",
                        "refinebio_platform": "Illumina Genome Analyzer (GPL1111)",
                        "refinebio_processed": True,
                        "refinebio_processor_name": "Tximport"
                    }
                },
                "ks_statistic": 0.438,
                "ks_pvalue": 0.0,
                "quantile_normalized": True
            }
            
            json_file = temp_path / "aggregated_metadata.json"
            with open(json_file, 'w') as f:
                json.dump(json_data, f)
            
            # Create test configuration
            config = EnhancedETLConfig(
                connection_string="Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=TestDB;Trusted_Connection=yes;",
                base_path=str(temp_path),
                illness_overrides={
                    "SRR1652895": "CONTROL",
                    "SRR1652896": "SEPSIS"
                }
            )
            
            # Mock database operations
            with patch('pyodbc.connect') as mock_connect:
                mock_connection = Mock()
                mock_cursor = Mock()
                mock_connect.return_value = mock_connection
                mock_connection.cursor.return_value = mock_cursor
                mock_cursor.fetchone.side_effect = [
                    [1],  # study_key
                    [1],  # platform_key
                    [1],  # sample_key for SRR1652895
                    [2],  # sample_key for SRR1652896
                    [1],  # gene_key for ENSG00000000003
                    [2],  # gene_key for ENSG00000000005
                    [3]   # gene_key for ENSG00000000419
                ]
                
                # Run ETL pipeline
                orchestrator = EnhancedETLOrchestrator(config)
                results = orchestrator.execute_study_pipeline("SRP049820")
                
                # Verify results
                assert results['success'] == True
                assert results['study_code'] == "SRP049820"
                assert 'extraction_stats' in results
                assert 'transformation_stats' in results
                assert 'load_stats' in results
                assert results['duration_seconds'] > 0
                
                # Verify extraction
                assert results['extraction_stats']['records_extracted'] == 6  # 3 genes * 2 samples
                
                # Verify transformation
                assert results['transformation_stats']['samples_processed'] == 2
                assert results['transformation_stats']['samples_processed'] == 2

class TestPerformance:
    """Performance testing"""
    
    def test_large_matrix_processing(self):
        """Test processing of large expression matrix"""
        # Create large test matrix (1000 genes x 100 samples)
        n_genes = 1000
        n_samples = 100
        
        genes = [f"ENSG{str(i).zfill(11)}" for i in range(n_genes)]
        samples = [f"SRR{str(i+1652895)}" for i in range(n_samples)]
        
        # Create expression data
        expression_data = np.random.lognormal(mean=1.0, sigma=1.0, size=(n_genes, n_samples))
        
        # Create DataFrame
        df = pd.DataFrame(expression_data, index=genes, columns=samples)
        df.index.name = 'Gene'
        df = df.reset_index()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tsv', delete=False) as f:
            df.to_csv(f.name, sep='\t', index=False)
            temp_file = Path(f.name)
        
        try:
            config = EnhancedETLConfig(
                connection_string="test",
                chunk_size=100  # Small chunk size for testing
            )
            extractor = EnhancedDataExtractor(config)
            
            start_time = datetime.now()
            
            # Test streaming processing
            total_records = 0
            for chunk_df in extractor.extract_expression_matrix_streaming(
                temp_file, "LARGE_STUDY"
            ):
                total_records += len(chunk_df)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Verify all records were processed
            assert total_records == n_genes * n_samples
            assert processing_time < 60  # Should complete within 60 seconds
            
            # Verify performance metrics
            records_per_second = total_records / processing_time
            assert records_per_second > 1000  # Should process at least 1000 records per second
            
        finally:
            temp_file.unlink()

def main():
    """Run all tests"""
    # Configure logging for tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    test_classes = [
        TestEnhancedETLConfig,
        TestIllnessInferenceEngine,
        TestPlatformNormalizationEngine,
        TestEnhancedDataExtractor,
        TestEnhancedDataTransformer,
        TestEnhancedETLOrchestrator,
        TestIntegration,
        TestPerformance
    ]
    
    results = {
        'tests_run': 0,
        'tests_passed': 0,
        'tests_failed': 0,
        'failures': []
    }
    
    for test_class in test_classes:
        print(f"\n=== Running {test_class.__name__} ===")
        
        test_instance = test_class()
        test_methods = [method for method in dir(test_instance) if method.startswith('test_')]
        
        for test_method in test_methods:
            results['tests_run'] += 1
            try:
                getattr(test_instance, test_method)()
                results['tests_passed'] += 1
                print(f"✓ {test_method}")
            except Exception as e:
                results['tests_failed'] += 1
                results['failures'].append(f"{test_class.__name__}.{test_method}: {str(e)}")
                print(f"✗ {test_method}: {str(e)}")
    
    # Print summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {results['tests_run']}")
    print(f"Tests passed: {results['tests_passed']}")
    print(f"Tests failed: {results['tests_failed']}")
    
    if results['failures']:
        print(f"\nFailures:")
        for failure in results['failures']:
            print(f"  - {failure}")
    
    print(f"{'='*60}")
    
    return results['tests_failed'] == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
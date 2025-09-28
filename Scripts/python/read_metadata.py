import json

# Load and analyze the metadata file structure
with open('aggregated_metadata.json', 'r') as f:
    metadata = json.load(f)

# Analyze the key structure and create mappings
print("=== METADATA STRUCTURE ANALYSIS ===")
print(f"Number of experiments: {metadata.get('num_experiments', 'Unknown')}")
print(f"Number of samples: {metadata.get('num_samples', 'Unknown')}")
print(f"Created: {metadata.get('created_at', 'Unknown')}")
print(f"Aggregated by: {metadata.get('aggregate_by', 'Unknown')}")

# Get experiment details
experiments = metadata.get('experiments', {})
if experiments:
    exp_key = list(experiments.keys())[0]  # Get first experiment
    exp_data = experiments[exp_key]
    print(f"\n=== EXPERIMENT DETAILS ({exp_key}) ===")
    print(f"Accession Code: {exp_data.get('accession_code')}")
    print(f"Title: {exp_data.get('title')}")
    print(f"Technology: {exp_data.get('technology')}")
    print(f"Organism: {exp_data.get('organisms')}")
    print(f"Source Database: {exp_data.get('samples', [{}])[0].get('refinebio_source_database') if 'samples' in exp_data else 'Unknown'}")
    
    # Show sample structure
    sample_accession_codes = exp_data.get('sample_accession_codes', [])
    print(f"Number of sample accession codes: {len(sample_accession_codes)}")
    print(f"Sample codes (first 5): {sample_accession_codes[:5]}")

# Get sample details
samples = metadata.get('samples', {})
if samples:
    sample_key = list(samples.keys())[0]  # Get first sample
    sample_data = samples[sample_key]
    print(f"\n=== SAMPLE DETAILS ({sample_key}) ===")
    print(f"Title: {sample_data.get('refinebio_title')}")
    print(f"Organism: {sample_data.get('refinebio_organism')}")
    print(f"Platform: {sample_data.get('refinebio_platform')}")
    print(f"Source Database: {sample_data.get('refinebio_source_database')}")
    print(f"Processed: {sample_data.get('refinebio_processed')}")

print("\n=== KEY FIELD MAPPINGS FOR ETL ===")
print("Study Level:")
print("- study_key: experiments.accession_code")
print("- study_title: experiments.title")
print("- study_technology: experiments.technology")
print("- study_organism: experiments.organisms[0]")

print("\nSample Level:")
print("- sample_key: sample accession code (from TSV columns)")
print("- sample_title: samples[sample_id].refinebio_title")
print("- sample_platform: samples[sample_id].refinebio_platform")

print("\nExpression Level:")
print("- gene_key: First column 'Gene' in TSV")
print("- expression_value: Value at intersection of gene row and sample column")
# Read and analyze the TSV file structure
import pandas as pd

# Read the first few rows to understand structure
df_sample = pd.read_csv('SRP049820-Copy.tsv', sep='\t', nrows=5)
print("=== TSV FILE STRUCTURE ===")
print(f"Number of columns: {len(df_sample.columns)}")
print(f"First column (Gene): {df_sample.columns[0]}")
print(f"Sample columns (first 10): {list(df_sample.columns[1:11])}")
print(f"Sample columns (last 5): {list(df_sample.columns[-5:])}")

print("\n=== SAMPLE DATA ===")
print(df_sample.iloc[:3, :6])  # Show first 3 rows, 6 columns

# Count total rows
full_df = pd.read_csv('SRP049820-Copy.tsv', sep='\t')
print(f"\nTotal genes (rows): {len(full_df)}")
print(f"Total samples (columns - 1): {len(full_df.columns) - 1}")

print("\n=== DATA TRANSFORMATION MAPPING ===")
print("Final ETL Output Structure:")
print("sample_key | gene_key | expression_value | study_key")
print("------------------------------------------------")
print("Each row in TSV becomes multiple rows in output:")
for i, sample_col in enumerate(df_sample.columns[1:6]):  # Show first 5 samples
    gene = df_sample.iloc[0, 0]  # First gene
    expression = df_sample.iloc[0, i+1]  # First expression value
    print(f"{sample_col} | {gene} | {expression:.3f} | SRP049820")

print(f"\nTotal output records expected: {len(full_df)} genes × {len(full_df.columns)-1} samples = {len(full_df) * (len(full_df.columns)-1):,} records")
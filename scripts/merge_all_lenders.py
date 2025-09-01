# Configuration
OUTPUT_DIR = 'output'  # Directory to save results (optional)
RESULT_DIR = 'result'  # Directory to save the final merged result
OUTPUT_FILE = 'all-lenders-exports.csv'

import pandas as pd
import os
import csv
from datetime import datetime

def validate_directories(output_dir, result_dir):
    """Validate output and result directories existence and permissions."""
    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"Output directory not found at: {output_dir}. Please ensure it exists.")
    try:
        os.makedirs(result_dir, exist_ok=True)
        if not os.access(result_dir, os.W_OK):
            raise PermissionError(f"No write permission for directory: {result_dir}")
    except Exception as e:
        raise RuntimeError(f"Failed to create or access result directory {result_dir}: {str(e)}")
    print(f"Current working directory: {os.getcwd()}")

def union_csv_files(output_dir, result_dir, output_file):
    """Union all CSV files from output_dir into a single file in result_dir."""
    # List all CSV files in the output directory
    csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    if not csv_files:
        raise ValueError(f"No CSV files found in {output_dir}")

    # Initialize an empty list to store DataFrames
    dataframes = []

    # Read and concatenate all CSV files
    for csv_file in csv_files:
        file_path = os.path.join(output_dir, csv_file)
        try:
            df = pd.read_csv(
                file_path,
                sep='\t',  # Match the tab-delimited format from save_to_csv
                encoding='utf-8-sig',  # Match UTF-8 with BOM from save_to_csv
                dtype=str,  # Initial read as strings to handle mixed types
                na_values='',  # Treat empty strings as NaN
                keep_default_na=False  # Avoid default NaN conversions
            )
            dataframes.append(df)
            print(f"Successfully read: {csv_file}")
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
            continue

    if not dataframes:
        raise RuntimeError("No DataFrames were successfully loaded from CSV files.")

    # Concatenate all DataFrames
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Clean and validate the combined DataFrame
    if 'time' in combined_df.columns:
        combined_df['time'] = pd.to_datetime(combined_df['time'], errors='coerce')
    print(f"Combined DataFrame shape: {combined_df.shape}")
    print(f"Null counts:\n{combined_df.isnull().sum()}")

    # Save the combined DataFrame to the result file
    output_path = os.path.join(result_dir, output_file)
    try:
        combined_df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',
            sep='\t',
            quoting=csv.QUOTE_ALL,
            na_rep='',
            date_format='%Y-%m-%d %H:%M:%S%z',
            lineterminator='\n',
            escapechar='\\',
            doublequote=True
        )
        print(f"Saved combined results to: {output_path}")

        # Validate the saved CSV
        with open(output_path, 'r', encoding='utf-8-sig') as f:
            csv_reader = csv.reader(f, delimiter='\t')
            header = next(csv_reader)
            expected_columns = combined_df.columns.tolist()
            if header != expected_columns:
                print(f"Warning: CSV header mismatch. Expected: {expected_columns}, Got: {header}")
            else:
                print(f"CSV header validated: {header}")
            first_row = next(csv_reader, None)
            if first_row and len(first_row) != len(expected_columns):
                print(f"Warning: CSV row length mismatch. Expected {len(expected_columns)} columns, Got {len(first_row)}")
    except Exception as e:
        print(f"Error saving or validating CSV: {str(e)}")

def main():
    """Main function to orchestrate the union of CSV files."""
    try:
        validate_directories(OUTPUT_DIR, RESULT_DIR)
        union_csv_files(OUTPUT_DIR, RESULT_DIR, OUTPUT_FILE)
    except Exception as e:
        print(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
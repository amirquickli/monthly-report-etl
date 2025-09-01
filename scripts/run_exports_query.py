# Configuration
SQL_FILE_PATH = 'exports_results.sql'
OUTPUT_DIR = 'output'  # Directory to save results (optional)
import duckdb
import re
import pandas as pd
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

def load_config():
    """Load configuration settings for file paths, MotherDuck token, and report date."""
    # Load environment variables from .env file
    load_dotenv()

    return {
        'MOTHERDUCK_TOKEN': os.getenv('MOTHERDUCK_TOKEN'),
        'SQL_FILE_PATH': '/Users/amirshareghi/Documents/projects/monthly-report-prep/exports_results.sql',
        'OUTPUT_DIR': '/Users/amirshareghi/Documents/projects/monthly-report-prep/output',
        'START_DATE': '2025-01-01T00:00:00Z',  # Update this before each run
        'END_DATE': '2025-08-01T00:00:00Z',     # Update this before each run
        'REPORT_DATE': '2025-08-28'  # Default to today, e.g., 2025-08-28
    }

def validate_paths(sql_file_path, output_dir):
    """Validate SQL file and output directory existence and permissions."""
    if not os.path.isfile(sql_file_path):
        raise FileNotFoundError(f"SQL file not found at: {sql_file_path}. Please ensure the file exists.")
    try:
        os.makedirs(output_dir, exist_ok=True)
        if not os.access(output_dir, os.W_OK):
            raise PermissionError(f"No write permission for directory: {output_dir}")
    except Exception as e:
        raise RuntimeError(f"Failed to create or access output directory {output_dir}: {str(e)}")
    print(f"Current working directory: {os.getcwd()}")

def get_lenders(connection):
    """Fetch distinct lenders from the MotherDuck database."""
    try:
        return connection.execute(
            "SELECT DISTINCT exportedLender FROM quickli_labs.main.\"exports-deals-view\" WHERE exportedLender IS NOT NULL"
        ).fetchall()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch lenders: {str(e)}")

def clean_dataframe(df):
    """Clean DataFrame for CSV export by formatting columns and removing problematic characters."""
    string_columns = ['associated_lender', 'exportedLender', 'primaryIncome', 'rateType', 
                     'loanPurpose', 'lvrBucket', 'transactionType', 'performance', 'scenarioId']
    numeric_columns = ['totalProposedLoanAmount', 'lvr', 'paygIncome', 'weeklyRentalIncome', 
                      'selfEmployedIncome', 'count_all_loan_purpose', 'count_all_unique_scenario_id', 
                      'sum_all_total_proposed_loan_amount']
    
    if 'time' in df.columns:
        df['time'] = df['time'].astype(str)
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(r'[\[\]\{\}"\\,]', '', regex=True)
            comma_values = df[col].str.contains(',', na=False)
            json_pattern = r'[\[\{].*[\]\}]'
            json_values = df[col].str.contains(json_pattern, na=False)
            if comma_values.any():
                print(f"Warning: Commas found in {col}:\n{df[comma_values][col].head()}")
            if json_values.any():
                print(f"Warning: JSON-like content found in {col}:\n{df[json_values][col].head()}")
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def save_to_csv(df, output_path):
    """Save DataFrame to a tab-delimited CSV file and validate its structure."""
    df.to_csv(
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
    print(f"Saved results to: {output_path}")
    
    try:
        with open(output_path, 'r', encoding='utf-8-sig') as f:
            csv_reader = csv.reader(f, delimiter='\t')
            header = next(csv_reader)
            expected_columns = df.columns.tolist()
            if header != expected_columns:
                print(f"Warning: CSV header mismatch. Expected: {expected_columns}, Got: {header}")
            else:
                print(f"CSV header validated: {header}")
            first_row = next(csv_reader, None)
            if first_row and len(first_row) != len(expected_columns):
                print(f"Warning: CSV row length mismatch. Expected {len(expected_columns)} columns, Got {len(first_row)}")
    except Exception as e:
        print(f"Error validating CSV: {str(e)}")

def prepare_rank_data(result_df, current_month, one_month_before, two_months_before):
    """Prepare rank data by filtering, grouping, pivoting, and merging ranks into the DataFrame."""
    # Ensure time is in datetime format and handle timezone
    if result_df['time'].dt.tz is not None:
        result_df['time'] = result_df['time'].dt.tz_convert('UTC').dt.tz_localize(None)

    # Filter data for the last three months
    mask = (
        (result_df['time'].dt.to_period('M') == current_month.to_period('M')) |
        (result_df['time'].dt.to_period('M') == one_month_before.to_period('M')) |
        (result_df['time'].dt.to_period('M') == two_months_before.to_period('M'))
    )
    result_df_filtered = result_df[mask].copy()

    # Calculate the count of scenarioId per Tier, exportedLender, and month
    count_df = result_df_filtered.groupby(['Tier', 'exportedLender', result_df_filtered['time'].dt.to_period('M')])['scenarioId'].count().reset_index(name='scenario_count')
    count_df = count_df.rename(columns={'time': 'Month'})

    # Calculate rank within each Tier for each month
    count_df['rank_in_tier'] = count_df.groupby(['Tier', 'Month'])['scenario_count'].rank(ascending=False, method='min').astype(int)

    # Pivot the data to have separate columns for each month’s rank and count
    pivot_df = count_df.pivot_table(
        index=['Tier', 'exportedLender'],
        columns='Month',
        values=['scenario_count', 'rank_in_tier'],
        fill_value=0
    ).reset_index()

    # Flatten the multi-level columns, replacing NaT with empty string
    pivot_df.columns = [
        f"{col[0]}_" if pd.isna(col[1]) else f"{col[0]}_{str(col[1])}"
        for col in pivot_df.columns.values
    ]

    # Debug: Print column names and values to identify issues
    print("Pivot_df columns:", pivot_df.columns)
    print("Sample data:", pivot_df.head())

    # Rename columns with verified month periods
    pivot_df = pivot_df.rename(columns={
        'Tier_': 'Tier',
        'exportedLender_': 'exportedLender',
        f'rank_in_tier_{one_month_before.to_period("M")}': 'rank_in_tier_one_month',
        f'rank_in_tier_{two_months_before.to_period("M")}': 'rank_in_tier_two_months',
        f'scenario_count_{current_month.to_period("M")}': 'scenario_count_current',
        f'scenario_count_{one_month_before.to_period("M")}': 'scenario_count_one_month',
        f'scenario_count_{two_months_before.to_period("M")}': 'scenario_count_two_months'
    })

    # Merge the pivoted data back into the original result_df
    result_df = result_df.merge(
        pivot_df[['Tier', 'exportedLender', 'rank_in_tier_one_month', 'rank_in_tier_two_months']],
        on=['Tier', 'exportedLender'],
        how='left'
    )

    # Additional debug to verify dates
    print("Current month:", current_month, current_month.to_period('M'))
    print("One month before:", one_month_before, one_month_before.to_period('M'))
    print("Two months before:", two_months_before, two_months_before.to_period('M'))

    return result_df

def main():
    """Main function to orchestrate query execution and CSV export."""
    # Load configuration
    config = load_config()
    sql_file_path = config['SQL_FILE_PATH']
    output_dir = config['OUTPUT_DIR']
    motherduck_token = config['MOTHERDUCK_TOKEN']
    start_date = config['START_DATE']
    end_date = config['END_DATE']
    report_date = pd.Timestamp(config['REPORT_DATE'], tz='UTC').tz_localize(None)  # Convert to datetime and remove timezone

    # Define date variables based on report_date
    current_month = report_date.replace(day=1)  # First day of the report month
    one_month_before = current_month - pd.DateOffset(months=1)
    two_months_before = current_month - pd.DateOffset(months=2)

    # Validate paths
    validate_paths(sql_file_path, output_dir)
    
    # Read SQL query
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
    
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Report date: {report_date}")
    
    # Connect to MotherDuck
    try:
        con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MotherDuck: {str(e)}")
    
    try:
        # Get lenders
        lenders = get_lenders(con)
        
        # Process each lender
        for lender in lenders:
            lender_name = lender[0]
            # if lender_name != 'redzed': continue  # Process only 'redzed' for now
            print(f"\nRunning query for lender: {lender_name}")
            
            # Format query with placeholders
            query = sql_query.format(
                start_date=start_date,
                end_date=end_date,
                lender_name=lender_name
            )
            
            try:
                # Execute query
                result_df = con.execute(query).fetchdf()
                tier_df = pd.read_csv('competitor-list.csv')
                result_df = result_df.merge(tier_df, how='left', left_on='exportedLender', right_on='Lender')
                result_df = result_df.drop(columns=['Lender'])

                # Prepare rank data
                result_df = prepare_rank_data(result_df, current_month, one_month_before, two_months_before)
                
                # Log DataFrame details
                print(f"Results for {lender_name}: {len(result_df)} rows")
                print(f"Null counts:\n{result_df.isnull().sum()}")
                
                # Clean DataFrame
                result_df = clean_dataframe(result_df)
                
                # Save to CSV
                output_path = os.path.join(output_dir, f"results_{lender_name}.csv")
                save_to_csv(result_df, output_path)
                
            except Exception as e:
                print(f"Error running query for {lender_name}: {str(e)}")
                
    finally:
        con.close()
        print("All files are successfully processed.")

if __name__ == "__main__":
    main()
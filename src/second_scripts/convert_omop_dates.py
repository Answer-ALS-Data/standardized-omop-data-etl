import pandas as pd
import os
from datetime import datetime
import glob


def convert_date(date_str):
    """Convert date from dd/mm/yyyy to yyyy-mm-dd format"""
    if pd.isna(date_str) or date_str == "":
        return date_str
    try:
        # Try parsing as dd/mm/yyyy
        date_obj = datetime.strptime(str(date_str), "%d/%m/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        # If it's already in yyyy-mm-dd format or invalid, return as is
        return date_str


def process_file(file_path):
    """Process a single CSV file and convert date columns"""
    print(f"Processing {file_path}...")

    # Read the CSV file
    df = pd.read_csv(file_path)

    # Find columns that contain '_date' in their name
    date_columns = [col for col in df.columns if "_date" in col.lower()]

    if not date_columns:
        print(f"No date columns found in {file_path}")
        return

    # Convert dates in each date column
    for col in date_columns:
        df[col] = df[col].apply(convert_date)

    # Save the modified file
    output_path = file_path
    df.to_csv(output_path, index=False)
    print(f"Updated {file_path}")


def main():
    # Get all CSV files in the processed_source directory
    omop_files = glob.glob("processed_source/*.csv")

    for file_path in omop_files:
        process_file(file_path)


if __name__ == "__main__":
    main()

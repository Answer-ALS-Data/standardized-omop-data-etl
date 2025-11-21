import pandas as pd
import os
import logging

# List of tables to process
tables = [
    "condition_occurrence",
    "death",
    "device_exposure",
    "drug_exposure",
    "measurement",
    "observation",
    "procedure_occurrence",
]

# Directory containing the CSV files
input_dir = "combined_omop"
output_dir = "combined_omop"

logging.basicConfig(
            filename="logs/create_table_ids.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Process each table
for table in tables:
    input_file = os.path.join(input_dir, f"{table}.csv")
    output_file = os.path.join(output_dir, f"{table}.csv")

    print(f"Processing {table}...")

    # Read the CSV file
    df = pd.read_csv(input_file)

    # Create the ID column name
    id_column_name = f"{table}_id"

    # Check if ID column already exists
    if id_column_name in df.columns:
        print(f"Column {id_column_name} already exists in {table}.csv")
    else:
        # Create the ID column
        df.insert(0, id_column_name, range(1, len(df) + 1))
        print(f"Added {id_column_name} to {table}.csv")

    # Save the modified DataFrame back to CSV
    df.to_csv(output_file, index=False)

print("All tables processed successfully!")

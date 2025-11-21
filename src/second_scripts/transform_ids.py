import pandas as pd
import os


def transform_id(original_id):
    """
    Transform an ID to a 9-digit format with:
    - First 2 digits: 11
    - Middle digits: zeros
    - Last digits: original ID
    """
    # Convert original_id to string and remove any non-numeric characters
    original_id = str(original_id)
    original_id = "".join(filter(str.isdigit, original_id))

    # Calculate how many zeros we need
    zeros_needed = 7 - len(original_id)  # 9 (total) - 2 (prefix) = 7
    if zeros_needed < 0:
        # If original ID is too long, truncate it
        original_id = original_id[-7:]
        zeros_needed = 0

    # Create the new ID
    new_id = f"11{'0' * zeros_needed}{original_id}"
    return new_id


def transform_table_ids(table_name, input_dir="combined_omop", output_dir="final_omop"):
    """
    Transform IDs in a table to the new 9-digit format.
    """
    input_file = os.path.join(input_dir, f"{table_name}.csv")
    output_file = os.path.join(output_dir, f"{table_name}.csv")

    print(f"Processing {table_name}...")
    print(f"  Input file: {input_file}")
    print(f"  Output file: {output_file}")
    print(f"  Exists: {os.path.exists(input_file)}")
    if os.path.exists(input_file):
        print(f"  Size: {os.path.getsize(input_file)} bytes")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Check if the file is empty
    if not os.path.exists(input_file) or os.path.getsize(input_file) == 0:
        print(f"Warning: {input_file} is empty or does not exist. Skipping.")
        return

    # Read the CSV file
    print(f"  Reading {input_file}...")
    df = pd.read_csv(input_file)
    print(f"  Read {len(df)} rows and {len(df.columns)} columns.")

    # Get the ID column name
    id_column_name = f"{table_name}_id"

    # Transform the IDs
    if id_column_name in df.columns:
        df[id_column_name] = df[id_column_name].apply(transform_id)
        print(f"Transformed {id_column_name} in {table_name}.csv")

    # Also transform person_id and visit_occurrence_id if they exist
    for col in ["person_id", "visit_occurrence_id"]:
        if col in df.columns:
            df[col] = df[col].apply(transform_id)
            print(f"Transformed {col} in {table_name}.csv")

    # Save the modified DataFrame back to CSV
    df.to_csv(output_file, index=False)
    print(f"Saved transformed {table_name}.csv")


def main():
    # List of tables to process
    tables = [
        "condition_occurrence",
        "death",
        "device_exposure",
        "drug_exposure",
        "measurement",
        "observation_period",
        "observation",
        "person",
        "procedure_occurrence",
        "visit_occurrence",
    ]

    # Process each table
    for table in tables:
        transform_table_ids(table)

    print("All tables processed successfully!")


if __name__ == "__main__":
    main()

import pandas as pd
import os
from collections import defaultdict


def load_concept_mappings():
    """Load concept mappings from all CSV files in the usagi directory."""
    concept_mappings = {}
    usagi_dir = "source_tables/usagi"

    # Get all CSV files in the usagi directory
    mapping_files = [f for f in os.listdir(usagi_dir) if f.endswith(".csv")]

    for file in mapping_files:
        file_path = os.path.join(usagi_dir, file)
        try:
            df = pd.read_csv(file_path)
            if "conceptId" in df.columns and "conceptName" in df.columns:
                for _, row in df.iterrows():
                    concept_id = row["conceptId"]
                    concept_name = row["conceptName"]
                    if pd.notna(concept_id) and pd.notna(concept_name):
                        concept_mappings[int(concept_id)] = concept_name
        except Exception as e:
            print(f"Error processing mapping file {file}: {str(e)}")

    return concept_mappings


def find_redundant_concept_ids():
    # Load concept mappings
    concept_mappings = load_concept_mappings()

    # Directory containing OMOP tables
    omop_dir = "processed_source"

    # Dictionary to store concept IDs by person_id and table
    person_concept_ids = defaultdict(lambda: defaultdict(set))

    # Get all CSV files
    csv_files = [f for f in os.listdir(omop_dir) if f.endswith(".csv")]

    # Process each file
    for file in csv_files:
        table_name = file.split("--")[0] if "--" in file else file.split(".")[0]
        file_path = os.path.join(omop_dir, file)

        try:
            # Read the CSV file
            df = pd.read_csv(file_path)

            # Check if file has person_id and any concept_id columns
            if "person_id" in df.columns:
                # Get concept_id columns, excluding _type_concept_id
                concept_id_cols = [
                    col
                    for col in df.columns
                    if "_concept_id" in col and "_type_concept_id" not in col
                ]

                if concept_id_cols:
                    # For each person and concept_id column, store the values
                    for _, row in df.iterrows():
                        person_id = row["person_id"]
                        for col in concept_id_cols:
                            if pd.notna(row[col]):
                                person_concept_ids[person_id][table_name].add(
                                    (col, row[col])
                                )

        except Exception as e:
            print(f"Error processing {file}: {str(e)}")

    # Find redundancies
    redundancies = []
    for person_id, table_concepts in person_concept_ids.items():
        # Create a dictionary to track concept_id values across tables
        concept_values = defaultdict(list)

        for table, concepts in table_concepts.items():
            for col, value in concepts:
                concept_values[(col, value)].append(table)

        # Check for values that appear in multiple tables
        for (col, value), tables in concept_values.items():
            if len(tables) > 1:
                concept_name = concept_mappings.get(int(value), "Unknown")
                redundancies.append(
                    {
                        "person_id": person_id,
                        "concept_column": col,
                        "concept_id": value,
                        "concept_name": concept_name,
                        "tables": ";".join(
                            tables
                        ),  # Join tables with semicolon for CSV
                    }
                )

    # Convert to DataFrame and save to CSV
    if redundancies:
        df_redundancies = pd.DataFrame(redundancies)
        output_file = "redundant_concept_ids.csv"
        df_redundancies.to_csv(output_file, index=False)
        print(
            f"\nFound {len(redundancies)} redundant concept IDs. Results saved to {output_file}"
        )
    else:
        print("\nNo redundant concept IDs found across different tables.")


if __name__ == "__main__":
    find_redundant_concept_ids()

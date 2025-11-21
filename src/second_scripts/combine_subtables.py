import pandas as pd
import os
from pathlib import Path
import logging

# Define priorities for each table type
PRIORITIES = {
    "condition_occurrence": ["aalshxfx", "neurolog", "medical_history"],
    "observation": [
        "aalshxfx",
        "aalsdxfx",
        "alsfrs_r",
        "family_history_log",
        "environmental_questionnaire",
    ],
    "measurement": ["als_gene_mutations", "auxiliary_chemistry_labs", "vital_signs"],
    "drug_exposure": ["answer_als_medications_log", "medical_history"],
}

# Define concept_id column names for each table type
CONCEPT_ID_COLUMNS = {
    "condition_occurrence": "condition_concept_id",
    "observation": "observation_concept_id",
    "measurement": "measurement_concept_id",
    "drug_exposure": "drug_concept_id",
}




def get_concept_info(df, concept_id_col):
    """Extract concept_id and person_id from a dataframe"""
    if concept_id_col in df.columns and "person_id" in df.columns:
        # Convert to integer type
        df[concept_id_col] = pd.to_numeric(df[concept_id_col], errors="coerce").astype(
            "Int64"
        )
        return df[["person_id", concept_id_col]].drop_duplicates()
    return pd.DataFrame()


def combine_tables():
    # Create output directories
    combined_omop_dir = Path("combined_omop")
    redundant_dir = Path("redundant")
    combined_omop_dir.mkdir(exist_ok=True)
    redundant_dir.mkdir(exist_ok=True)

    # Process each table type with priorities
    for table_type, priorities in PRIORITIES.items():
        print(f"Processing {table_type}...")

        # Initialize empty DataFrames for combined_omop and redundant data
        combined_omop_df = pd.DataFrame()
        redundant_concepts = pd.DataFrame()

        # Get the concept_id column name for this table type
        concept_id_col = CONCEPT_ID_COLUMNS[table_type]

        # Process files in priority order
        for source in priorities:
            pattern = f"{source}--{table_type}.csv"
            matching_files = list(Path("processed_source").glob(pattern))

            if not matching_files:
                print(f"Warning: No file found matching {pattern}")
                continue

            file_path = matching_files[0]
            print(f"Processing {file_path}")

            # Read the CSV file
            df = pd.read_csv(file_path)

            # Check if required columns exist
            if concept_id_col not in df.columns:
                print(f"Warning: {concept_id_col} not found in {file_path}")
                continue
                
            if "person_id" not in df.columns:
                print(f"Warning: person_id not found in {file_path}")
                continue

            # Convert concept_id column to integer type
            df[concept_id_col] = pd.to_numeric(
                df[concept_id_col], errors="coerce"
            ).astype("Int64")

            if combined_omop_df.empty:
                # First file becomes the base
                combined_omop_df = df
                continue

            # Get concepts from current file
            current_concepts = get_concept_info(df, concept_id_col)
            current_concepts["source_file"] = file_path.name

            # Find redundant concepts
            existing_concepts = get_concept_info(
                combined_omop_df, concept_id_col
            )
            existing_concepts["source_file"] = "previously_combined_omop"

            # Merge to find redundant concepts
            redundant = pd.merge(
                current_concepts, existing_concepts, on=[concept_id_col, "person_id"], how="inner"
            )

            if not redundant.empty:
                # Add to redundant concepts
                redundant_concepts = pd.concat([redundant_concepts, redundant])

                # Remove redundant rows from current dataframe (person-specific)
                # Create a composite key for comparison
                redundant_keys = redundant[["person_id", concept_id_col]].drop_duplicates()
                df_keys = df[["person_id", concept_id_col]].drop_duplicates()
                
                # Remove rows where both person_id and concept_id match
                df = df[~df.set_index(["person_id", concept_id_col]).index.isin(
                    redundant_keys.set_index(["person_id", concept_id_col]).index
                )]

            # Combine with existing data
            combined_omop_df = pd.concat([combined_omop_df, df], ignore_index=True)

        # Save combined_omop table
        output_file = combined_omop_dir / f"{table_type}.csv"
        combined_omop_df.to_csv(output_file, index=False)
        print(f"Saved combined_omop {table_type} to {output_file}")

        # Save redundant concepts if any
        if not redundant_concepts.empty:
            # Clean up redundant concepts DataFrame
            redundant_concepts = redundant_concepts.rename(
                columns={
                    f"{concept_id_col}_x": concept_id_col,
                    "source_file_x": "source_file",
                    "source_file_y": "existing_source",
                    "person_id_x": "person_id",
                }
            )

            # Add additional information
            redundant_concepts["table_type"] = table_type
            redundant_concepts["priority_order"] = redundant_concepts[
                "source_file"
            ].apply(
                lambda x: (
                    priorities.index(x.split("--")[0])
                    if x != "previously_combined_omop"
                    else -1
                )
            )

            redundant_file = redundant_dir / f"{table_type}_redundant.csv"
            redundant_concepts.to_csv(redundant_file, index=False)
            print(f"Saved redundant concepts for {table_type} to {redundant_file}")

    # Process tables without priorities
    for file in Path("processed_source").glob("*.csv"):
        if "--" not in file.name:  # Skip files that are already combined_omop
            continue

        source, table_type = file.name.split("--")
        table_type = table_type.replace(".csv", "")

        if table_type not in PRIORITIES:  # Only process tables without priorities
            print(f"Processing {table_type}...")
            df = pd.read_csv(file)
            output_file = combined_omop_dir / f"{table_type}.csv"
            df.to_csv(output_file, index=False)
            print(f"Saved {table_type} to {output_file}")


if __name__ == "__main__":
    logging.basicConfig(
        filename="logs/combine_subtables.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    combine_tables()

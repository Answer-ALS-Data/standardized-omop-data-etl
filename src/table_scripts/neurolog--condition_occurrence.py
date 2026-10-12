import pandas as pd
import logging
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)
import os
from pathlib import Path
from datetime import datetime

# Get the script's directory
script_dir = Path(__file__).parent
project_root = script_dir.parent

# Create necessary directories
source_tables_dir = project_root / "source_tables"

# Set up logging
logging.basicConfig(
    filename="logs/neurolog--condition_occurrence.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def prepare_mapping(usagi_mapping):
    """Trim label edges only; ambiguous keys require review, never first-row wins."""
    mapping = usagi_mapping.copy()
    mapping.index = mapping["sourceName"].fillna("").astype(str).str.strip()
    if mapping.index.duplicated().any():
        raise ValueError("Ambiguous NEUROLOG source labels after trimming")
    return mapping


def find_mapping(source_value, mapping):
    if pd.isna(source_value):
        return None
    key = str(source_value).strip()
    if not key or key not in mapping.index:
        return None
    return mapping.loc[key]


def main():
    try:
        # Read source data
        source_data = pd.read_csv("source_tables/neurolog.csv")
        usagi_mapping = prepare_mapping(pd.read_csv("source_tables/usagi/neurolog_mapping_v3.csv"))

        # Initialize output dataframe
        output_columns = [
            "person_id",
            "condition_concept_id",
            "condition_source_value",
            "condition_start_date",
            "condition_type_concept_id",
            "visit_occurrence_id",
        ]
        output_data = pd.DataFrame(columns=output_columns)

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")
        default_date = datetime.strptime("1900-01-01", "%Y-%m-%d")

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Get condition_start_date using relative_day_to_date, defaulting to 1900-01-01 if empty
            condition_start_date = (
                relative_day_to_date(row["date1"], index_date)
                if pd.notna(row["date1"])
                else default_date
            )

            # Create visit_occurrence_id using helper function
            visit_occurrence_id = get_visit_occurrence_id(
                person_id, row.get("Visit_Date")
            )

            # Process hidden2 value
            hidden2_value = row["hidden2"]
            mapping = find_mapping(hidden2_value, usagi_mapping)
            if mapping is None:
                continue
            condition_concept_id = mapping["conceptId"]
            mapping_equivalence = mapping.get("equivalence", "EQUAL")

            # Create condition_source_value with new format
            source_parts = []
            
            # Add hidden2 value (neurolog+hidden2)
            if pd.notna(hidden2_value) and hidden2_value.strip():
                source_parts.append(f"neurolog+hidden2 (neurological disease): {hidden2_value}")
            
            # Add other value if present (neurolog+other)
            if pd.notna(row.get("other")) and row["other"].strip():
                other_value = row["other"].strip()
                source_parts.append(f"neurolog+other (specified type): {other_value}")
            
            # Add USAGI mapping equivalence
            source_parts.append(f"+equivalence (usagi omop mapping equivalence): {mapping_equivalence}")
            
            # Join parts with pipes if multiple, otherwise just use the single part
            condition_source_value = " | ".join(source_parts) if len(source_parts) > 1 else source_parts[0] if source_parts else ""

            # Add row to output
            output_data = pd.concat(
                [
                    output_data,
                    pd.DataFrame(
                        [
                            {
                                "person_id": person_id,
                                "condition_concept_id": condition_concept_id,
                                "condition_source_value": condition_source_value,
                                "condition_start_date": condition_start_date,
                                "condition_type_concept_id": 32851,  # Updated type
                                "visit_occurrence_id": visit_occurrence_id,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, "condition_concept_id")

        # Save output
        output_data.to_csv("processed_source/neurolog--condition_occurrence.csv", index=False)

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

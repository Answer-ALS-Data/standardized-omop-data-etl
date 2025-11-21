import pandas as pd
import logging
from helpers import year_to_date, check_missing_concept_ids, get_visit_occurrence_id
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
    filename="logs/medical_history--device_exposure.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def medical_history_medhxdsc_to_device_exposure_device_concept_id(
    source_value, usagi_mapping
):
    """Convert Medical History medhxdsc values to device concept IDs using USAGI mapping"""
    try:
        # Convert both source value and mapping values to lowercase for case-insensitive matching
        source_value_lower = str(source_value).lower().strip()
        mappings = usagi_mapping[
            (usagi_mapping["sourceName"].str.lower().str.strip() == source_value_lower)
            & (usagi_mapping["domainId"] == "Device")
        ]
        if not mappings.empty:
            return (
                mappings[["conceptId", "conceptName", "equivalence"]].values.tolist(),
                True,
            )
        return [(0, "No Matching Concept", "")], False
    except Exception as e:
        logging.error(f"Error mapping medhxdsc value {source_value}: {str(e)}")
        return [(0, "No Matching Concept", "")], False


def main():
    try:
        # Read source data
        source_data = pd.read_csv("source_tables/medical_history.csv")
        usagi_mapping = pd.read_csv("source_tables/usagi/medical_history_conditions_v2.csv")

        # Filter USAGI mapping to only include concepts with domainId = "Device"
        usagi_mapping = usagi_mapping[usagi_mapping["domainId"] == "Device"]

        # Initialize output dataframe
        output_columns = [
            "person_id",
            "device_concept_id",
            "device_source_value",
            "device_exposure_start_date",
            "device_type_concept_id",
            "visit_occurrence_id",
        ]
        output_data = pd.DataFrame(columns=output_columns)

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Get device_exposure_start_date using year_to_date
            device_exposure_start_date = year_to_date(row["medhxyr"])

            # Create visit_occurrence_id using helper function
            visit_occurrence_id = get_visit_occurrence_id(
                person_id, row.get("Visit_Date")
            )

            # Process medhxdsc value
            medhxdsc_value = row["medhxdsc"]
            concept_mappings, has_match = (
                medical_history_medhxdsc_to_device_exposure_device_concept_id(
                    medhxdsc_value, usagi_mapping
                )
            )

            # Only add rows to output if there was a match in the USAGI file
            if has_match:
                # Create a row for each matching concept
                for concept_id, concept_name, equivalence in concept_mappings:
                    output_data = pd.concat(
                        [
                            output_data,
                            pd.DataFrame(
                                [
                                    {
                                        "person_id": person_id,
                                        "device_concept_id": concept_id,
                                        "device_source_value": f"medical_history+medhxdsc (Description): {medhxdsc_value} | +equivalence (usagi omop mapping equivalence): {equivalence}",
                                        "device_exposure_start_date": device_exposure_start_date,
                                        "device_type_concept_id": 32851,  # Healthcare professional filled survey
                                        "visit_occurrence_id": visit_occurrence_id,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, "device_concept_id")

        # Save output
        output_data.to_csv("processed_source/medical_history--device_exposure.csv", index=False)

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

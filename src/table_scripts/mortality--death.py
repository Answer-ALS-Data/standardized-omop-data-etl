import pandas as pd
import logging
from helpers import relative_day_to_date, check_missing_concept_ids
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
    filename="logs/mortality--death.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    try:
        # Read source data - the mapping file already contains the concept mappings
        source_data = pd.read_csv("source_tables/usagi/Mortality OMOP Mapping.csv")

        # Initialize output dataframe
        output_columns = [
            "person_id",
            "death_date",
            "death_type_concept_id",
            "cause_concept_id",
            "cause_source_value",
        ]
        output_data = pd.DataFrame(columns=output_columns)

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id (from Participant_ID)
            person_id = row["Participant_ID"]

            # Get death_date using relative_day_to_date, default to 1900-01-01 if empty
            if pd.isna(row["dieddt"]):
                death_date = "1900-01-01"
                dieddt_value = ""
            else:
                death_date = relative_day_to_date(row["dieddt"], index_date).strftime(
                    "%Y-%m-%d"
                )
                dieddt_value = str(int(row["dieddt"]))

            # Get cause information from source data
            diedcaus_value = (
                str(row["diedcaus"]).strip() if pd.notna(row["diedcaus"]) else ""
            )
            icd10cm_value = (
                str(row["icd10cm"]).strip() if pd.notna(row["icd10cm"]) else ""
            )

            # Create cause_source_value using new format
            source_parts = []
            
            # Add dieddt if it has a value
            if dieddt_value:
                source_parts.append(f"mortality+dieddt (days since intake): {dieddt_value}")
            
            # Add diedcaus if it has a value
            if diedcaus_value:
                source_parts.append(f"mortality+diedcaus (death cause): {diedcaus_value}")
            
            # Add icd10cm if it has a value
            if icd10cm_value:
                source_parts.append(f"mortality+icd10cm (ICD-10-CM code): {icd10cm_value}")
            
            # If no values, indicate presence of death record
            if not source_parts:
                source_parts.append("mortality+dieddt (days since intake)")
            
            cause_source_value = " | ".join(source_parts)

            # Get concept details from the mapping file
            cause_concept_id = (
                int(row["cause_concept_id"]) if pd.notna(row["cause_concept_id"]) else 0
            )

            # Add row to output
            output_data = pd.concat(
                [
                    output_data,
                    pd.DataFrame(
                        [
                            {
                                "person_id": person_id,
                                "death_date": death_date,
                                "death_type_concept_id": 32851,  # Healthcare professional filled survey
                                "cause_concept_id": cause_concept_id,
                                "cause_source_value": cause_source_value,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, "cause_concept_id")

        # Save output
        output_data.to_csv("processed_source/mortality--death.csv", index=False)

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

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
logs_dir = project_root / "logs"
processed_source_dir = project_root / "processed_source"
source_tables_dir = project_root / "source_tables"

# Set up logging
logging.basicConfig(
    filename="logs/medical_history--drug_exposure.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def format_source_value(table, var, var_interpretation=None, value=None, value_interpretation=None):
    """
    Format source value as: table+var (var_interpretation): value (val_interpretation)
    Omit interpretation if same as term before or missing. Use actual variable name if no variable.
    No pipes for singles.
    """
    # Table and variable
    if var:
        left = f"{table}+{var}"
    else:
        left = f"{table}+source_column"
    
    # Variable interpretation
    if var_interpretation and var_interpretation.strip() and var_interpretation.strip().lower() != var.strip().lower():
        left += f" ({var_interpretation})"
    
    # Value
    if value is not None:
        right = f"{value}"
        # Value interpretation
        if value_interpretation and str(value_interpretation).strip() and str(value_interpretation).strip().lower() != str(value).strip().lower():
            right += f" ({value_interpretation})"
        return f"{left}: {right}"
    else:
        # No value provided - just return the variable with interpretation
        return left

def medical_history_medhxdsc_to_drug_exposure_drug_concept_id(source_value, usagi_mapping):
    """Convert Medical History medhxdsc values to drug concept IDs using USAGI mapping"""
    try:
        # Convert both source value and mapping values to lowercase for case-insensitive matching
        source_value_lower = str(source_value).lower().strip()
        mappings = usagi_mapping[
            (usagi_mapping["sourceName"].str.lower().str.strip() == source_value_lower)
            & (usagi_mapping["domainId"] == "Drug")
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

        # Filter USAGI mapping to only include concepts with domainId = "Drug"
        usagi_mapping = usagi_mapping[usagi_mapping["domainId"] == "Drug"]

        # Initialize output dataframe
        output_columns = [
            "person_id",
            "drug_concept_id",
            "drug_source_value",
            "drug_exposure_start_date",
            "drug_exposure_end_date",
            "verbatim_end_date",
            "drug_type_concept_id",
            "route_concept_id",
            "route_source_value",
            "visit_occurrence_id",
        ]
        output_data = pd.DataFrame(columns=output_columns)

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Get drug_exposure_start_date using year_to_date
            drug_exposure_start_date = year_to_date(row["medhxyr"])
            drug_exposure_end_date = drug_exposure_start_date  # Use same date for end date
            verbatim_end_date = drug_exposure_start_date

            # Create visit_occurrence_id using helper function
            visit_occurrence_id = get_visit_occurrence_id(
                person_id, row.get("Visit_Date")
            )

            # Process medhxdsc value
            medhxdsc_value = row["medhxdsc"]
            concept_mappings, has_match = (
                medical_history_medhxdsc_to_drug_exposure_drug_concept_id(
                    medhxdsc_value, usagi_mapping
                )
            )

            # Only add rows to output if there was a match in the USAGI file
            if has_match:
                # Create a row for each matching concept
                for concept_id, concept_name, equivalence in concept_mappings:
                    # Build drug_source_value using new format
                    source_parts = []
                    
                    # Add description
                    if medhxdsc_value and pd.notna(medhxdsc_value):
                        source_parts.append(format_source_value("medical_history", "medhxdsc", "Description", medhxdsc_value))
                    
                    # Add year
                    if pd.notna(row.get("medhxyr")):
                        source_parts.append(format_source_value("medical_history", "medhxyr", "Year of Diagnosis", row["medhxyr"]))
                    
                    # Add still present status
                    if pd.notna(row.get("medhxprs")):
                        present_value = int(row["medhxprs"])
                        present_text = "yes" if present_value == 1 else "no" if present_value == 2 else "unknown"
                        source_parts.append(format_source_value("medical_history", "medhxprs", "Still Present", present_value, present_text))
                    
                    # Add equivalence from mapping
                    if equivalence:
                        source_parts.append(format_source_value("", "equivalence", "usagi omop mapping equivalence", equivalence))
                    
                    # Join parts with pipes if multiple, otherwise just use the single part
                    drug_source_value = " | ".join(source_parts) if len(source_parts) > 1 else source_parts[0] if source_parts else ""

                    output_data = pd.concat(
                        [
                            output_data,
                            pd.DataFrame(
                                [
                                    {
                                        "person_id": person_id,
                                        "drug_concept_id": concept_id,
                                        "drug_source_value": drug_source_value,
                                        "drug_exposure_start_date": drug_exposure_start_date,
                                        "drug_exposure_end_date": drug_exposure_end_date,
                                        "verbatim_end_date": verbatim_end_date,
                                        "drug_type_concept_id": 32851,  # Healthcare professional filled survey
                                        "route_concept_id": 0,  # No route information available
                                        "route_source_value": "BLANK",
                                        "visit_occurrence_id": visit_occurrence_id,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, ["drug_concept_id", "route_concept_id"])

        # Save output
        output_data.to_csv("processed_source/medical_history--drug_exposure.csv", index=False)

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()

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

# Ensure only the directories exist
# Set up logging
logging.basicConfig(
    filename="logs/environmental_questionnaire--observation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def is_numeric_value(value):
    """
    Check if a value can be converted to a number.
    
    Args:
        value: The value to check
        
    Returns:
        bool: True if the value is numeric, False otherwise
    """
    if pd.isna(value):
        return False
    
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def safe_numeric_value(value):
    """
    Safely convert a value to a number, returning None if conversion fails.
    
    Args:
        value: The value to convert
        
    Returns:
        float or None: The numeric value if conversion succeeds, None otherwise
    """
    if pd.isna(value):
        return None
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def format_source_value(table_name, variable_name, interpretation=None, value=None, value_interpretation=None, include_interpretation=True):
    """
    Format a single source value according to the new specification.
    
    Args:
        table_name: Name of the source table
        variable_name: Name of the variable
        interpretation: Interpretation of the variable (optional)
        value: The actual value (optional)
        value_interpretation: Interpretation of the value (optional)
        include_interpretation: Whether to include variable interpretation (default True)
    
    Returns:
        Formatted source value string
    """
    # Start with table+var
    result = f"{table_name}+{variable_name}"
    
    # Add interpretation if provided, different from variable name, and requested
    if interpretation and interpretation.lower() != variable_name.lower() and include_interpretation:
        result += f" ({interpretation})"
    
    # Add value and value interpretation if provided
    if value is not None:
        result += f": {value}"
        if value_interpretation and str(value_interpretation).lower() != str(value).lower():
            result += f" ({value_interpretation})"
    
    return result

def format_multiple_source_values(source_parts):
    """
    Format multiple source values with pipe separators.
    
    Args:
        source_parts: List of formatted source value strings
    
    Returns:
        Combined source value string with pipe separators
    """
    if len(source_parts) == 1:
        return source_parts[0]
    return " | ".join(source_parts)

# Mapping of source variables to their corresponding concept IDs and names
OCCUPATION_MAPPINGS = {
    "mock": {
        "concept_id": 36310392,
        "concept_name": "Management Occupations",
        "source_value": "Management Occupations",
    },
    "bfopo": {
        "concept_id": 36308234,
        "concept_name": "Business and Financial Operations Occupations",
        "source_value": "Business and Financial Operations Occupations",
    },
    "cmock": {
        "concept_id": 36309431,
        "concept_name": "Computer and Mathematical Occupations",
        "source_value": "Computer and Mathematical Occupations",
    },
    "aeock": {
        "concept_id": 36311194,
        "concept_name": "Architecture and Engineering Occupations",
        "source_value": "Architecture and Engineering Occupations",
    },
    "lpssock": {
        "concept_id": 36308008,
        "concept_name": "Life, Physical, and Social Science Occupations",
        "source_value": "Life, Physical, and Social Science Occupations",
    },
    "cssock": {
        "concept_id": 36308579,
        "concept_name": "Community and Social Service Occupations",
        "source_value": "Community and Social Service Occupations",
    },
    "lock": {
        "concept_id": 36307876,
        "concept_name": "Legal Occupations",
        "source_value": "Legal Occupations",
    },
    "etlock": {
        "concept_id": 36310455,
        "concept_name": "Education, Training, and Library Occupations",
        "source_value": "Education, Training, and Library Occupations",
    },
    "adesmock": {
        "concept_id": 36309675,
        "concept_name": "Arts, Design, Entertainment, Sports, and Media Occupations",
        "source_value": "Arts, Design, Entertainment, Sports, and Media Occupations",
    },
    "hptock": {
        "concept_id": 36308137,
        "concept_name": "Healthcare Practitioners and Technical Occupations",
        "source_value": "Healthcare Practitioners and Technical Occupations",
    },
    "hsock": {
        "concept_id": 36307824,
        "concept_name": "Healthcare Support Occupations",
        "source_value": "Healthcare Support Occupations",
    },
    "psock": {
        "concept_id": 36309614,
        "concept_name": "Protective Service Occupations",
        "source_value": "Protective Service Occupations",
    },
    "fpsrock": {
        "concept_id": 36307878,
        "concept_name": "Food Preparation and Serving Related Occupations",
        "source_value": "Food Preparation and Serving Related Occupations",
    },
    "bgclmock": {
        "concept_id": 36310459,
        "concept_name": "Building and Grounds Cleaning and Maintenance Occupations",
        "source_value": "Building and Grounds Cleaning and Maintenance Occupations",
    },
    "pcsock": {
        "concept_id": 36309317,
        "concept_name": "Personal Care and Service Occupations",
        "source_value": "Personal Care and Service Occupations",
    },
    "srock": {
        "concept_id": 36310875,
        "concept_name": "Sales and Related Occupations",
        "source_value": "Sales and Related Occupations",
    },
    "oasock": {
        "concept_id": 36307554,
        "concept_name": "Office and Administrative Support Occupations",
        "source_value": "Office and Administrative Support Occupations",
    },
    "fffock": {
        "concept_id": 36307977,
        "concept_name": "Farming, Fishing, and Forestry Occupations",
        "source_value": "Farming, Fishing, and Forestry Occupations",
    },
    "ceock": {
        "concept_id": 36307566,
        "concept_name": "Construction and Extraction Occupations",
        "source_value": "Construction and Extraction Occupations",
    },
    "imrock": {
        "concept_id": 36308876,
        "concept_name": "Installation, Maintenance, and Repair Occupations",
        "source_value": "Installation, Maintenance, and Repair Occupations",
    },
    "pock": {
        "concept_id": 36310607,
        "concept_name": "Production Occupations",
        "source_value": "Production Occupations",
    },
    "tmmock": {
        "concept_id": 36309959,
        "concept_name": "Transportation and Material Moving Occupations",
        "source_value": "Transportation and Material Moving Occupations",
    },
    "msock": {
        "concept_id": 36309362,
        "concept_name": "Military Specific Occupations",
        "source_value": "Military Specific Occupations",
    },
}


def milirb_to_concept_id(value):
    """Convert military service response to concept ID"""
    if value == 1:
        return 45877994  # Yes
    elif value == 0:
        return 45878245  # No
    return None


def smokerb_to_concept_id(value):
    """Convert smoking history response to concept ID"""
    if value == 1:
        return 45877994  # Yes
    elif value == 0:
        return 45878245  # No
    return None


def main():
    try:
        # Read source data
        source_data = pd.read_csv("source_tables/environmental_questionnaire.csv")
        logging.info(f"Read source data with {len(source_data)} rows")

        # Initialize list to store records
        records = []

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Process occupation variables
            for occ_var, mapping in OCCUPATION_MAPPINGS.items():
                if row.get(occ_var) == 1:
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 44786930,
                            "observation_source_value": format_source_value("environmental_questionnaire", occ_var, mapping["concept_name"]),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"],
                            "value_source_value": format_source_value("environmental_questionnaire", occ_var, mapping["concept_name"], 1, "Yes", include_interpretation=False),
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": None,
                            "unit_source_value": None,
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )

            # Process exercise frequency
            if pd.notna(row.get("exerdd")):
                if is_numeric_value(row["exerdd"]):
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 4036426,
                            "observation_source_value": format_source_value("environmental_questionnaire", "exerdd", "Prior to your symptom onset, how many days per week do you exercise at least moderately"),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": safe_numeric_value(row["exerdd"]),
                            "value_as_string": None,
                            "value_as_concept_id": None,
                            "value_source_value": format_source_value("environmental_questionnaire", "exerdd", "Prior to your symptom onset, how many days per week do you exercise at least moderately", row["exerdd"], include_interpretation=False),
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": 8621,
                            "unit_source_value": format_source_value("environmental_questionnaire", "exerdd", "day per week"),
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )
                else:
                    logging.warning(f"Skipping non-numeric exercise frequency value for person_id {person_id}: {row['exerdd']}")

            # Process military service
            if pd.notna(row.get("milirb")):
                mil_concept_id = milirb_to_concept_id(row["milirb"])
                if mil_concept_id:
                    source_parts = []
                    source_parts.append(format_source_value("environmental_questionnaire", "milirb", "Were you in the military?", row["milirb"], "Yes" if row["milirb"] == 1 else "No", include_interpretation=False))
                    if pd.notna(row.get("outusrb")):
                        source_parts.append(format_source_value("environmental_questionnaire", "outusrb", "Were you deployed outside the US?", row["outusrb"], "Yes" if row["outusrb"] == 1 else "No", include_interpretation=False))
                    if pd.notna(row.get("yrsout")):
                        source_parts.append(format_source_value("environmental_questionnaire", "yrsout", "If so, what years?", row["yrsout"], include_interpretation=False))
                    if pd.notna(row.get("where")):
                        source_parts.append(format_source_value("environmental_questionnaire", "where", "To where?", row["where"], include_interpretation=False))
                    value_source = format_multiple_source_values(source_parts)

                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 37162399,
                            "observation_source_value": format_source_value("environmental_questionnaire", "milirb", "Were you in the military?"),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mil_concept_id,
                            "value_source_value": value_source,
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": None,
                            "unit_source_value": None,
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )

            # Process years in military
            if pd.notna(row.get("yrstb")):
                if is_numeric_value(row["yrstb"]):
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 4073594,
                            "observation_source_value": format_source_value("environmental_questionnaire", "yrstb", "How many years were you in the military?"),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": safe_numeric_value(row["yrstb"]),
                            "value_as_string": None,
                            "value_as_concept_id": None,
                            "value_source_value": format_source_value("environmental_questionnaire", "yrstb", "How many years were you in the military", row["yrstb"], include_interpretation=False),
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": 9448,
                            "unit_source_value": format_source_value("environmental_questionnaire", "yrstb", "years"),
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )
                else:
                    logging.warning(f"Skipping non-numeric years in military value for person_id {person_id}: {row['yrstb']}")

            # Process head injury
            if row.get("headrb") == 1 or row.get("edrb") == 1:
                source_parts = []
                if row.get("headrb") == 1:
                    source_parts.append(format_source_value("environmental_questionnaire", "headrb", "Admitted to hospital for head injury", 1, "Yes", include_interpretation=False))
                if row.get("edrb") == 1:
                    source_parts.append(format_source_value("environmental_questionnaire", "edrb", "Seen in ED for head injury", 1, "Yes", include_interpretation=False))
                value_source = format_multiple_source_values(source_parts)

                records.append(
                    {
                        "person_id": person_id,
                        "observation_concept_id": 1340204,
                        "observation_source_value": format_source_value("environmental_questionnaire", "headrb", "Head injury (more than one year prior to symptom onset)"),
                        "observation_date": relative_day_to_date(
                            row["Visit_Date"], index_date
                        ),
                        "observation_type_concept_id": 32851,
                        "value_as_number": None,
                        "value_as_string": None,
                        "value_as_concept_id": 375415,
                        "value_source_value": value_source,
                        "qualifier_concept_id": None,
                        "qualifier_source_value": None,
                        "unit_concept_id": None,
                        "unit_source_value": None,
                        "visit_occurrence_id": get_visit_occurrence_id(
                            person_id, row["Visit_Date"]
                        ),
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                    }
                )

            # Process concussions
            if row.get("concussrb") == 1:
                source_parts = []
                source_parts.append(format_source_value("environmental_questionnaire", "concussrb", "History of concussions", 1, "Yes", include_interpretation=False))
                if pd.notna(row.get("concusstb")):
                    source_parts.append(format_source_value("environmental_questionnaire", "concusstb", "Number of concussions", row["concusstb"], include_interpretation=False))

                records.append(
                    {
                        "person_id": person_id,
                        "observation_concept_id": 1340204,
                        "observation_source_value": format_source_value("environmental_questionnaire", "concussrb", "Concussion history"),
                        "observation_date": relative_day_to_date(
                            row["Visit_Date"], index_date
                        ),
                        "observation_type_concept_id": 32851,
                        "value_as_number": None,
                        "value_as_string": None,
                        "value_as_concept_id": 4001336,
                        "value_source_value": format_multiple_source_values(source_parts),
                        "qualifier_concept_id": None,
                        "qualifier_source_value": None,
                        "unit_concept_id": None,
                        "unit_source_value": None,
                        "visit_occurrence_id": get_visit_occurrence_id(
                            person_id, row["Visit_Date"]
                        ),
                        "observation_event_id": None,
                        "obs_event_field_concept_id": None,
                    }
                )

            # Process smoking history
            if pd.notna(row.get("smokerb")):
                smk_concept_id = smokerb_to_concept_id(row["smokerb"])
                if smk_concept_id:
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 3012697,
                            "observation_source_value": format_source_value("environmental_questionnaire", "smokerb", "Have you ever been a smoker?"),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": smk_concept_id,

                            "value_source_value": format_source_value("environmental_questionnaire", "smokerb", "Have you ever been a smoker?", row["smokerb"], "Yes" if row["smokerb"] == 1 else "No", include_interpretation=False),
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": None,
                            "unit_source_value": None,
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )

            # Process pack-years
            if pd.notna(row.get("yrssmktb")) and pd.notna(row.get("smkavgtb")):
                if is_numeric_value(row["yrssmktb"]) and is_numeric_value(row["smkavgtb"]):
                    try:
                        years = float(row["yrssmktb"])
                        packs_per_day = float(row["smkavgtb"])
                        pack_years = years * packs_per_day * 365
                        records.append(
                            {
                                "person_id": person_id,
                                "observation_concept_id": 903650,
                                "observation_source_value": format_source_value("environmental_questionnaire", "yrssmktb", "How many years were you a smoker? How many packs per day (on average)?"),
                                "observation_date": relative_day_to_date(
                                    row["Visit_Date"], index_date
                                ),
                                "observation_type_concept_id": 32851,
                                "value_as_number": pack_years,
                                "value_as_string": None,
                                "value_as_concept_id": None,
                                "value_source_value": format_multiple_source_values([
                                    format_source_value("environmental_questionnaire", "yrssmktb", "How many years were you a smoker?", row["yrssmktb"], include_interpretation=False),
                                    format_source_value("environmental_questionnaire", "smkavgtb", "Average packs per day", row["smkavgtb"], include_interpretation=False)
                                ]),
                                "qualifier_concept_id": None,
                                "qualifier_source_value": None,
                                "unit_concept_id": None,
                                "unit_source_value": None,
                                "visit_occurrence_id": get_visit_occurrence_id(
                                    person_id, row["Visit_Date"]
                                ),
                                "observation_event_id": None,
                                "obs_event_field_concept_id": None,
                            }
                        )
                    except (ValueError, TypeError) as e:
                        logging.warning(
                            f"Could not calculate pack-years for person_id {person_id}: {str(e)}"
                        )
                else:
                    logging.warning(f"Skipping non-numeric pack-years values for person_id {person_id}: yrssmktb={row['yrssmktb']}, smkavgtb={row['smkavgtb']}")

            # Process alcohol consumption
            if pd.notna(row.get("driavgtb")):
                if is_numeric_value(row["driavgtb"]):
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": 3043872,
                            "observation_source_value": format_source_value("environmental_questionnaire", "driavgtb", "In the 10 years prior to your diagnosis, approximately how much alcohol did you drink"),
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": safe_numeric_value(row["driavgtb"]),
                            "value_as_string": None,
                            "value_as_concept_id": None,
                            "value_source_value": format_source_value("environmental_questionnaire", "driavgtb", "In the 10 years prior to your diagnosis, approximately how much alcohol did you drink", row["driavgtb"], include_interpretation=False),
                            "qualifier_concept_id": None,
                            "qualifier_source_value": None,
                            "unit_concept_id": 44777559,
                            "unit_source_value": format_source_value("environmental_questionnaire", "driavgtb", "drinks per week"),
                            "visit_occurrence_id": get_visit_occurrence_id(
                                person_id, row["Visit_Date"]
                            ),
                            "observation_event_id": None,
                            "obs_event_field_concept_id": None,
                        }
                    )
                else:
                    logging.warning(f"Skipping non-numeric alcohol consumption value for person_id {person_id}: {row['driavgtb']}")

        # Create DataFrame from records with specified column order
        column_order = [
            "person_id",
            "observation_concept_id",
            "observation_source_value",
            "observation_date",
            "observation_type_concept_id",
            "value_as_number",
            "value_as_string",
            "value_as_concept_id",
            "value_source_value",
            "qualifier_concept_id",
            "qualifier_source_value",
            "unit_concept_id",
            "unit_source_value",
            "visit_occurrence_id",
            "observation_event_id",
            "obs_event_field_concept_id",
        ]

        # Remove any duplicate rows that might have been created
        output_data = pd.DataFrame(records)[column_order].drop_duplicates()
        logging.info(f"Created output DataFrame with {len(output_data)} rows")

        # Check for missing concept IDs only for rows that should have them
        # (i.e., not for rows with numeric values)
        concept_rows = output_data[output_data["value_as_number"].isna()]
        check_missing_concept_ids(
            concept_rows, ["observation_concept_id", "value_as_concept_id"]
        )

        # Copy the fixed concept rows back to the main dataframe
        for col in ["value_as_concept_id"]:
            output_data.loc[concept_rows.index, col] = concept_rows[col]

        # Save output
        output_path = "processed_source/environmental_questionnaire--observation.csv"
        output_data.to_csv(output_path, index=False)
        logging.info(f"Saved output to {output_path}")

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

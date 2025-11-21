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

# Ensure only the directories exist
Path("source_tables").mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    filename=os.path.join("logs", "aalshxfx--observation.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Load concept mapping
concept_csv_path = os.path.join("source_tables", "omop_tables", "concept.csv")
concept_df = pd.read_csv(concept_csv_path, dtype={"concept_id": str})
concept_id_to_name = dict(zip(concept_df["concept_id"], concept_df["concept_name"]))

# Load subject group mapping
subjects_csv_path = os.path.join("source_tables", "subjects.csv")
subjects_df = pd.read_csv(subjects_csv_path, dtype={"Participant_ID": str, "subject_group_id": str})
subject_group_map = dict(zip(subjects_df["Participant_ID"], subjects_df["subject_group_id"]))

# Add subject group interpretation mapping
subject_group_interpretation = {
    "1": "ALS",
    "17": "Non-ALS MND",
}

# Mapping of source variables to their corresponding concept IDs and names
SITE_MAPPINGS = {
    "hxgen": {
        "concept_id": 4082829,
        "concept_name": "Nonspecific site",
        "source_value": "Generalized",
    },
    "hxblb": {
        "concept_id": "bulbar_dys",
        "concept_name": "Bulbar dysfunction",
        "source_value": "Bulbar",
    },
    "hxblbsch": {
        "concept_id": 4047485,
        "concept_name": "Speech dysfunction",
        "source_value": "Speech",
    },
    "hxblbsw": {
        "concept_id": 4125274,
        "concept_name": "Difficulty swallowing",
        "source_value": "Swallowing",
    },
    "hxax": {"concept_id": 4086896, "concept_name": "Axial", "source_value": "Axial"},
    "hxaxnk": {
        "concept_id": 4260843,
        "concept_name": "Neck structure",
        "source_value": "Neck",
    },
    "hxaxtr": {
        "concept_id": 4042529,
        "concept_name": "Trunk structure",
        "source_value": "Trunk",
    },
    "hxaxtrrp": {
        "concept_id": 4156081,
        "concept_name": "Respiratory tract structure",
        "source_value": "Respiratory",
    },
    "hxli": {
        "concept_id": 4282006,
        "concept_name": "Limb structure",
        "source_value": "Limb",
    },
    "hxliu": {
        "concept_id": 4200396,
        "concept_name": "Upper limb structure",
        "source_value": "Upper",
    },
    "hxliul": {
        "concept_id": 4215746,
        "concept_name": "Structure of left upper limb",
        "source_value": "Left Upper",
    },
    "hxliur": {
        "concept_id": 4286959,
        "concept_name": "Structure of right upper limb",
        "source_value": "Right Upper",
    },
    "hxliuhnd": {
        "concept_id": [4309650, 4302584],
        "concept_name": ["Structure of left hand", "Structure of right hand"],
        "source_value": ["Left Hand/fingers", "Right Hand/fingers"],
    },
    "hxliuarm": {
        "concept_id": [4215746, 4286959],
        "concept_name": [
            "Structure of left upper limb",
            "Structure of right upper limb",
        ],
        "source_value": ["Left Arm", "Right Arm"],
    },
    "hxlil": {
        "concept_id": 4267861,
        "concept_name": "Lower limb structure",
        "source_value": "Lower",
    },
    "hxlill": {
        "concept_id": 4136825,
        "concept_name": "Structure of left lower limb",
        "source_value": "Left Lower",
    },
    "hxlilr": {
        "concept_id": 4268743,
        "concept_name": "Structure of right lower limb",
        "source_value": "Right Lower",
    },
    "hxlilft": {
        "concept_id": [4320144, 4298982],
        "concept_name": ["Structure of left foot", "Structure of right foot"],
        "source_value": ["Left Ankle/foot/toes", "Right Ankle/foot/toes"],
    },
    "hxlilleg": {
        "concept_id": [4136825, 4268743],
        "concept_name": [
            "Structure of left lower limb",
            "Structure of right lower limb",
        ],
        "source_value": ["Left Leg", "Right Leg"],
    },
}

# Update SITE_MAPPINGS['hxblb'] to use concept_id and concept_name from concept.csv
SITE_MAPPINGS["hxblb"]["concept_id"] = "2000002017"
SITE_MAPPINGS["hxblb"]["concept_name"] = concept_id_to_name["2000002017"]


def check_limb_combination(row, side_var, part_vars):
    """
    Check if a limb combination is valid (e.g., left upper + hand/arm)
    Args:
        row: The data row
        side_var: The side variable (e.g., 'hxliul' for left upper)
        part_vars: List of part variables to check (e.g., ['hxliuhnd', 'hxliuarm'])
    Returns:
        bool: True if the combination is valid
    """
    if row.get(side_var) != 1:
        return False
    return any(row.get(part_var) == 1 for part_var in part_vars)


# Formatting function for *_source_value fields

def format_source_value(table, var, var_interpretation, value, value_interpretation):
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


def main():
    try:
        # Read source data
        source_data = pd.read_csv(os.path.join("source_tables", "aalshxfx.csv"))
        logging.info(f"Read source data with {len(source_data)} rows")

        # Initialize list to store records
        records = []

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]
            subject_group_id = subject_group_map.get(person_id)
            if subject_group_id not in ("1", "17"):
                continue  # Only process ALS or MND

            # Determine concept for anatomical site of symptom onset
            if subject_group_id == "1":
                site_concept_id = "2000000396"
            elif subject_group_id == "17":
                site_concept_id = "2000002018"

            # Process each site variable
            for site_var, mapping in SITE_MAPPINGS.items():
                # Skip specific limb parts that need combination checks
                if site_var in ["hxliuhnd", "hxliuarm", "hxlilft", "hxlilleg"]:
                    continue

                # Only create entry if value is 1
                if row.get(site_var) == 1:
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+{site_var} (Site of onset)"
                    if isinstance(mapping["concept_id"], list):
                        for i in range(len(mapping["concept_id"])):
                            value_source_value = f"aalshxfx+{site_var} ({mapping['source_value'][i]}): 1 (Yes)"
                            records.append(
                                {
                                    "person_id": person_id,
                                    "observation_concept_id": site_concept_id,
                                    "observation_source_value": obs_source_value,
                                    "observation_date": relative_day_to_date(
                                        row["Visit_Date"], index_date
                                    ),
                                    "observation_type_concept_id": 32851,
                                    "value_as_number": None,
                                    "value_as_string": None,
                                    "value_as_concept_id": mapping["concept_id"][i],
                                    "value_source_value": value_source_value,
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
                    else:
                        value_source_value = f"aalshxfx+{site_var} ({mapping['source_value']}): 1 (Yes)"
                        records.append(
                            {
                                "person_id": person_id,
                                "observation_concept_id": site_concept_id,
                                "observation_source_value": obs_source_value,
                                "observation_date": relative_day_to_date(
                                    row["Visit_Date"], index_date
                                ),
                                "observation_type_concept_id": 32851,
                                "value_as_number": None,
                                "value_as_string": None,
                                "value_as_concept_id": mapping["concept_id"],
                                "value_source_value": value_source_value,
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

            # Handle upper limb combinations
            if check_limb_combination(row, "hxliul", ["hxliuhnd", "hxliuarm"]):
                if row.get("hxliuhnd") == 1:
                    mapping = SITE_MAPPINGS["hxliuhnd"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxliuhnd (Site of onset)"
                    value_source_value = f"aalshxfx+hxliuhnd ({mapping['source_value'][0]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][0],
                            "value_source_value": value_source_value,
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
                if row.get("hxliuarm") == 1:
                    mapping = SITE_MAPPINGS["hxliuarm"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxliuarm (Site of onset)"
                    value_source_value = f"aalshxfx+hxliuarm ({mapping['source_value'][0]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][0],
                            "value_source_value": value_source_value,
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

            if check_limb_combination(row, "hxliur", ["hxliuhnd", "hxliuarm"]):
                if row.get("hxliuhnd") == 1:
                    mapping = SITE_MAPPINGS["hxliuhnd"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxliuhnd (Site of onset)"
                    value_source_value = f"aalshxfx+hxliuhnd ({mapping['source_value'][1]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][1],
                            "value_source_value": value_source_value,
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
                if row.get("hxliuarm") == 1:
                    mapping = SITE_MAPPINGS["hxliuarm"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxliuarm (Site of onset)"
                    value_source_value = f"aalshxfx+hxliuarm ({mapping['source_value'][1]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][1],
                            "value_source_value": value_source_value,
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

            # Handle lower limb combinations
            if check_limb_combination(row, "hxlill", ["hxlilft", "hxlilleg"]):
                if row.get("hxlilft") == 1:
                    mapping = SITE_MAPPINGS["hxlilft"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxlilft (Site of onset)"
                    value_source_value = f"aalshxfx+hxlilft ({mapping['source_value'][0]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][0],
                            "value_source_value": value_source_value,
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
                if row.get("hxlilleg") == 1:
                    mapping = SITE_MAPPINGS["hxlilleg"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxlilleg (Site of onset)"
                    value_source_value = f"aalshxfx+hxlilleg ({mapping['source_value'][0]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][0],
                            "value_source_value": value_source_value,
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

            if check_limb_combination(row, "hxlilr", ["hxlilft", "hxlilleg"]):
                if row.get("hxlilft") == 1:
                    mapping = SITE_MAPPINGS["hxlilft"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxlilft (Site of onset)"
                    value_source_value = f"aalshxfx+hxlilft ({mapping['source_value'][1]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][1],
                            "value_source_value": value_source_value,
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
                if row.get("hxlilleg") == 1:
                    mapping = SITE_MAPPINGS["hxlilleg"]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    obs_source_value = f"{group_part} | aalshxfx+hxlilleg (Site of onset)"
                    value_source_value = f"aalshxfx+hxlilleg ({mapping['source_value'][1]}): 1 (Yes)"
                    records.append(
                        {
                            "person_id": person_id,
                            "observation_concept_id": site_concept_id,
                            "observation_source_value": obs_source_value,
                            "observation_date": relative_day_to_date(
                                row["Visit_Date"], index_date
                            ),
                            "observation_type_concept_id": 32851,
                            "value_as_number": None,
                            "value_as_string": None,
                            "value_as_concept_id": mapping["concept_id"][1],
                            "value_source_value": value_source_value,
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

            # Handle Other case - create exactly one entry when hxot is 1
            if row.get("hxot") == 1:
                # Get the text from hxotsp if it exists
                value_source_value = "aalshxfx+hxot: Other"
                if pd.notna(row.get("hxotsp")):
                    value_source_value = f"aalshxfx+hxot: Other: {str(row['hxotsp']).strip()}"

                # Create single Other entry
                group_interp = subject_group_interpretation.get(subject_group_id, "")
                if group_interp:
                    group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                else:
                    group_part = f"subjects+subject_group_id: {subject_group_id}"
                obs_source_value = f"{group_part} | aalshxfx+hxot (Site of onset)"
                records.append(
                    {
                        "person_id": person_id,
                        "observation_concept_id": site_concept_id,
                        "observation_source_value": obs_source_value,
                        "observation_date": relative_day_to_date(
                            row["Visit_Date"], index_date
                        ),
                        "observation_type_concept_id": 32851,
                        "value_as_number": None,
                        "value_as_string": None,
                        "value_as_concept_id": 9177,
                        "value_source_value": value_source_value,
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

        # Check for missing concept IDs
        check_missing_concept_ids(
            output_data, ["observation_concept_id", "value_as_concept_id"]
        )

        # Save output
        output_path = os.path.join("processed_source", "aalshxfx--observation.csv")
        output_data.to_csv(output_path, index=False)
        logging.info(f"Saved output to {output_path}")

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

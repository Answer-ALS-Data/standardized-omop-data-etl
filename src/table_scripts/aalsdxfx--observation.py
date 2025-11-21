import pandas as pd
import logging
from datetime import datetime
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)

# Set up logging
logging.basicConfig(
    filename="logs/aalsdxfx--observation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def aalsdx1_to_observation_value_as_concept_id(value):
    """Convert alsdx1 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx1_to_observation_value_source_value(value):
    """Convert alsdx1 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx2_to_observation_value_as_concept_id(value):
    """Convert alsdx2 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx2_to_observation_value_source_value(value):
    """Convert alsdx2 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx3_to_observation_value_as_concept_id(value):
    """Convert alsdx3 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx3_to_observation_value_source_value(value):
    """Convert alsdx3 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


# Load concept.csv for concept lookups
def load_concept_lookup():
    concept_df = pd.read_csv("source_tables/omop_tables/concept.csv", dtype=str)
    concept_id_to_name = dict(zip(concept_df["concept_id"].astype(int), concept_df["concept_name"]))
    return concept_id_to_name

CONCEPT_ID_TO_NAME = load_concept_lookup()


def elescrlr_to_observation_value_as_concept_id(value):
    """Convert elescrlr value to concept ID, using concept.csv if possible"""
    mapping = {
        1: 2000000062,  # Suspected
        2: 2000000058,  # Possible
        3: 2000000060,  # Probable Laboratory Supported
        4: 2000000059,  # Probable
        5: 2000000057,  # Definite
    }
    concept_id = mapping.get(value, 0)
    # Only return if present in concept.csv, else fallback to hardcoded
    if concept_id in CONCEPT_ID_TO_NAME:
        return concept_id
    return concept_id  # fallback (could be 0)


def elescrlr_to_observation_value_source_value(value):
    """Convert elescrlr value to source value"""
    mapping = {
        1: "Suspected",
        2: "Possible",
        3: "Probable Laboratory Supported",
        4: "Probable",
        5: "Definite",
    }
    return mapping.get(value, "Unknown")


def clinical_indicator_to_observation_value_as_concept_id(value):
    """Convert clinical indicator value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def clinical_indicator_to_observation_value_source_value(value):
    """Convert clinical indicator value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def emg_indicator_to_observation_value_as_concept_id(value):
    """Convert EMG indicator value to concept ID"""
    mapping = {
        1: 45877994,  # Yes (Denervation)
        2: 45878245,  # No (No Denervation)
        90: 45881531,  # Not assessed (Not Done)
    }
    return mapping.get(value, 0)


def emg_indicator_to_observation_value_source_value(value):
    """Convert EMG indicator value to source value"""
    mapping = {
        1: "Denervation",
        2: "No Denervation",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx1_to_observation_value_as_concept_name(value):
    """Convert alsdx1 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def aalsdx2_to_observation_value_as_concept_name(value):
    """Convert alsdx2 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def aalsdx3_to_observation_value_as_concept_name(value):
    """Convert alsdx3 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def elescrlr_to_observation_value_as_concept_name(value):
    """Convert elescrlr value to concept name using concept.csv only"""
    value_to_concept_id = {
        1: 2000000062,  # Suspected
        2: 2000000058,  # Possible
        3: 2000000060,  # Probable Laboratory Supported
        4: 2000000059,  # Probable
        5: 2000000057,  # Definite
    }
    concept_id = value_to_concept_id.get(value)
    if concept_id is not None:
        return CONCEPT_ID_TO_NAME[concept_id]
    return "Unknown"


def clinical_indicator_to_observation_value_as_concept_name(value):
    """Convert clinical indicator value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def emg_indicator_to_observation_value_as_concept_name(value):
    """Convert EMG indicator value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def format_source_value(table, var, var_interpretation, value, value_interpretation):
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


def process_aalsdxfx_to_observation(source_file, index_date):
    """
    Process AALSDXFX data into OMOP observation format

    Args:
        source_file (str): Path to the source AALSDXFX.csv file
        index_date (datetime): Reference date for relative day calculations
    """
    try:
        # Read source data
        df = pd.read_csv(source_file)
        logging.info(f"Successfully read {len(df)} rows from {source_file}")

        # Initialize empty list to store observations
        observations = []

        # Define observation items and their mappings
        observation_items = [
            {
                "source_column": "alsdx1",
                "concept_id": 2000002000,
                "concept_name": CONCEPT_ID_TO_NAME[2000002000],
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx1",
                "concept_id": 2000002001,
                "concept_name": CONCEPT_ID_TO_NAME[2000002001],
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx1",
                "concept_id": 2000000020,
                "concept_name": CONCEPT_ID_TO_NAME[2000000020],
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx2",
                "concept_id": 2000000021,
                "concept_name": CONCEPT_ID_TO_NAME[2000000021],
                "source_value": "Exclusion by electrophysiological testing of all other processes including conduction block that might explain the underlying signs and symptoms?",
                "value_converter": aalsdx2_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx2_to_observation_value_source_value,
                "concept_name_converter": aalsdx2_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx3",
                "concept_id": 2000000022,
                "concept_name": CONCEPT_ID_TO_NAME[2000000022],
                "source_value": "Exclusion by neuroimaging of other disease processes such as myelopathy or radiculopathy that might explain observed clinical and electrophysiological signs?",
                "value_converter": aalsdx3_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx3_to_observation_value_source_value,
                "concept_name_converter": aalsdx3_to_observation_value_as_concept_name,
            },
            {
                "source_column": "elescrlr",
                "concept_id": 2000000061,
                "concept_name": CONCEPT_ID_TO_NAME[2000000061],
                "source_value": "Revised El Escorial Criteria for ALS",
                "value_converter": elescrlr_to_observation_value_as_concept_id,
                "source_value_converter": elescrlr_to_observation_value_source_value,
                "concept_name_converter": elescrlr_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbcumn",
                "concept_id": 2000000035,
                "concept_name": CONCEPT_ID_TO_NAME[2000000035],
                "source_value": "Bulbar upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "luecumn",
                "concept_id": 2000002002,
                "concept_name": CONCEPT_ID_TO_NAME[2000002002],
                "source_value": "Left upper extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "ruecumn",
                "concept_id": 2000002003,
                "concept_name": CONCEPT_ID_TO_NAME[2000002003],
                "source_value": "Right upper extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkcumn",
                "concept_id": 2000002004,
                "concept_name": CONCEPT_ID_TO_NAME[2000002004],
                "source_value": "Trunk upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "llecumn",
                "concept_id": 2000002005,
                "concept_name": CONCEPT_ID_TO_NAME[2000002005],
                "source_value": "Left lower extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rlecumn",
                "concept_id": 2000002006,
                "concept_name": CONCEPT_ID_TO_NAME[2000002006],
                "source_value": "Right lower extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbclmn",
                "concept_id": 2000000029,
                "concept_name": CONCEPT_ID_TO_NAME[2000000029],
                "source_value": "Bulbar lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lueclmn",
                "concept_id": 2000002007,
                "concept_name": CONCEPT_ID_TO_NAME[2000002007],
                "source_value": "Left upper extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rueclmn",
                "concept_id": 2000002008,
                "concept_name": CONCEPT_ID_TO_NAME[2000002008],
                "source_value": "Right upper extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkclmn",
                "concept_id": 2000002009,
                "concept_name": CONCEPT_ID_TO_NAME[2000002009],
                "source_value": "Trunk lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lleclmn",
                "concept_id": 2000002010,
                "concept_name": CONCEPT_ID_TO_NAME[2000002010],
                "source_value": "Left lower extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rleclmn",
                "concept_id": 2000002011,
                "concept_name": CONCEPT_ID_TO_NAME[2000002011],
                "source_value": "Right lower extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbelmn",
                "concept_id": 2000000030,
                "concept_name": CONCEPT_ID_TO_NAME[2000000030],
                "source_value": "Bulbar lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lueelmn",
                "concept_id": 2000002012,
                "concept_name": CONCEPT_ID_TO_NAME[2000002012],
                "source_value": "Left upper extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rueelmn",
                "concept_id": 2000002013,
                "concept_name": CONCEPT_ID_TO_NAME[2000002013],
                "source_value": "Right upper extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkelmn",
                "concept_id": 2000002014,
                "concept_name": CONCEPT_ID_TO_NAME[2000002014],
                "source_value": "Trunk lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lleelmn",
                "concept_id": 2000002015,
                "concept_name": CONCEPT_ID_TO_NAME[2000002015],
                "source_value": "Left lower extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rleelmn",
                "concept_id": 2000002016,
                "concept_name": CONCEPT_ID_TO_NAME[2000002016],
                "source_value": "Right lower extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
        ]

        # Process each row in the source data
        for _, row in df.iterrows():
            person_id = row["Participant_ID"]

            # Handle NaN values in alsdxdt
            if pd.isna(row["alsdxdt"]):
                visit_occurrence_id = None
            else:
                visit_occurrence_id = get_visit_occurrence_id(
                    person_id, int(row["Visit_Date"])
                )

            # Process each observation item
            for item in observation_items:
                if item["source_column"] in row and pd.notna(
                    row[item["source_column"]]
                ):
                    value = int(row[item["source_column"]])
                    value_as_concept_id = item["value_converter"](value)
                    value_source_value = item["source_value_converter"](value)

                    # Calculate observation date from alsdxdt
                    observation_date = None
                    if pd.notna(row["alsdxdt"]):
                        relative_day = int(row["alsdxdt"])
                        observation_date = relative_day_to_date(
                            relative_day, index_date
                        )
                        logging.info(
                            f"Converting alsdxdt: {relative_day} to date: {observation_date}"
                        )

                    # observation_source_value: table+source_column (interpretation) - no value indicates presence of variable led to entry
                    obs_source_value = f"aalsdxfx+{item['source_column']} ({item['source_value']})"
                    # value_source_value: table+var: value (interpretation), omit parentheses if interpretation is missing or same as value
                    var_name = item["source_column"]
                    if value_source_value and str(value_source_value).strip().lower() != str(value).strip().lower():
                        val_source_value = f"aalsdxfx+{var_name}: {value} ({value_source_value})"
                    else:
                        val_source_value = f"aalsdxfx+{var_name}: {value}"
                    qualifier_source_value = ""
                    unit_source_value = ""

                    observation = {
                        "person_id": person_id,
                        "observation_concept_id": item["concept_id"],
                        "observation_source_value": obs_source_value,
                        "observation_date": observation_date,
                        "observation_type_concept_id": 32851,  # Healthcare professional filled survey
                        "value_as_number": "",  # Intentionally empty
                        "value_as_string": "",  # Intentionally empty
                        "value_as_concept_id": value_as_concept_id,
                        "value_source_value": val_source_value,
                        "qualifier_concept_id": "",  # Intentionally empty
                        "qualifier_source_value": qualifier_source_value,
                        "unit_concept_id": "",  # Intentionally empty
                        "unit_source_value": unit_source_value,
                        "visit_occurrence_id": visit_occurrence_id,
                        "observation_event_id": "",  # Intentionally empty
                        "obs_event_field_concept_id": "",  # Intentionally empty
                    }
                    observations.append(observation)

        # Create DataFrame from observations
        result_df = pd.DataFrame(observations)

        # Check for missing concept IDs only on relevant columns
        concept_columns = [
            "observation_concept_id",
            "value_as_concept_id",
        ]
        result_df_concepts = result_df[concept_columns].copy()
        result_df_concepts = check_missing_concept_ids(result_df_concepts)

        # Update only the concept columns in the original DataFrame
        for col in concept_columns:
            result_df[col] = result_df_concepts[col]

        # Save to output file
        output_file = "processed_source/aalsdxfx--observation.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(
            f"Successfully saved {len(result_df)} observations to {output_file}"
        )

        return result_df

    except Exception as e:
        logging.error(f"Error processing AALSDXFX data: {str(e)}")
        raise


if __name__ == "__main__":
    # Set index date (example: 2016-01-01)
    index_date = datetime(2016, 1, 1)

    # Process the data
    source_file = "source_tables/aalsdxfx.csv"
    process_aalsdxfx_to_observation(source_file, index_date)

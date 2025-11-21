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
    filename="logs/alsfrs_r--observation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def process_alsfrs_r_to_observation(source_file, index_date):
    """
    Process ALSFRS_R data into OMOP observation format

    Args:
        source_file (str): Path to the source ALSFRS_R.csv file
        index_date (datetime): Reference date for relative day calculations
    """
    try:
        # Read source data - ensure numeric columns are read as integers
        numeric_cols = [
            "alsfrs1",
            "alsfrs2",
            "alsfrs3",
            "alsfrs4",
            "alsfrs5a",
            "alsfrs5b",
            "alsfrs6",
            "alsfrs7",
            "alsfrs8",
            "alsfrs9",
            "alsfrsr1",
            "alsfrsr2",
            "alsfrsr3",
            "alsfrst",
            "alsfrsdt",
        ]
        df = pd.read_csv(source_file, dtype={col: "Int64" for col in numeric_cols})
        logging.info(f"Successfully read {len(df)} rows from {source_file}")

        # Initialize empty list to store observations
        observations = []

        # Define ALSFRS-R items and their corresponding concept IDs and value mappings
        alsfrs_items = {
            "alsfrs1": {
                "concept_id": 42529071,
                "name": "Speech [ALSFRS-R]",
                "question": "Speech",
                "variable_meaning": "speech function assessment",
                "values": {
                    4: "Normal speech processes",
                    3: "Detectable speech disturbances",
                    2: "Intelligible with repeating",
                    1: "Speech combined with nonvocal communication",
                    0: "Loss of useful speech",
                },
            },
            "alsfrs2": {
                "concept_id": 42529072,
                "name": "Salivation [ALSFRS-R]",
                "question": "Salivation",
                "variable_meaning": "saliva control assessment",
                "values": {
                    4: "Normal",
                    3: "Slight but definite excess of saliva in mouth; may have nighttime drooling",
                    2: "Moderately excessive saliva; may have minimal drooling",
                    1: "Marked excess of saliva with some drooling",
                    0: "Marked drooling; requires constant tissue or handkerchief",
                },
            },
            "alsfrs3": {
                "concept_id": 42529073,
                "name": "Swallowing [ALSFRS-R]",
                "question": "Swallowing",
                "variable_meaning": "swallowing function assessment",
                "values": {
                    4: "Normal eating habits",
                    3: "Early eating problems – occasional choking",
                    2: "Dietary consistency changes",
                    1: "Needs supplemental tube feeding",
                    0: "NPO (exclusively parenteral or enteral feeding)",
                },
            },
            "alsfrs4": {
                "concept_id": 42529074,
                "name": "Handwriting [ALSFRS-R]",
                "question": "Handwriting",
                "variable_meaning": "handwriting ability assessment",
                "values": {
                    4: "Normal",
                    3: "Slow or sloppy; all words are legible",
                    2: "Not all words are legible",
                    1: "Able to grip pen but unable to write",
                    0: "Unable to grip pen",
                },
            },
            "alsfrs5a": {
                "concept_id": 42529075,
                "name": "Cutting food and handling utensils (patients without gastrostomy) [ALSFRS-R]",
                "question": "Cutting Food (no gastrostomy)",
                "variable_meaning": "food preparation ability for patients without gastrostomy",
                "values": {
                    4: "Normal",
                    3: "Somewhat slow and clumsy, but no help needed",
                    2: "Can cut most foods, although clumsy and slow; some help needed",
                    1: "Food must be cut by someone, but can still feed slowly",
                    0: "Needs to be fed",
                },
            },
            "alsfrs5b": {
                "concept_id": 42529076,
                "name": "Cutting food and handling utensils (patients with gastrostomy) [ALSFRS-R]",
                "question": "Cutting Food (gastrostomy)",
                "variable_meaning": "food preparation ability for patients with gastrostomy",
                "values": {
                    4: "Normal",
                    3: "Clumsy but able to perform all manipulations independently",
                    2: "Some help needed with closures and fasteners",
                    1: "Provides minimal assistance to caregivers",
                    0: "Unable to perform any aspect of task",
                },
            },
            "alsfrs6": {
                "concept_id": 42529077,
                "name": "Dressing and hygiene [ALSFRS-R]",
                "question": "Dressing and Hygiene",
                "variable_meaning": "self-care ability assessment",
                "values": {
                    4: "Normal function",
                    3: "Independent and complete self-care with effort or decreased efficiency",
                    2: "Intermittent assistance or substitute methods",
                    1: "Needs attendant for self-care",
                    0: "Total dependence",
                },
            },
            "alsfrs7": {
                "concept_id": 42529078,
                "name": "Turning in bed and adjusting bed clothes [ALSFRS-R]",
                "question": "Turning in Bed",
                "variable_meaning": "bed mobility assessment",
                "values": {
                    4: "Normal",
                    3: "Somewhat slow and clumsy, but no help needed",
                    2: "Can turn alone or adjust sheets, but with great difficulty",
                    1: "Can initiate, but not turn or adjust sheets alone",
                    0: "Helpless",
                },
            },
            "alsfrs8": {
                "concept_id": 42529079,
                "name": "Walking [ALSFRS-R]",
                "question": "Walking",
                "variable_meaning": "ambulation ability assessment",
                "values": {
                    4: "Normal",
                    3: "Early ambulation difficulties",
                    2: "Walks with assistance",
                    1: "Nonambulatory functional movement only",
                    0: "No purposeful leg movement",
                },
            },
            "alsfrs9": {
                "concept_id": 42529080,
                "name": "Climbing stairs [ALSFRS-R]",
                "question": "Climbing Stairs",
                "variable_meaning": "stair climbing ability assessment",
                "values": {
                    4: "Normal",
                    3: "Slow",
                    2: "Mild unsteadiness or fatigue",
                    1: "Needs assistance",
                    0: "Cannot do",
                },
            },
            "alsfrsr1": {
                "concept_id": 42529081,
                "name": "Dyspnea [ALSFRS-R]",
                "question": "R-1 Dyspnea",
                "variable_meaning": "breathing difficulty assessment",
                "values": {
                    4: "None",
                    3: "Occurs when walking",
                    2: "Occurs with one or more of: eating, bathing, dressing",
                    1: "Occurs at rest, difficulty breathing when either sitting or lying",
                    0: "Significant difficulty, considering mechanical respiratory support",
                },
            },
            "alsfrsr2": {
                "concept_id": 42529082,
                "name": "Orthopnea [ALSFRS-R]",
                "question": "R-2 Orthopnea",
                "variable_meaning": "sleep-related breathing difficulty assessment",
                "values": {
                    4: "None",
                    3: "Some difficulty sleeping due to shortness of breath, not using more than 2 pillows",
                    2: "Needs extra pillows (>2) to sleep",
                    1: "Can only sleep sitting up",
                    0: "Unable to sleep without mechanical assistance",
                },
            },
            "alsfrsr3": {
                "concept_id": 42529083,
                "name": "Respiratory insufficiency [ALSFRS-R]",
                "question": "R-3 Respiratory Insufficiency",
                "variable_meaning": "respiratory support requirement assessment",
                "values": {
                    4: "None",
                    3: "Intermittent use of NIPPV",
                    2: "Continuous use of NIPPV at night",
                    1: "Continuous use of NIPPV day and night",
                    0: "Invasive mechanical ventilation (intubation/tracheostomy)",
                },
            },
            "alsfrst": {
                "concept_id": 42529084,
                "name": "Total score [ALSFRS-R]",
                "question": "Total score",
                "variable_meaning": "overall functional assessment score",
                "values": {},  # Total score doesn't have specific value mappings
            },
        }

        # Process each row in the source data
        for _, row in df.iterrows():
            person_id = row["Participant_ID"]
            visit_date = relative_day_to_date(row["alsfrsdt"], index_date)
            # Use raw date value for visit_occurrence_id
            visit_occurrence_id = f"{person_id}_{row['alsfrsdt']}"

            # Process each ALSFRS-R item
            for item, concept_info in alsfrs_items.items():
                if item in row and pd.notna(row[item]):
                    # Convert to integer
                    value = int(row[item])
                    
                    # Format value source using new format: table+var: value (interpretation)
                    value_description = concept_info["values"].get(value, "")
                    if value_description:
                        value_source = f"alsfrs_r+{item}: {value} ({value_description})"
                    else:
                        value_source = f"alsfrs_r+{item}: {value}"

                    observation = {
                        "person_id": person_id,
                        "observation_concept_id": concept_info["concept_id"],
                        "observation_source_value": f"alsfrs_r+{item} ({concept_info['variable_meaning']})",
                        "observation_date": visit_date,
                        "observation_type_concept_id": 32851,  # Healthcare professional filled survey
                        "value_as_number": value,
                        "value_as_string": "",
                        "value_as_concept_id": "",
                        "value_source_value": value_source,
                        "qualifier_concept_id": "",
                        "qualifier_source_value": "",
                        "unit_concept_id": "",
                        "unit_source_value": "",
                        "visit_occurrence_id": visit_occurrence_id,
                        "observation_event_id": "",
                        "obs_event_field_concept_id": "",
                    }
                    observations.append(observation)

        # Create DataFrame from observations
        result_df = pd.DataFrame(observations)

        # Check for missing concept IDs
        result_df = check_missing_concept_ids(result_df)

        # Ensure specified fields are blank
        blank_fields = [
            "value_as_concept_id",
            "qualifier_concept_id",
            "unit_concept_id",
        ]
        for field in blank_fields:
            result_df[field] = ""

        # Ensure value_as_number is integer
        result_df["value_as_number"] = result_df["value_as_number"].astype("Int64")

        # Save to output file
        output_file = "processed_source/alsfrs_r--observation.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(
            f"Successfully saved {len(result_df)} observations to {output_file}"
        )

        return result_df

    except Exception as e:
        logging.error(f"Error processing ALSFRS_R data: {str(e)}")
        raise


if __name__ == "__main__":
    # Set index date (example: 2016-01-01)
    index_date = datetime(2016, 1, 1)

    # Process the data
    source_file = "source_tables/alsfrs_r.csv"
    process_alsfrs_r_to_observation(source_file, index_date)

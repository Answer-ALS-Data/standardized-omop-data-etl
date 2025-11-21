import pandas as pd
import logging
from datetime import datetime
from difflib import SequenceMatcher
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)
from pathlib import Path

# Set up logging
logging.basicConfig(
    filename="logs/vital_signs--measurement.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def convert_fahrenheit_to_celsius(fahrenheit):
    """Convert Fahrenheit to Celsius"""
    return (fahrenheit - 32) * 5/9


def convert_pounds_to_kg(pounds):
    """Convert pounds to kilograms"""
    return pounds * 0.453592


def convert_inches_to_cm(inches):
    """Convert inches to centimeters"""
    return inches * 2.54


def safe_convert_to_float(value, field_name, person_id):
    """
    Safely convert a value to float, handling non-numerical characters.
    Returns None if conversion fails, which will cause the row to be skipped.
    """
    if pd.isna(value):
        return None
    
    # Convert to string and clean
    value_str = str(value).strip()
    
    # Remove common non-numerical characters that might be in the data
    # Keep only digits, decimal points, and minus signs
    cleaned_value = ''.join(char for char in value_str if char.isdigit() or char in '.-')
    
    # Handle edge cases
    if not cleaned_value or cleaned_value == '.' or cleaned_value == '-':
        logging.warning(f"Invalid {field_name} value '{value}' for person_id {person_id} - skipping")
        return None
    
    try:
        return float(cleaned_value)
    except ValueError:
        logging.warning(f"Could not convert {field_name} value '{value}' to float for person_id {person_id} - skipping")
        return None


def is_similar_to_temporal(text):
    """
    Check if the given text is similar to 'Temporal' using fuzzy string matching.
    Returns True if the similarity ratio is above 0.8 (adjustable threshold).
    Case insensitive comparison.
    """
    if not isinstance(text, str):
        return False

    # Convert to lowercase for case-insensitive comparison
    text = text.lower()
    target = "temporal"

    # Direct match check
    if target in text:
        return True

    # Fuzzy match check
    ratio = SequenceMatcher(None, text, target).ratio()
    return ratio > 0.8


def vital_signs_to_measurement(source_df, index_date_str):
    """
    Transform vital signs data into OMOP measurement table format.

    Args:
        source_df (pd.DataFrame): Source data from Vital_Signs.csv
        index_date_str (str): Index date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: Transformed data in OMOP measurement table format
    """
    logging.info("Starting vital signs to measurement transformation")
    logging.info(f"Source columns: {source_df.columns.tolist()}")

    # Convert index date string to datetime
    index_date = datetime.strptime(index_date_str, "%Y-%m-%d")

    # Initialize empty list to store transformed rows
    transformed_rows = []

    # Define the mapping of source variables to measurement concepts and meanings
    vital_sign_mappings = {
        "temp": {
            "temprt_mapping": {1: "Axillary", 2: "Oral", 3: "Rectal", 4: "Tympanic"},
            "concept_ids": {
                "Axillary": 4188706,
                "Oral": 3006322,
                "Rectal": 3022060,
                "Tympanic": 4215364,
                "Temporal": 46235152,
            },
            "unit_concept_id": 586323,  # Celsius (always convert to metric)
        },
        "bpsys": {
            "concept_id": 4152194,
            "unit_concept_id": 37546954,  # mmHg
            "value_as_concept_ids": {
                1: 4060833,  # Standing
                2: 4060834,  # Sitting
                3: 4060832,  # Supine
            },
        },
        "bpdias": {
            "concept_id": 4154790,
            "unit_concept_id": 37546954,  # mmHg
            "value_as_concept_ids": {
                1: 4060833,  # Standing
                2: 4060834,  # Sitting
                3: 4060832,  # Supine
            },
        },
        "hr": {"concept_id": 3027018, "unit_concept_id": 4118124},  # beats/min
        "rr": {"concept_id": 4313591, "unit_concept_id": 4117833},  # breaths/min
        "weight": {
            "concept_id": 3025315,
            "unit_concept_id": 9529,  # kilogram (always convert to metric)
        },
        "height": {
            "concept_id": 3036277,
            "unit_concept_id": 8582,  # centimeter (always convert to metric)
        },
        "bmi": {"concept_id": 3038553, "unit_concept_id": 8523},  # ratio
    }

    # Process each row in the source data
    for _, row in source_df.iterrows():
        person_id = row["Participant_ID"]

        # Calculate visit date using relative_day_to_date and format as YYYY-MM-DD
        # If vsdt is empty, use 1900-01-01 as default
        if pd.isna(row["vsdt"]):
            visit_date = datetime(1900, 1, 1)
            logging.warning(
                f"Using default date 1900-01-01 for person_id {person_id} due to empty vsdt"
            )
        else:
            visit_date = relative_day_to_date(row["vsdt"], index_date)
            if visit_date is None:
                logging.warning(
                    f"Skipping row for person_id {person_id} due to invalid vsdt: {row['vsdt']}"
                )
                continue
        visit_date_str = visit_date.strftime("%Y-%m-%d")

        # Process temperature measurements
        if not pd.isna(row["temp"]):
            logging.info(
                f"Processing temperature for person_id {person_id}: value={row['temp']}, temprt={row.get('temprt')}, temprtsp={row.get('temprtsp', 'N/A')}"
            )

            # First try to get temperature type from temprt integer mapping
            temp_type = None
            if not pd.isna(row.get("temprt")):
                try:
                    temp_type = vital_sign_mappings["temp"]["temprt_mapping"].get(
                        int(row["temprt"])
                    )
                    logging.info(f"Mapped temprt {row['temprt']} to {temp_type}")
                except (ValueError, TypeError):
                    logging.warning(f"Invalid temprt value: {row['temprt']}")

            # If no valid temprt, check temprtsp for Temporal
            if temp_type is None and not pd.isna(row.get("temprtsp")):
                if is_similar_to_temporal(row["temprtsp"]):
                    temp_type = "Temporal"
                    logging.info(
                        f"Found Temporal temperature type (fuzzy match) in temprtsp: {row['temprtsp']}"
                    )

            logging.info(f"Final temp_type: {temp_type}")

            if temp_type in vital_sign_mappings["temp"]["concept_ids"]:
                concept_id = vital_sign_mappings["temp"]["concept_ids"][temp_type]
                temp_value = safe_convert_to_float(row["temp"], "temperature", person_id)
                
                if temp_value is None:
                    continue
                
                original_unit = "F" if row["tempu"] == 1 else "C"
                
                # Convert to Celsius if needed
                conversion_happened = False
                if row["tempu"] == 1:  # Fahrenheit
                    converted_temp = round(convert_fahrenheit_to_celsius(temp_value), 2)
                    conversion_happened = True
                    logging.info(f"Converted temperature from {temp_value}°F to {converted_temp}°C")
                else:  # Celsius or inferred
                    converted_temp = temp_value
                
                # If unit is missing, try to infer from value range
                if pd.isna(row["tempu"]):
                    # Check if value falls in normal Celsius range
                    if 35 <= temp_value <= 40:
                        converted_temp = temp_value
                        original_unit = "C"
                        logging.info(
                            f"Inferred Celsius units for temperature {temp_value} (within 35-40°C range)"
                        )
                    # Check if value falls in normal Fahrenheit range
                    elif 95 <= temp_value <= 104:
                        converted_temp = round(convert_fahrenheit_to_celsius(temp_value), 2)
                        original_unit = "F"
                        conversion_happened = True
                        logging.info(
                            f"Inferred Fahrenheit units for temperature {temp_value} (within 95-104°F range), converted to {converted_temp}°C"
                        )
                    else:
                        logging.warning(
                            f"Could not infer temperature units for value {temp_value} - skipping record"
                        )
                        continue

                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": concept_id,
                    "measurement_source_value": f"vital_signs+temp ({temp_type} Temperature)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": converted_temp,
                    "unit_concept_id": vital_sign_mappings["temp"]["unit_concept_id"],
                    "unit_source_value": f"{original_unit} -> C" if conversion_happened else "C",
                    "value_source_value": f"vital_signs+temp ({temp_type}): {row['temp']}°{original_unit}{' -> ' + str(converted_temp) + '°C' if conversion_happened else ''}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)
            else:
                logging.warning(f"Unrecognized temperature type: {temp_type}")

        # Process blood pressure measurements
        if not pd.isna(row["bpsys"]):
            bpsys_value = safe_convert_to_float(row["bpsys"], "systolic blood pressure", person_id)
            
            if bpsys_value is not None:
                # Handle blood pressure position interpretation
                bppos_value = row.get("bppos")
                position_interpretation = None
                value_as_concept_id = None
                
                if not pd.isna(bppos_value):
                    position_interpretation = (
                        "Standing" if bppos_value == 1
                        else "Sitting" if bppos_value == 2
                        else "Supine" if bppos_value == 3
                        else None
                    )
                    value_as_concept_id = vital_sign_mappings["bpsys"]["value_as_concept_ids"].get(bppos_value)
                
                # Create value_source_value with position interpretation to the left of the value
                if position_interpretation:
                    value_source_value = f"vital_signs+bppos (blood pressure position): {bppos_value} ({position_interpretation}) | vital_signs+bpsys: {row['bpsys']}"
                else:
                    value_source_value = f"vital_signs+bppos (blood pressure position): BLANK | vital_signs+bpsys: {row['bpsys']}"
                
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["bpsys"]["concept_id"],
                    "measurement_source_value": "vital_signs+bpsys (Systolic Blood Pressure)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": bpsys_value,
                    "unit_concept_id": vital_sign_mappings["bpsys"]["unit_concept_id"],
                    "unit_source_value": "mmHG",
                    "value_as_concept_id": value_as_concept_id,
                    "value_source_value": value_source_value,
                    "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
                }
                transformed_rows.append(transformed_row)

        if not pd.isna(row["bpdias"]):
            bpdias_value = safe_convert_to_float(row["bpdias"], "diastolic blood pressure", person_id)
            
            if bpdias_value is not None:
                # Handle blood pressure position interpretation
                bppos_value = row.get("bppos")
                position_interpretation = None
                value_as_concept_id = None

                
                if not pd.isna(bppos_value):
                    position_interpretation = (
                        "Standing" if bppos_value == 1
                        else "Sitting" if bppos_value == 2
                        else "Supine" if bppos_value == 3
                        else None
                    )
                    value_as_concept_id = vital_sign_mappings["bpdias"]["value_as_concept_ids"].get(bppos_value)
                
                # Create value_source_value with position interpretation to the left of the value
                if position_interpretation:
                    value_source_value = f"vital_signs+bppos (blood pressure position): {bppos_value} ({position_interpretation}) | vital_signs+bpdias: {row['bpdias']}"
                else:
                    value_source_value = f"vital_signs+bppos (blood pressure position): BLANK | vital_signs+bpdias: {row['bpdias']}"
                
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["bpdias"]["concept_id"],
                    "measurement_source_value": "vital_signs+bpdias (Diastolic Blood Pressure)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": bpdias_value,
                    "unit_concept_id": vital_sign_mappings["bpdias"]["unit_concept_id"],
                    "unit_source_value": "mmHG",
                    "value_as_concept_id": value_as_concept_id,
                    "value_source_value": value_source_value,
                    "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
                }
                transformed_rows.append(transformed_row)

        # Process heart rate
        if not pd.isna(row["hr"]):
            hr_value = safe_convert_to_float(row["hr"], "heart rate", person_id)
            
            if hr_value is not None:
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["hr"]["concept_id"],
                    "measurement_source_value": "vital_signs+hr (Heart rate)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": hr_value,
                    "unit_concept_id": vital_sign_mappings["hr"]["unit_concept_id"],
                    "unit_source_value": "Beats / min",
                    "value_source_value": f"vital_signs+hr: {row['hr']}",
                    "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
                }
                transformed_rows.append(transformed_row)

        # Process respiratory rate
        if not pd.isna(row["rr"]):
            rr_value = safe_convert_to_float(row["rr"], "respiratory rate", person_id)
            
            if rr_value is not None:
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["rr"]["concept_id"],
                    "measurement_source_value": "vital_signs+rr (Respiratory Rate)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": rr_value,
                    "unit_concept_id": vital_sign_mappings["rr"]["unit_concept_id"],
                    "unit_source_value": "Breaths / min",
                    "value_source_value": f"vital_signs+rr: {row['rr']}",
                    "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
                }
                transformed_rows.append(transformed_row)

        # Process weight
        if not pd.isna(row["weight"]):
            weight_value = safe_convert_to_float(row["weight"], "weight", person_id)
            
            if weight_value is not None:
                original_unit = "lb" if row["weightu"] == 1 else "kg"
                
                # Convert to kilograms if needed
                conversion_happened = False
                if row["weightu"] == 1:  # Pounds
                    converted_weight = round(convert_pounds_to_kg(weight_value), 2)
                    conversion_happened = True
                    logging.info(f"Converted weight from {weight_value} lb to {converted_weight} kg")
                else:  # Kilograms
                    converted_weight = weight_value
                
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["weight"]["concept_id"],
                    "measurement_source_value": "vital_signs+weight (Weight)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": converted_weight,
                    "unit_concept_id": vital_sign_mappings["weight"]["unit_concept_id"],
                    "unit_source_value": f"{original_unit} -> kg" if conversion_happened else "kg",
                    "value_source_value": f"vital_signs+weight: {row['weight']} {original_unit}{' -> ' + str(converted_weight) + ' kg' if conversion_happened else ''}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)

        # Process height
        if not pd.isna(row["height"]):
            height_value = safe_convert_to_float(row["height"], "height", person_id)
            
            if height_value is not None:
                original_unit = "in" if row["heightu"] == 1 else "cm"
                
                # Convert to centimeters if needed
                conversion_happened = False
                if row["heightu"] == 1:  # Inches
                    converted_height = round(convert_inches_to_cm(height_value), 2)
                    conversion_happened = True
                    logging.info(f"Converted height from {height_value} in to {converted_height} cm")
                else:  # Centimeters
                    converted_height = height_value
                
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["height"]["concept_id"],
                    "measurement_source_value": "vital_signs+height (Height)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": converted_height,
                    "unit_concept_id": vital_sign_mappings["height"]["unit_concept_id"],
                    "unit_source_value": f"{original_unit} -> cm" if conversion_happened else "cm",
                    "value_source_value": f"vital_signs+height: {row['height']} {original_unit}{' -> ' + str(converted_height) + ' cm' if conversion_happened else ''}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)

        # Process BMI
        if not pd.isna(row["bmi"]):
            bmi_value = safe_convert_to_float(row["bmi"], "BMI", person_id)
            
            if bmi_value is not None:
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["bmi"]["concept_id"],
                    "measurement_source_value": "vital_signs+bmi (BMI)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": bmi_value,
                    "unit_concept_id": vital_sign_mappings["bmi"]["unit_concept_id"],
                    "unit_source_value": "BMI",
                    "value_source_value": f"vital_signs+bmi: {row['bmi']}",
                    "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
                }
                transformed_rows.append(transformed_row)

    # Create DataFrame from transformed rows
    result_df = pd.DataFrame(transformed_rows)

    # Check for missing concept IDs
    check_missing_concept_ids(result_df, "measurement_concept_id")

    # Ensure all required columns are present
    required_columns = [
        "person_id",
        "measurement_concept_id",
        "measurement_source_value",
        "measurement_date",
        "measurement_type_concept_id",
        "value_as_number",
        "value_as_concept_id",
        "value_source_value",
        "unit_concept_id",
        "unit_source_value",
        "visit_occurrence_id",
    ]

    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = None

    # Reorder columns
    result_df = result_df[required_columns]

    logging.info(
        f"Transformation complete. Created {len(result_df)} measurement records"
    )
    logging.info(
        f"Measurements by type: {result_df['measurement_source_value'].value_counts().to_dict()}"
    )
    return result_df


def main():
    try:
        # Read source data file
        source_df = pd.read_csv(
            "source_tables/vital_signs.csv", dtype={"vsdt": "Int64"}
        )
        logging.info(f"Read {len(source_df)} rows from vital_signs.csv")

        # Set index date to 2016-01-01
        index_date_str = "2016-01-01"

        # Transform data
        result_df = vital_signs_to_measurement(source_df, index_date_str)

        # Save to OMOP tables directory
        output_path = "processed_source/vital_signs--measurement.csv"
        result_df.to_csv(output_path, index=False)
        logging.info(f"Saved transformed data to {output_path}")

    except Exception as e:
        logging.error(f"Error in transformation: {str(e)}")
        raise


if __name__ == "__main__":
    main()

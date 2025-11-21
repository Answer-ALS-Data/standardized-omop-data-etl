import pandas as pd
import logging
import os
from helpers import relative_day_to_year, check_missing_concept_ids

# Set up logging
logging.basicConfig(
    filename="logs/demographics--person.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def demographics_sex_to_person_gender(sex_value):
    """Convert demographics sex to OMOP gender concept"""
    mapping = {1: (8507, "MALE"), 2: (8532, "FEMALE")}
    if sex_value in mapping:
        return mapping[sex_value]
    return (0, "No Matching Concept")


def demographics_ethnicity_to_person_ethnicity(ethnic_value):
    """Convert demographics ethnicity to OMOP ethnicity concept"""
    mapping = {
        1: (38003563, "Hispanic or Latino"),
        2: (38003564, "Not Hispanic or Latino"),
    }
    if ethnic_value in mapping:
        return mapping[ethnic_value]
    return (0, "No Matching Concept")


def process_demographics_to_person():
    try:
        # Read source data
        source_file = "source_tables/demographics.csv"
        subjects_file = "source_tables/subjects.csv"
        omic_sex_file = "source_tables/other/omic_inferred_sex_if_different.csv"
        logging.info(f"Reading source data from {source_file}")
        df = pd.read_csv(source_file)
        
        # Read subjects data for disease status
        logging.info(f"Reading subjects data from {subjects_file}")
        subjects_df = pd.read_csv(subjects_file)
        
        # Read omic inferred sex data
        logging.info(f"Reading omic inferred sex data from {omic_sex_file}")
        omic_sex_df = pd.read_csv(omic_sex_file)
        # Create a lookup dictionary for omic inferred sex
        omic_sex_lookup = dict(zip(omic_sex_df['Participant_ID'], omic_sex_df['omic_inferred_sex_if_different']))
        
        # Merge demographics with subjects data
        df = df.merge(subjects_df[['Participant_ID', 'subject_group_id']], on='Participant_ID', how='left')
        
        # Create disease status mapping
        disease_status_mapping = {
            1: "ALS",
            5: "Healthy Control", 
            11: "Asymptomatic ALS Gene carrier",
            17: "Non-ALS MND"
        }

        # Initialize result DataFrame
        result = pd.DataFrame()

        # Process each row
        for _, row in df.iterrows():
            # Get disease status
            subject_group_id = row.get('subject_group_id')
            disease_status = disease_status_mapping.get(subject_group_id, "Unknown")
            
            # Create person source value with disease status
            person_source_parts = [f"demographics+Participant_ID (participant identifier): {row['Participant_ID']}"]
            if pd.isna(subject_group_id) or subject_group_id == "":
                person_source_parts.append("subjects+subject_group_id (disease status): BLANK")
            else:
                person_source_parts.append(f"subjects+subject_group_id (disease status): {subject_group_id} ({disease_status})")
            
            person_data = {
                "person_id": row["Participant_ID"],
                "person_source_value": " | ".join(person_source_parts),
                "care_site_id": 11,  # AALS care site ID
            }

            # Process gender
            gender_concept_id, _ = demographics_sex_to_person_gender(
                row["sex"]
            )
            if pd.isna(row["sex"]) or row["sex"] == "":
                gender_source_value = "demographics+sex (biological sex according to survey): BLANK"
            elif row["sex"] == 1:
                gender_source_value = "demographics+sex (biological sex according to survey): 1 (Male)"
            elif row["sex"] == 2:
                gender_source_value = "demographics+sex (biological sex according to survey): 2 (Female)"
            else:
                gender_source_value = f"demographics+sex (biological sex according to survey): {row['sex']} (Unknown value)"
            
            # Add omic inferred sex if present for this participant
            participant_id = row['Participant_ID']
            if participant_id in omic_sex_lookup:
                omic_sex_value = omic_sex_lookup[participant_id]
                gender_source_value += f" | +omic_inferred_sex_if_different (omic inferred sex if different from survey): {omic_sex_value}"
            
            person_data.update(
                {
                    "gender_concept_id": gender_concept_id,
                    "gender_source_value": gender_source_value,
                }
            )

            # Process ethnicity
            ethnicity_concept_id, _ = (
                demographics_ethnicity_to_person_ethnicity(row["ethnic"])
            )
            if pd.isna(row["ethnic"]) or row["ethnic"] == "":
                ethnicity_source_value = "demographics+ethnic (ethnicity): BLANK"
            elif row["ethnic"] == 1:
                ethnicity_source_value = "demographics+ethnic (ethnicity): 1 (Hispanic or Latino)"
            elif row["ethnic"] == 2:
                ethnicity_source_value = "demographics+ethnic (ethnicity): 2 (Not Hispanic or Latino)"
            else:
                ethnicity_source_value = f"demographics+ethnic (ethnicity): {row['ethnic']} (Unknown value)"
            
            person_data.update(
                {
                    "ethnicity_concept_id": ethnicity_concept_id,
                    "ethnicity_source_value": ethnicity_source_value,
                }
            )

            # Process year of birth
            person_data["year_of_birth"] = relative_day_to_year(row["dob"])
            # Note: year_of_birth doesn't have a source_value column in OMOP PERSON table

            # Process race
            race_columns = ["raceamin", "raceasn", "raceblk", "racenh", "racewt"]
            race_values = [row[col] for col in race_columns]
            race_mapping = {
                "raceamin": (
                    8657,
                    "American Indian or Alaska Native",
                    "American Indian/Alaska Native",
                ),
                "raceasn": (8515, "Asian", "Asian"),
                "raceblk": (
                    8516,
                    "Black or African American",
                    "Black/African American",
                ),
                "racenh": (
                    8557,
                    "Native Hawaiian or Other Pacific Islander",
                    "Native Hawaiian/Pacific Islander",
                ),
                "racewt": (8527, "White", "White"),
            }

            # Check if all race values are blank/missing
            all_blank = all(pd.isna(row[col]) or row[col] == "" or row[col] == 0 for col in race_columns)
            
            if all_blank:
                race_source_parts = []
                for col in race_columns:
                    if pd.isna(row[col]) or row[col] == "":
                        race_source_parts.append(f"demographics+{col} (race): BLANK")
                    else:
                        race_source_parts.append(f"demographics+{col} (race): {row[col]}")
                person_data.update(
                    {
                        "race_concept_id": 0,

                        "race_source_value": " | ".join(race_source_parts),
                    }
                )
            # If multiple races are selected, list them all
            elif sum(race_values) > 1:
                race_source_parts = []
                for col in race_columns:
                    if row[col] == 1:
                        race_name = race_mapping[col][2]  # Get source value
                        race_source_parts.append(f"demographics+{col} (race): 1 ({race_name})")
                    elif pd.isna(row[col]) or row[col] == "":
                        race_source_parts.append(f"demographics+{col} (race): BLANK")
                    else:
                        race_source_parts.append(f"demographics+{col} (race): {row[col]}")
                person_data.update(
                    {
                        "race_concept_id": 0,
                        "race_source_value": " | ".join(race_source_parts),
                    }
                )
            else:
                # Map single race
                race_found = False
                for col, (
                    concept_id,
                    concept_name,
                    source_value,
                ) in race_mapping.items():
                    if row[col] == 1:
                        person_data.update(
                            {
                                "race_concept_id": concept_id,
                                "race_source_value": f"demographics+{col} (race): 1 ({source_value})",
                            }
                        )
                        race_found = True
                        break
                
                # If no race was found, create source value showing all blank values
                if not race_found:
                    race_source_parts = []
                    for col in race_columns:
                        if pd.isna(row[col]) or row[col] == "":
                            race_source_parts.append(f"demographics+{col} (race): BLANK")
                        else:
                            race_source_parts.append(f"demographics+{col} (race): {row[col]}")
                    person_data.update(
                        {
                            "race_concept_id": 0,
                            "race_source_value": " | ".join(race_source_parts),
                        }
                    )

            # Add to result
            result = pd.concat([result, pd.DataFrame([person_data])], ignore_index=True)

        # Ensure all required columns are present
        required_columns = [
            "person_id",
            "person_source_value",
            "gender_concept_id",
            "gender_source_value",
            "year_of_birth",
            "race_concept_id",
            "race_source_value",
            "ethnicity_concept_id",
            "ethnicity_source_value",
            "care_site_id",
        ]

        for col in required_columns:
            if col not in result.columns:
                result[col] = None

        # Check for any missing concept_ids
        result = check_missing_concept_ids(result)

        # Reorder columns
        result = result[required_columns]

        # Save to OMOP tables directory
        output_file = "processed_source/demographics--person.csv"
        result.to_csv(output_file, index=False)
        logging.info(f"Successfully saved OMOP PERSON table to {output_file}")

    except Exception as e:
        logging.error(f"Error processing demographics to person: {str(e)}")
        raise


if __name__ == "__main__":
    process_demographics_to_person()

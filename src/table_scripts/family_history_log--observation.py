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
    filename="logs/family_history_log--observation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Disease concept mappings
DISEASE_CONCEPTS = {
    "fhalz": {
        "id": 378419,
        "name": "Alzheimer's disease",
        "source": "Alzheimer's Disease",
        "equivalence": "EQUAL",
    },
    "fhals": {
        "id": 373182,
        "name": "Amyotrophic lateral sclerosis",
        "source": "Amyotrophic Lateral Sclerosis",
        "equivalence": "EQUAL",
    },
    "fhdem": {"id": 4182210, "name": "Dementia", "source": "Dementia", "equivalence": "EQUAL"},
    "fhdown": {
        "id": 4320803,
        "name": "Anomaly of chromosome pair 21",
        "source": "Down's Syndrome",
        "equivalence": "EQUAL",
    },
    "fhftd": {
        "id": 4043378,
        "name": "Frontotemporal dementia",
        "source": "Frontotemporal Dementia",
        "equivalence": "EQUAL",
    },
    "fhhd": {
        "id": 374341,
        "name": "Huntington's chorea",
        "source": "Huntington's Disease",
        "equivalence": "EQUAL",
    },
    "fhpd": {
        "id": 381270,
        "name": "Parkinson's disease",
        "source": "Parkinson's Disease",
        "equivalence": "EQUAL",
    },
    "fhpsy": {
        "id": 36308213,
        "name": "Psychiatric disorder",
        "source": "Psychiatric Disorder",
        "equivalence": "EQUAL",
    },
    "fharth": {"id": 4291025, "name": "Arthritis", "source": "Arthritis", "equivalence": "EQUAL"},
    "fhasth": {"id": 317009, "name": "Asthma", "source": "Asthma", "equivalence": "EQUAL"},
    "fhcanc": {
        "id": 443392,
        "name": "Malignant neoplastic disease",
        "source": "Cancer",
        "equivalence": "EQUAL",
    },
    "fhcirc": {
        "id": 443784,
        "name": "Vascular disorder",
        "source": "Circulation Problems",
        "equivalence": "EQUAL",
    },
    "fhdiab": {"id": 201820, "name": "Diabetes mellitus", "source": "Diabetes", "equivalence": "EQUAL"},
    "fhhrt": {"id": 321588, "name": "Heart disease", "source": "Heart Disease", "equivalence": "EQUAL"},
    "fhhbp": {
        "id": 316866,
        "name": "Hypertensive disorder",
        "source": "High Blood Pressure",
        "equivalence": "EQUAL",
    },
    "fhlung": {
        "id": 320136,
        "name": "Disorder of respiratory system",
        "source": "Lung Disease",
        "equivalence": "WIDER",
    },
    "fhstk": {"id": 381316, "name": "Cerebrovascular accident", "source": "Stroke", "equivalence": "WIDER"},
}

# Gene concept mappings
GENE_CONCEPTS = {
    "fhgnang": {
        "id": 35961859,
        "name": "ANG (angiogenin) gene variant measurement",
        "source": "ANG",
        "equivalence": "EQUAL",
    },
    "fhgnc9": {
        "id": 35954626,
        "name": "C9orf72 (C9orf72-SMCR8 complex subunit) gene variant measurement",
        "source": "C90RF72",
        "equivalence": "EQUAL",
    },
    "fhgnfus": {
        "id": 19643404,
        "name": "FUS gene rearrangement measurement",
        "source": "FUS",
        "equivalence": "EQUAL",
    },
    "fhgnprg": {
        "id": 35951629,
        "name": "GRN (granulin precursor) gene variant measurement",
        "source": "Progranulin",
        "equivalence": "EQUAL",
    },
    "fhgnsetx": {
        "id": 35958907,
        "name": "SETX (senataxin) gene variant measurement",
        "source": "SETX",
        "equivalence": "EQUAL",
    },
    "fhgnsod1": {
        "id": 35948140,
        "name": "SOD1 (superoxide dismutase 1) gene variant measurement",
        "source": "SOD1",
        "equivalence": "EQUAL",
    },
    "fhgntau": {
        "id": 35946715,
        "name": "MAPT (microtubule associated protein tau) gene variant measurement",
        "source": "TAU",
        "equivalence": "EQUAL",
    },
    "fhgntdp": {
        "id": 35964178,
        "name": "TARDBP (TAR DNA binding protein) gene variant measurement",
        "source": "TDP-43",
        "equivalence": "EQUAL",
    },
    "fhgnvapb": {
        "id": 35956055,
        "name": "VAPB (VAMP associated protein B and C) gene variant measurement",
        "source": "VAPB",
        "equivalence": "EQUAL",
    },
    "fhgnvcp": {
        "id": 35958302,
        "name": "VCP (valosin containing protein) gene variant measurement",
        "source": "VCP",
        "equivalence": "EQUAL",
    },
}

# Family relationship equivalence mappings
FAMILY_RELATIONSHIP_EQUIVALENCE = {
    "mother": "EQUAL",
    "father": "EQUAL",
    "sister": "EQUAL",
    "brother": "EQUAL",
    "sister2": "WIDER",  # Half-sister
    "brother2": "EQUAL",  # Half-brother
    "daughter": "WIDER",
    "son": "EQUAL",
    "grandmother_maternal": "EQUAL",
    "grandmother_paternal": "EQUAL",
    "grandfather_maternal": "EQUAL",
    "grandfather_paternal": "EQUAL",
    "aunt": "EQUAL",
    "uncle": "EQUAL",
    "cousin": "EQUAL",
}


def get_relative_concept(famrel, famher):
    """Map family relationship and heredity to concept ID and name"""
    # Convert famrel to string if it's a valid value
    famrel = str(famrel).strip().lower() if pd.notna(famrel) else ""
    # Handle famher value - could be float or string
    if pd.notna(famher):
        # Convert float to int if numeric
        if isinstance(famher, (float, int)):
            famher = str(int(famher))
        else:
            famher = str(famher).strip()
    else:
        famher = ""

    logging.info(
        f"get_relative_concept - famher after processing: {famher}, type: {type(famher)}"
    )

    # Convert numeric values to text keys
    number_to_text = {
        "1": "mother",
        "2": "father",
        "3": "sister",
        "4": "brother",
        "5": "sister2",
        "6": "brother2",
        "7": "daughter",
        "8": "son",
        "9": "grandmother",
        "10": "grandfather",
        "11": "aunt",
        "12": "uncle",
        "13": "cousin",
    }

    # If famrel is a number, convert it to text
    if famrel.replace(".", "").isdigit():
        famrel = number_to_text.get(str(int(float(famrel))), "")

    # Define concepts with descriptive text keys
    concepts = {
        "mother": (
            4051255,
            "Family history with explicit context pertaining to mother",
        ),
        "father": (
            4051256,
            "Family history with explicit context pertaining to father",
        ),
        "sister": (
            4051258,
            "Family history with explicit context pertaining to sister",
        ),
        "brother": (
            4051262,
            "Family history with explicit context pertaining to brother",
        ),
        "sister2": (
            4051258,
            "Family history with explicit context pertaining to sister",
        ),
        "brother2": (
            4051262,
            "Family history with explicit context pertaining to brother",
        ),
        "daughter": (
            4054433,
            "Family history with explicit context pertaining to daughter",
        ),
        "son": (4052795, "Family history with explicit context pertaining to son"),
        "aunt": (4050943, "Family history with explicit context pertaining to aunt"),
        "uncle": (4051265, "Family history with explicit context pertaining to uncle"),
        "cousin": (713135, "Family history with explicit context pertaining to cousin"),
    }

    # Special handling for grandparents with default to paternal if heredity is blank
    if famrel == "grandmother":
        # For source value tracking
        heredity_source = (
            "BLANK" if not famher else ("maternal" if famher == "2" else "paternal")
        )
        # Default to paternal if blank
        is_maternal = famher == "2"
        equivalence_key = "grandmother_maternal" if is_maternal else "grandmother_paternal"
        logging.info(f"Grandmother case - famher: {famher}, is_maternal: {is_maternal}")
        return (
            (
                (
                    4050942,
                    "Family history with explicit context pertaining to maternal grandmother",
                )
                if is_maternal
                else (
                    4052802,
                    "Family history with explicit context pertaining to paternal grandmother",
                )
            ),
            heredity_source,
            FAMILY_RELATIONSHIP_EQUIVALENCE.get(equivalence_key, ""),
        )
    elif famrel == "grandfather":
        # For source value tracking
        heredity_source = (
            "BLANK" if not famher else ("maternal" if famher == "2" else "paternal")
        )
        # Default to paternal if blank
        is_maternal = famher == "2"
        equivalence_key = "grandfather_maternal" if is_maternal else "grandfather_paternal"
        logging.info(f"Grandfather case - famher: {famher}, is_maternal: {is_maternal}")
        return (
            (
                (
                    4052800,
                    "Family history with explicit context pertaining to maternal grandfather",
                )
                if is_maternal
                else (
                    4052801,
                    "Family history with explicit context pertaining to paternal grandfather",
                )
            ),
            heredity_source,
            FAMILY_RELATIONSHIP_EQUIVALENCE.get(equivalence_key, ""),
        )

    # Return the concept if found, along with None for heredity_source and equivalence for this relationship
    equivalence = FAMILY_RELATIONSHIP_EQUIVALENCE.get(famrel, "")
    return concepts.get(famrel, (None, None)), None, equivalence


def create_base_observation(row, index_date):
    """Create base observation record with common fields"""
    return {
        "person_id": row["Participant_ID"],
        "observation_date": relative_day_to_date(row["Visit_Date"], index_date),
        "observation_type_concept_id": 32851,  # Healthcare professional filled survey
        "value_as_number": None,
        "value_as_string": None,
        "value_as_concept_id": None,

        "value_source_value": None,
        "qualifier_concept_id": None,

        "qualifier_source_value": None,
        "unit_concept_id": None,

        "unit_source_value": None,
        "visit_occurrence_id": get_visit_occurrence_id(
            row["Participant_ID"], row["Visit_Date"]
        ),
        "observation_event_id": None,
        "obs_event_field_concept_id": None,
    }


def process_family_history(row, index_date):
    """Process family history with conditions/genes as values"""
    observations = []

    try:
        # Get the family relationship concept
        (concept_id, concept_name), heredity_source, family_equivalence = get_relative_concept(
            row["famrel"], row.get("famher", "")
        )

        if concept_id is None:
            return []

        # Convert numeric values to text descriptions
        number_to_text = {
            "1": "mother",
            "2": "father",
            "3": "sister",
            "4": "brother",
            "5": "sister2",
            "6": "brother2",
            "7": "daughter",
            "8": "son",
            "9": "grandmother",
            "10": "grandfather",
            "11": "aunt",
            "12": "uncle",
            "13": "cousin",
        }

        famrel = str(row["famrel"]) if pd.notna(row["famrel"]) else ""
        # Don't convert famher to lowercase, just strip whitespace
        raw_famher = (
            str(row.get("famher", "")).strip() if pd.notna(row.get("famher")) else ""
        )

        # Get gender value and handle numeric types
        raw_famgen = row.get("famgen")
        if pd.notna(raw_famgen):
            # Convert float/int to string integer
            if isinstance(raw_famgen, (float, int)):
                raw_famgen = str(int(raw_famgen))
            else:
                raw_famgen = str(raw_famgen).strip()
            gender_text = (
                "male"
                if raw_famgen == "1"
                else ("female" if raw_famgen == "2" else "BLANK")
            )
        else:
            gender_text = "BLANK"

        # Convert numeric famrel to text
        if famrel.replace(".", "").isdigit():
            famrel_text = number_to_text.get(str(int(float(famrel))), "unknown")
        else:
            famrel_text = famrel.strip().lower()

        # Create family member source value using new format
        family_source_parts = []
        
        # Add relative information
        if famrel_text and famrel_text != "unknown":
            famrel_value = int(float(row['famrel'])) if pd.notna(row['famrel']) else row['famrel']
            family_source_parts.append(f"family_history_log+famrel (family relationship): {famrel_value} ({famrel_text})")
        
        # Add heredity information
        if heredity_source is not None:
            if heredity_source == "BLANK":
                family_source_parts.append(f"family_history_log+famher (heredity): BLANK")
            else:
                famher_value = int(float(raw_famher)) if raw_famher and raw_famher.replace('.', '').isdigit() else raw_famher
                family_source_parts.append(f"family_history_log+famher (heredity): {famher_value} ({heredity_source})")
        elif raw_famher:
            # Compare raw_famher with "2" for maternal, anything else (including "1") is paternal
            heredity_text = "maternal" if raw_famher == "2" else "paternal"
            famher_value = int(float(raw_famher)) if raw_famher and raw_famher.replace('.', '').isdigit() else raw_famher
            family_source_parts.append(f"family_history_log+famher (heredity): {famher_value} ({heredity_text})")
        
        # Add gender information
        if gender_text == "BLANK":
            family_source_parts.append(f"family_history_log+famgen (gender): BLANK")
        elif gender_text:
            famgen_value = int(float(raw_famgen)) if raw_famgen and str(raw_famgen).replace('.', '').isdigit() else raw_famgen
            family_source_parts.append(f"family_history_log+famgen (gender): {famgen_value} ({gender_text})")
        
        # Add equivalence information
        if family_equivalence:
            family_source_parts.append(f"+equivalence (usagi omop mapping equivalence): {family_equivalence}")
        
        family_source = " | ".join(family_source_parts) if family_source_parts else None

        # Process diseases
        for var, concept in DISEASE_CONCEPTS.items():
            if var in row and pd.notna(row[var]) and row[var] == 1:
                observation = create_base_observation(row, index_date)
                
                # Create value source using new format
                value_source_parts = [f"family_history_log+{var} ({concept['source']}): 1 (yes)"]
                
                # Add specific details if available
                if f"{var}sp" in row and pd.notna(row[f"{var}sp"]):
                    value_source_parts.append(f"family_history_log+{var}sp (specific details): {row[f'{var}sp']}")
                
                # Add equivalence information
                if concept.get('equivalence'):
                    value_source_parts.append(f"+equivalence (usagi omop mapping equivalence): {concept['equivalence']}")
                
                value_source = " | ".join(value_source_parts)

                observation.update(
                    {
                        "observation_concept_id": concept_id,
                        "observation_source_value": family_source,
                        "value_as_concept_id": concept["id"],
                        "value_source_value": value_source,
                    }
                )
                observations.append(observation)

        # Process genes
        for var, concept in GENE_CONCEPTS.items():
            if var in row and pd.notna(row[var]) and row[var] == 1:
                observation = create_base_observation(row, index_date)
                
                # Create value source using new format
                value_source_parts = [f"family_history_log+{var} ({concept['source']}): 1 (yes)"]

                # Add equivalence information
                if concept.get('equivalence'):
                    value_source_parts.append(f"+equivalence (usagi omop mapping equivalence): {concept['equivalence']}")

                value_source = " | ".join(value_source_parts)

                observation.update(
                    {
                        "observation_concept_id": concept_id,
                        "observation_source_value": family_source,
                        "value_as_concept_id": concept["id"],
                        "value_source_value": value_source,
                    }
                )
                observations.append(observation)

        return observations
    except Exception as e:
        logging.error(f"Error processing family history: {str(e)}")
        return []


def main():
    try:
        # Read source data
        source_file = "source_tables/family_history_log.csv"
        source_df = pd.read_csv(source_file)

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")

        # Transform data
        observations = []
        for _, row in source_df.iterrows():
            observations.extend(process_family_history(row, index_date))

        # Create DataFrame
        result_df = pd.DataFrame(observations)

        # Ensure columns are in the correct order
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
        result_df = result_df[column_order]

        # Save to OMOP tables directory
        output_file = "processed_source/family_history_log--observation.csv"
        result_df.to_csv(output_file, index=False)

        logging.info("Successfully transformed family history log to observation table")

    except Exception as e:
        logging.error(f"Error in main function: {str(e)}")
        raise


if __name__ == "__main__":
    main()

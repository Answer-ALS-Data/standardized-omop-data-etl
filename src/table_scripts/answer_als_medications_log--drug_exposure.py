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
Path("source_tables").mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    filename="logs/answer_als_medications_log--drug_exposure.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def build_source_value(table_name, var_name, value=None, var_interpretation=None, val_interpretation=None):
    """Build source value in the new format: table+var (var_interpretation): value (val_interpretation)"""
    base = f"{table_name}+{var_name}"
    
    if var_interpretation:
        base += f" ({var_interpretation})"
    
    if value is not None:
        result = f"{base}: {value}"
        if val_interpretation:
            result += f" ({val_interpretation})"
        return result
    else:
        return base


def answer_als_medications_log_route_to_drug_exposure_route_concept_id(route_value):
    """Convert route values to route concept IDs"""
    route_mapping = {
        1: (4132161, "Oral"),
        2: (4171047, "Intravenous"),
        3: (4142048, "Subcutaneous"),
        4: (4263689, "Topical"),
        5: (40486069, "Respiratory tract"),
        6: (4262099, "Transdermal"),
        7: (4290759, "Rectal"),
        8: (4302612, "Intramuscular"),
        9: (4292110, "Sublingual"),
        10: (4177987, "Percutaneous"),
        99: (0, "No Matching Concept"),
    }
    try:
        route_value = int(route_value)
        return route_mapping.get(route_value, (0, "No Matching Concept"))
    except (ValueError, TypeError):
        return (0, "No Matching Concept")


def answer_als_medications_log_route_to_text(route_value):
    """Convert route values to route text values"""
    route_mapping = {
        1: "oral",
        2: "intravenous", 
        3: "subcutaneous",
        4: "topical",
        5: "inhalation",
        6: "transdermal",
        7: "rectal",
        8: "intramuscular",
        9: "sublingual",
        10: "PEG",
        99: "other (please specify)",
    }
    try:
        route_value = int(route_value)
        return route_mapping.get(route_value, "")
    except (ValueError, TypeError):
        return ""


def answer_als_medications_log_route_to_drug_exposure_route_source_value(route_value, route_other_specify=None):
    """Convert route values to route source values in new format"""
    # Check if route_value is blank (NaN or empty)
    if pd.isna(route_value) or route_value == "":
        return build_source_value("answer_als_medications_log", "medrte", "BLANK", "medication route")
    
    route_text = answer_als_medications_log_route_to_text(route_value)
    if route_text:
        try:
            route_int = int(route_value)
            # If route is "other (please specify)" and we have other specify text, include it as separate field
            if route_int == 99 and route_other_specify and pd.notna(route_other_specify):
                route_base = build_source_value("answer_als_medications_log", "medrte", str(route_int), "medication route", route_text)
                route_other = build_source_value("answer_als_medications_log", "medrtesp", route_other_specify, "route other specify")
                return f"{route_base} | {route_other}"
            else:
                return build_source_value("answer_als_medications_log", "medrte", str(route_int), "medication route", route_text)
        except (ValueError, TypeError):
            return build_source_value("answer_als_medications_log", "medrte", route_text, "medication route")
    else:
        return build_source_value("answer_als_medications_log", "medrte", var_interpretation="medication route")


def answer_als_medications_log_medu_to_unit_text(unit_value, other_specify=None):
    """Convert medication unit codes to text values"""
    unit_mapping = {
        1: "micrograms (ucg)",
        2: "milligrams (mg)",
        3: "grams (g)",
        4: "tablet(s)",
        5: "capsule(s)",
        6: "gtt",
        7: "milliequivalent (meq)",
        8: "international units (IU)",
        9: "units (U)",
        99: other_specify if other_specify else "other",
    }
    try:
        unit_value = int(unit_value)
        return unit_mapping.get(unit_value, "")
    except (ValueError, TypeError):
        return ""


def answer_als_medications_log_medfreq_to_frequency_text(
    freq_value, other_specify=None
):
    """Convert medication frequency codes to text values"""
    freq_mapping = {
        1: "QD",
        2: "BID",
        3: "TID",
        4: "QID",
        5: "QHS",
        6: "continuous IV",
        7: "PRN",
        99: other_specify if other_specify else "other",
    }
    try:
        freq_value = int(freq_value)
        return freq_mapping.get(freq_value, "")
    except (ValueError, TypeError):
        return ""


def main():
    try:
        # Read source data
        source_data = pd.read_csv(Path("source_tables") / "answer_als_medications_log.csv")
        usagi_mapping = pd.read_csv(Path("source_tables") / "usagi" / "medications_v2.csv")

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

        # Set index date for relative day calculations
        index_date = datetime(2016, 1, 1)

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Get medication concept mapping from USAGI
            med = row["med"]
            mappings = usagi_mapping[
                usagi_mapping["sourceName"].str.lower().str.strip()
                == str(med).lower().strip()
            ]

            # If no mappings found, create a single row with concept_id 0
            if mappings.empty:
                mappings = pd.DataFrame(
                    [
                        {
                            "conceptId": 0,
                            "conceptName": "No Matching Concept",
                            "equivalence": "",
                        }
                    ]
                )

            # Process each matching concept (could be multiple)
            for _, mapping in mappings.iterrows():
                # Get route information
                route_concept_id, _ = (
                    answer_als_medications_log_route_to_drug_exposure_route_concept_id(
                        row["medrte"]
                    )
                )
                route_source_value = answer_als_medications_log_route_to_drug_exposure_route_source_value(
                    row["medrte"], row.get("medrtesp", "")
                )

                # Get unit and frequency information
                unit_text = answer_als_medications_log_medu_to_unit_text(
                    row["medu"], row.get("meduotsp", "")
                )
                freq_text = answer_als_medications_log_medfreq_to_frequency_text(
                    row["medfreq"], row.get("medfrqsp", "")
                )

                # Format the dose part - if meddose is NaN or empty, use empty string
                dose_part = (
                    f"{row.get('meddose', '')}"
                    if pd.notna(row.get("meddose", ""))
                    else ""
                )
                unit_part = f" {unit_text}" if unit_text else ""

                # Handle indication - if nan or empty, use empty string
                indication = row.get("medind", "")
                indication = "" if pd.isna(indication) else str(indication)

                # Calculate dates with the new logic
                medstdt_blank = pd.isna(row["medstdt"])
                medenddt_blank = pd.isna(row["medenddt"])
                
                # Default date for when both are blank
                default_date = datetime.strptime("1900-01-01", "%Y-%m-%d")
                
                if medstdt_blank and medenddt_blank:
                    # Both are blank - use 1900-01-01 for both
                    start_date = default_date
                    end_date = default_date
                    verbatim_end_date = None
                elif not medstdt_blank and medenddt_blank:
                    # medstdt is not blank, but medenddt is - set medenddt = medstdt
                    start_date = relative_day_to_date(row["medstdt"], index_date)
                    end_date = start_date
                    verbatim_end_date = None
                elif medstdt_blank and not medenddt_blank:
                    # medstdt is blank, but medenddt isn't - set medstdt = medenddt
                    end_date = relative_day_to_date(row["medenddt"], index_date)
                    start_date = end_date
                    verbatim_end_date = end_date
                else:
                    # Both are not blank - treat normally
                    start_date = relative_day_to_date(row["medstdt"], index_date)
                    end_date = relative_day_to_date(row["medenddt"], index_date)
                    verbatim_end_date = end_date

                # Format dates as yyyy-mm-dd
                start_date_str = start_date.strftime("%Y-%m-%d")
                end_date_str = end_date.strftime("%Y-%m-%d")
                verbatim_end_date_str = (
                    verbatim_end_date.strftime("%Y-%m-%d") if verbatim_end_date else ""
                )

                # Build drug source value in new format
                source_parts = []
                
                # Add medication name
                if med:
                    source_parts.append(build_source_value("answer_als_medications_log", "med", med, "medication name"))
                
                # Add dose information
                if dose_part:
                    source_parts.append(build_source_value("answer_als_medications_log", "meddose", dose_part, "medication dose"))
                
                # Add unit information
                if unit_text:
                    unit_value = row.get("medu", "")
                    if pd.notna(unit_value) and unit_value != "":
                        # Convert to int to ensure it's an integer
                        try:
                            unit_int = int(unit_value)
                            source_parts.append(build_source_value("answer_als_medications_log", "medu", str(unit_int), "medication unit", unit_text))
                        except (ValueError, TypeError):
                            source_parts.append(build_source_value("answer_als_medications_log", "medu", unit_text, "medication unit"))
                    else:
                        source_parts.append(build_source_value("answer_als_medications_log", "medu", unit_text, "medication unit"))
                
                # Add unit other specify if exists
                unit_other = row.get("meduotsp", "")
                if unit_other and pd.notna(unit_other):
                    source_parts.append(build_source_value("answer_als_medications_log", "meduotsp", unit_other, "unit other specify"))
                
                # Add frequency information
                if freq_text:
                    freq_value = row.get("medfreq", "")
                    if pd.notna(freq_value) and freq_value != "":
                        # Convert to int to ensure it's an integer
                        try:
                            freq_int = int(freq_value)
                            source_parts.append(build_source_value("answer_als_medications_log", "medfreq", str(freq_int), "medication frequency", freq_text))
                        except (ValueError, TypeError):
                            source_parts.append(build_source_value("answer_als_medications_log", "medfreq", freq_text, "medication frequency"))
                    else:
                        source_parts.append(build_source_value("answer_als_medications_log", "medfreq", freq_text, "medication frequency"))
                
                # Add frequency other specify if exists
                freq_other = row.get("medfrqsp", "")
                if freq_other and pd.notna(freq_other):
                    source_parts.append(build_source_value("answer_als_medications_log", "medfrqsp", freq_other, "frequency other specify"))
                
                # Add indication
                if indication:
                    source_parts.append(build_source_value("answer_als_medications_log", "medind", indication, "medication indication"))
                
                # Add equivalence from mapping
                if mapping.get('equivalence'):
                    source_parts.append(build_source_value("", "equivalence", mapping['equivalence'], "usagi omop mapping equivalence"))
                
                # Join all parts with pipes
                drug_source_value = " | ".join(source_parts) if len(source_parts) > 1 else source_parts[0] if source_parts else ""
                
                # Create new row
                new_row = {
                    "person_id": person_id,
                    "drug_concept_id": mapping["conceptId"],
                    "drug_source_value": drug_source_value,
                    "drug_exposure_start_date": start_date_str,
                    "drug_exposure_end_date": end_date_str,
                    "verbatim_end_date": verbatim_end_date_str,
                    "drug_type_concept_id": 32851,  # Healthcare professional filled survey
                    "route_concept_id": route_concept_id,
                    "route_source_value": route_source_value,
                    "visit_occurrence_id": f"{person_id}_0",  # Default to 0 since we don't have visit date
                }

                output_data = pd.concat(
                    [output_data, pd.DataFrame([new_row])], ignore_index=True
                )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, ["drug_concept_id", "route_concept_id"])

        # Save output
        output_data.to_csv(
            Path("processed_source") / "answer_als_medications_log--drug_exposure.csv",
            index=False,
        )

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()

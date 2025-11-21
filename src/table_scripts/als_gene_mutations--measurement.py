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
    filename="logs/als_gene_mutations--measurement.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def als_gene_mutations_to_measurement(source_df, index_date_str):
    """
    Transform ALS gene mutations data into OMOP measurement table format.

    Args:
        source_df (pd.DataFrame): Source data from ALS_Gene_Mutations.csv
        index_date_str (str): Index date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: Transformed data in OMOP measurement table format
    """
    logging.info("Starting ALS gene mutations to measurement transformation")

    # Convert index date string to datetime
    index_date = datetime.strptime(index_date_str, "%Y-%m-%d")

    # Initialize empty list to store transformed rows
    transformed_rows = []

    # Define the mapping of test variables to measurement concepts and meanings
    gene_mappings = {
        "ang": {
            "concept_id": 35961859,
            "concept_name": "ANG (angiogenin) gene variant measurement",
            "source_meaning": "ANG Mutation",
            "nd_var": "angnd",
        },
        "c9orf72": {
            "concept_id": 35954626,
            "concept_name": "C9orf72 (C9orf72-SMCR8 complex subunit) gene variant measurement",
            "source_meaning": "C9ORF72 Mutation",
            "nd_var": "c9orfnd",
        },
        "fus": {
            "concept_id": 19643404,
            "concept_name": "FUS gene rearrangement measurement",
            "source_meaning": "FUS Mutation",
            "nd_var": "fusnd",
        },
        "mutot": {
            "concept_id": 0,
            "concept_name": "No Matching Concept",
            "source_meaning": "Other Mutation: positive",
            "nd_var": None,  # No proper nd variable for mutot
        },
        "progran": {
            "concept_id": 35951629,
            "concept_name": "GRN (granulin precursor) gene variant measurement",
            "source_meaning": "PROGRANULIN Mutation",
            "nd_var": "prgrnnd",
        },
        "setx": {
            "concept_id": 35958907,
            "concept_name": "SETX (senataxin) gene variant measurement",
            "source_meaning": "SETX Mutation",
            "nd_var": "setxnd",
        },
        "sod1": {
            "concept_id": 35948140,
            "concept_name": "SOD1 (superoxide dismutase 1) gene variant measurement",
            "source_meaning": "SOD1 Mutation",
            "nd_var": "sod1nd",
        },
        "tau": {
            "concept_id": 35946715,
            "concept_name": "MAPT (microtubule associated protein tau) gene variant measurement",
            "source_meaning": "TAU Mutation",
            "nd_var": "taund",
        },
        "tdp43": {
            "concept_id": 35964178,
            "concept_name": "TARDBP (TAR DNA binding protein) gene variant measurement",
            "source_meaning": "TDP-43 Mutation",
            "nd_var": "tdp43nd",
        },
        "vapb": {
            "concept_id": 35956055,
            "concept_name": "VAPB (VAMP associated protein B and C) gene variant measurement",
            "source_meaning": "VAPB Mutation",
            "nd_var": "vapbnd",
        },
        "vcp": {
            "concept_id": 35958302,
            "concept_name": "VCP (valosin containing protein) gene variant measurement",
            "source_meaning": "VCP Mutation",
            "nd_var": "vcpnd",
        },
    }

    # Process each row in the source data
    for _, row in source_df.iterrows():
        person_id = row["Participant_ID"]
        # Calculate visit date using relative_day_to_date and format as YYYY-MM-DD
        visit_date = relative_day_to_date(row["Visit_Date"], index_date).strftime(
            "%Y-%m-%d"
        )

        # Process each gene mutation
        for test_var, mapping in gene_mappings.items():
            if test_var not in row or pd.isna(row[test_var]):
                continue

            # Only create record if test_var has a meaningful result (1 = positive, 2 = negative)
            if row[test_var] in [1, 2]:
                # Get the text value for the test result
                result_text = "Positive" if row[test_var] == 1 else "Negative"

                # Build source values using the new format
                source_parts = []
                
                # Add the ___nd variable status first (if it exists)
                nd_var = mapping["nd_var"]
                gene_name = mapping['source_meaning'].replace(' Mutation', '')
                
                if nd_var is not None:  # Only process nd_var if it exists
                    if pd.isna(row.get(nd_var)):
                        nd_status = "BLANK"
                    elif row[nd_var] == 0:
                        nd_status = "0 (tested)"
                    elif row[nd_var] == 1:
                        nd_status = "1 (not tested)"
                    else:
                        nd_status = f"{int(row[nd_var])} (unknown)"
                    
                    source_parts.append(f"als_gene_mutations+{nd_var} ({gene_name}): {nd_status}")
                
                # Add the test result
                source_parts.append(f"als_gene_mutations+{test_var}: {int(row[test_var])} ({result_text})")
                
                # For SOD1, add the mutation text if available
                if test_var == "sod1" and pd.notna(row.get("sod1muta")):
                    source_parts.append(f"als_gene_mutations+sod1muta: {row['sod1muta']}")
                
                # Join the parts with ' | ' separator
                value_source_value = " | ".join(source_parts)
                
                # For measurement_source_value, use the test variable with gene interpretation
                gene_name = mapping['source_meaning'].replace(' Mutation', '')
                measurement_source_value = f"als_gene_mutations+{test_var} ({gene_name})"

                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": mapping["concept_id"],
                    "measurement_source_value": measurement_source_value,
                    "measurement_date": visit_date,
                    "measurement_type_concept_id": 32851,  # Healthcare professional filled survey
                    "value_as_number": None,
                    "value_as_concept_id": (
                        9191 if row[test_var] == 1 else 9189
                    ),  # Positive or Negative
                    "value_source_value": value_source_value,
                    "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",  # Use unconverted Visit_Date
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
    return result_df


def main():
    try:
        # Read source data - ensure Visit_Date is read as integer
        source_file = "source_tables/als_gene_mutations.csv"
        source_df = pd.read_csv(
            source_file, dtype={"Visit_Date": "Int64"}
        )

        # Set index date to 2016-01-01
        index_date_str = "2016-01-01"

        # Transform data
        result_df = als_gene_mutations_to_measurement(source_df, index_date_str)

        # Save to OMOP tables directory
        output_file = "processed_source/als_gene_mutations--measurement.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(f"Saved transformed data to {output_file}")

    except Exception as e:
        logging.error(f"Error in transformation: {str(e)}")
        raise


if __name__ == "__main__":
    main()

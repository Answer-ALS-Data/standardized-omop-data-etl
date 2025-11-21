import pandas as pd
import os
from datetime import datetime, timedelta
import logging

# Configuration flag - set to 'create' or 'remove'
MODE = 'remove'  # Options: 'create' (normal visit creation) or 'remove' (remove visit_occurrence_id columns)

logging.basicConfig(level=logging.INFO)


def relative_day_to_date(relative_day, index_date):
    """Convert relative day to actual date

    Args:
        relative_day (int): Number of days relative to index date
        index_date (datetime): Reference date as datetime object

    Returns:
        datetime: Actual date, or None if conversion fails
    """
    try:
        return index_date + timedelta(days=relative_day)
    except Exception as e:
        logging.error(f"Error converting relative day {relative_day} to date: {str(e)}")
        return None


def load_table_if_has_visits(filename):
    """Load a table if it has visit_occurrence_id column"""
    try:
        # Check if file is empty
        if os.path.getsize(f"combined_omop/{filename}") == 0:
            logging.warning(f"File {filename} is empty - skipping")
            return None

        df = pd.read_csv(f"combined_omop/{filename}")
        if df.empty:
            logging.warning(f"File {filename} has no data - skipping")
            return None

        if "visit_occurrence_id" in df.columns:
            logging.info(f"Found visit_occurrence_id in {filename}")
            return df[["visit_occurrence_id"]].drop_duplicates()
        else:
            logging.info(f"No visit_occurrence_id column in {filename}")
            return None
    except Exception as e:
        logging.error(f"Error processing {filename}: {str(e)}")
        return None


def extract_visit_components(visit_id):
    """Extract case ID and relative day from visit_occurrence_id
    Format: CASE-NEUxxxxx_123 where xxxxx is the case ID and 123 is the relative day
    """
    try:
        # If visit_id is already an integer, it's already been processed
        if isinstance(visit_id, int):
            logging.warning(f"Visit ID {visit_id} is already an integer - skipping")
            return None, None
            
        # Convert to string and split
        visit_id_str = str(visit_id)
        case_id, relative_day = visit_id_str.split("_")
        return case_id, int(relative_day)
    except Exception as e:
        logging.error(f"Error extracting components from visit_id {visit_id}: {str(e)}")
        return None, None


def remove_visit_columns():
    """Remove visit_occurrence_id columns from all tables"""
    csv_files = [f for f in os.listdir("combined_omop") if f.endswith(".csv")]
    logging.info(f"Processing {len(csv_files)} CSV files to remove visit_occurrence_id columns")
    
    removed_count = 0
    for file in csv_files:
        try:
            df = pd.read_csv(f"combined_omop/{file}")
            if "visit_occurrence_id" in df.columns:
                df = df.drop(columns=["visit_occurrence_id"])
                df.to_csv(f"combined_omop/{file}", index=False)
                logging.info(f"Removed visit_occurrence_id column from {file}")
                removed_count += 1
            else:
                logging.info(f"No visit_occurrence_id column found in {file}")
        except Exception as e:
            logging.error(f"Error processing {file}: {str(e)}")
    
    logging.info(f"Removed visit_occurrence_id columns from {removed_count} files")


def create_visits():
    # List all CSV files in combined_omop directory
    csv_files = [f for f in os.listdir("combined_omop") if f.endswith(".csv")]
    logging.info(f"Found {len(csv_files)} CSV files to process")

    # Collect all unique visit IDs
    all_visits = []
    for file in csv_files:
        df = load_table_if_has_visits(file)
        if df is not None and not df.empty:
            all_visits.append(df)

    if not all_visits:
        logging.error("No tables with visit_occurrence_id found")
        return

    # Combine all visit IDs and get unique values
    visits_df = pd.concat(all_visits, ignore_index=True).drop_duplicates()
    logging.info(f"Found {len(visits_df)} unique visit IDs")

    # Create components for visit_occurrence table
    visit_data = []

    # Use 2016-01-01 as reference date
    reference_date = datetime(2016, 1, 1)

    for _, row in visits_df.iterrows():
        case_id, relative_day = extract_visit_components(row["visit_occurrence_id"])
        if case_id is not None and relative_day is not None:
            visit_date = relative_day_to_date(relative_day, reference_date)
            if visit_date:
                visit_data.append(
                    {
                        "visit_occurrence_id": row[
                            "visit_occurrence_id"
                        ],  # Original ID
                        "person_id": case_id,  # Using the full CASE-NEU ID as person_id
                        "visit_start_date": visit_date.strftime("%Y-%m-%d"),
                        "visit_end_date": visit_date.strftime(
                            "%Y-%m-%d"
                        ),  # Same as start date for now
                        "visit_type_c": 32851,  # Outpatient visit
                        "care_site_id": 111219,  # Default care site
                    }
                )

    if not visit_data:
        logging.error("No valid visits were created")
        return

    # Create final dataframe
    result_df = pd.DataFrame(visit_data)

    # Add sequential ID and source column
    result_df["visit_source_value"] = result_df["visit_occurrence_id"]
    result_df["visit_occurrence_id"] = range(1, len(result_df) + 1)

    # Save to CSV
    result_df.to_csv("combined_omop/visit_occurrence.csv", index=False)
    logging.info(f"Created visit_occurrence.csv with {len(result_df)} visits")

    # Create mapping dictionary for updating other tables
    visit_mapping = dict(
        zip(result_df["visit_source_value"], result_df["visit_occurrence_id"])
    )

    # Update visit IDs in other tables
    for file in csv_files:
        if file != "visit_occurrence.csv":
            try:
                df = pd.read_csv(f"combined_omop/{file}")
                if "visit_occurrence_id" in df.columns:
                    df["visit_occurrence_id"] = df["visit_occurrence_id"].map(
                        visit_mapping
                    )
                    df.to_csv(f"combined_omop/{file}", index=False)
                    logging.info(f"Updated visit IDs in {file}")
            except Exception as e:
                logging.error(f"Error updating {file}: {str(e)}")


def create_observation_periods():
    """Create observation_period table from visit_occurrence data"""
    try:
        # Read visit_occurrence table
        visits_df = pd.read_csv("combined_omop/visit_occurrence.csv")

        # Group by person_id and get first and last visit dates
        observation_periods = (
            visits_df.groupby("person_id")
            .agg({"visit_start_date": "min", "visit_end_date": "max"})
            .reset_index()
        )

        # Rename columns to match OMOP schema
        observation_periods.columns = [
            "person_id",
            "observation_period_start_date",
            "observation_period_end_date",
        ]

        # Add observation_period_id and period_type_concept_id
        observation_periods.insert(
            0, "observation_period_id", range(1, len(observation_periods) + 1)
        )
        observation_periods["period_type_concept_id"] = 32851

        # Save to CSV
        observation_periods.to_csv("combined_omop/observation_period.csv", index=False)
        logging.info(
            f"Created observation_period.csv with {len(observation_periods)} records"
        )

    except Exception as e:
        logging.error(f"Error creating observation periods: {str(e)}")


def main():
    if MODE == 'create':
        logging.info("Running in CREATE mode - creating visits normally")
        create_visits()
        create_observation_periods()
    elif MODE == 'remove':
        logging.info("Running in REMOVE mode - removing visit_occurrence_id columns")
        remove_visit_columns()
    else:
        logging.error(f"Invalid MODE: {MODE}. Must be 'create' or 'remove'")


if __name__ == "__main__":
    main()

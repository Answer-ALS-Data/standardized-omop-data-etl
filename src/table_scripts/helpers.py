import logging
from datetime import datetime, timedelta
import pandas as pd


def relative_day_to_year(relative_day, index_date="2016-01-01"):
    """Convert relative day to year of birth

    Args:
        relative_day (int): Number of days relative to index date
        index_date (str): Reference date in YYYY-MM-DD format (default: '2016-01-01')

    Returns:
        int: Year of birth, or None if conversion fails
    """
    try:
        index_date = datetime.strptime(index_date, "%Y-%m-%d")
        birth_date = index_date + timedelta(days=relative_day)
        return birth_date.year
    except Exception as e:
        logging.error(f"Error converting relative day {relative_day} to year: {str(e)}")
        return None


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


def check_missing_concept_ids(df, concept_id_columns=None):
    """Check for missing concept_ids and set them to 0 with 'No Matching Concept'

    Args:
        df (pd.DataFrame): DataFrame to check
        concept_id_columns (list): List of concept_id column names to check

    Returns:
        pd.DataFrame: Updated DataFrame with missing concept_ids handled
    """
    if concept_id_columns is None:
        concept_id_columns = [col for col in df.columns if col.endswith("_concept_id")]

    for col in concept_id_columns:
        name_col = col.replace("_id", "_name")
        if name_col in df.columns:
            df.loc[df[col].isna() | (df[col] == ""), [col, name_col]] = [
                0,
                "No Matching Concept",
            ]

    return df


def year_to_date(year_str):
    """
    Convert a year string to a date object (January 1st of that year).
    Handles various year formats and validates the year is reasonable.
    If year is missing or invalid, defaults to year 1900.

    Args:
        year_str (str): Year string to convert

    Returns:
        datetime.date: Date object for January 1st of the year
    """
    try:
        # Convert to string and strip any whitespace
        year_str = str(year_str).strip()

        # Handle empty or invalid inputs
        if not year_str or year_str.lower() in ["nan", "none", "null"]:
            return datetime(1900, 1, 1).date()

        # Convert to integer
        year = int(year_str)

        # Validate year is reasonable (between 1900 and current year + 1)
        current_year = datetime.now().year
        if year < 1900 or year > current_year + 1:
            return datetime(1900, 1, 1).date()

        # Return January 1st of the year
        return datetime(year, 1, 1).date()

    except (ValueError, TypeError):
        return datetime(1900, 1, 1).date()


def get_visit_occurrence_id(person_id, visit_date):
    """
    Create a visit_occurrence_id from person_id and visit_date.
    If visit_date is missing or empty, use 0.

    Args:
        person_id (str): The participant ID
        visit_date (str): The visit date

    Returns:
        str: Formatted visit_occurrence_id
    """
    if pd.isna(visit_date) or not str(visit_date).strip():
        return f"{person_id}_0"
    return f"{person_id}_{visit_date}"

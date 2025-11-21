import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)


def get_date_columns(df):
    """Get all date columns from a dataframe"""
    date_columns = []
    for col in df.columns:
        if 'date' in col.lower():
            date_columns.append(col)
    return date_columns


def is_valid_date(date_val):
    """Check if a date is valid and not a placeholder (01-01-1900)"""
    if pd.isna(date_val):
        return False
    
    try:
        # Convert to datetime if it's not already
        if not isinstance(date_val, datetime):
            date_val = pd.to_datetime(date_val)
        
        # Filter out 01-01-1900 placeholder date
        if date_val == datetime(1900, 1, 1):
            return False
            
        return True
        
    except Exception:
        return False


def get_person_date_range(person_id, tables):
    """Get the earliest and latest dates for a specific person across all tables"""
    all_dates = []
    
    for table in tables:
        try:
            file_path = f"combined_omop/{table}.csv"
            
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                continue
            
            df = pd.read_csv(file_path, low_memory=False)
            
            if df.empty or 'person_id' not in df.columns:
                continue
            
            # Filter data for this person and create a copy to avoid warnings
            person_data = df[df['person_id'] == person_id].copy()
            
            if person_data.empty:
                continue
            
            # Get all date columns
            date_columns = get_date_columns(person_data)
            
            for col in date_columns:
                # Convert column to datetime, ignoring errors
                person_data[col] = pd.to_datetime(person_data[col], errors='coerce')
                
                # Filter out invalid dates
                valid_dates = person_data[col][person_data[col].apply(is_valid_date)]
                
                if not valid_dates.empty:
                    all_dates.extend(valid_dates.tolist())
                    
        except Exception as e:
            logging.debug(f"Error processing {table} for person {person_id}: {str(e)}")
            continue
    
    if not all_dates:
        return None, None
    
    return min(all_dates), max(all_dates)


def create_observation_periods():
    """Create observation_period table with individual periods for each person"""
    
    # Tables to check for date ranges
    tables = [
        'observation',
        'measurement', 
        'drug_exposure',
        'death',
        'condition_occurrence',
        'procedure_occurrence'
    ]
    
    logging.info("Starting observation period creation...")
    
    # Read person table to get all person_ids
    try:
        person_df = pd.read_csv("combined_omop/person.csv")
        if person_df.empty:
            logging.error("Person table is empty")
            return
        
        logging.info(f"Found {len(person_df)} persons")
        
        # Create observation periods for each person
        observation_periods = []
        valid_periods = 0
        default_periods = 0
        
        for idx, person_row in person_df.iterrows():
            person_id = person_row['person_id']
            
            # Get individual date range for this person
            earliest_date, latest_date = get_person_date_range(person_id, tables)
            
            if earliest_date and latest_date:
                observation_periods.append({
                    'observation_period_id': idx + 1,
                    'person_id': person_id,
                    'observation_period_start_date': earliest_date.strftime('%Y-%m-%d'),
                    'observation_period_end_date': latest_date.strftime('%Y-%m-%d'),
                    'period_type_concept_id': 32851  # Standard observation period
                })
                valid_periods += 1
                
                if valid_periods % 100 == 0:
                    logging.info(f"Processed {valid_periods} persons...")
            else:
                # Use default date range for persons with no valid dates
                default_start = datetime(2016, 1, 1)
                default_end = datetime(2016, 1, 1)
                
                observation_periods.append({
                    'observation_period_id': idx + 1,
                    'person_id': person_id,
                    'observation_period_start_date': default_start.strftime('%Y-%m-%d'),
                    'observation_period_end_date': default_end.strftime('%Y-%m-%d'),
                    'period_type_concept_id': 32851  # Standard observation period
                })
                default_periods += 1
                logging.info(f"No valid dates found for person {person_id}, using default period: 2016-01-01 to 2016-01-01")
        
        if not observation_periods:
            logging.error("No observation periods created")
            return
        
        # Create dataframe
        result_df = pd.DataFrame(observation_periods)
        
        # Save to CSV
        output_path = "combined_omop/observation_period.csv"
        result_df.to_csv(output_path, index=False)
        
        logging.info(f"Created observation_period.csv with {len(result_df)} records")
        logging.info(f"Successfully created observation periods for {valid_periods} persons with valid dates")
        logging.info(f"Used default period for {default_periods} persons with no valid dates")
        
        # Log some statistics
        if not result_df.empty:
            start_dates = pd.to_datetime(result_df['observation_period_start_date'])
            end_dates = pd.to_datetime(result_df['observation_period_end_date'])
            
            logging.info(f"Overall date range: {start_dates.min().date()} to {end_dates.max().date()}")
            logging.info(f"Average observation period length: {(end_dates - start_dates).mean().days:.1f} days")
        
    except Exception as e:
        logging.error(f"Error creating observation periods: {str(e)}")


def main():
    """Main function"""
    logging.info("Starting observation period creation script...")
    create_observation_periods()
    logging.info("Observation period creation completed.")


if __name__ == "__main__":
    main()

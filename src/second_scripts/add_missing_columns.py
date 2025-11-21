#!/usr/bin/env python3
"""
Script to add missing columns to existing OMOP tables in the final_omop folder.
This script will add the required columns for each table and keep them empty.
"""

import pandas as pd
import os
import sys
from pathlib import Path

def add_missing_columns_to_table(file_path, required_columns):
    """
    Add missing columns to a CSV table file.
    
    Args:
        file_path (str): Path to the CSV file
        required_columns (list): List of required column names
    """
    try:
        # Read the existing CSV file
        df = pd.read_csv(file_path)
        print(f"Processing {file_path}")
        print(f"Current columns: {list(df.columns)}")
        
        # Find missing columns
        existing_columns = set(df.columns)
        missing_columns = [col for col in required_columns if col not in existing_columns]
        
        if missing_columns:
            print(f"Adding missing columns: {missing_columns}")
            
            # Add missing columns with empty values
            for col in missing_columns:
                df[col] = ''
            
            # Reorder columns to match the required order
            # Keep existing columns in their current order, then add missing ones
            final_columns = []
            for col in required_columns:
                if col in df.columns:
                    final_columns.append(col)
            
            # Add any remaining columns that weren't in required_columns
            for col in df.columns:
                if col not in final_columns:
                    final_columns.append(col)
            
            df = df[final_columns]
            
            # Save the updated file
            df.to_csv(file_path, index=False)
            print(f"Successfully updated {file_path}")
        else:
            print(f"No missing columns found for {file_path}")
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

def main():
    """Main function to add missing columns to all OMOP tables."""
    
    # Define the required columns for each table
    table_columns = {
        'person': [
            'person_id',
            'gender_concept_id',
            'year_of_birth',
            'month_of_birth',
            'day_of_birth',
            'birth_datetime',
            'race_concept_id',
            'ethnicity_concept_id',
            'location_id',
            'provider_id',
            'care_site_id',
            'person_source_value',
            'gender_source_value',
            'gender_source_concept_id',
            'race_source_value',
            'race_source_concept_id',
            'ethnicity_source_value',
            'ethnicity_source_concept_id'
        ],
        
        'observation_period': [
            'observation_period_id',
            'person_id',
            'observation_period_start_date',
            'observation_period_end_date',
            'period_type_concept_id'
        ],
        
        'condition_occurrence': [
            'condition_occurrence_id',
            'person_id',
            'condition_concept_id',
            'condition_start_date',
            'condition_start_datetime',
            'condition_end_date',
            'condition_end_datetime',
            'condition_type_concept_id',
            'condition_status_concept_id',
            'stop_reason',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'condition_source_value',
            'condition_source_concept_id',
            'condition_status_source_value'
        ],
        
        'drug_exposure': [
            'drug_exposure_id',
            'person_id',
            'drug_concept_id',
            'drug_exposure_start_date',
            'drug_exposure_start_datetime',
            'drug_exposure_end_date',
            'drug_exposure_end_datetime',
            'verbatim_end_date',
            'drug_type_concept_id',
            'stop_reason',
            'refills',
            'quantity',
            'days_supply',
            'sig',
            'route_concept_id',
            'lot_number',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'drug_source_value',
            'drug_source_concept_id',
            'route_source_value',
            'dose_unit_source_value'
        ],
        
        'procedure_occurrence': [
            'procedure_occurrence_id',
            'person_id',
            'procedure_concept_id',
            'procedure_date',
            'procedure_datetime',
            'procedure_end_date',
            'procedure_end_datetime',
            'procedure_type_concept_id',
            'modifier_concept_id',
            'quantity',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'procedure_source_value',
            'procedure_source_concept_id',
            'modifier_source_value'
        ],
        
        'device_exposure': [
            'device_exposure_id',
            'person_id',
            'device_concept_id',
            'device_exposure_start_date',
            'device_exposure_start_datetime',
            'device_exposure_end_date',
            'device_exposure_end_datetime',
            'device_type_concept_id',
            'unique_device_id',
            'production_id',
            'quantity',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'device_source_value',
            'device_source_concept_id',
            'unit_concept_id',
            'unit_source_value',
            'unit_source_concept_id'
        ],
        
        'measurement': [
            'measurement_id',
            'person_id',
            'measurement_concept_id',
            'measurement_date',
            'measurement_datetime',
            'measurement_time',
            'measurement_type_concept_id',
            'operator_concept_id',
            'value_as_number',
            'value_as_concept_id',
            'unit_concept_id',
            'range_low',
            'range_high',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'measurement_source_value',
            'measurement_source_concept_id',
            'unit_source_value',
            'unit_source_concept_id',
            'value_source_value',
            'measurement_event_id',
            'meas_event_field_concept_id'
        ],
        
        'observation': [
            'observation_id',
            'person_id',
            'observation_concept_id',
            'observation_date',
            'observation_datetime',
            'observation_type_concept_id',
            'value_as_number',
            'value_as_string',
            'value_as_concept_id',
            'qualifier_concept_id',
            'unit_concept_id',
            'provider_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'observation_source_value',
            'observation_source_concept_id',
            'unit_source_value',
            'qualifier_source_value',
            'value_source_value',
            'observation_event_id',
            'obs_event_field_concept_id'
        ],
        
        'death': [
            'person_id',
            'death_date',
            'death_datetime',
            'death_type_concept_id',
            'cause_concept_id',
            'cause_source_value',
            'cause_source_concept_id'
        ],
        
        'care_site': [
            'care_site_id',
            'care_site_name',
            'place_of_service_concept_id',
            'location_id',
            'care_site_source_value',
            'place_of_service_source_value'
        ],
        
        'concept': [
            'concept_id',
            'concept_name',
            'domain_id',
            'vocabulary_id',
            'concept_class_id',
            'standard_concept',
            'concept_code',
            'valid_start_date',
            'valid_end_date',
            'invalid_reason'
        ]
    }
    
    # Path to the final_omop folder (relative to project root)
    # Get the script's directory and navigate to project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    final_omop_path = project_root / 'final_omop'
    
    if not final_omop_path.exists():
        print(f"Error: {final_omop_path} directory does not exist")
        sys.exit(1)
    
    print("Starting to add missing columns to OMOP tables...")
    print("=" * 60)
    
    # Process each table
    for table_name, required_cols in table_columns.items():
        file_path = final_omop_path / f"{table_name}.csv"
        
        if file_path.exists():
            print(f"\nProcessing table: {table_name}")
            add_missing_columns_to_table(str(file_path), required_cols)
        else:
            print(f"\nTable {table_name}.csv not found in {final_omop_path}, skipping...")
    
    print("\n" + "=" * 60)
    print("Column addition process completed!")

if __name__ == "__main__":
    main()

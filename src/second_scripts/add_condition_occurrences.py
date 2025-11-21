import pandas as pd
import os
from pathlib import Path
import logging

NON_ALS_MND_IDS = [
    "CASE-NEUAT520TKK",
    "CASE-NEUAY510EHC",
    "CASE-NEUEK829PHX",
    "CASE-NEUJA933JEL",
    "CASE-NEUMU866VX7",
    "CASE-NEUVL876PUV"
]

ALS_IDS = [
    "CASE-NEUDY379GKK", "CASE-NEUHW627XG1", "CASE-NEUJA552VNY", "CASE-NEULF263XJQ", "CASE-NEULL442GF9", "CASE-NEUVM467KF6", "CASE-NEUYR856CJA" 
]


HEALTHY_CONTROL_IDS = [
    "CTRL-NEUCV809LL4"
]

def add_condition_occurrences():
    """
    Add condition occurrences for MND, ALS, and healthy controls based on hardcoded ID lists.
    - Non-ALS MND IDs: add condition_concept_id 374631
    - ALS IDs: add condition_concept_id 373182  
    - Healthy Controls IDs: remove condition_concept_id 373182 if present
    """
    
    print("Adding condition occurrences based on hardcoded ID lists...")
    
    # Read demographics--person.csv to get person_id mappings
    demographics_df = pd.read_csv("processed_source/demographics--person.csv")
    print(f"Loaded {len(demographics_df)} persons from demographics--person.csv")
    
    # Create a mapping from Participant_ID to person_id
    person_id_mapping = {}
    for _, row in demographics_df.iterrows():
        # Extract Participant_ID from person_source_value
        source_value = row['person_source_value']
        if 'demographics+Participant_ID (participant identifier): ' in source_value:
            participant_id = source_value.split('demographics+Participant_ID (participant identifier): ')[1].split(' |')[0]
            person_id_mapping[participant_id] = row['person_id']
    
    print(f"Created mapping for {len(person_id_mapping)} participants")
    
    # Create condition occurrence records
    condition_records = []
    
    # Process Non-ALS MND IDs
    for participant_id in NON_ALS_MND_IDS:
        if participant_id not in person_id_mapping:
            print(f"Warning: No person_id found for participant {participant_id}")
            continue
            
        person_id = person_id_mapping[participant_id]
        condition_records.append({
            'person_id': person_id,
            'condition_concept_id': 374631,
            'condition_source_value': f'subjects+subject_group_id (disease status): 17 (Non-ALS MND)',
            'condition_start_date': "1900-01-01",
            'condition_type_concept_id': 32851,
        })
    
    # Process ALS IDs
    for participant_id in ALS_IDS:
        if participant_id not in person_id_mapping:
            print(f"Warning: No person_id found for participant {participant_id}")
            continue
            
        person_id = person_id_mapping[participant_id]
        condition_records.append({
            'person_id': person_id,
            'condition_concept_id': 373182,
            'condition_source_value': f'subjects+subject_group_id (disease status): 1 (ALS)',
            'condition_start_date': "1900-01-01",
            'condition_type_concept_id': 32851,
        })
    
    # Read the combined condition_occurrence table
    combined_file = "combined_omop/condition_occurrence.csv"
    
    if not os.path.exists(combined_file):
        print(f"Error: {combined_file} not found. Make sure combine_subtables.py has been run first.")
        return
    
    print(f"Reading combined condition_occurrence table from {combined_file}")
    combined_df = pd.read_csv(combined_file)
    print(f"Loaded {len(combined_df)} existing condition occurrence records")
    
    # For healthy controls, remove ALS conditions if present
    if 'condition_concept_id' in combined_df.columns:
        for participant_id in HEALTHY_CONTROL_IDS:
            if participant_id in person_id_mapping:
                person_id = person_id_mapping[participant_id]
                # Remove ALS conditions (373182) and condition 2000000397 for healthy controls
                combined_df = combined_df[
                    ~((combined_df['person_id'] == person_id) & 
                      ((combined_df['condition_concept_id'] == 373182) | 
                       (combined_df['condition_concept_id'] == 2000000397)))
                ]
        print(f"After removing ALS conditions and condition 2000000397 from healthy controls: {len(combined_df)} records")
    
    # Add new condition records
    if condition_records:
        new_conditions_df = pd.DataFrame(condition_records)
        print(f"Created {len(new_conditions_df)} new condition occurrence records")
        
        # Combine with existing records
        final_conditions_df = pd.concat([combined_df, new_conditions_df], ignore_index=True)
        print(f"Combined total: {len(final_conditions_df)} condition occurrence records")
    else:
        final_conditions_df = combined_df
        print("No new condition occurrence records to add")
    
    # Save back to combined_omop directory
    final_conditions_df.to_csv(combined_file, index=False)
    print(f"Updated condition_occurrence table saved to {combined_file}")

if __name__ == "__main__":
    add_condition_occurrences()

import subprocess
import os
import pandas as pd
import glob
import shutil
from pathlib import Path

run_table_scripts = True
run_second_scripts = True

def cleanup_folders():
    """Delete all files in specified folders before starting the pipeline."""
    print("Cleaning up folders before starting pipeline...")
    print("=" * 50)
    
    # Folders to clean up
    folders_to_clean = [
        "combined_omop",
        "final_omop", 
        "processed_source",
        "redundant",
        "logs"
    ]
    
    for folder_name in folders_to_clean:
        folder_path = Path(folder_name)
        
        if not folder_path.exists():
            print(f"Warning: Folder '{folder_name}' does not exist.")
            continue
        
        if not folder_path.is_dir():
            print(f"Warning: '{folder_name}' is not a directory.")
            continue
        
        deleted_count = 0
        for item in folder_path.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                    print(f"Deleted file: {item}")
                    deleted_count += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    print(f"Deleted directory: {item}")
                    deleted_count += 1
            except Exception as e:
                print(f"Error deleting {item}: {e}")
        
        print(f"Total items deleted from {folder_name}: {deleted_count}")
    
    print("=" * 50)
    print("Cleanup completed!")
    print()

# Set working directory to one level above this script (i.e., OMOP_ETL_MVP/)
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def rename_csv_to_lowercase():
    """Rename all CSV files in source_tables directory to lowercase"""
    source_dir = "source_tables"
    
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    
    renamed_count = 0
    for file_path in csv_files:
        # Get the directory and filename
        directory = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        
        # Create lowercase filename
        lowercase_filename = filename.lower()
        new_file_path = os.path.join(directory, lowercase_filename)
        
        # Only rename if the filename is not already lowercase
        if filename != lowercase_filename:
            try:
                os.rename(file_path, new_file_path)
                print(f"Renamed: {filename} -> {lowercase_filename}")
                renamed_count += 1
            except OSError as e:
                print(f"Error renaming {filename}: {e}")
        else:
            print(f"Already lowercase: {filename}")
    
    print(f"Total files renamed: {renamed_count}")
    return renamed_count


table_scripts_dir = os.path.join("src", "table_scripts")
second_scripts_dir = os.path.join("src", "second_scripts")

table_scripts = [
    "aalsdxfx--observation.py",
    "aalshxfx--condition_occurrence.py",
    "aalshxfx--observation.py",
    "als_gene_mutations--measurement.py",
    "alsfrs_r--observation.py",
    "answer_als_medications_log--drug_exposure.py",
    "auxiliary_chemistry_labs--measurement.py",
    "demographics--person.py",
    "environmental_questionnaire--observation.py",
    "family_history_log--observation.py",
    "medical_history--condition_occurrence.py",
    "medical_history--device_exposure.py",
    "medical_history--drug_exposure.py",
    "medical_history--procedure_occurrence.py",
    "mortality--death.py",
    "neurolog--condition_occurrence.py",
    "vital_signs--measurement.py"
]

second_scripts = [
    "combine_subtables.py",
    "add_condition_occurrences.py",
    "create_table_ids.py",
    "person_id_map.py",
    "create_observation_periods.py",
    "transform_ids.py",
    "add_missing_columns.py"
]

def transform_table_ids(table_name, input_dir="../combined_omop", output_dir="../combined_omop"):
    input_file = os.path.join(input_dir, f"{table_name}.csv")
    output_file = os.path.join(output_dir, f"{table_name}.csv")

    print(f"Processing {table_name}...")

    # Check if the file is empty
    if os.path.getsize(input_file) == 0:
        print(f"Warning: {input_file} is empty. Skipping.")
        return

    # Read the CSV file
    df = pd.read_csv(input_file)
    # ... rest of your code ...


def copy_omop_reference_files():
    """Copy OMOP reference files from source_tables/omop_tables/ to final_omop/"""
    reference_files = ["concept.csv", "care_site.csv"]
    
    for filename in reference_files:
        source_file = f"source_tables/omop_tables/{filename}"
        destination_file = f"final_omop/{filename}"
        
        try:
            shutil.copy2(source_file, destination_file)
            print(f"Successfully copied {source_file} to {destination_file}")
        except FileNotFoundError:
            print(f"Error: {source_file} not found")
        except Exception as e:
            print(f"Error copying {filename}: {e}")

if __name__ == "__main__":
    # Step 0: Clean up folders before starting
    cleanup_folders()
    
    # Ensure logs and processed_source directories exist in OMOP_ETL_MVP/
    os.makedirs("logs", exist_ok=True)
    os.makedirs("processed_source", exist_ok=True)
    os.makedirs("combined_omop", exist_ok=True)
    os.makedirs("final_omop", exist_ok=True)

    # Step 1: Convert CSV filenames to lowercase
    print("Step 1: Converting CSV filenames to lowercase...")
    rename_csv_to_lowercase()
    print()

    if run_table_scripts:
        for script in table_scripts:
            script_path = os.path.join(table_scripts_dir, script)
            print(f"Running {script}...")
            subprocess.run(["python3", script_path], check=True)
    if run_second_scripts:
        for script in second_scripts:
            script_path = os.path.join(second_scripts_dir, script)
            print(f"Running {script}...")
            subprocess.run(["python3", script_path], check=True)
    
    # Step 3: Copy OMOP reference files to final_omop folder
    print("Step 3: Copying OMOP reference files to final_omop folder...")
    copy_omop_reference_files()
    print()
import pandas as pd
import os

# Read the original person.csv file
person_df = pd.read_csv("combined_omop/person.csv")

# Create a mapping DataFrame with original IDs and new sequential IDs
mapping_df = pd.DataFrame(
    {
        "Participant_ID": person_df["person_id"],
        "person_id": range(1, len(person_df) + 1),
    }
)

# Save the mapping to a new CSV file
mapping_df.to_csv("combined_omop/person_id_mapping.csv", index=False)

# Update the person.csv file with new IDs
person_df["person_id"] = mapping_df["person_id"]
person_df.to_csv("combined_omop/person.csv", index=False)

print(f"Created mapping file with {len(mapping_df)} entries")
print("Updated person.csv with new sequential IDs")

# Create a dictionary for faster lookups
id_mapping = dict(zip(mapping_df["Participant_ID"], mapping_df["person_id"]))

# List of tables that might contain person_id
tables_to_update = [
    "procedure_occurrence.csv",
    "observation.csv",
    "visit_occurrence.csv",
    "condition_occurrence.csv",
    "measurement.csv",
    "device_exposure.csv",
    "death.csv",
    "drug_exposure.csv",
    "observation_period.csv",
]

# Process each table
for table in tables_to_update:
    file_path = os.path.join("combined_omop", table)
    if os.path.exists(file_path):
        print(f"\nProcessing {table}...")
        df = pd.read_csv(file_path)

        if "person_id" in df.columns:
            # Update person_ids using the mapping
            df["person_id"] = df["person_id"].map(id_mapping)
            # Save the updated file
            df.to_csv(file_path, index=False)
            print(f"Updated {table} with new person_ids")
        else:
            print(f"Skipping {table} - no person_id column found")

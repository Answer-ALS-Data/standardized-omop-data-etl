#!/bin/bash
# Script to activate the OMOP ETL environment

echo "Activating OMOP ETL environment..."
source omop_etl_env/bin/activate
echo "Environment activated! You can now run the ETL pipeline."
echo ""
echo "To run the main pipeline:"
echo "python3 src/pipeline_process_subtables_to_final.py"
echo ""
echo "To deactivate the environment later, run:"
echo "deactivate" 
#!/bin/bash
# Script to start Airflow with correct AIRFLOW_HOME

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set AIRFLOW_HOME to the airflow directory
export AIRFLOW_HOME="$SCRIPT_DIR"

echo "Starting Airflow..."
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "DAGs folder: $AIRFLOW_HOME/dags"
echo ""

# Check if DAG file exists
if [ -f "$AIRFLOW_HOME/dags/maternoscope_pipeline.py" ]; then
    echo "✓ Found DAG file: maternoscope_pipeline.py"
else
    echo "✗ WARNING: DAG file not found at $AIRFLOW_HOME/dags/maternoscope_pipeline.py"
fi

echo ""
echo "Starting Airflow standalone..."
echo "Press Ctrl+C to stop"
echo ""

# Start Airflow standalone
airflow standalone


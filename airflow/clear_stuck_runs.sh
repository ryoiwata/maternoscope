#!/bin/bash
# Script to clear stuck/old DAG runs that are blocking new runs

AIRFLOW_HOME="${AIRFLOW_HOME:-$(dirname "$0")}"
export AIRFLOW_HOME

DAG_ID="maternoscope_pipeline"

echo "=== Clearing Stuck DAG Runs ==="
echo "DAG: $DAG_ID"
echo ""

# Option 1: Clear all running DAG runs (mark them as failed)
echo "Option 1: Mark all running runs as failed..."
echo "This will allow new runs to start."
echo ""

# Get all running DAG runs and mark them as failed
# Using Airflow API or direct database access would be better, but for CLI:
echo "To clear stuck runs, you have these options:"
echo ""
echo "A) Via Airflow UI (Recommended):"
echo "   1. Go to http://localhost:8080"
echo "   2. Click on 'maternoscope_pipeline' DAG"
echo "   3. Go to 'DAG Runs' tab"
echo "   4. Select all running runs"
echo "   5. Click 'Clear' or 'Mark as Failed'"
echo ""
echo "B) Via Python script (see below)"
echo ""
echo "C) Via SQL (if you have direct DB access):"
echo "   UPDATE dag_run SET state='failed' WHERE dag_id='$DAG_ID' AND state='running';"
echo ""

# Create a Python script to clear runs
cat > /tmp/clear_runs.py << 'PYTHON_SCRIPT'
import sys
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunType

@provide_session
def clear_running_runs(session=None):
    """Mark all running DAG runs as failed."""
    dag_id = "maternoscope_pipeline"
    
    # Get all running runs
    running_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.state == 'running'
    ).all()
    
    if not running_runs:
        print("No running DAG runs found.")
        return
    
    print(f"Found {len(running_runs)} running DAG run(s):")
    for run in running_runs:
        print(f"  - {run.run_id} (created: {run.run_type})")
    
    # Mark them as failed
    for run in running_runs:
        run.state = 'failed'
        session.merge(run)
    
    session.commit()
    print(f"\n✓ Marked {len(running_runs)} run(s) as failed")
    print("You can now trigger a new DAG run.")

if __name__ == "__main__":
    clear_running_runs()
PYTHON_SCRIPT

echo "Created Python script at /tmp/clear_runs.py"
echo ""
echo "To run it:"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  python /tmp/clear_runs.py"
echo ""


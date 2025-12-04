#!/bin/bash
# Script to check why a DAG run isn't starting

AIRFLOW_HOME="${AIRFLOW_HOME:-$(dirname "$0")}"
export AIRFLOW_HOME

echo "=== Checking DAG Status ==="
echo ""

# Check if DAG is paused
echo "1. Checking if DAG is paused..."
DAG_STATE=$(airflow dags state maternoscope_pipeline 2>/dev/null || echo "unknown")
if [ "$DAG_STATE" = "paused" ]; then
    echo "   ❌ DAG IS PAUSED - This is why tasks aren't running!"
    echo "   Fix: airflow dags unpause maternoscope_pipeline"
else
    echo "   ✓ DAG is running (not paused)"
fi
echo ""

# Check active DAG runs using Python
echo "2. Checking active DAG runs..."
python3 << 'PYTHON_SCRIPT' 2>/dev/null
import os
import sys
airflow_home = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.environ.get('AIRFLOW_HOME', '.')
sys.path.insert(0, os.path.join(airflow_home, '..', 'envs', 'maternoscope', 'lib', 'python3.12', 'site-packages'))
os.environ['AIRFLOW_HOME'] = airflow_home

try:
    from airflow.models import DagRun
    from airflow import settings
    session = settings.Session()
    
    # Get all runs
    all_runs = session.query(DagRun).filter(DagRun.dag_id == 'maternoscope_pipeline').all()
    
    # Count by state
    state_counts = {}
    blocking_runs = []
    for run in all_runs:
        state = run.state
        state_counts[state] = state_counts.get(state, 0) + 1
        if state in ['running', 'queued']:
            blocking_runs.append(run)
    
    print(f"   Total runs: {len(all_runs)}")
    print(f"   States breakdown:")
    for state, count in sorted(state_counts.items()):
        print(f"     {state}: {count}")
    
    if blocking_runs:
        print(f"   ⚠ Found {len(blocking_runs)} blocking run(s) (running/queued) - may block new runs (max_active_runs=1)")
        print(f"   First few blocking runs:")
        for run in blocking_runs[:5]:
            print(f"     - {run.run_id} ({run.state})")
    else:
        print(f"   ✓ No blocking runs found")
except Exception as e:
    print(f"   ⚠ Could not check runs: {e}")
PYTHON_SCRIPT
echo ""

# Check queued tasks
echo "3. Checking for queued tasks..."
QUEUED=$(airflow tasks list-queued-dag-runs -d maternoscope_pipeline 2>/dev/null | wc -l)
if [ "$QUEUED" -gt 0 ]; then
    echo "   ⚠ Found queued tasks"
else
    echo "   ✓ No queued tasks"
fi
echo ""

# Check scheduler status
echo "4. Checking scheduler status..."
if pgrep -f "airflow scheduler" > /dev/null; then
    echo "   ✓ Scheduler is running"
else
    echo "   ❌ Scheduler is NOT running!"
fi
echo ""

# Check latest DAG run
echo "5. Latest DAG runs:"
python3 << 'PYTHON_SCRIPT' 2>/dev/null
import os
import sys
airflow_home = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.environ.get('AIRFLOW_HOME', '.')
sys.path.insert(0, os.path.join(airflow_home, '..', 'envs', 'maternoscope', 'lib', 'python3.12', 'site-packages'))
os.environ['AIRFLOW_HOME'] = airflow_home

try:
    from airflow.models import DagRun
    from airflow import settings
    session = settings.Session()
    
    # Get latest 5 runs
    latest_runs = session.query(DagRun).filter(
        DagRun.dag_id == 'maternoscope_pipeline'
    ).order_by(DagRun.execution_date.desc()).limit(5).all()
    
    if latest_runs:
        print("   Latest runs:")
        for run in latest_runs:
            print(f"     {run.run_id} - {run.state} ({run.run_type})")
    else:
        print("   No runs found")
except Exception as e:
    print(f"   Could not list runs: {e}")
PYTHON_SCRIPT
echo ""

# Check for import errors
echo "6. Checking for DAG import errors..."
IMPORT_ERRORS=$(airflow dags list-import-errors 2>/dev/null | grep -i maternoscope)
if [ -n "$IMPORT_ERRORS" ]; then
    echo "   ❌ DAG has import errors:"
    echo "$IMPORT_ERRORS"
else
    echo "   ✓ No import errors"
fi
echo ""

echo "=== Summary ==="
if [ "$DAG_STATE" = "paused" ]; then
    echo "🔴 DAG IS PAUSED - Unpause it to run tasks"
    echo "   Command: airflow dags unpause maternoscope_pipeline"
else
    echo "🟡 If you see blocking runs above, clear them with:"
    echo "   python airflow/clear_runs.py"
    echo ""
    echo "🟢 DAG appears to be configured correctly"
    echo "   If tasks still don't run, check scheduler logs"
fi


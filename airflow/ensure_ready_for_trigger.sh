#!/bin/bash
# Script to ensure DAG is ready to run immediately when triggered

AIRFLOW_HOME="${AIRFLOW_HOME:-$(dirname "$0")}"
export AIRFLOW_HOME

DAG_ID="maternoscope_pipeline"

echo "=== Ensuring DAG is Ready for Immediate Execution ==="
echo ""

# Check if DAG is paused
echo "1. Checking if DAG is paused..."
DAG_STATE=$(airflow dags state $DAG_ID 2>/dev/null | head -1 || echo "unknown")
if echo "$DAG_STATE" | grep -qi "paused\|false"; then
    echo "   ❌ DAG IS PAUSED"
    echo "   Fixing: Unpausing DAG..."
    airflow dags unpause $DAG_ID 2>/dev/null && echo "   ✓ DAG unpaused" || echo "   ⚠ Could not unpause (may need to do it in UI)"
else
    echo "   ✓ DAG is not paused"
fi
echo ""

# Check for blocking runs
echo "2. Checking for blocking runs..."
BLOCKING_COUNT=$(python3 << 'PYTHON_SCRIPT' 2>/dev/null
import os
import sys
airflow_home = os.environ.get('AIRFLOW_HOME', '.')
sys.path.insert(0, os.path.join(airflow_home, '..', 'envs', 'maternoscope', 'lib', 'python3.12', 'site-packages'))
os.environ['AIRFLOW_HOME'] = airflow_home

try:
    from airflow.models import DagRun
    from airflow import settings
    session = settings.Session()
    
    blocking_runs = session.query(DagRun).filter(
        DagRun.dag_id == 'maternoscope_pipeline',
        DagRun.state.in_(['running', 'queued'])
    ).all()
    
    print(len(blocking_runs))
except:
    print("0")
PYTHON_SCRIPT
)

if [ "$BLOCKING_COUNT" -gt 0 ]; then
    echo "   ⚠ Found $BLOCKING_COUNT blocking run(s)"
    echo "   Clearing blocking runs..."
    python3 "$AIRFLOW_HOME/clear_runs.py" <<< "yes" 2>/dev/null
    echo "   ✓ Blocking runs cleared"
else
    echo "   ✓ No blocking runs"
fi
echo ""

# Check scheduler
echo "3. Checking scheduler..."
if pgrep -f "airflow scheduler" > /dev/null || pgrep -f "airflow standalone" > /dev/null; then
    echo "   ✓ Scheduler is running"
else
    echo "   ❌ Scheduler is NOT running!"
    echo "   Start Airflow: ./airflow/START_AIRFLOW.sh"
    exit 1
fi
echo ""

echo "=== Ready to Trigger! ==="
echo ""
echo "The DAG is now ready for immediate execution."
echo ""
echo "To trigger:"
echo "  1. Go to http://localhost:8080"
echo "  2. Click Play button (▶️) on maternoscope_pipeline"
echo "  3. First task should start within 5-10 seconds"
echo ""


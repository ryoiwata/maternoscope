#!/usr/bin/env python3
"""
Script to clear stuck/old DAG runs that are blocking new runs.
This marks all running DAG runs as failed so new runs can start.
"""

import os
import sys

# Add Airflow to path
airflow_home = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(airflow_home, '..', 'envs', 'maternoscope', 'lib', 'python3.12', 'site-packages'))

from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow import settings

@provide_session
def clear_running_runs(session=None):
    """Mark all running/queued DAG runs as failed."""
    dag_id = "maternoscope_pipeline"
    
    # Get all runs in states that block new runs
    # 'running' and 'queued' states can block new runs when max_active_runs=1
    blocking_states = ['running', 'queued']
    blocking_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.state.in_(blocking_states)
    ).all()
    
    # Also get all runs to show statistics
    all_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id
    ).all()
    
    # Count by state
    state_counts = {}
    for run in all_runs:
        state_counts[run.state] = state_counts.get(run.state, 0) + 1
    
    print(f"\nDAG Run Statistics:")
    for state, count in sorted(state_counts.items()):
        print(f"  {state}: {count}")
    
    if not blocking_runs:
        print("\n✓ No blocking DAG runs found (no running or queued runs).")
        return 0
    
    print(f"\nFound {len(blocking_runs)} blocking DAG run(s) (running or queued):")
    for run in blocking_runs[:10]:  # Show first 10
        print(f"  - {run.run_id} (state: {run.state}, type: {run.run_type})")
    if len(blocking_runs) > 10:
        print(f"  ... and {len(blocking_runs) - 10} more")
    
    # Ask for confirmation
    response = input(f"\nMark all {len(blocking_runs)} blocking run(s) as failed? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        return 1
    
    # Mark them as failed
    count = 0
    for run in blocking_runs:
        run.state = 'failed'
        session.merge(run)
        count += 1
    
    session.commit()
    print(f"\n✓ Marked {count} run(s) as failed")
    print("✓ You can now trigger a new DAG run!")
    return 0

if __name__ == "__main__":
    # Set AIRFLOW_HOME
    airflow_home = os.path.dirname(os.path.abspath(__file__))
    os.environ['AIRFLOW_HOME'] = airflow_home
    
    sys.exit(clear_running_runs())


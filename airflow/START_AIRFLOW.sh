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

# Function to kill processes using a specific port
kill_port_processes() {
    local port=$1
    local pids=$(lsof -ti :$port 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "Found processes using port $port: $pids"
        echo "Killing processes..."
        kill -9 $pids 2>/dev/null
        sleep 1
        # Verify they're gone
        local remaining=$(lsof -ti :$port 2>/dev/null)
        if [ -n "$remaining" ]; then
            echo "Warning: Some processes on port $port may still be running"
        else
            echo "✓ Port $port is now free"
        fi
    fi
}

# Function to kill any remaining Airflow processes
kill_airflow_processes() {
    echo "Checking for existing Airflow processes..."
    local airflow_pids=$(ps aux | grep -E "[a]irflow (standalone|scheduler|triggerer|dag-processor|api_server|serve-logs|worker)" | awk '{print $2}')
    if [ -n "$airflow_pids" ]; then
        echo "Found Airflow processes: $airflow_pids"
        echo "Killing Airflow processes..."
        kill -9 $airflow_pids 2>/dev/null
        sleep 1
        echo "✓ Airflow processes cleaned up"
    else
        echo "✓ No existing Airflow processes found"
    fi
}

# Clean up any existing Airflow instances
echo "Cleaning up any existing Airflow instances..."
kill_port_processes 8080  # Web server port
kill_port_processes 8793  # Log server port (scheduler)
kill_port_processes 8794  # Log server port (triggerer)
kill_airflow_processes

echo ""

# Clear any blocking DAG runs (if Airflow is already initialized)
echo "=== Clearing Blocking DAG Runs ==="
if [ -f "$AIRFLOW_HOME/clear_runs.py" ]; then
    # Check if Airflow database exists (indicates Airflow is initialized)
    # Try to run clear_runs.py - it will fail gracefully if DB doesn't exist
    python3 "$AIRFLOW_HOME/clear_runs.py" <<< "yes" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "   ✓ Blocking runs cleared (if any existed)"
    else
        echo "   ℹ Airflow not initialized yet or no blocking runs, skipping cleanup"
    fi
else
    echo "   ⚠ clear_runs.py not found, skipping..."
fi
echo ""

# Ensure DAG is ready (unpause if needed)
echo "=== Ensuring DAG is Ready ==="
DAG_ID="maternoscope_pipeline"
# Check if DAG is paused and unpause if needed (only if Airflow is initialized)
if command -v airflow >/dev/null 2>&1; then
    DAG_STATE=$(airflow dags state $DAG_ID 2>/dev/null | head -1 || echo "unknown")
    if echo "$DAG_STATE" | grep -qi "paused\|false"; then
        echo "   Unpausing DAG..."
        airflow dags unpause $DAG_ID 2>/dev/null && echo "   ✓ DAG unpaused" || echo "   ⚠ Could not unpause (will try after Airflow starts)"
    else
        echo "   ✓ DAG is not paused (or not found yet)"
    fi
else
    echo "   ℹ Airflow command not available yet, will check after startup"
fi
echo ""

echo "Starting Airflow standalone..."
echo "Press Ctrl+C to stop"
echo ""

# Start Airflow standalone
airflow standalone


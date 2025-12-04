# How to Trigger DAG and Ensure Immediate Execution

## Quick Steps to Trigger DAG Immediately

### 1. **Clear Any Blocking Runs First** (Important!)

Before triggering, ensure no runs are blocking:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
python airflow/clear_runs.py
```

This will clear any stuck/queued runs that might block new runs.

### 2. **Verify DAG is Not Paused**

In Airflow UI:
- Check that the toggle next to `maternoscope_pipeline` is **ON** (green)

Or via CLI:
```bash
airflow dags unpause maternoscope_pipeline
```

### 3. **Trigger the DAG**

**Via Airflow UI:**
1. Go to http://localhost:8080
2. Find `maternoscope_pipeline` DAG
3. Click the **Play button** (▶️) in the top right
4. The first task should start within **5-10 seconds**

**Via CLI:**
```bash
airflow dags trigger maternoscope_pipeline
```

### 4. **Monitor Execution**

- Watch the Grid view to see tasks change from "no_state" → "queued" → "running"
- First task (`scrape_reddit_posts`) should start within 5-10 seconds
- If it doesn't start, check scheduler logs

## Why Tasks Might Not Start Immediately

### Common Causes:

1. **Blocking Runs** (Most Common)
   - Another run is still active/queued
   - Fix: Run `python airflow/clear_runs.py`

2. **DAG is Paused**
   - Toggle is OFF in UI
   - Fix: Unpause in UI or `airflow dags unpause maternoscope_pipeline`

3. **Scheduler Delay**
   - Scheduler checks every 5 seconds (configurable)
   - Normal delay: 5-10 seconds is expected

4. **Task Dependencies**
   - First task has no dependencies, so should start immediately
   - Check if there are any upstream dependencies

## Current DAG Configuration

Your DAG is already configured for immediate execution:

- ✅ `depends_on_past=False` - Tasks don't wait for previous runs
- ✅ `max_active_runs=1` - Prevents too many concurrent runs
- ✅ No task-level delays
- ✅ Scheduler heartbeat: 5 seconds (reasonable)

## Troubleshooting

If tasks don't start within 10 seconds:

1. **Check for blocking runs:**
   ```bash
   python airflow/clear_runs.py
   ```

2. **Check scheduler logs:**
   ```bash
   tail -f airflow/logs/scheduler/latest/*.log | grep -i "maternoscope\|scrape_reddit"
   ```

3. **Verify DAG is loaded:**
   ```bash
   airflow dags list | grep maternoscope
   ```

4. **Check task state:**
   - In UI, click on the DAG run
   - Check if first task shows "no_state" (not scheduled yet) or "queued" (waiting)

## Expected Behavior

When you trigger a DAG:
1. **0-5 seconds**: DAG run created, scheduler picks it up
2. **5-10 seconds**: First task moves to "queued" state
3. **10-15 seconds**: First task moves to "running" state
4. **Task executes**: Duration depends on task (Reddit scraping can take 30+ seconds)

If it takes longer than 15 seconds to start, something is blocking it.


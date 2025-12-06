# How to Run Streamlit Dashboards

This guide explains how to run the Streamlit dashboards in the MaternoScope project.

## Prerequisites

1. **Python Environment**: Ensure you have Python 3.8+ installed
2. **Dependencies**: Install required packages
3. **Environment Variables**: Configure Snowflake credentials

## Installation

### 1. Install Dependencies

From the project root directory:

```bash
# Install all dependencies (includes streamlit)
pip install -r requirements.txt

# Or install streamlit separately if needed
pip install streamlit
```

### 2. Configure Environment Variables

Create a `.env` file in the project root with your Snowflake credentials:

```bash
# .env file
SNOWFLAKE_USERNAME=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MATERNOSCOPE
SNOWFLAKE_SCHEMA=ANALYTICS_GOLD  # or ANALYTICS_ML for audit dashboard
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

## Running the Dashboards

### Option 1: Clinician Review Dashboard

**File**: `src/dashboard/clinician_review_dashboard.py`

This dashboard allows clinicians to review and rate LLM annotations.

```bash
# From project root
streamlit run src/dashboard/clinician_review_dashboard.py

# Or with full path
streamlit run /home/riwata/Documents/projects/data_studies/maternoscope/src/dashboard/clinician_review_dashboard.py
```

**Features**:
- View Reddit posts with LLM annotations
- Filter by subreddit, topic, urgency
- Rate quality on multiple dimensions
- Submit ratings to Snowflake

**Access**: Opens at `http://localhost:8501`

### Option 2: LLM Annotation Audit Dashboard

**File**: `src/dashboard/llm_annotation_audit.py`

This dashboard is designed for **Streamlit in Snowflake (SiS)** but can also run locally.

#### For Streamlit in Snowflake (SiS):

1. Upload the file to your SiS environment
2. Update configuration variables in the script:
   ```python
   DATABASE_NAME = "MATERNOSCOPE"
   LLM_EVAL_SCHEMA = "ANALYTICS_ML"
   LLM_ANNOTATIONS_TABLE = "REDDIT_POSTS_ANNOTATED"
   HUMAN_EVALUATION_TABLE = "HUMAN_EVALUATION"
   ```
3. Deploy and run in SiS

#### For Local Development:

You'll need to modify the connection function. See `LLM_AUDIT_DASHBOARD_README.md` for details.

**Features**:
- Audit LLM annotations with corrections
- Write-back to Snowflake evaluation table
- Auto-advance to next un-evaluated post

## Quick Start

### Step-by-Step:

1. **Navigate to project root**:
   ```bash
   cd /home/riwata/Documents/projects/data_studies/maternoscope
   ```

2. **Activate your conda environment** (if using):
   ```bash
   conda activate maternoscope
   # or
   source envs/maternoscope/bin/activate
   ```

3. **Verify streamlit is installed**:
   ```bash
   streamlit --version
   ```

4. **Run the dashboard**:
   ```bash
   streamlit run src/dashboard/clinician_review_dashboard.py
   ```

5. **Access the dashboard**:
   - The terminal will show: "You can now view your Streamlit app in your browser."
   - Open: `http://localhost:8501`
   - Or click the local/network URL shown in the terminal

## Troubleshooting

### Port Already in Use

If port 8501 is already in use:

```bash
# Use a different port
streamlit run src/dashboard/clinician_review_dashboard.py --server.port 8502
```

### Module Not Found Errors

```bash
# Ensure you're in the project root
cd /home/riwata/Documents/projects/data_studies/maternoscope

# Install/reinstall dependencies
pip install -r requirements.txt
```

### Snowflake Connection Issues

1. **Check `.env` file exists** in project root
2. **Verify credentials** are correct
3. **Test connection**:
   ```python
   import snowflake.connector
   import os
   from dotenv import load_dotenv
   load_dotenv()
   
   conn = snowflake.connector.connect(
       user=os.getenv("SNOWFLAKE_USERNAME"),
       password=os.getenv("SNOWFLAKE_PASSWORD"),
       account=os.getenv("SNOWFLAKE_ACCOUNT"),
       warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
       database=os.getenv("SNOWFLAKE_DATABASE"),
       schema=os.getenv("SNOWFLAKE_SCHEMA")
   )
   print("Connected successfully!")
   conn.close()
   ```

### Dashboard Not Loading Data

1. **Check Snowflake permissions** - ensure you can read from the source tables
2. **Verify table names** match your Snowflake schema
3. **Check logs** in the Streamlit terminal output

## Running in Background

To run the dashboard in the background:

```bash
# Using nohup
nohup streamlit run src/dashboard/clinician_review_dashboard.py > dashboard.log 2>&1 &

# Or using screen
screen -S dashboard
streamlit run src/dashboard/clinician_review_dashboard.py
# Press Ctrl+A then D to detach
```

## Stopping the Dashboard

- **In terminal**: Press `Ctrl+C`
- **If running in background**: Find process and kill:
  ```bash
  ps aux | grep streamlit
  kill <PID>
  ```

## Additional Resources

- **Clinician Review Dashboard**: See `src/dashboard/README.md`
- **LLM Audit Dashboard**: See `src/dashboard/LLM_AUDIT_DASHBOARD_README.md`
- **Streamlit Docs**: https://docs.streamlit.io/


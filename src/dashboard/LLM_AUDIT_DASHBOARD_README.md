# LLM Annotation Audit Dashboard

A Streamlit in Snowflake (SiS) application for auditing LLM annotations with write-back functionality.

## Overview

This dashboard allows human reviewers to:
- View LLM-annotated Reddit posts
- Review LLM outputs (classifications, responses, keywords, safety flags)
- Provide corrections and evaluations
- Save evaluations back to Snowflake

## Features

1. **Automatic Post Loading**: Finds and displays the next un-evaluated post
2. **LLM Output Display**: Shows all LLM annotations in a clear layout
3. **Evaluation Form**: Structured form for corrections and feedback
4. **Write-Back**: Saves evaluations directly to Snowflake
5. **Auto-Advance**: Automatically loads next post after submission

## Setup for Streamlit in Snowflake (SiS)

### Prerequisites

1. Access to Streamlit in Snowflake (SiS)
2. Permissions to read from `LLM_ANNOTATIONS` table
3. Permissions to write to `HUMAN_EVALUATION` table

### Configuration

Before deploying, update the configuration variables in `llm_annotation_audit.py`:

```python
DATABASE_NAME = "YOUR_DATABASE_NAME"  # e.g., "MATERNOSCOPE"
LLM_EVAL_SCHEMA = "LLM_EVAL_SCHEMA"  # e.g., "ANALYTICS_ML"
LLM_ANNOTATIONS_TABLE = "REDDIT_POSTS_ANNOTATED"
HUMAN_EVALUATION_TABLE = "HUMAN_EVALUATION"
```

### Database Schema

#### Source Table: `LLM_ANNOTATIONS` (e.g., `REDDIT_POSTS_ANNOTATED`)

Expected columns:
- `post_id` (VARCHAR)
- `text_for_llm` (VARCHAR)
- `primary_group` (VARCHAR)
- `primary_topic` (VARCHAR)
- `secondary_topics` (ARRAY)
- `trimester` (VARCHAR)
- `sentiment` (VARCHAR)
- `urgency_0_3` (INTEGER)
- `keywords` (ARRAY)
- `safety_flags` (ARRAY)
- `post_summary` (VARCHAR)
- `care_response` (VARCHAR)
- `model_name` (VARCHAR)
- `model_version` (VARCHAR)
- `prompt_hash` (VARCHAR)
- `annotated_at` (TIMESTAMP_TZ)

#### Target Table: `HUMAN_EVALUATION`

The dashboard will automatically create this table if it doesn't exist:

```sql
CREATE TABLE IF NOT EXISTS YOUR_DATABASE_NAME.LLM_EVAL_SCHEMA.HUMAN_EVALUATION (
    post_id VARCHAR(255) PRIMARY KEY,
    corrected_primary_group VARCHAR(50),
    corrected_primary_topic VARCHAR(100),
    corrected_trimester VARCHAR(20),
    corrected_keywords ARRAY,
    accuracy_score INTEGER,
    evaluation_comments VARCHAR(5000),
    evaluated_at TIMESTAMP_TZ,
    evaluator_name VARCHAR(255)
)
```

### Deployment Steps

1. **Upload to SiS**:
   - Navigate to Streamlit in Snowflake
   - Create a new Streamlit app
   - Upload `llm_annotation_audit.py` as the main file

2. **Update Configuration**:
   - Edit the configuration variables at the top of the file
   - Set `DATABASE_NAME`, `LLM_EVAL_SCHEMA`, and table names

3. **Set Permissions**:
   - Ensure the app has read access to the LLM annotations table
   - Ensure the app has write access to create/write to the evaluation table

4. **Run the App**:
   - The app will automatically connect using `get_active_session()`
   - No additional credentials needed (uses SiS session)

## Usage

### Workflow

1. **Load Post**: The dashboard automatically loads the next un-evaluated post
2. **Review**: Review the original post text and LLM annotations
3. **Evaluate**: Fill out the evaluation form:
   - Select corrected primary group, topic, and trimester
   - Enter corrected keywords (comma-separated)
   - Rate accuracy (1-5)
   - Add evaluation comments
4. **Submit**: Click "Save Evaluation & Load Next Post"
5. **Continue**: The next un-evaluated post loads automatically

### Evaluation Form Fields

- **Correction: Primary Group**: Select from taxonomy groups
- **Correction: Primary Topic**: Select from topics (filtered by group)
- **Correction: Trimester**: Select from trimester options
- **Corrected Keywords**: Comma-separated list of keywords
- **Accuracy Score**: Radio button selection (1-5)
- **Evaluation Comments**: Free-text feedback

### Taxonomy

The dashboard uses the same taxonomy as the LLM annotation system:

**Primary Groups**:
- clinical
- mental_health
- lifestyle_parenting
- access_navigation
- community_info
- meta_context

**Trimester Options**:
- preconception
- first
- second
- third
- pregnant
- postpartum
- miscarriage
- unclear

## Troubleshooting

### No Posts Found

- Verify that posts exist in the LLM annotations table
- Check that the LEFT JOIN query is correctly identifying unevaluated posts
- Ensure the evaluation table schema matches expectations

### Write-Back Fails

- Check Snowflake permissions for INSERT operations
- Verify the evaluation table exists (will be auto-created)
- Check for SQL injection issues with special characters in text fields

### Connection Issues

- Ensure you're running in Streamlit in Snowflake (SiS) environment
- `get_active_session()` requires SiS context
- For local testing, you'll need to modify the connection logic

## Local Development (Optional)

For local testing, you would need to modify the `get_snowflake_session()` function:

```python
def get_snowflake_session() -> Session:
    """Get Snowflake session for local development."""
    from snowflake.snowpark import Session
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USERNAME"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE")
    }
    
    return Session.builder.configs(connection_parameters).create()
```

Then run with:
```bash
streamlit run src/dashboard/llm_annotation_audit.py
```

## Notes

- The dashboard uses session state to manage post loading
- Evaluations are saved immediately on form submission
- The app automatically advances to the next post after saving
- All timestamps use Snowflake's `CURRENT_TIMESTAMP()`
- Evaluator name is captured using `CURRENT_USER()`


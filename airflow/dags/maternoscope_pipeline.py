"""
MaternoScope Data Pipeline DAG

This DAG orchestrates the complete data pipeline:
1. Reddit Data Ingestion (Bronze Layer)
2. dbt Staging Transformations (Silver Layer)
3. LLM Annotation (ML Layer)
4. dbt Marts Transformations (Gold Layer)

Excludes: Visualization and Dashboard components
"""

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sdk.exceptions import AirflowRuntimeError
import pendulum
import os
import sys

# Import PythonOperator with fallback for compatibility
try:
    from airflow.providers.standard.operators.python import (
        PythonOperator
    )
except ImportError:
    # Fallback for older Airflow versions or if standard providers
    # not installed
    from airflow.operators.python import PythonOperator

# Add project root to path for imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

# Default arguments for the DAG
default_args = {
    'owner': 'maternoscope',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.now() - timedelta(days=1),
}

# DAG definition
dag = DAG(
    'maternoscope_pipeline',
    default_args=default_args,
    description='MaternoScope Reddit data ingestion and annotation pipeline',
    schedule='@daily',  # Run daily
    catchup=False,
    tags=['maternoscope', 'reddit', 'llm', 'dbt'],
    max_active_runs=1,
)

# Configuration - can be overridden via Airflow Variables
# Set these in Airflow UI: Admin -> Variables
# Example: reddit_subreddits = "pregnant,BabyBumps,pregnancy"


def get_airflow_var(key, default):
    """Get Airflow Variable or return default."""
    try:
        return Variable.get(key)
    except (KeyError, AirflowRuntimeError) as e:
        # Handle both KeyError (older Airflow) and AirflowRuntimeError
        # (newer Airflow with VARIABLE_NOT_FOUND)
        error_str = str(e)
        if isinstance(e, KeyError) or 'VARIABLE_NOT_FOUND' in error_str:
            return default
        # Re-raise if it's a different error
        raise


DEFAULT_SUBREDDITS = (
    'pregnant,BabyBumps,TryingForABaby,beyondthebump,'
    'newborns,Miscarriage,NewParents'
)
REDDIT_SUBREDDITS = get_airflow_var(
    'reddit_subreddits', DEFAULT_SUBREDDITS
).split(',')
REDDIT_TIME_FILTER = get_airflow_var('reddit_time_filter', 'day')
REDDIT_MAX_POSTS = int(get_airflow_var('reddit_max_posts', '1000'))
REDDIT_FLAIR_FILTER = get_airflow_var('reddit_flair_filter', None) or None
SNOWFLAKE_TABLE = get_airflow_var('snowflake_table', 'REDDIT_POSTS')
LLM_ANNOTATION_LIMIT = int(get_airflow_var('llm_annotation_limit', '100'))
LLM_BATCH_SIZE = int(get_airflow_var('llm_batch_size', '20'))
DBT_PROJECT_DIR = os.path.join(project_root, 'dbt_maternoscope')


# Find dbt executable - check conda environment first, then system PATH
def find_dbt_executable():
    """Find dbt executable in conda environment or system PATH."""
    import shutil

    # Check conda environment first (most likely location)
    conda_dbt = os.path.join(
        project_root, 'envs', 'maternoscope', 'bin', 'dbt'
    )
    if os.path.exists(conda_dbt) and os.access(conda_dbt, os.X_OK):
        return conda_dbt

    # Fall back to system PATH
    dbt_path = shutil.which('dbt')
    if dbt_path:
        return dbt_path

    return None


def run_dbt_staging(**context):
    """
    Run dbt staging models to create PII-redacted staging tables.
    This runs: stg_reddit_posts and stg_reddit_posts_pii
    """
    import subprocess
    from dotenv import load_dotenv

    # Load environment variables from .env file
    env_file = os.path.join(project_root, '.env')
    if os.path.exists(env_file):
        load_dotenv(env_file)
        print(f"✓ Loaded .env file from {env_file}")
    else:
        print(f"⚠ Warning: .env file not found at {env_file}")

    # Find dbt executable
    dbt_path = find_dbt_executable()
    if not dbt_path:
        raise Exception(
            "dbt command not found. Please ensure dbt is installed. "
            "Expected location: envs/maternoscope/bin/dbt or in system PATH. "
            "Install with: pip install dbt-snowflake"
        )
    print(f"✓ Found dbt at: {dbt_path}")

    # Verify dbt project directory exists
    if not os.path.exists(DBT_PROJECT_DIR):
        raise Exception(f"dbt project directory not found: {DBT_PROJECT_DIR}")

    # Check for dbt_project.yml
    dbt_project_yml = os.path.join(DBT_PROJECT_DIR, 'dbt_project.yml')
    if not os.path.exists(dbt_project_yml):
        raise Exception(
            f"dbt_project.yml not found in: {DBT_PROJECT_DIR}"
        )

    print(f"✓ Using dbt project directory: {DBT_PROJECT_DIR}")

    # Run dbt for staging models only
    cmd = [
        dbt_path, 'run', '--select', 'staging.*',
        '--project-dir', DBT_PROJECT_DIR
    ]

    print(f"Running command: {' '.join(cmd)}")
    print(f"Working directory: {DBT_PROJECT_DIR}")

    # Pass environment variables to subprocess
    env = os.environ.copy()

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR,
        env=env
    )

    # Print both stdout and stderr for debugging
    if result.stdout:
        print("=== dbt stdout ===")
        print(result.stdout)
    if result.stderr:
        print("=== dbt stderr ===")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt staging failed (exit code {result.returncode}):\n"
            f"STDERR: {result.stderr}\n"
            f"STDOUT: {result.stdout}"
        )

    print("✓ dbt staging completed successfully")
    return result.stdout


def annotate_posts_with_llm(**context):
    """
    Run LLM annotation on posts that need annotation.
    This task runs the annotate_reddit_posts.py script.
    """
    import subprocess

    script_path = os.path.join(
        project_root, 'src', 'llm', 'annotate_reddit_posts.py'
    )

    cmd = [
        'python', script_path,
        '--limit', str(LLM_ANNOTATION_LIMIT),
        '--batch-size', str(LLM_BATCH_SIZE),
        '--save-logs'
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=project_root
    )

    if result.returncode != 0:
        print(f"LLM annotation error: {result.stderr}")
        raise Exception(f"LLM annotation failed: {result.stderr}")

    print(f"LLM annotation output: {result.stdout}")
    return result.stdout


def run_dbt_marts(**context):
    """
    Run dbt marts models to create final gold layer tables.
    This runs all marts models including fct_reddit_posts_annotated
    """
    import subprocess
    from dotenv import load_dotenv

    # Load environment variables from .env file
    env_file = os.path.join(project_root, '.env')
    if os.path.exists(env_file):
        load_dotenv(env_file)
        print(f"✓ Loaded .env file from {env_file}")
    else:
        print(f"⚠ Warning: .env file not found at {env_file}")

    # Find dbt executable
    dbt_path = find_dbt_executable()
    if not dbt_path:
        raise Exception(
            "dbt command not found. Please ensure dbt is installed. "
            "Expected location: envs/maternoscope/bin/dbt or in system PATH. "
            "Install with: pip install dbt-snowflake"
        )
    print(f"✓ Found dbt at: {dbt_path}")

    # Verify dbt project directory exists
    if not os.path.exists(DBT_PROJECT_DIR):
        raise Exception(f"dbt project directory not found: {DBT_PROJECT_DIR}")

    print(f"✓ Using dbt project directory: {DBT_PROJECT_DIR}")

    # Run dbt for marts models only
    cmd = [
        dbt_path, 'run', '--select', 'marts.*',
        '--project-dir', DBT_PROJECT_DIR
    ]

    print(f"Running command: {' '.join(cmd)}")
    print(f"Working directory: {DBT_PROJECT_DIR}")

    # Pass environment variables to subprocess
    env = os.environ.copy()

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR,
        env=env
    )

    # Print both stdout and stderr for debugging
    if result.stdout:
        print("=== dbt stdout ===")
        print(result.stdout)
    if result.stderr:
        print("=== dbt stderr ===")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt marts failed (exit code {result.returncode}):\n"
            f"STDERR: {result.stderr}\n"
            f"STDOUT: {result.stdout}"
        )

    print("✓ dbt marts completed successfully")
    return result.stdout


def scrape_reddit_posts(**context):
    """
    Scrape Reddit posts and save to Snowflake.
    Uses Python scraper directly for better Airflow integration.
    """
    from src.ingestion.praw_scraper import (
        TopPostsScraper, SnowflakeConnector
    )

    scraper = TopPostsScraper()
    snowflake_connector = None

    try:
        snowflake_connector = SnowflakeConnector()

        total_posts = 0
        successful_subreddits = []
        failed_subreddits = []

        for subreddit in REDDIT_SUBREDDITS:
            subreddit = subreddit.strip()
            if not subreddit:
                continue

            try:
                print(f"Scraping r/{subreddit} for {REDDIT_TIME_FILTER}...")

                # Get posts
                posts = scraper.get_top_posts(
                    subreddit_name=subreddit,
                    time_filter=REDDIT_TIME_FILTER,
                    max_posts=REDDIT_MAX_POSTS,
                    flair_filter=REDDIT_FLAIR_FILTER
                )

                if posts:
                    # Save to Snowflake
                    snowflake_connector.save_to_snowflake(
                        posts_data=posts,
                        table_name=SNOWFLAKE_TABLE,
                        time_filter=REDDIT_TIME_FILTER
                    )
                    total_posts += len(posts)
                    successful_subreddits.append(subreddit)
                    print(f"✓ Saved {len(posts)} posts from r/{subreddit}")
                else:
                    print(f"⚠ No posts found for r/{subreddit}")
                    successful_subreddits.append(subreddit)

            except Exception as e:
                print(f"✗ Error scraping r/{subreddit}: {e}")
                failed_subreddits.append(subreddit)
                # Continue with other subreddits instead of failing entire task
                continue

        print("\n=== Scraping Summary ===")
        print(f"Total posts scraped: {total_posts}")
        print(f"Successful subreddits: {len(successful_subreddits)}")
        print(f"Failed subreddits: {len(failed_subreddits)}")
        if failed_subreddits:
            print(f"Failed: {', '.join(failed_subreddits)}")

        # Return summary for downstream tasks
        return {
            'total_posts': total_posts,
            'successful_subreddits': successful_subreddits,
            'failed_subreddits': failed_subreddits,
            'subreddit_count': len(REDDIT_SUBREDDITS)
        }

    except Exception as e:
        print(f"Error in scrape_reddit_posts: {e}")
        raise
    finally:
        if snowflake_connector:
            snowflake_connector.close()


# Task definitions
task_scrape_reddit = PythonOperator(
    task_id='scrape_reddit_posts',
    python_callable=scrape_reddit_posts,
    dag=dag,
)

task_dbt_staging = PythonOperator(
    task_id='run_dbt_staging',
    python_callable=run_dbt_staging,
    dag=dag,
)

task_llm_annotation = PythonOperator(
    task_id='annotate_posts_with_llm',
    python_callable=annotate_posts_with_llm,
    dag=dag,
)

task_dbt_marts = PythonOperator(
    task_id='run_dbt_marts',
    python_callable=run_dbt_marts,
    dag=dag,
)

# Task dependencies
# 1. Scrape Reddit -> 2. dbt Staging -> 3. LLM Annotation -> 4. dbt Marts
task_scrape_reddit >> task_dbt_staging >> task_llm_annotation >> task_dbt_marts

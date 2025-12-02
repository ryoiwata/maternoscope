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
from airflow.operators.python import PythonOperator
import pendulum
import os
import sys

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
# These can be set in Airflow UI: Admin -> Variables
REDDIT_SUBREDDITS = [
    'pregnant', 'BabyBumps', 'pregnancy', 'Mommit', 'beyondthebump'
]
REDDIT_TIME_FILTER = 'day'  # hour, day, week, month, year, all
REDDIT_MAX_POSTS = 1000
REDDIT_FLAIR_FILTER = None  # Optional flair filter
# Target table in INGEST schema (full path)
SNOWFLAKE_TABLE = 'INGEST.REDDIT_POSTS'
LLM_ANNOTATION_LIMIT = 100  # Number of posts to annotate per run
LLM_BATCH_SIZE = 20
DBT_PROJECT_DIR = os.path.join(project_root, 'dbt_maternoscope')


def scrape_reddit_posts(**context):
    """
    Scrape Reddit posts and save to Snowflake.
    This task runs the praw_scraper.py script for each configured subreddit.
    """
    from src.ingestion.praw_scraper import (
        TopPostsScraper, SnowflakeConnector
    )

    subreddits = REDDIT_SUBREDDITS
    time_filter = REDDIT_TIME_FILTER
    max_posts = REDDIT_MAX_POSTS
    flair_filter = REDDIT_FLAIR_FILTER
    table_name = SNOWFLAKE_TABLE

    scraper = TopPostsScraper()
    snowflake_connector = None

    try:
        snowflake_connector = SnowflakeConnector()

        total_posts = 0
        for subreddit in subreddits:
            print(f"Scraping r/{subreddit} for {time_filter}...")

            # Get posts
            posts = scraper.get_top_posts(
                subreddit_name=subreddit,
                time_filter=time_filter,
                max_posts=max_posts,
                flair_filter=flair_filter
            )

            if posts:
                # Save to Snowflake
                snowflake_connector.save_to_snowflake(
                    posts_data=posts,
                    table_name=table_name,
                    time_filter=time_filter
                )
                total_posts += len(posts)
                print(f"Saved {len(posts)} posts from r/{subreddit}")
            else:
                print(f"No posts found for r/{subreddit}")

        print(f"Total posts scraped: {total_posts}")
        return {'total_posts': total_posts, 'subreddits': subreddits}

    except Exception as e:
        print(f"Error in scrape_reddit_posts: {e}")
        raise
    finally:
        if snowflake_connector:
            snowflake_connector.close()


def run_dbt_staging(**context):
    """
    Run dbt staging models to create PII-redacted staging tables.
    This runs: stg_reddit_posts and stg_reddit_posts_pii
    """
    import subprocess

    # Run dbt for staging models only
    cmd = [
        'dbt', 'run', '--select', 'staging.*',
        '--project-dir', DBT_PROJECT_DIR
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR
    )

    if result.returncode != 0:
        print(f"dbt staging error: {result.stderr}")
        raise Exception(f"dbt staging failed: {result.stderr}")

    print(f"dbt staging output: {result.stdout}")
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

    # Run dbt for marts models only
    cmd = [
        'dbt', 'run', '--select', 'marts.*',
        '--project-dir', DBT_PROJECT_DIR
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR
    )

    if result.returncode != 0:
        print(f"dbt marts error: {result.stderr}")
        raise Exception(f"dbt marts failed: {result.stderr}")

    print(f"dbt marts output: {result.stdout}")
    return result.stdout


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

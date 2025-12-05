#!/usr/bin/env python3
"""
Reddit Post LLM Annotation Script

This script:
1. Reads from Snowflake table: BRONZE.STG_REDDIT_POSTS_PII
2. Calls OpenAI API to annotate posts with taxonomy categorization
3. Writes results back to Snowflake in appropriate schema

Usage:
    python src/llm/annotate_reddit_posts.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from typing import Dict, Any, List
import hashlib
import yaml
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables
load_dotenv()

# Default logging setup (will be reconfigured in main with user-specified directory)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class LLMAnnotator:
    def __init__(self, experiment_name: str = "default_run"):
        """Initialize OpenAI and Snowflake connections.
        
        Args:
            experiment_name: Name of the experiment configuration to load from YAML
        """
        # Load configuration
        self.config = self.load_config(experiment_name)
        self.model_name = self.config['model_name']
        self.model_version = self.config['version']
        
        # Load prompts from external files
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        self.system_text = self._load_prompt_file(
            os.path.join(project_root, self.config['system_prompt_file'])
        )
        self.user_template = self._load_prompt_file(
            os.path.join(project_root, self.config['user_prompt_file'])
        )
        
        # Calculate prompt hash from concatenated system and user prompts
        combined_prompts = self.system_text + "\n\n" + self.user_template
        self.prompt_hash = hashlib.sha256(combined_prompts.encode()).hexdigest()[:16]
        
        # Initialize LangChain components
        self.llm = ChatOpenAI(
            model=self.model_name,
            api_key=os.getenv("OPENAI_API_KEY"),
            organization=os.getenv("OPENAI_ORG_ID", None),
            temperature=0,
            model_kwargs={"response_format": {"type": "json_object"}}
        )
        
        # Create prompt template with system and user messages
        self.prompt_template = ChatPromptTemplate.from_messages([
            ("system", self.system_text),
            ("user", self.user_template)
        ])
        
        # Snowflake connection
        self.snowflake_conn = None
        self.connect_snowflake()
    
    def load_config(self, experiment_name: str) -> Dict[str, Any]:
        """Load experiment configuration from YAML file."""
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        config_path = os.path.join(project_root, 'config', 'llm_experiments.yaml')
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        if experiment_name not in config_data:
            raise KeyError(
                f"Experiment '{experiment_name}' not found in config file. "
                f"Available experiments: {list(config_data.keys())}"
            )
        
        return config_data[experiment_name]
    
    def _load_prompt_file(self, file_path: str) -> str:
        """Load prompt content from a text file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Prompt file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
        
    def connect_snowflake(self):
        """Connect to Snowflake."""
        try:
            self.snowflake_conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def fetch_posts_to_annotate(self, limit: int = None) -> pd.DataFrame:
        """Fetch posts that need annotation from Snowflake.
        
        Uses LEFT JOIN with compound key (post_id, model_name, prompt_hash)
        to implement idempotent read logic. Only returns posts that don't
        have an existing annotation with the current model and prompt version.
        """
        try:
            # Use BRONZE schema (matches dbt staging models)
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            
            # Safely escape model_name and prompt_hash for SQL
            model_name_escaped = self.model_name.replace("'", "''")
            prompt_hash_escaped = self.prompt_hash.replace("'", "''")
            
            # Use LEFT JOIN with compound key for idempotent filtering
            query = f"""
            SELECT 
                t1.post_id,
                t1.text_for_llm,
                t1.text_raw
            FROM {database}.BRONZE.STG_REDDIT_POSTS_PII t1
            LEFT JOIN {database}.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED t2
                ON t1.post_id = t2.post_id
                AND t2.model_name = '{model_name_escaped}'
                AND t2.prompt_hash = '{prompt_hash_escaped}'
            WHERE t1.needs_annotation = TRUE
                AND t2.post_id IS NULL
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            if limit:
                logger.info(
                    f"Fetching posts to annotate (limit={limit}, "
                    f"model={self.model_name}, prompt_hash={self.prompt_hash[:8]}...)..."
                )
            else:
                logger.info(
                    f"Fetching all posts to annotate (no limit, "
                    f"model={self.model_name}, prompt_hash={self.prompt_hash[:8]}...)..."
                )
            df = pd.read_sql(query, self.snowflake_conn)
            # Snowflake returns uppercase column names, convert to lowercase
            df.columns = [col.lower() for col in df.columns]
            logger.info(f"Found {len(df)} posts to annotate")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching posts: {e}")
            raise
    
    def annotate_post(self, post_id: str, post_text: str) -> Dict[str, Any]:
        """Call OpenAI API to annotate a post using LangChain LCEL."""
        try:
            # Limit text length
            post_text_limited = post_text[:2000]
            
            # Get formatted messages from prompt template
            formatted_messages = self.prompt_template.format_messages(
                post_id=post_id,
                post_text=post_text_limited
            )
            
            # Invoke LLM to get response with metadata
            llm_response = self.llm.invoke(formatted_messages)
            
            # Extract token usage from response metadata
            input_tokens = 0
            output_tokens = 0
            if hasattr(llm_response, 'response_metadata'):
                usage = llm_response.response_metadata.get('token_usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)
            
            # Parse JSON from LLM response content
            content = llm_response.content
            annotation = json.loads(content)
            
            # Add metadata
            annotation['post_id'] = post_id  # Ensure post_id is included
            annotation['model_name'] = self.model_name
            annotation['model_version'] = self.model_version
            annotation['prompt_hash'] = self.prompt_hash
            annotation['input_tokens'] = input_tokens
            annotation['output_tokens'] = output_tokens
            annotation['annotated_at'] = datetime.now(timezone.utc).isoformat()
            
            logger.info(f"Annotated post {post_id} (tokens: {input_tokens} + {output_tokens})")
            return annotation
            
        except Exception as e:
            logger.error(f"Error annotating post {post_id}: {e}")
            return None
    
    def create_annotation_table(self):
        """Create the ML annotation table if it doesn't exist."""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Create schema if it doesn't exist
            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS ANALYTICS_ML"
            cursor.execute(create_schema_sql)
            logger.info("Schema ANALYTICS_ML created or already exists")
            
            # Create table
            # Note: Primary key is on post_id only, but idempotency is enforced
            # via MERGE using compound key (post_id, model_name, prompt_hash)
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ANALYTICS_ML.REDDIT_POSTS_ANNOTATED (
                post_id VARCHAR(255) PRIMARY KEY,
                primary_group VARCHAR(50),
                primary_topic VARCHAR(100),
                secondary_topics ARRAY,
                trimester VARCHAR(20),
                sentiment VARCHAR(20),
                urgency_0_3 INTEGER,
                keywords ARRAY,
                safety_flags ARRAY,
                post_summary VARCHAR(1000),
                care_response VARCHAR(2000),
                text_for_llm VARCHAR(16777216),
                model_name VARCHAR(100),
                model_version VARCHAR(50),
                prompt_hash VARCHAR(50),
                input_tokens INTEGER,
                output_tokens INTEGER,
                annotated_at TIMESTAMP_TZ
            )
            """
            
            cursor.execute(create_table_sql)
            
            # Check and add missing columns for existing tables (migration support)
            required_columns = {
                'TEXT_FOR_LLM': 'VARCHAR(16777216)',
                'MODEL_NAME': 'VARCHAR(100)',
                'MODEL_VERSION': 'VARCHAR(50)',
                'PROMPT_HASH': 'VARCHAR(50)'
            }
            
            try:
                for column_name, column_type in required_columns.items():
                    check_column_sql = f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = 'ANALYTICS_ML' 
                    AND TABLE_NAME = 'REDDIT_POSTS_ANNOTATED' 
                    AND COLUMN_NAME = '{column_name}'
                    """
                    cursor.execute(check_column_sql)
                    column_exists = cursor.fetchone()[0] > 0
                    
                    if not column_exists:
                        alter_table_sql = f"""
                        ALTER TABLE ANALYTICS_ML.REDDIT_POSTS_ANNOTATED 
                        ADD COLUMN {column_name.lower()} {column_type}
                        """
                        cursor.execute(alter_table_sql)
                        logger.info(f"Added {column_name.lower()} column to existing table")
            except Exception as e:
                logger.warning(f"Could not check/add required columns: {e}")
            
            cursor.close()
            logger.info("Annotation table created or already exists (idempotency via compound key)")
            
        except Exception as e:
            logger.error(f"Error creating annotation table: {e}")
            raise
    
    def save_annotations(self, annotations: List[Dict[str, Any]]):
        """Save annotations to Snowflake using MERGE for idempotency.
        
        Implements atomic upsert based on compound key (post_id, model_name, prompt_hash):
        1. Loads annotations into a temporary staging table
        2. Executes MERGE INTO to upsert based on compound key
        3. Cleans up the temporary staging table
        
        Updates existing annotations if post_id, model_name, and prompt_hash match.
        """
        if not annotations:
            logger.warning("No annotations to save")
            return
        
        cursor = None
        temp_table_name = None
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(annotations)
            
            # Sanitize string columns to prevent SQL injection and quote issues
            string_columns = ['post_summary', 'care_response', 'text_for_llm']
            for col in string_columns:
                if col in df.columns:
                    # Replace problematic characters that could cause SQL issues
                    df[col] = df[col].astype(str).str.replace('"', "'", regex=False)
                    df[col] = df[col].str.replace('\n', ' ', regex=False)
                    df[col] = df[col].str.replace('\r', ' ', regex=False)
            
            # Convert column names to UPPERCASE for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            # Create unique temporary staging table name
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
            temp_table_name = f"TEMP_LLM_ANNOTATIONS_{timestamp}"
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            temp_table_full = f"{database}.ANALYTICS_ML.{temp_table_name}"
            
            logger.info(f"Loading {len(annotations)} annotations into staging table: {temp_table_name}")
            
            # Load DataFrame into temporary staging table
            write_pandas(
                self.snowflake_conn,
                df,
                temp_table_name,
                auto_create_table=True,
                overwrite=True,
                use_logical_type=True,
                schema='ANALYTICS_ML'
            )
            
            logger.info(f"Staging table created with {len(annotations)} rows")
            
            # Execute MERGE INTO statement with compound key
            cursor = self.snowflake_conn.cursor()
            
            merge_sql = f"""
            MERGE INTO {database}.ANALYTICS_ML.REDDIT_POSTS_ANNOTATED AS target
            USING {temp_table_full} AS source
            ON target.post_id = source.post_id
                AND target.model_name = source.model_name
                AND target.prompt_hash = source.prompt_hash
            WHEN MATCHED THEN
                UPDATE SET
                    primary_group = source.primary_group,
                    primary_topic = source.primary_topic,
                    secondary_topics = source.secondary_topics,
                    trimester = source.trimester,
                    sentiment = source.sentiment,
                    urgency_0_3 = source.urgency_0_3,
                    keywords = source.keywords,
                    safety_flags = source.safety_flags,
                    post_summary = source.post_summary,
                    care_response = source.care_response,
                    text_for_llm = source.text_for_llm,
                    model_version = source.model_version,
                    input_tokens = source.input_tokens,
                    output_tokens = source.output_tokens,
                    annotated_at = source.annotated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    post_id, primary_group, primary_topic, secondary_topics,
                    trimester, sentiment, urgency_0_3, keywords, safety_flags,
                    post_summary, care_response, text_for_llm,
                    model_name, model_version, prompt_hash,
                    input_tokens, output_tokens, annotated_at
                )
                VALUES (
                    source.post_id, source.primary_group, source.primary_topic,
                    source.secondary_topics, source.trimester, source.sentiment,
                    source.urgency_0_3, source.keywords, source.safety_flags,
                    source.post_summary, source.care_response, source.text_for_llm,
                    source.model_name, source.model_version, source.prompt_hash,
                    source.input_tokens, source.output_tokens, source.annotated_at
                )
            """
            
            cursor.execute(merge_sql)
            rows_affected = cursor.rowcount
            self.snowflake_conn.commit()
            
            logger.info(
                f"MERGE completed: {rows_affected} rows affected "
                f"({len(annotations)} annotations processed)"
            )
            
            # Clean up temporary staging table
            drop_sql = f"DROP TABLE IF EXISTS {temp_table_full}"
            cursor.execute(drop_sql)
            logger.info(
                f"Cleaned up temporary staging table: {temp_table_name}"
            )
            
        except Exception as e:
            logger.error(f"Error saving annotations: {e}")
            # Attempt cleanup on error
            if cursor and temp_table_name:
                try:
                    database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
                    temp_table_full = f"{database}.ANALYTICS_ML.{temp_table_name}"
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table_full}")
                    logger.info("Cleaned up temporary staging table after error")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temp table: {cleanup_error}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def close(self):
        """Close Snowflake connection."""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            logger.info("Snowflake connection closed")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Annotate Reddit posts using OpenAI')
    parser.add_argument('--limit', type=int, default=None, help='Maximum number of posts to annotate (default: no limit, annotate all)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of posts to process before saving')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and display posts without annotating')
    parser.add_argument('--save-csv', action='store_true', help='Save annotations to timestamped CSV file')
    parser.add_argument('--save-logs', action='store_true', help='Save logs and errors to files')
    parser.add_argument('--csv-dir', type=str, default='data/processed', help='Directory to save CSV files (default: data/processed)')
    parser.add_argument('--log-dir', type=str, default='logs/llm', help='Directory to save log files if --save-logs is used (default: logs/llm)')
    
    args = parser.parse_args()
    
    # Set up logging with optional file output
    log_file = None
    error_file = None
    
    if args.save_logs:
        # Create directory if it doesn't exist
        os.makedirs(args.log_dir, exist_ok=True)
        log_file = f"{args.log_dir}/annotate_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        error_file = f"{args.log_dir}/errors_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        
        # Reconfigure logging with file handlers
        logger.handlers.clear()
        logger.addHandler(logging.FileHandler(log_file))
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)
        
        # Create separate error handler
        error_handler = logging.FileHandler(error_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(error_handler)
        
        logger.info(f"Log files will be saved to: {args.log_dir}")
        if log_file:
            logger.info(f"Log file: {log_file}")
            logger.info(f"Error log: {error_file}")
    
    # Log the command that was run
    cmd_str = ' '.join(sys.argv)
    logger.info(f"Command executed: {cmd_str}")
    logger.info(f"Starting annotation run (limit={args.limit}, batch_size={args.batch_size})")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Error log: {error_file}")
    
    # Initialize annotator
    annotator = LLMAnnotator()
    
    try:
        # Create annotation table
        annotator.create_annotation_table()
        
        # Fetch posts to annotate
        posts_df = annotator.fetch_posts_to_annotate(limit=args.limit)
        
        if args.dry_run:
            logger.info("DRY RUN: Would annotate these posts:")
            print(posts_df[['post_id', 'text_for_llm']])
            return
        
        if len(posts_df) == 0:
            logger.info("No posts to annotate")
            return
        
        # Process posts in batches
        annotations = []
        total_posts = len(posts_df)
        successful_annotations = 0
        failed_annotations = 0
        
        for idx, row in posts_df.iterrows():
            post_id = row['post_id']
            post_text = row['text_for_llm']
            
            logger.info(f"Annotating post {idx+1}/{total_posts}: {post_id}")
            
            try:
                annotation = annotator.annotate_post(post_id, post_text)
                
                if annotation:
                    # Add text_for_llm to the annotation before saving
                    annotation['text_for_llm'] = post_text
                    annotations.append(annotation)
                    successful_annotations += 1
                    
                    # Save in batches
                    if len(annotations) >= args.batch_size:
                        try:
                            annotator.save_annotations(annotations)
                            logger.info(f"Saved batch of {len(annotations)} annotations to Snowflake")
                        except Exception as save_error:
                            logger.error(f"Error saving batch to Snowflake: {save_error}")
                            logger.error(f"Failed to save batch of {len(annotations)} annotations")
                        finally:
                            annotations = []
                
            except Exception as e:
                failed_annotations += 1
                logger.error(f"Error annotating post {post_id}: {e}")
                logger.error("Continuing with next post...")
                continue
        
        # Save remaining annotations
        if annotations:
            try:
                annotator.save_annotations(annotations)
                logger.info(f"Saved final batch of {len(annotations)} annotations to Snowflake")
            except Exception as save_error:
                logger.error(f"Error saving final batch to Snowflake: {save_error}")
                logger.error(f"Failed to save final batch of {len(annotations)} annotations")
            
            # Optionally save to CSV file with timestamp
            if args.save_csv:
                os.makedirs(args.csv_dir, exist_ok=True)
                csv_file = f"{args.csv_dir}/annotations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
                
                # Save to CSV
                annotations_df = pd.DataFrame(annotations)
                annotations_df.to_csv(csv_file, index=False)
                
                logger.info(f"Saved {len(annotations)} annotations to {csv_file}")
        
        logger.info("=" * 50)
        logger.info("Annotation complete!")
        logger.info(f"Total posts processed: {total_posts}")
        logger.info(f"Successful annotations: {successful_annotations}")
        logger.info(f"Failed annotations: {failed_annotations}")
        logger.info("=" * 50)
        if log_file:
            logger.info(f"Full log available at: {log_file}")
            logger.info(f"Error log available at: {error_file}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    finally:
        annotator.close()


if __name__ == "__main__":
    main()

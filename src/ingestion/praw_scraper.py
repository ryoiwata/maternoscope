# src/ingestion/top_posts_scraper.py

import praw
import pandas as pd
import argparse
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

# Load environment variables
load_dotenv()

# Disable AWS botocore debug logging
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TopPostsScraper:
    def __init__(self):
        """Initialize Reddit API connection using PRAW."""
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv(
                "REDDIT_USER_AGENT",
                "Maternoscope Top Posts Scraper 1.0"
            )
        )

    def get_top_posts(self, subreddit_name, time_filter, max_posts=None, flair_filter=None):
        """
        Get top posts from a subreddit for a specific time period with optional flair filtering.

        Args:
            subreddit_name (str): Name of the subreddit (without r/)
            time_filter (str): Time period ('today', 'this_week', 'this_month', 'this_year', 'all')
            max_posts (int): Maximum number of posts to retrieve (None for all)
            flair_filter (str): Optional flair to filter by (exact match)

        Returns:
            list: List of dictionaries containing post data
        """
        try:
            logger.info(f"Fetching top posts from r/{subreddit_name} for {time_filter}")
            if flair_filter:
                logger.info(f"Filtering by flair: {flair_filter}")

            # Get subreddit
            subreddit = self.reddit.subreddit(subreddit_name)

            posts_data = []
            seen_post_ids = set()
            post_count = 0

            # Get top posts for the specified time period
            try:
                submissions = subreddit.top(time_filter=time_filter, limit=1000)
                
                for submission in submissions:
                    if max_posts and post_count >= max_posts:
                        break
                    
                    # Skip if we've already seen this post
                    if submission.id in seen_post_ids:
                        continue
                    
                    # Apply flair filter if specified
                    if flair_filter:
                        post_flair = getattr(submission, 'link_flair_text', None)
                        if not post_flair or flair_filter.lower() not in post_flair.lower():
                            continue
                    
                    # Extract post data
                    post_data = self._extract_post_data(submission)
                    if post_data:
                        posts_data.append(post_data)
                        seen_post_ids.add(submission.id)
                        post_count += 1
                        
                        if post_count % 50 == 0:
                            logger.info(f"Collected {post_count} posts so far...")
                
                logger.info(f"Successfully collected {post_count} posts from r/{subreddit_name} for {time_filter}")
                
            except Exception as e:
                logger.error(f"Error fetching top posts: {e}")
                return []

            return posts_data

        except Exception as e:
            logger.error(f"Error in get_top_posts: {e}")
            return []

    def _extract_post_data(self, submission):
        """Extract relevant data from a Reddit submission."""
        try:
            # Get full URL - use permalink for self-posts, url for external links
            full_url = submission.url
            if submission.is_self:
                # For self-posts, construct full reddit URL from permalink
                full_url = f"https://www.reddit.com{submission.permalink}"
            
            return {
                'post_id': submission.id,
                'post_date': datetime.fromtimestamp(submission.created_utc),
                'post_timestamp': submission.created_utc,
                'post_flair': getattr(submission, 'link_flair_text', None),
                'title': submission.title,
                'url': full_url,
                'content': submission.selftext if hasattr(submission, 'selftext') else '',
                'score': submission.score,
                'num_comments': submission.num_comments,
                'subreddit': submission.subreddit.display_name,
                'scraped_at': datetime.now()
            }
        except Exception as e:
            logger.warning(f"Error extracting data from post {submission.id}: {e}")
            return None

    def save_to_csv(self, posts_data, filename):
        """Save posts data to CSV file."""
        try:
            df = pd.DataFrame(posts_data)
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to CSV: {filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def save_to_json(self, posts_data, filename):
        """Save posts data to JSON file."""
        try:
            def datetime_converter(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, default=datetime_converter, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to JSON: {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")

    def check_existing_csv(self, subreddit, time_filter, output_dir):
        """Check if CSV files already exist for this subreddit and time filter."""
        try:
            pattern = f"top_posts_{subreddit}_{time_filter}_*.csv"
            import glob
            existing_files = glob.glob(os.path.join(output_dir, pattern))
            return len(existing_files) > 0
        except Exception as e:
            logger.warning(f"Error checking for existing CSV files: {e}")
            return False


class SnowflakeConnector:
    # Define table schema once - used for both main and staging tables
    TABLE_SCHEMA = {
        'POST_ID': 'VARCHAR(255)',
        'SUBREDDIT': 'VARCHAR(255)',
        'POST_TITLE': 'VARCHAR(2000)',
        'POST_CONTENT': 'VARCHAR(16777216)',
        'POST_URL': 'VARCHAR(2000)',
        'POST_FLAIR': 'VARCHAR(500)',
        'SCORE': 'NUMBER',
        'NUM_COMMENTS': 'NUMBER',
        'CREATED_UTC': 'TIMESTAMP_NTZ',
        'POST_TIMESTAMP': 'NUMBER',
        'SCRAPED_AT': 'TIMESTAMP_TZ',
        'SCRAPE_DATE': 'DATE'
    }
    
    def __init__(self):
        """Initialize Snowflake connection."""
        self.conn = None
        self.connect()
    
    def get_table_schema_sql(self, include_primary_key=True):
        """
        Generate CREATE TABLE SQL with consistent schema.
        
        Args:
            include_primary_key: If True, adds PRIMARY KEY constraint to POST_ID
        
        Returns:
            str: SQL column definitions
        """
        columns = []
        for col_name, col_type in self.TABLE_SCHEMA.items():
            if col_name == 'POST_ID' and include_primary_key:
                columns.append(f"{col_name} {col_type} PRIMARY KEY")
            else:
                columns.append(f"{col_name} {col_type}")
        return ",\n                ".join(columns)

    def connect(self):
        """Connect to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise

    def create_table_if_not_exists(self, table_name="REDDIT_POSTS"):
        """
        Create Snowflake raw table if it doesn't exist.
        Uses canonical "ever seen" table pattern with post_id as primary key.
        Also checks if existing table has old schema and handles migration.
        """
        try:
            cursor = self.conn.cursor()
            
            # Get schema and database from environment
            schema = os.getenv("SNOWFLAKE_SCHEMA", "INGEST")
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            
            # Ensure schema is set as current schema (use fully qualified schema name)
            cursor.execute(f"USE SCHEMA {database}.{schema}")
            
            # If table_name already includes schema, use it as-is; otherwise qualify it
            if '.' in table_name:
                qualified_table_name = table_name
            else:
                qualified_table_name = f"{database}.{schema}.{table_name}"
            
            # Check if table exists and get its columns
            check_table_sql = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' 
              AND TABLE_NAME = '{table_name.split('.')[-1]}'
            ORDER BY ORDINAL_POSITION
            """
            cursor.execute(check_table_sql)
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            if existing_columns:
                # Table exists - check if it has the new schema
                has_new_schema = 'POST_TITLE' in existing_columns
                has_old_schema = 'TITLE' in existing_columns
                
                if has_old_schema and not has_new_schema:
                    logger.warning(f"Table {qualified_table_name} has old schema. Attempting to migrate...")
                    # Try to add new columns (if they don't exist)
                    # Note: This is a simple migration - in production you might want more sophisticated handling
                    try:
                        alter_sql = f"""
                        ALTER TABLE {qualified_table_name}
                        ADD COLUMN IF NOT EXISTS POST_TITLE VARCHAR(2000),
                        ADD COLUMN IF NOT EXISTS POST_CONTENT VARCHAR(16777216),
                        ADD COLUMN IF NOT EXISTS POST_URL VARCHAR(2000),
                        ADD COLUMN IF NOT EXISTS POST_FLAIR VARCHAR(500),
                        ADD COLUMN IF NOT EXISTS CREATED_UTC TIMESTAMP_NTZ,
                        ADD COLUMN IF NOT EXISTS POST_TIMESTAMP NUMBER,
                        ADD COLUMN IF NOT EXISTS SCRAPE_DATE DATE
                        """
                        cursor.execute(alter_sql)
                        logger.info("Added new columns to existing table")
                    except Exception as e:
                        logger.warning(f"Could not migrate table schema: {e}")
                        logger.warning("You may need to manually migrate the table or recreate it")
                elif has_new_schema:
                    logger.info(f"Table {qualified_table_name} already has correct schema")
                else:
                    logger.info(f"Table {qualified_table_name} exists with unknown schema")
            else:
                # Table doesn't exist - create it with new schema
                schema_sql = self.get_table_schema_sql(include_primary_key=True)
                create_table_sql = f"""
                CREATE TABLE {qualified_table_name} (
                    {schema_sql}
                )
                """
                cursor.execute(create_table_sql)
                logger.info(f"Created table {qualified_table_name} with new schema")
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error creating/checking table: {e}")
            raise

    def save_to_snowflake(self, posts_data, table_name="REDDIT_POSTS", time_filter="unknown"):
        """
        Save posts data to Snowflake using MERGE pattern for idempotency.
        
        Uses staging table + MERGE to handle:
        - New posts (insert)
        - Existing posts (update scores, comment counts, etc.)
        - Duplicate runs (idempotent)
        """
        try:
            if not posts_data:
                logger.warning("No data to save to Snowflake")
                return

            # Get schema and database, and qualify table name
            schema = os.getenv("SNOWFLAKE_SCHEMA", "INGEST")
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            if '.' in table_name:
                qualified_table_name = table_name
                simple_table_name = table_name.split('.')[-1]
            else:
                qualified_table_name = f"{database}.{schema}.{table_name}"
                simple_table_name = table_name
            
            # Create main table if it doesn't exist
            self.create_table_if_not_exists(table_name)

            # Convert to DataFrame and prepare columns
            df = pd.DataFrame(posts_data)
            
            # Map to new schema structure
            # Add scrape_date (the "day bucket" for this job)
            scrape_date = datetime.now().date()
            df['scrape_date'] = scrape_date
            
            # Map columns to match new schema
            column_mapping = {
                'post_id': 'POST_ID',
                'subreddit': 'SUBREDDIT',
                'title': 'POST_TITLE',
                'content': 'POST_CONTENT',
                'url': 'POST_URL',
                'post_flair': 'POST_FLAIR',
                'score': 'SCORE',
                'num_comments': 'NUM_COMMENTS',
                'post_date': 'CREATED_UTC',
                'post_timestamp': 'POST_TIMESTAMP',
                'scraped_at': 'SCRAPED_AT',
                'scrape_date': 'SCRAPE_DATE'
            }
            
            # Create new DataFrame with mapped columns
            df_mapped = pd.DataFrame()
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df_mapped[new_col] = df[old_col]
            
            # Ensure timestamps are properly formatted
            if 'CREATED_UTC' in df_mapped.columns:
                # CREATED_UTC should be TIMESTAMP_NTZ (no timezone)
                df_mapped['CREATED_UTC'] = pd.to_datetime(
                    df_mapped['CREATED_UTC'], utc=True
                ).dt.tz_localize(None)
            
            if 'SCRAPED_AT' in df_mapped.columns:
                # SCRAPED_AT should be TIMESTAMP_TZ (with timezone)
                df_mapped['SCRAPED_AT'] = pd.to_datetime(
                    df_mapped['SCRAPED_AT'], utc=True
                )
            
            if 'SCRAPE_DATE' in df_mapped.columns:
                df_mapped['SCRAPE_DATE'] = pd.to_datetime(df_mapped['SCRAPED_AT']).dt.date
            
            # Ensure all column names are uppercase
            df_mapped.columns = [col.upper() for col in df_mapped.columns]
            
            logger.info(f"Prepared {len(df_mapped)} posts for staging")
            logger.debug(f"DataFrame columns: {list(df_mapped.columns)}")

            # Set schema context
            cursor = self.conn.cursor()
            cursor.execute(f"USE SCHEMA {database}.{schema}")
            
            # Create or replace staging table (temp table) with same schema as main table
            # Use the same schema definition to ensure columns match exactly
            stage_table_name = f"{simple_table_name}_STAGE"
            qualified_stage_table = f"{database}.{schema}.{stage_table_name}"
            
            logger.info(f"Creating staging table {qualified_stage_table}")
            # Use same schema but without PRIMARY KEY (staging table doesn't need it)
            schema_sql = self.get_table_schema_sql(include_primary_key=False)
            create_stage_sql = f"""
            CREATE OR REPLACE TEMP TABLE {qualified_stage_table} (
                {schema_sql}
            )
            """
            cursor.execute(create_stage_sql)
            
            # Load data into staging table
            logger.info(f"Loading {len(df_mapped)} rows into staging table...")
            success, nchunks, nrows, _ = write_pandas(
                self.conn,
                df_mapped,
                stage_table_name,
                auto_create_table=False,
                overwrite=True,
                use_logical_type=True
            )
            
            if not success:
                cursor.close()
                raise Exception("Failed to load data into staging table")
            
            logger.info(f"Loaded {nrows} rows into staging table")
            
            # Get statistics before MERGE (to know what will be updated vs inserted)
            stats_sql = f"""
            SELECT 
                COUNT(CASE WHEN t.POST_ID IS NOT NULL THEN 1 END) as matched,
                COUNT(CASE WHEN t.POST_ID IS NULL THEN 1 END) as new
            FROM {qualified_stage_table} s
            LEFT JOIN {qualified_table_name} t ON s.POST_ID = t.POST_ID
            """
            cursor.execute(stats_sql)
            stats = cursor.fetchone()
            matched_count = stats[0] if stats else 0
            new_count = stats[1] if stats else 0
            
            logger.info(f"Pre-merge stats: {matched_count} existing posts will be updated, {new_count} new posts will be inserted")
            
            # Perform MERGE to make the job idempotent
            logger.info("Performing MERGE to update/insert posts...")
            merge_sql = f"""
            MERGE INTO {qualified_table_name} t
            USING {qualified_stage_table} s
              ON t.POST_ID = s.POST_ID
            WHEN MATCHED THEN UPDATE SET
                t.SCORE = s.SCORE,
                t.NUM_COMMENTS = s.NUM_COMMENTS,
                t.POST_TITLE = s.POST_TITLE,
                t.POST_CONTENT = s.POST_CONTENT,
                t.POST_URL = s.POST_URL,
                t.POST_FLAIR = s.POST_FLAIR,
                t.SCRAPED_AT = s.SCRAPED_AT,
                t.SCRAPE_DATE = s.SCRAPE_DATE
            WHEN NOT MATCHED THEN INSERT (
                POST_ID,
                SUBREDDIT,
                POST_TITLE,
                POST_CONTENT,
                POST_URL,
                POST_FLAIR,
                SCORE,
                NUM_COMMENTS,
                CREATED_UTC,
                POST_TIMESTAMP,
                SCRAPED_AT,
                SCRAPE_DATE
            ) VALUES (
                s.POST_ID,
                s.SUBREDDIT,
                s.POST_TITLE,
                s.POST_CONTENT,
                s.POST_URL,
                s.POST_FLAIR,
                s.SCORE,
                s.NUM_COMMENTS,
                s.CREATED_UTC,
                s.POST_TIMESTAMP,
                s.SCRAPED_AT,
                s.SCRAPE_DATE
            )
            """
            
            cursor.execute(merge_sql)
            logger.info("MERGE completed successfully")
            logger.info(f"Final results: {matched_count} posts updated, {new_count} posts inserted")
            logger.info(f"Total posts processed: {len(df_mapped)}")
            
            cursor.close()
                
        except Exception as e:
            logger.error(f"Error saving to Snowflake: {e}")
            raise

    def check_existing_data(self, subreddit, time_filter, table_name="REDDIT_POSTS", 
                           check_recent=False, hours_threshold=24):
        """
        Check if data already exists in Snowflake for this subreddit.
        
        Args:
            subreddit: Subreddit name to check
            time_filter: Time filter used (for logging)
            table_name: Table name to check
            check_recent: If True, only consider data scraped within hours_threshold
            hours_threshold: Number of hours to look back for recent data
        
        Returns:
            tuple: (exists: bool, count: int, message: str)
        """
        try:
            cursor = self.conn.cursor()
            
            # Get schema and database, and qualify table name
            schema = os.getenv("SNOWFLAKE_SCHEMA", "INGEST")
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            if '.' in table_name:
                qualified_table_name = table_name
            else:
                qualified_table_name = f"{database}.{schema}.{table_name}"
            
            # Ensure schema is set as current schema
            cursor.execute(f"USE SCHEMA {database}.{schema}")
            
            # Build query based on whether we're checking for recent data
            if check_recent:
                query = f"""
                SELECT COUNT(*) 
                FROM {qualified_table_name} 
                WHERE SUBREDDIT = %s
                  AND SCRAPED_AT >= DATEADD(hour, -%s, CURRENT_TIMESTAMP())
                """
                cursor.execute(query, (subreddit, hours_threshold))
            else:
                query = f"""
                SELECT COUNT(*) 
                FROM {qualified_table_name} 
                WHERE SUBREDDIT = %s
                """
                cursor.execute(query, (subreddit,))
            
            count = cursor.fetchone()[0]
            cursor.close()
            
            exists = count > 0
            if check_recent:
                message = f"Found {count} posts for r/{subreddit} scraped within last {hours_threshold} hours"
            else:
                message = f"Found {count} existing posts for r/{subreddit} in Snowflake"
            
            return exists, count, message
        except Exception as e:
            # If table doesn't exist, return False
            if "does not exist" in str(e) or "Table" in str(e):
                return False, 0, f"Table {table_name} does not exist yet"
            logger.warning(f"Error checking existing data in Snowflake: {e}")
            return False, 0, f"Error checking existing data: {e}"

    def close(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape top Reddit posts from a subreddit for a specific time period'
    )
    parser.add_argument('subreddit', help='Subreddit name (without r/)')
    parser.add_argument('time_filter', 
                       choices=['hour', 'day', 'week', 'month', 'year', 'all'],
                       help='Time period for top posts (hour, day, week, month, year, all)')
    parser.add_argument('--max-posts', type=int,
                       help='Maximum number of posts to retrieve')
    parser.add_argument('--flair', 
                       help='Filter posts by flair (exact match)')
    parser.add_argument('--output-csv', help='Output CSV filename')
    parser.add_argument('--output-json', help='Output JSON filename')
    parser.add_argument('--save-to-snowflake', action='store_true',
                       help='Save data to Snowflake table')
    parser.add_argument('--snowflake-table', default='REDDIT_POSTS',
                       help='Snowflake table name (default: REDDIT_POSTS)')
    parser.add_argument('--check-duplicates', action='store_true',
                       help='Check for existing data before scraping')
    parser.add_argument('--skip-if-exists', action='store_true',
                       help='Skip scraping entirely if data already exists in Snowflake')
    parser.add_argument('--check-recent-hours', type=int, default=None,
                       help='When checking for existing data, only consider posts scraped within this many hours')
    parser.add_argument('--output-dir', default='.',
                       help='Output directory for CSV/JSON files (default: current directory)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize scraper
    scraper = TopPostsScraper()

    # Check for existing data if requested
    snowflake_connector = None
    if args.check_duplicates or args.skip_if_exists or args.save_to_snowflake:
        logger.info("Checking for existing data...")
        
        # Check CSV files
        csv_exists = scraper.check_existing_csv(args.subreddit, args.time_filter, args.output_dir)
        
        # Check Snowflake if enabled
        snowflake_exists = False
        snowflake_count = 0
        snowflake_message = ""
        
        if args.save_to_snowflake or args.skip_if_exists:
            try:
                snowflake_connector = SnowflakeConnector()
                check_recent = args.check_recent_hours is not None
                hours_threshold = args.check_recent_hours if check_recent else 24
                
                snowflake_exists, snowflake_count, snowflake_message = \
                    snowflake_connector.check_existing_data(
                        args.subreddit, 
                        args.time_filter, 
                        args.snowflake_table,
                        check_recent=check_recent,
                        hours_threshold=hours_threshold
                    )
                
                # If skip_if_exists is set and data exists, exit early
                if args.skip_if_exists and snowflake_exists:
                    logger.info(snowflake_message)
                    logger.info(f"Skipping scrape for r/{args.subreddit} - data already exists")
                    if snowflake_connector:
                        snowflake_connector.close()
                    return
                
            except Exception as e:
                logger.warning(f"Could not check Snowflake for existing data: {e}")
                if args.skip_if_exists:
                    logger.warning("Cannot verify if data exists, proceeding with scrape...")
        
        # Log existing data findings
        if csv_exists or snowflake_exists:
            logger.info("Existing data found:")
            if csv_exists:
                logger.info("  - CSV files exist")
            if snowflake_exists:
                logger.info(f"  - {snowflake_message}")
            
            if not args.skip_if_exists:
                logger.info("Continuing with scrape (duplicates will be filtered during save)...")

    # Get posts
    posts = scraper.get_top_posts(args.subreddit, args.time_filter, 
                                 args.max_posts, args.flair)

    if not posts:
        logger.warning(f"No posts found for r/{args.subreddit} with time filter '{args.time_filter}'")
        if args.flair:
            logger.warning(f"and flair filter '{args.flair}'")
        return

    # Generate default filenames if not provided
    if not args.output_csv and not args.output_json:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        flair_suffix = f"_{args.flair.replace(' ', '_')}" if args.flair else ""
        args.output_csv = os.path.join(args.output_dir, 
                                     f"top_posts_{args.subreddit}_{args.time_filter}{flair_suffix}_"
                                     f"{timestamp}.csv")
        args.output_json = os.path.join(args.output_dir,
                                      f"top_posts_{args.subreddit}_{args.time_filter}{flair_suffix}_"
                                      f"{timestamp}.json")

    # Save data
    if args.output_csv:
        scraper.save_to_csv(posts, args.output_csv)

    if args.output_json:
        scraper.save_to_json(posts, args.output_json)

    # Save to Snowflake if requested
    # Reuse existing connection if we created one earlier
    if args.save_to_snowflake:
        try:
            if not snowflake_connector:
                snowflake_connector = SnowflakeConnector()
            # save_to_snowflake is now idempotent - it will skip duplicates automatically
            snowflake_connector.save_to_snowflake(posts, args.snowflake_table, args.time_filter)
        except Exception as e:
            logger.error(f"Failed to save to Snowflake: {e}")
        finally:
            if snowflake_connector:
                snowflake_connector.close()

    # Print summary
    print("\nSummary:")
    print(f"Subreddit: r/{args.subreddit}")
    print(f"Time filter: {args.time_filter}")
    if args.flair:
        print(f"Flair filter: {args.flair}")
    print(f"Posts collected: {len(posts)}")
    if args.output_csv:
        print(f"CSV file: {args.output_csv}")
    if args.output_json:
        print(f"JSON file: {args.output_json}")
    if args.save_to_snowflake:
        print(f"Snowflake table: {args.snowflake_table}")


if __name__ == "__main__":
    main()

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
    def __init__(self):
        """Initialize Snowflake connection."""
        self.conn = None
        self.connect()

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
        """Create Snowflake table if it doesn't exist."""
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
            
            # Create the table if it doesn't exist using fully qualified name
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {qualified_table_name} (
                POST_ID VARCHAR(255) PRIMARY KEY,
                POST_DATE TIMESTAMP_TZ,
                POST_TIMESTAMP NUMBER,
                POST_FLAIR VARCHAR(500),
                TITLE VARCHAR(2000),
                URL VARCHAR(2000),
                CONTENT VARCHAR(16777216),
                SCORE NUMBER,
                NUM_COMMENTS NUMBER,
                SUBREDDIT VARCHAR(255),
                SCRAPED_AT TIMESTAMP_TZ
            )
            """
            
            cursor.execute(create_table_sql)
            logger.info(f"Table {qualified_table_name} created or already exists")
            
            cursor.close()
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def get_existing_post_ids(self, table_name="REDDIT_POSTS", post_ids=None):
        """
        Get set of existing POST_IDs from Snowflake table.
        
        Args:
            table_name: Name of the table to check
            post_ids: Optional list of post IDs to check. If None, returns all existing IDs.
        
        Returns:
            set: Set of existing POST_IDs
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
            
            # Build query to check for existing post IDs
            if post_ids:
                # Check for specific post IDs
                placeholders = ','.join(['%s'] * len(post_ids))
                query = f"""
                SELECT POST_ID 
                FROM {qualified_table_name} 
                WHERE POST_ID IN ({placeholders})
                """
                cursor.execute(query, post_ids)
            else:
                # Get all existing post IDs
                query = f"SELECT POST_ID FROM {qualified_table_name}"
                cursor.execute(query)
            
            existing_ids = {row[0] for row in cursor.fetchall()}
            cursor.close()
            
            return existing_ids
        except Exception as e:
            # If table doesn't exist yet, return empty set
            if "does not exist" in str(e) or "Table" in str(e):
                logger.debug(f"Table {table_name} does not exist yet, no existing IDs")
                return set()
            logger.warning(f"Error checking existing post IDs: {e}")
            return set()

    def save_to_snowflake(self, posts_data, table_name="REDDIT_POSTS", time_filter="unknown"):
        """Save posts data to Snowflake, skipping duplicates (idempotent)."""
        try:
            if not posts_data:
                logger.warning("No data to save to Snowflake")
                return

            # Get schema and database, and qualify table name
            schema = os.getenv("SNOWFLAKE_SCHEMA", "INGEST")
            database = os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE")
            if '.' in table_name:
                qualified_table_name = table_name
            else:
                qualified_table_name = f"{database}.{schema}.{table_name}"

            # Create table if it doesn't exist (pass unqualified name, method will qualify it)
            self.create_table_if_not_exists(table_name)

            # Extract post IDs from the data
            post_ids = [post.get('post_id') for post in posts_data if post.get('post_id')]
            
            if not post_ids:
                logger.warning("No valid post IDs found in data")
                return

            # Check for existing posts (idempotency check)
            logger.info("Checking for existing posts in Snowflake...")
            existing_ids = self.get_existing_post_ids(table_name, post_ids)
            
            # Filter out posts that already exist
            new_posts = [post for post in posts_data 
                        if post.get('post_id') and post.get('post_id') not in existing_ids]
            
            if existing_ids:
                logger.info(f"Found {len(existing_ids)} existing posts, skipping duplicates")
            
            if not new_posts:
                logger.info(f"All {len(posts_data)} posts already exist in Snowflake. Nothing to insert.")
                return

            logger.info(f"Inserting {len(new_posts)} new posts (out of {len(posts_data)} total)")

            # Convert to DataFrame
            df = pd.DataFrame(new_posts)
            
            # Ensure post_date is timezone-aware UTC datetime
            df['post_date'] = pd.to_datetime(df['post_date'], utc=True)
            df['scraped_at'] = pd.to_datetime(df['scraped_at'], utc=True)
            
            # Convert column names to uppercase for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            logger.debug(f"DataFrame columns: {list(df.columns)}")
            logger.debug(f"Sample POST_DATE values: {df['POST_DATE'].head().tolist()}")
            logger.debug(f"POST_DATE dtype: {df['POST_DATE'].dtype}")

            # Set schema context before writing
            # write_pandas works best with unqualified table names when schema is set
            cursor = self.conn.cursor()
            cursor.execute(f"USE SCHEMA {database}.{schema}")
            cursor.close()
            
            # Extract just the table name (remove any qualification)
            simple_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
            
            # Save to Snowflake using simple table name (schema is already set)
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                simple_table_name, 
                auto_create_table=False,
                overwrite=False,
                use_logical_type=True
            )
            
            if success:
                logger.info(f"Successfully saved {nrows} new rows to Snowflake table {qualified_table_name}")
                if len(existing_ids) > 0:
                    logger.info(f"Skipped {len(existing_ids)} duplicate posts")
            else:
                logger.error(f"Failed to save data to Snowflake table {qualified_table_name}")
                
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

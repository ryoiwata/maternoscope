# src/ingestion/top_posts_scraper.py

import praw
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

# Load environment variables
load_dotenv()

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
            return {
                'post_id': submission.id,
                'post_date': datetime.fromtimestamp(submission.created_utc),
                'post_timestamp': submission.created_utc,
                'post_flair': getattr(submission, 'link_flair_text', None),
                'title': submission.title,
                'url': submission.url,
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

    def create_table_if_not_exists(self, table_name="top_reddit_posts"):
        """Create Snowflake table if it doesn't exist."""
        try:
            cursor = self.conn.cursor()
            
            # First, create the table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
            
            # Check if TIME_FILTER column exists, if not add it
            try:
                check_column_sql = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name.upper()}' 
                AND COLUMN_NAME = 'TIME_FILTER'
                """
                cursor.execute(check_column_sql)
                column_exists = cursor.fetchone()[0] > 0
                
                if not column_exists:
                    logger.info(f"Adding TIME_FILTER column to {table_name}")
                    alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN TIME_FILTER VARCHAR(50)"
                    cursor.execute(alter_table_sql)
                    logger.info(f"Successfully added TIME_FILTER column to {table_name}")
                else:
                    logger.info(f"TIME_FILTER column already exists in {table_name}")
                    
            except Exception as e:
                logger.warning(f"Could not check/add TIME_FILTER column: {e}")
                # Continue anyway, the table creation was successful
            
            cursor.close()
            logger.info(f"Table {table_name} created or already exists")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def save_to_snowflake(self, posts_data, table_name="top_reddit_posts", time_filter="unknown"):
        """Save posts data to Snowflake."""
        try:
            if not posts_data:
                logger.warning("No data to save to Snowflake")
                return

            # Create table if it doesn't exist
            self.create_table_if_not_exists(table_name)

            # Convert to DataFrame
            df = pd.DataFrame(posts_data)
            
            # Ensure post_date is timezone-aware UTC datetime
            df['post_date'] = pd.to_datetime(df['post_date'], utc=True)
            df['scraped_at'] = pd.to_datetime(df['scraped_at'], utc=True)
            
            # Add time_filter column
            df['time_filter'] = time_filter
            
            # Convert column names to uppercase for Snowflake
            df.columns = [col.upper() for col in df.columns]
            
            logger.info(f"DataFrame columns: {list(df.columns)}")
            logger.info(f"Sample POST_DATE values: {df['POST_DATE'].head().tolist()}")
            logger.info(f"POST_DATE dtype: {df['POST_DATE'].dtype}")

            # Save to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                self.conn, 
                df, 
                table_name, 
                auto_create_table=False,
                overwrite=False,
                use_logical_type=True
            )
            
            if success:
                logger.info(f"Successfully saved {nrows} rows to Snowflake table {table_name}")
            else:
                logger.error("Failed to save data to Snowflake")
                
        except Exception as e:
            logger.error(f"Error saving to Snowflake: {e}")
            raise

    def check_existing_data(self, subreddit, time_filter, table_name="top_reddit_posts"):
        """Check if data already exists in Snowflake for this subreddit and time filter."""
        try:
            cursor = self.conn.cursor()
            
            query = f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE SUBREDDIT = %s AND TIME_FILTER = %s
            """
            
            cursor.execute(query, (subreddit, time_filter))
            count = cursor.fetchone()[0]
            cursor.close()
            
            return count > 0
        except Exception as e:
            logger.warning(f"Error checking existing data in Snowflake: {e}")
            return False

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
    parser.add_argument('--snowflake-table', default='top_reddit_posts',
                       help='Snowflake table name (default: top_reddit_posts)')
    parser.add_argument('--check-duplicates', action='store_true',
                       help='Check for existing data before scraping')
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
    if args.check_duplicates:
        logger.info("Checking for existing data...")
        
        # Check CSV files
        csv_exists = scraper.check_existing_csv(args.subreddit, args.time_filter, args.output_dir)
        
        # Check Snowflake if enabled
        snowflake_exists = False
        if args.save_to_snowflake:
            try:
                snowflake_connector = SnowflakeConnector()
                snowflake_exists = snowflake_connector.check_existing_data(
                    args.subreddit, args.time_filter, args.snowflake_table
                )
                snowflake_connector.close()
            except Exception as e:
                logger.warning(f"Could not check Snowflake for existing data: {e}")
        
        # If data exists in either location, ask user what to do
        if csv_exists or snowflake_exists:
            logger.warning("Existing data found!")
            if csv_exists:
                logger.warning("  - CSV files exist")
            if snowflake_exists:
                logger.warning("  - Snowflake data exists")
            
            response = input("Do you want to continue scraping anyway? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                logger.info("Scraping cancelled by user.")
                return
            else:
                logger.info("Continuing with scraping...")

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
    snowflake_connector = None
    if args.save_to_snowflake:
        try:
            snowflake_connector = SnowflakeConnector()
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

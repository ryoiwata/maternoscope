# src/ingestion/reddit_scraper.py

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

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditScraper:
    def __init__(self):
        """Initialize Reddit API connection using PRAW."""
        self.reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv(
                "REDDIT_USER_AGENT",
                "Maternoscope Data Collection Bot 1.0"
            )
        )

    def get_posts_for_date(self, subreddit_name, target_date, max_posts=None):
        """
        Get all posts from a subreddit for a specific date.

        Args:
            subreddit_name (str): Name of the subreddit (without r/)
            target_date (str): Target date in YYYY-MM-DD format
            max_posts (int): Maximum number of posts to retrieve (None for all)

        Returns:
            list: List of dictionaries containing post data
        """
        try:
            # Parse target date
            target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
            start_of_day = target_datetime.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end_of_day = start_of_day + timedelta(days=1)

            logger.info(
                f"Fetching posts from r/{subreddit_name} for {target_date}"
            )
            logger.info(f"Date range: {start_of_day} to {end_of_day}")

            # Get subreddit
            subreddit = self.reddit.subreddit(subreddit_name)

            posts_data = []
            post_count = 0

            # Get posts from different time periods to ensure we capture all
            # posts from the day
            # Reddit API has limitations, so we'll try multiple approaches
            for time_filter in ['day', 'week']:
                if max_posts and post_count >= max_posts:
                    break

                logger.info(f"Fetching posts with time filter: {time_filter}")

                try:
                    # Get hot posts first
                    for submission in subreddit.hot(limit=None):
                        if max_posts and post_count >= max_posts:
                            break

                        post_date = datetime.fromtimestamp(
                            submission.created_utc
                        )

                        # Check if post is within our target date range
                        if start_of_day <= post_date < end_of_day:
                            post_data = self._extract_post_data(submission)
                            posts_data.append(post_data)
                            post_count += 1

                            if post_count % 100 == 0:
                                logger.info(
                                    f"Collected {post_count} posts so far..."
                                )

                        # If we've gone past our target date, break
                        elif post_date < start_of_day:
                            break

                    # Get new posts
                    for submission in subreddit.new(limit=None):
                        if max_posts and post_count >= max_posts:
                            break

                        post_date = datetime.fromtimestamp(
                            submission.created_utc
                        )

                        # Check if post is within our target date range
                        if start_of_day <= post_date < end_of_day:
                            # Check if we already have this post
                            if not any(p['post_id'] == submission.id
                                       for p in posts_data):
                                post_data = self._extract_post_data(submission)
                                posts_data.append(post_data)
                                post_count += 1

                                if post_count % 100 == 0:
                                    logger.info(
                                        f"Collected {post_count} posts "
                                        f"so far..."
                                    )

                        # If we've gone past our target date, break
                        elif post_date < start_of_day:
                            break

                except Exception as e:
                    logger.warning(
                        f"Error fetching posts with {time_filter} "
                        f"filter: {e}"
                    )
                    continue

                # Add small delay to respect rate limits
                time.sleep(1)

            # Sort posts by creation time
            posts_data.sort(key=lambda x: x['post_date'])

            logger.info(
                f"Successfully collected {len(posts_data)} posts from "
                f"r/{subreddit_name} for {target_date}"
            )
            return posts_data

        except Exception as e:
            logger.error(f"Error fetching posts: {e}")
            return []

    def _extract_post_data(self, submission):
        """
        Extract relevant data from a Reddit submission.

        Args:
            submission: PRAW submission object

        Returns:
            dict: Dictionary containing post data
        """
        try:
            # Get post content (selftext for text posts, URL for link posts)
            content = (submission.selftext if submission.selftext
                       else submission.url)

            # Get post flair
            flair = (submission.link_flair_text
                     if submission.link_flair_text else None)

            # Get post URL
            post_url = f"https://reddit.com{submission.permalink}"

            post_data = {
                'post_id': submission.id,
                'post_date': datetime.fromtimestamp(submission.created_utc),
                'post_timestamp': submission.created_utc,
                'post_flair': flair,
                'title': submission.title,
                'url': post_url,
                'content': content,
                'score': submission.score,
                'num_comments': submission.num_comments,
                'subreddit': submission.subreddit.display_name
            }

            return post_data

        except Exception as e:
            logger.error(
                f"Error extracting data from post {submission.id}: {e}"
            )
            return None

    def save_to_csv(self, posts_data, filename):
        """
        Save posts data to CSV file.

        Args:
            posts_data (list): List of post dictionaries
            filename (str): Output filename
        """
        try:
            df = pd.DataFrame(posts_data)
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def save_to_json(self, posts_data, filename):
        """
        Save posts data to JSON file.

        Args:
            posts_data (list): List of post dictionaries
            filename (str): Output filename
        """
        try:
            import json
            from datetime import datetime
            
            # Convert datetime objects to strings for JSON serialization
            def datetime_converter(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False, default=datetime_converter)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")

    def check_existing_csv(self, subreddit, target_date, output_dir="."):
        """
        Check if CSV files already exist for a given subreddit and date.
        
        Args:
            subreddit (str): Subreddit name
            target_date (str): Target date in YYYY-MM-DD format
            output_dir (str): Output directory to search
            
        Returns:
            bool: True if CSV files exist, False otherwise
        """
        try:
            import glob
            import os
            
            # Search for CSV files matching the pattern
            pattern = os.path.join(output_dir, f"reddit_posts_{subreddit}_{target_date}_*.csv")
            existing_files = glob.glob(pattern)
            
            if existing_files:
                logger.info(f"Found {len(existing_files)} existing CSV file(s) for r/{subreddit} on {target_date}")
                for file in existing_files:
                    logger.info(f"  - {file}")
                return True
            else:
                logger.info(f"No existing CSV files found for r/{subreddit} on {target_date}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking existing CSV files: {e}")
            return False


class SnowflakeConnector:
    def __init__(self):
        """Initialize Snowflake connection using environment variables."""
        self.connection = None
        self.connect()

    def connect(self):
        """Establish connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                database=os.getenv("SNOWFLAKE_DATABASE", "MATERNOSCOPE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
                role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
            )
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise

    def create_table_if_not_exists(self, table_name="reddit_posts"):
        """Create the reddit_posts table if it doesn't exist."""
        try:
            cursor = self.connection.cursor()
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
                SCRAPED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
            """

            cursor.execute(create_table_sql)
            cursor.close()
            logger.info(f"Table {table_name} created or already exists")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def save_to_snowflake(self, posts_data, table_name="reddit_posts"):
        """
        Save posts data to Snowflake table.

        Args:
            posts_data (list): List of post dictionaries
            table_name (str): Target table name
        """
        try:
            if not posts_data:
                logger.warning("No data to save to Snowflake")
                return
            # Create table if it doesn't exist
            self.create_table_if_not_exists(table_name)
            # Convert to DataFrame
            df = pd.DataFrame(posts_data)
            
            # Ensure post_date is datetime and in UTC timezone
            df['post_date'] = pd.to_datetime(df['post_date'], utc=True)
            
            # Add scraped_at timestamp in UTC
            df['scraped_at'] = pd.Timestamp.now(tz='UTC')
            
            # Ensure column names are uppercase for Snowflake compatibility
            df.columns = [col.upper() for col in df.columns]
            
            # Log DataFrame info for debugging
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"DataFrame columns: {list(df.columns)}")
            logger.info(f"Sample POST_DATE values: {df['POST_DATE'].head().tolist()}")
            logger.info(f"POST_DATE dtype: {df['POST_DATE'].dtype}")

            # Write to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                table_name,
                auto_create_table=False,
                overwrite=False,
                use_logical_type=True
            )

            if success:
                logger.info(
                    f"Successfully saved {nrows} rows to Snowflake table "
                    f"{table_name}"
                )
            else:
                logger.error("Failed to save data to Snowflake")
        except Exception as e:
            logger.error(f"Error saving to Snowflake: {e}")
            raise

    def check_existing_data(self, subreddit, target_date, table_name="reddit_posts"):
        """
        Check if data already exists for a given subreddit and date.
        
        Args:
            subreddit (str): Subreddit name
            target_date (str): Target date in YYYY-MM-DD format
            table_name (str): Snowflake table name
            
        Returns:
            bool: True if data exists, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            
            # Check if table exists first
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = UPPER('{table_name}')
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if not table_exists:
                cursor.close()
                return False
            
            # Check for existing data
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {table_name} 
                WHERE UPPER(SUBREDDIT) = UPPER('{subreddit}') 
                AND DATE(POST_DATE) = '{target_date}'
            """)
            
            count = cursor.fetchone()[0]
            cursor.close()
            
            if count > 0:
                logger.info(f"Found {count} existing records for r/{subreddit} on {target_date}")
                return True
            else:
                logger.info(f"No existing records found for r/{subreddit} on {target_date}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking existing data: {e}")
            return False
    
    def close(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")


def main():
    """Main function to run the Reddit scraper."""
    parser = argparse.ArgumentParser(
        description='Scrape Reddit posts from a subreddit for a specific date'
    )
    parser.add_argument('subreddit', help='Subreddit name (without r/)')
    parser.add_argument('date', help='Target date in YYYY-MM-DD format')
    parser.add_argument('--max-posts', type=int,
                        help='Maximum number of posts to retrieve')
    parser.add_argument('--output-csv', help='Output CSV filename')
    parser.add_argument('--output-json', help='Output JSON filename')
    parser.add_argument('--save-to-snowflake', action='store_true',
                        help='Save data to Snowflake table')
    parser.add_argument('--snowflake-table', default='reddit_posts',
                        help='Snowflake table name (default: reddit_posts)')
    parser.add_argument('--check-duplicates', action='store_true',
                        help='Check for existing data before scraping')
    parser.add_argument('--output-dir', default='.',
                        help='Output directory for CSV/JSON files (default: current directory)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD format.")
        return

    # Initialize scraper
    scraper = RedditScraper()

    # Check for existing data if requested
    if args.check_duplicates:
        logger.info("Checking for existing data...")
        
        # Check CSV files
        csv_exists = scraper.check_existing_csv(args.subreddit, args.date, args.output_dir)
        
        # Check Snowflake if enabled
        snowflake_exists = False
        if args.save_to_snowflake:
            try:
                snowflake_connector = SnowflakeConnector()
                snowflake_exists = snowflake_connector.check_existing_data(
                    args.subreddit, args.date, args.snowflake_table
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
    posts = scraper.get_posts_for_date(args.subreddit, args.date,
                                       args.max_posts)

    if not posts:
        logger.warning("No posts found for the specified date and subreddit.")
        return

    # Generate default filenames if not provided
    if not args.output_csv and not args.output_json:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_csv = os.path.join(args.output_dir, 
                                       f"reddit_posts_{args.subreddit}_{args.date}_"
                                       f"{timestamp}.csv")
        args.output_json = os.path.join(args.output_dir,
                                        f"reddit_posts_{args.subreddit}_{args.date}_"
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
            snowflake_connector.save_to_snowflake(posts, args.snowflake_table)
        except Exception as e:
            logger.error(f"Failed to save to Snowflake: {e}")
        finally:
            if snowflake_connector:
                snowflake_connector.close()

    # Print summary
    print("\nSummary:")
    print(f"Subreddit: r/{args.subreddit}")
    print(f"Date: {args.date}")
    print(f"Posts collected: {len(posts)}")
    if args.output_csv:
        print(f"CSV file: {args.output_csv}")
    if args.output_json:
        print(f"JSON file: {args.output_json}")
    if args.save_to_snowflake:
        print(f"Snowflake table: {args.snowflake_table}")


if __name__ == "__main__":
    main()

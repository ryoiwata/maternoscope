#!/usr/bin/env python3
"""
Reddit scraper using PullPush (formerly Pushshift) API for historical data.
This scraper is more reliable for getting posts from specific dates.
"""

import requests
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import logging
import json
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


class PullPushScraper:
    def __init__(self):
        """Initialize PullPush API scraper."""
        self.base_url = "https://api.pullpush.io/reddit/search/submission"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': os.getenv("REDDIT_USER_AGENT", "Maternoscope Data Collection Bot 1.0")
        })

    def get_posts_for_date(self, subreddit_name, target_date, max_posts=None):
        """
        Get all posts from a subreddit for a specific date using PullPush API.

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

            # Convert to Unix timestamps
            start_timestamp = int(start_of_day.timestamp())
            end_timestamp = int(end_of_day.timestamp())

            logger.info(
                f"Fetching posts from r/{subreddit_name} for {target_date}"
            )
            logger.info(f"Date range: {start_of_day} to {end_of_day}")
            logger.info(f"Timestamp range: {start_timestamp} to {end_timestamp}")

            posts_data = []
            after = None
            post_count = 0

            while True:
                # Prepare API parameters
                params = {
                    'subreddit': subreddit_name,
                    'after': start_timestamp,
                    'before': end_timestamp,
                    'size': min(100, max_posts - post_count) if max_posts else 100,
                    'sort': 'created_utc',
                    'sort_type': 'asc'
                }

                if after:
                    params['after'] = after

                logger.info(f"Making API request with params: {params}")

                try:
                    response = self.session.get(self.base_url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    if 'data' not in data or not data['data']:
                        logger.info("No more posts found")
                        break

                    posts = data['data']
                    logger.info(f"Retrieved {len(posts)} posts from API")

                    for post in posts:
                        if max_posts and post_count >= max_posts:
                            break

                        post_data = self._extract_post_data(post)
                        if post_data:
                            posts_data.append(post_data)
                            post_count += 1

                            if post_count % 50 == 0:
                                logger.info(f"Collected {post_count} posts so far...")

                    # Check if we have more posts to fetch
                    if len(posts) < params['size']:
                        logger.info("Reached end of available posts")
                        break

                    # Set 'after' to the last post's timestamp for pagination
                    after = posts[-1]['created_utc']
                    
                    # Add delay to respect API rate limits
                    time.sleep(1)

                except requests.exceptions.RequestException as e:
                    logger.error(f"API request failed: {e}")
                    break
                except KeyError as e:
                    logger.error(f"Unexpected API response format: {e}")
                    break

            # Sort posts by creation time
            posts_data.sort(key=lambda x: x['post_date'])

            logger.info(
                f"Successfully collected {len(posts_data)} posts from "
                f"r/{subreddit_name} for {target_date}"
            )

            if len(posts_data) == 0:
                logger.warning(f"No posts found for r/{subreddit_name} on {target_date}")
                logger.info("This could be due to:")
                logger.info("1. No posts were made on that specific date")
                logger.info("2. Subreddit doesn't exist or is private")
                logger.info("3. PullPush API is down or rate limited")
                logger.info("4. Date is too far in the past or future")

            return posts_data

        except Exception as e:
            logger.error(f"Error fetching posts: {e}")
            return []

    def _extract_post_data(self, post):
        """
        Extract relevant data from a PullPush API post.

        Args:
            post (dict): Post data from PullPush API

        Returns:
            dict: Dictionary containing post data
        """
        try:
            # Convert timestamp to datetime
            post_date = datetime.fromtimestamp(post['created_utc'])

            # Get post content (selftext for text posts, URL for link posts)
            content = post.get('selftext', '') or post.get('url', '')

            # Get post flair
            flair = post.get('link_flair_text', None)

            # Get post URL
            post_url = f"https://reddit.com{post.get('permalink', '')}"

            post_data = {
                'post_id': post['id'],
                'post_date': post_date,
                'post_timestamp': post['created_utc'],
                'post_flair': flair,
                'title': post.get('title', ''),
                'url': post_url,
                'content': content,
                'score': post.get('score', 0),
                'num_comments': post.get('num_comments', 0),
                'subreddit': post.get('subreddit', '')
            }

            return post_data

        except Exception as e:
            logger.error(f"Error extracting data from post {post.get('id', 'unknown')}: {e}")
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
            pattern = os.path.join(output_dir, f"pullpush_posts_{subreddit}_{target_date}_*.csv")
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
        """Create the pullpush_reddit_posts table if it doesn't exist."""
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
    """Main function to run the PullPush scraper."""
    parser = argparse.ArgumentParser(
        description='Scrape Reddit posts from a subreddit for a specific date using PullPush API'
    )
    parser.add_argument('subreddit', help='Subreddit name (without r/)')
    parser.add_argument('date', help='Target date in YYYY-MM-DD format')
    parser.add_argument('--max-posts', type=int,
                        help='Maximum number of posts to retrieve')
    parser.add_argument('--output-csv', help='Output CSV filename (optional)')
    parser.add_argument('--output-json', help='Output JSON filename (optional)')
    parser.add_argument('--check-duplicates', action='store_true',
                        help='Check for existing data before scraping')
    parser.add_argument('--output-dir', default='.',
                        help='Output directory for CSV/JSON files (default: current directory)')
    parser.add_argument('--save-to-snowflake', action='store_true',
                        help='Save data to Snowflake table')
    parser.add_argument('--snowflake-table', default='reddit_posts',
                        help='Snowflake table name (default: reddit_posts)')
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
    scraper = PullPushScraper()

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
    posts = scraper.get_posts_for_date(args.subreddit, args.date, args.max_posts)

    if not posts:
        logger.warning("No posts found for the specified date and subreddit.")
        return

    # Save data only if explicitly requested
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

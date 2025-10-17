#!/usr/bin/env python3
"""
Utility script to check for existing data in Snowflake and CSV files.
This script is used by the batch_scraper.sh script.
"""

import sys
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'ingestion'))

from reddit_scraper import RedditScraper, SnowflakeConnector

def check_csv_data(subreddit, target_date, output_dir):
    """Check for existing CSV data."""
    scraper = RedditScraper()
    return scraper.check_existing_csv(subreddit, target_date, output_dir)

def check_snowflake_data(subreddit, target_date, table_name):
    """Check for existing Snowflake data."""
    try:
        snowflake_connector = SnowflakeConnector()
        exists = snowflake_connector.check_existing_data(subreddit, target_date, table_name)
        snowflake_connector.close()
        return exists
    except Exception as e:
        print(f"Error checking Snowflake: {e}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description='Check for existing data')
    parser.add_argument('subreddit', help='Subreddit name')
    parser.add_argument('date', help='Target date in YYYY-MM-DD format')
    parser.add_argument('--output-dir', default='.', help='Output directory for CSV files')
    parser.add_argument('--snowflake-table', default='reddit_posts', help='Snowflake table name')
    parser.add_argument('--check-csv', action='store_true', help='Check CSV files')
    parser.add_argument('--check-snowflake', action='store_true', help='Check Snowflake')
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    csv_exists = False
    snowflake_exists = False
    
    if args.check_csv:
        csv_exists = check_csv_data(args.subreddit, args.date, args.output_dir)
    
    if args.check_snowflake:
        snowflake_exists = check_snowflake_data(args.subreddit, args.date, args.snowflake_table)
    
    # Return appropriate exit code
    if csv_exists or snowflake_exists:
        print("EXISTS")
        sys.exit(0)
    else:
        print("NOT_EXISTS")
        sys.exit(1)

if __name__ == "__main__":
    main()

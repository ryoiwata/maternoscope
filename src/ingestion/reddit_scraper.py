# src/ingestion/reddit_scraper.py

import praw
import pandas as pd
import argparse
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import logging

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
                                logger.info(f"Collected {post_count} posts so far...")

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
                'post_date': datetime.fromtimestamp(
                    submission.created_utc
                ).strftime('%Y-%m-%d %H:%M:%S'),
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
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")


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

    # Get posts
    posts = scraper.get_posts_for_date(args.subreddit, args.date,
                                       args.max_posts)

    if not posts:
        logger.warning("No posts found for the specified date and subreddit.")
        return

    # Generate default filenames if not provided
    if not args.output_csv and not args.output_json:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_csv = (f"reddit_posts_{args.subreddit}_{args.date}_"
                           f"{timestamp}.csv")
        args.output_json = (f"reddit_posts_{args.subreddit}_{args.date}_"
                            f"{timestamp}.json")

    # Save data
    if args.output_csv:
        scraper.save_to_csv(posts, args.output_csv)

    if args.output_json:
        scraper.save_to_json(posts, args.output_json)

    # Print summary
    print("\nSummary:")
    print(f"Subreddit: r/{args.subreddit}")
    print(f"Date: {args.date}")
    print(f"Posts collected: {len(posts)}")
    if args.output_csv:
        print(f"CSV file: {args.output_csv}")
    if args.output_json:
        print(f"JSON file: {args.output_json}")


if __name__ == "__main__":
    main()
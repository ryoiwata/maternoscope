#!/bin/bash
# Batch scraper for top pregnancy-related Reddit posts
# Uses PRAW to scrape top posts from last day across multiple subreddits

set -e  # Exit on error

# Suppress AWS botocore debug messages
export AWS_EC2_METADATA_DISABLED=true
export BOTO_CONFIG=/dev/null

# Configuration
SUBREDDITS=("pregnant" "BabyBumps" "TryingForABaby" "beyondthebump")
TIME_FILTER="day"  # Top posts from past day
OUTPUT_DIR="data/raw/reddit"
SCRIPT_DIR="src/ingestion"
LOG_FILE="logs/ingestion/reddit_scrape_output_$(date +%Y%m%d_%H%M%S).log"
ERROR_LOG="logs/ingestion/reddit_scrape_errors_$(date +%Y%m%d_%H%M%S).log"

# Create necessary directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "logs/ingestion"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to scrape a single subreddit
scrape_subreddit() {
    local subreddit=$1
    log "=========================================="
    log "Starting scrape for r/$subreddit"
    log "Time filter: $TIME_FILTER"
    log "=========================================="
    
    # Run the PRAW scraper and capture errors
    if python3 "$SCRIPT_DIR/praw_scraper.py" \
        "$subreddit" \
        "$TIME_FILTER" \
        --output-dir "$OUTPUT_DIR" \
        --save-to-snowflake \
        --snowflake-table "REDDIT_POSTS" \
        --check-duplicates \
        --verbose 2>"$ERROR_LOG"; then
        log "Completed scrape for r/$subreddit successfully"
    else
        exit_code=$?
        log "ERROR: Failed to scrape r/$subreddit (exit code: $exit_code)"
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] Error scraping r/$subreddit (exit code: $exit_code)" >> "$ERROR_LOG"
    fi
    log ""
    
    # Add a small delay to avoid rate limits
    sleep 2
}

# Main execution
log "Starting batch Reddit scraper for pregnancy-related subreddits"
log "Target subreddits: ${SUBREDDITS[*]}"
log "Time filter: $TIME_FILTER"
log "Output directory: $OUTPUT_DIR"
log ""

# Loop through each subreddit and scrape
for subreddit in "${SUBREDDITS[@]}"; do
    scrape_subreddit "$subreddit"
done

log "=========================================="
log "Batch scraping completed!"
log "Total subreddits processed: ${#SUBREDDITS[@]}"
log "Log file: $LOG_FILE"
log "Error log: $ERROR_LOG"
log "=========================================="

# Check if error log has content
if [ -s "$ERROR_LOG" ]; then
    log "WARNING: Errors were encountered during scraping. Check $ERROR_LOG for details."
fi

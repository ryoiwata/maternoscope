#!/bin/bash

# Batch PullPush Reddit Scraper Script
# Usage: ./batch_scraper.sh [options]

# Note: We don't use 'set -e' because we handle return codes explicitly in the scraping logic

# Default values
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REDDIT_SCRIPT="$SCRIPT_DIR/pullpush_scraper.py"
SUBREDDITS=""
START_DATE=""
END_DATE=""
MAX_POSTS=""
OUTPUT_DIR="$SCRIPT_DIR/data"
SAVE_TO_SNOWFLAKE=false
SNOWFLAKE_TABLE="reddit_posts"
VERBOSE=false
DRY_RUN=false
TEST_PATTERN=false
NO_POSTS_THRESHOLD=3
RATE_LIMIT_WAIT=3600

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Batch PullPush Reddit Scraper

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -s, --subreddits SUBREDDITS    Comma-separated list of subreddits (required)
    -d, --start-date DATE          Start date in YYYY-MM-DD format (required)
    -e, --end-date DATE            End date in YYYY-MM-DD format (required)
    -m, --max-posts NUMBER         Maximum posts per subreddit per day (optional)
    -o, --output-dir DIR           Output directory (default: ./data)
    --snowflake                    Save to Snowflake (requires .env configuration)
    --snowflake-table TABLE        Snowflake table name (default: reddit_posts)
    --no-posts-threshold N         Wait for rate limit after N attempts on current combination (default: 3)
    --rate-limit-wait SECONDS      Wait time for rate limit reset in seconds (default: 3600)
    --max-retries N                Maximum retry attempts per attempt cycle (default: 5)
    -v, --verbose                  Enable verbose logging
    --dry-run                      Show what would be done without executing
    --test-pattern                 Test pattern matching for no-posts detection
    -h, --help                     Show this help message

EXAMPLES:
    # Scrape r/pregnancy and r/babybumps for a week
    $0 -s "pregnancy,babybumps" -d 2024-01-01 -e 2024-01-07

    # Scrape with post limit and save to Snowflake
    $0 -s "pregnancy" -d 2024-01-01 -e 2024-01-31 -m 100 --snowflake

    # Dry run to see what would be scraped
    $0 -s "pregnancy,babybumps" -d 2024-01-01 -e 2024-01-07 --dry-run

    # With custom rate limiting (wait after 5 attempts on current combination, wait 30 minutes)
    $0 -s "pregnancy" -d 2024-01-01 -e 2024-01-31 --no-posts-threshold 5 --rate-limit-wait 1800

ADVANTAGES OF PULLPUSH API:
    - Better for historical data
    - More reliable date-based filtering
    - No rate limiting issues
    - No Reddit API credentials required

REQUIREMENTS:
    - Python 3.7+
    - Snowflake credentials in .env file (if using --snowflake)
EOF
}

# Function to validate date format
validate_date() {
    local date=$1
    if ! date -d "$date" >/dev/null 2>&1; then
        print_error "Invalid date format: $date. Use YYYY-MM-DD format."
        exit 1
    fi
}

# Function to generate date range
generate_date_range() {
    local start_date=$1
    local end_date=$2
    
    local current_date="$start_date"
    local end_timestamp=$(date -d "$end_date" +%s)
    
    while true; do
        local current_timestamp=$(date -d "$current_date" +%s)
        if [[ $current_timestamp -gt $end_timestamp ]]; then
            break
        fi
        echo "$current_date"
        current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
    done
}

# Function to wait for rate limit reset
wait_for_rate_limit() {
    local wait_time=$RATE_LIMIT_WAIT
    local hours=$((wait_time / 3600))
    local minutes=$(((wait_time % 3600) / 60))
    local seconds=$((wait_time % 60))
    
    print_warning "Rate limit detected. Waiting $wait_time seconds (${hours}h ${minutes}m ${seconds}s) before continuing..."
    
    # Show countdown
    for ((i=wait_time; i>0; i--)); do
        printf "\r${YELLOW}[WAIT]${NC} Rate limit cooldown: %02d:%02d:%02d remaining..." $((i/3600)) $(((i%3600)/60)) $((i%60))
        sleep 1
    done
    echo ""
    print_info "Rate limit cooldown complete. Resuming scraping..."
}

# Function to log failed scrapes
log_failed_scrape() {
    local subreddit=$1
    local date=$2
    local reason=$3
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "$timestamp - r/$subreddit - $date - $reason" >> "$FAILED_SCRAPES_LOG"
    print_warning "Logged failed scrape: r/$subreddit on $date - $reason"
}

# Function to retry scraping with simple retry logic
retry_scrape() {
    local subreddit=$1
    local date=$2
    local max_posts=$3
    local output_dir=$4
    local snowflake_table=$5
    local save_to_snowflake=$6
    local verbose=$7
    
    local attempt=1
    local max_attempts=$max_retry_attempts
    
    while [[ $attempt -le $max_attempts ]]; do
        print_info "Retry attempt $attempt/$max_attempts for r/$subreddit on $date"
        
        # Scrape the data
        scrape_single "$subreddit" "$date" "$max_posts" "$output_dir" "$snowflake_table" "$save_to_snowflake" "$verbose"
        local scrape_result=$?
        
        if [[ "$verbose" == true ]]; then
            print_info "Scraper returned code: $scrape_result"
        fi
        
        case $scrape_result in
            0)  # Success with posts found
                print_success "Successfully scraped r/$subreddit on $date on attempt $attempt"
                return 0
                ;;
            1)  # Failure - retry immediately
                print_warning "Scrape failed for r/$subreddit on $date (attempt $attempt/$max_attempts)"
                if [[ $attempt -lt $max_attempts ]]; then
                    print_info "Retrying immediately..."
                fi
                ;;
            2)  # No posts found - retry immediately
                print_warning "No posts found for r/$subreddit on $date (attempt $attempt/$max_attempts)"
                if [[ $attempt -lt $max_attempts ]]; then
                    print_info "Retrying immediately..."
                else
                    print_warning "Max retry attempts reached for r/$subreddit on $date. Moving on."
                    log_failed_scrape "$subreddit" "$date" "No posts found after $max_attempts attempts"
                    return 2
                fi
                ;;
        esac
        
        attempt=$((attempt + 1))
    done
    
    # If we get here, all attempts failed
    log_failed_scrape "$subreddit" "$date" "Failed after $max_attempts attempts"
    return 1
}

# Function to check if data already exists
check_existing_data() {
    local subreddit=$1
    local date=$2
    local output_dir=$3
    local snowflake_table=$4
    local save_to_snowflake=$5
    
    local csv_exists=false
    local snowflake_exists=false
    
    # Check CSV files (PullPush doesn't create CSV by default, so this is optional)
    local csv_pattern="$output_dir/pullpush_posts_${subreddit}_${date}_*.csv"
    if ls $csv_pattern 1> /dev/null 2>&1; then
        local csv_count=$(ls $csv_pattern | wc -l)
        print_warning "Found $csv_count existing CSV file(s) for r/$subreddit on $date"
        csv_exists=true
    fi
    
    # Check Snowflake if enabled
    if [[ "$save_to_snowflake" == true ]]; then
        print_info "Checking Snowflake for existing data..."
        local check_script="$SCRIPT_DIR/check_existing_data.py"
        if [[ -f "$check_script" ]]; then
            local result=$(python "$check_script" "$subreddit" "$date" --check-snowflake --snowflake-table "$snowflake_table" 2>/dev/null)
            if [[ "$result" == "EXISTS" ]]; then
                print_warning "Found existing Snowflake data for r/$subreddit on $date"
                snowflake_exists=true
            fi
        else
            print_warning "Snowflake check script not found, skipping Snowflake check"
        fi
    fi
    
    # Return true if any data exists
    if [[ "$csv_exists" == true || "$snowflake_exists" == true ]]; then
        return 0
    else
        return 1
    fi
}

# Function to scrape a single subreddit/date combination
scrape_single() {
    local subreddit=$1
    local date=$2
    local max_posts=$3
    local output_dir=$4
    local snowflake_table=$5
    local save_to_snowflake=$6
    local verbose=$7
    
    print_info "Scraping r/$subreddit for $date using PullPush API..."
    
    # Build command
    local cmd="python $REDDIT_SCRIPT $subreddit $date"
    
    if [[ -n "$max_posts" ]]; then
        cmd="$cmd --max-posts $max_posts"
    fi
    
    if [[ "$save_to_snowflake" == true ]]; then
        cmd="$cmd --save-to-snowflake --snowflake-table $snowflake_table"
    fi
    
    if [[ "$verbose" == true ]]; then
        cmd="$cmd --verbose"
    fi
    
    if [[ "$DRY_RUN" == true ]]; then
        print_info "DRY RUN: $cmd"
        return 0
    fi
    
    # Execute command and capture output
    local output
    if output=$(eval $cmd 2>&1); then
        # Debug: Show the actual output for troubleshooting
        if [[ "$verbose" == true ]]; then
            echo "DEBUG - Scraper output:"
            echo "$output"
        fi
        
        # Check if posts were actually found by looking for "Successfully collected 0 posts" in output
        if echo "$output" | grep -q "Successfully collected 0 posts"; then
            print_warning "No posts found for r/$subreddit on $date"
            if [[ "$verbose" == true ]]; then
                print_info "DEBUG: Pattern 'Successfully collected 0 posts' matched in output"
            fi
            return 2  # Special return code for no posts found
        else
            # Also check for any posts count in the output
            local posts_count=$(echo "$output" | grep -o "Successfully collected [0-9]* posts" | grep -o "[0-9]*")
            if [[ -n "$posts_count" && "$posts_count" -gt 0 ]]; then
                print_success "Successfully scraped $posts_count posts from r/$subreddit for $date"
            else
                print_success "Successfully scraped r/$subreddit for $date"
            fi
            if [[ "$verbose" == true ]]; then
                print_info "DEBUG: Pattern 'Successfully collected 0 posts' NOT matched in output"
            fi
            return 0
        fi
    else
        print_error "Failed to scrape r/$subreddit for $date"
        return 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--subreddits)
            SUBREDDITS="$2"
            shift 2
            ;;
        -d|--start-date)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end-date)
            END_DATE="$2"
            shift 2
            ;;
        -m|--max-posts)
            MAX_POSTS="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --snowflake)
            SAVE_TO_SNOWFLAKE=true
            shift
            ;;
        --snowflake-table)
            SNOWFLAKE_TABLE="$2"
            shift 2
            ;;
        --no-posts-threshold)
            NO_POSTS_THRESHOLD="$2"
            shift 2
            ;;
        --rate-limit-wait)
            RATE_LIMIT_WAIT="$2"
            shift 2
            ;;
        --max-retries)
            max_retry_attempts="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --test-pattern)
            TEST_PATTERN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Test pattern matching if requested (do this before validation)
if [[ "$TEST_PATTERN" == true ]]; then
    print_info "Testing pattern matching for no-posts detection..."
    print_info "Running: python $REDDIT_SCRIPT pregnant 2025-09-15 --max-posts 5"
    
    output=$(python "$REDDIT_SCRIPT" pregnant 2025-09-15 --max-posts 5 2>&1)
    echo ""
    print_info "Full scraper output:"
    echo "$output"
    echo ""
    
    if echo "$output" | grep -q "Successfully collected 0 posts"; then
        print_success "✓ Pattern 'Successfully collected 0 posts' MATCHED"
        print_info "This should trigger return code 2 (no posts found)"
    else
        print_error "✗ Pattern 'Successfully collected 0 posts' NOT MATCHED"
        print_info "Looking for alternative patterns..."
        
        if echo "$output" | grep -q "collected.*0.*posts"; then
            print_info "Found alternative pattern: 'collected.*0.*posts'"
        fi
        
        if echo "$output" | grep -q "0 posts"; then
            print_info "Found pattern: '0 posts'"
        fi
    fi
    
    exit 0
fi

# Validate required arguments
if [[ -z "$SUBREDDITS" ]]; then
    print_error "Subreddits are required. Use -s or --subreddits."
    show_usage
    exit 1
fi

if [[ -z "$START_DATE" ]]; then
    print_error "Start date is required. Use -d or --start-date."
    show_usage
    exit 1
fi

if [[ -z "$END_DATE" ]]; then
    print_error "End date is required. Use -e or --end-date."
    show_usage
    exit 1
fi

# Validate dates
validate_date "$START_DATE"
validate_date "$END_DATE"

# Check if start date is before end date
if [[ "$START_DATE" > "$END_DATE" ]]; then
    print_error "Start date must be before or equal to end date."
    exit 1
fi

# Check if PullPush scraper script exists
if [[ ! -f "$REDDIT_SCRIPT" ]]; then
    print_error "PullPush scraper script not found: $REDDIT_SCRIPT"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Convert subreddits to array
IFS=',' read -ra SUBREDDIT_ARRAY <<< "$SUBREDDITS"

# Calculate total combinations
total_combinations=0
for subreddit in "${SUBREDDIT_ARRAY[@]}"; do
    subreddit=$(echo "$subreddit" | xargs)  # Trim whitespace
    date_count=$(generate_date_range "$START_DATE" "$END_DATE" | wc -l)
    total_combinations=$((total_combinations + date_count))
done

print_info "Starting PullPush batch scrape..."
print_info "Subreddits: ${SUBREDDIT_ARRAY[*]}"
print_info "Date range: $START_DATE to $END_DATE"
print_info "Total combinations: $total_combinations"
print_info "Output directory: $OUTPUT_DIR"

if [[ "$SAVE_TO_SNOWFLAKE" == true ]]; then
    print_info "Snowflake table: $SNOWFLAKE_TABLE"
fi

if [[ "$DRY_RUN" == true ]]; then
    print_warning "DRY RUN MODE - No actual scraping will be performed"
fi

# Initialize counters
current_combination=0
successful_scrapes=0
failed_scrapes=0
skipped_scrapes=0

# Create failed scrapes log file
FAILED_SCRAPES_LOG="$OUTPUT_DIR/failed_scrapes_$(date +%Y%m%d_%H%M%S).log"

# Main scraping loop
for subreddit in "${SUBREDDIT_ARRAY[@]}"; do
    subreddit=$(echo "$subreddit" | xargs)  # Trim whitespace
    print_info "Processing subreddit: r/$subreddit"
    
    # Generate dates for this subreddit
    while IFS= read -r date; do
        current_combination=$((current_combination + 1))
        
        print_info "[$current_combination/$total_combinations] Processing r/$subreddit for $date"
        
        # Check if data already exists
        if check_existing_data "$subreddit" "$date" "$OUTPUT_DIR" "$SNOWFLAKE_TABLE" "$SAVE_TO_SNOWFLAKE"; then
            print_warning "Skipping r/$subreddit for $date (data already exists)"
            skipped_scrapes=$((skipped_scrapes + 1))
            continue
        fi
        
        # Try to scrape with retries until rate limit threshold is reached
        scrape_success=false
        current_attempts=0
        
        while [[ "$scrape_success" == false ]]; do
            # Use retry mechanism
            retry_scrape "$subreddit" "$date" "$MAX_POSTS" "$OUTPUT_DIR" "$SNOWFLAKE_TABLE" "$SAVE_TO_SNOWFLAKE" "$VERBOSE"
            retry_result=$?
            current_attempts=$((current_attempts + 1))
            
            case $retry_result in
                0)  # Success
                    successful_scrapes=$((successful_scrapes + 1))
                    scrape_success=true
                    print_success "Successfully completed r/$subreddit for $date"
                    ;;
                1)  # Complete failure after all retries
                    failed_scrapes=$((failed_scrapes + 1))
                    print_error "Failed to scrape r/$subreddit for $date after all retries"
                    ;;
                2)  # No posts found after all retries
                    print_warning "No posts found for r/$subreddit on $date after all retries"
                    ;;
            esac
            
            # If we haven't succeeded, check if we should wait for rate limit
            if [[ "$scrape_success" == false ]]; then
                if [[ $current_attempts -ge $NO_POSTS_THRESHOLD ]]; then
                    print_warning "Rate limit threshold reached ($NO_POSTS_THRESHOLD attempts). Triggering wait..."
                    wait_for_rate_limit
                    print_info "Resuming scraping after rate limit wait..."
                    # Reset attempt counter and continue the loop to retry the current date/subreddit
                    current_attempts=0
                else
                    print_warning "Attempts: $current_attempts (threshold: $NO_POSTS_THRESHOLD)"
                    print_info "Retrying current combination..."
                fi
            fi
        done
        
        # If we still haven't succeeded after all attempts, log it and move on
        if [[ "$scrape_success" == false ]]; then
            print_error "Giving up on r/$subreddit for $date after all attempts"
            log_failed_scrape "$subreddit" "$date" "Gave up after rate limit threshold reached"
        fi
        
        # Add small delay between requests to be respectful to PullPush API
        sleep 2
        
    done < <(generate_date_range "$START_DATE" "$END_DATE")
done

# Print final summary
print_info "Batch scraping completed!"
print_success "Successful scrapes: $successful_scrapes"
if [[ $skipped_scrapes -gt 0 ]]; then
    print_warning "Skipped scrapes: $skipped_scrapes"
fi
if [[ $failed_scrapes -gt 0 ]]; then
    print_error "Failed scrapes: $failed_scrapes"
    print_warning "Failed scrapes have been logged to: $FAILED_SCRAPES_LOG"
fi

# Show failed scrapes log if it exists and has content
if [[ -f "$FAILED_SCRAPES_LOG" && -s "$FAILED_SCRAPES_LOG" ]]; then
    print_info "Failed scrapes log contents:"
    cat "$FAILED_SCRAPES_LOG"
fi

# Exit with appropriate code
if [[ $failed_scrapes -gt 0 ]]; then
    exit 1
else
    exit 0
fi

#!/bin/bash

# Batch PullPush Reddit Scraper Script
# Usage: ./batch_scraper.sh [options]

set -e  # Exit on any error

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
    -v, --verbose                  Enable verbose logging
    --dry-run                      Show what would be done without executing
    -h, --help                     Show this help message

EXAMPLES:
    # Scrape r/pregnancy and r/babybumps for a week
    $0 -s "pregnancy,babybumps" -d 2024-01-01 -e 2024-01-07

    # Scrape with post limit and save to Snowflake
    $0 -s "pregnancy" -d 2024-01-01 -e 2024-01-31 -m 100 --snowflake

    # Dry run to see what would be scraped
    $0 -s "pregnancy,babybumps" -d 2024-01-01 -e 2024-01-07 --dry-run

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
    
    # Execute command
    if eval $cmd; then
        print_success "Successfully scraped r/$subreddit for $date"
        return 0
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
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
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
        
        # Scrape the data
        if scrape_single "$subreddit" "$date" "$MAX_POSTS" "$OUTPUT_DIR" "$SNOWFLAKE_TABLE" "$SAVE_TO_SNOWFLAKE" "$VERBOSE"; then
            successful_scrapes=$((successful_scrapes + 1))
        else
            failed_scrapes=$((failed_scrapes + 1))
        fi
        
        # Add small delay between requests to be respectful to Reddit API
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
fi

# Exit with appropriate code
if [[ $failed_scrapes -gt 0 ]]; then
    exit 1
else
    exit 0
fi

#!/bin/bash
# Helper script to run dbt with environment variables loaded from .env

# Get project root directory (two levels up from this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT" || exit 1

# Load environment variables from .env file
# Handle quoted values properly (strip quotes)
if [ -f .env ]; then
    # Export variables, stripping quotes and comments
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip comments and empty lines
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line// }" ]] && continue
        
        # Remove leading/trailing whitespace
        line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        # Extract key and value, handling quoted values
        if [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)=(.+)$ ]]; then
            key="${BASH_REMATCH[1]}"
            value="${BASH_REMATCH[2]}"
            # Remove surrounding quotes if present
            value=$(echo "$value" | sed 's/^"\(.*\)"$/\1/')
            # Export the variable
            export "${key}=${value}"
        fi
    done < .env
    echo "✓ Loaded environment variables from .env"
else
    echo "⚠ Warning: .env file not found at $PROJECT_ROOT/.env"
    exit 1
fi

# Verify required variables are set
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo "✗ Error: SNOWFLAKE_ACCOUNT not set"
    exit 1
fi
if [ -z "$SNOWFLAKE_USERNAME" ]; then
    echo "✗ Error: SNOWFLAKE_USERNAME not set"
    exit 1
fi

# Change to dbt project directory
cd "$SCRIPT_DIR" || exit 1

# Run dbt with any arguments passed to this script
echo "Running dbt with arguments: $@"
echo "Using account: ${SNOWFLAKE_ACCOUNT}"
echo "Using database: ${SNOWFLAKE_DATABASE}"
echo "Using schema: ${SNOWFLAKE_SCHEMA}"
echo ""

# Verify variables are exported (for debugging)
if [ -z "$SNOWFLAKE_USERNAME" ]; then
    echo "✗ Error: SNOWFLAKE_USERNAME not exported"
    exit 1
fi

# Ensure all required variables are explicitly exported
export SNOWFLAKE_ACCOUNT
export SNOWFLAKE_USERNAME
export SNOWFLAKE_PASSWORD
export SNOWFLAKE_ROLE
export SNOWFLAKE_DATABASE
export SNOWFLAKE_WAREHOUSE
export SNOWFLAKE_SCHEMA

# Run dbt - environment variables will be inherited by the subprocess
dbt "$@"


#!/bin/bash
# Quick script to load .env variables into current shell
# Usage: source load_env.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    # Use set -a to auto-export, then source the .env file
    # This handles quoted values properly
    set -a
    source <(grep -v '^#' "$PROJECT_ROOT/.env" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/^"//;s/"$//' | grep -v '^$')
    set +a
    echo "✓ Environment variables loaded from .env"
    echo "  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT}"
    echo "  SNOWFLAKE_DATABASE: ${SNOWFLAKE_DATABASE}"
else
    echo "✗ Error: .env file not found at $PROJECT_ROOT/.env"
    return 1 2>/dev/null || exit 1
fi






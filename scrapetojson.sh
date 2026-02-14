#!/bin/bash

# Set project directory
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Change to project directory
cd "$PROJECT_DIR" || exit 1

{
    echo "=== $(date +'%Y-%m-%d %H:%M:%S') ==="
    echo "Running scrapetojson.sh"
    echo "Working directory: $(pwd)"
    # Random sleep between 0 and 900 seconds (15 minutes)
    # RANDOM_DELAY=$(($RANDOM % 901))
    RANDOM_DELAY=3
    echo "Sleeping for $RANDOM_DELAY seconds..."
    sleep $RANDOM_DELAY
} >> "$LOG_DIR/scraper_cron_stdout.log" 2>> "$LOG_DIR/scraper_cron_stderr.log"

# Run scrapetojson.py using the direct python executable path for cron reliability
/opt/anaconda3/envs/job-env/bin/python3 "$PROJECT_DIR/scrapetojson.py" "$@" >> "$LOG_DIR/scraper_cron_stdout.log" 2>> "$LOG_DIR/scraper_cron_stderr.log"

exit_code=$?

{
    echo "=== $(date +'%Y-%m-%d %H:%M:%S') ==="
    echo "Python script exited with code: $exit_code"
} >> "$LOG_DIR/scraper_cron_stdout.log"

if [ $exit_code -ne 0 ]; then
    {
        echo "=== $(date +'%Y-%m-%d %H:%M:%S') ==="
        echo "Scraper failed with exit code $exit_code"
        echo ""
    } >> "$LOG_DIR/scraper_cron_stderr.log"
    exit $exit_code
fi

echo "" >> "$LOG_DIR/scraper_cron_stdout.log"

exit 0
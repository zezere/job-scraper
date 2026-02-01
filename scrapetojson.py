import argparse
import json
import os
from datetime import datetime

import pandas as pd
from jobspy import scrape_jobs

from utils import setup_logging

# Use shared logging setup
logger = setup_logging("scrapetojson")


def run_scraper(
    output_dir: str,
    file_prefix: str,
    search_term: str,
    location: str,
    results_wanted: int,
    hours_old: int,
    linkedin_fetch_description: bool,
    verbose: int,
):
    # Construct filename: prefix_timestamp_ms.json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    filename = f"{file_prefix}_{timestamp}.json"
    output_file = os.path.join(output_dir, filename)

    params = {
        "output_dir": output_dir,
        "file_prefix": file_prefix,
        "search_term": search_term,
        "location": location,
        "results_wanted": results_wanted,
        "hours_old": hours_old,
        "output_file": output_file,
        "linkedin_fetch_description": linkedin_fetch_description,
        "verbose": verbose,
    }
    logger.info(f"Starting scrape with parameters: {json.dumps(params, default=str)}")

    try:
        jobs: pd.DataFrame = scrape_jobs(
            site_name="linkedin",
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
            linkedin_fetch_description=linkedin_fetch_description,
            verbose=verbose,
        )

        logger.info(f"Scrape complete. Found {len(jobs)} jobs.")

        if len(jobs) > 0:
            # Ensure output directory exists
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Save to JSON - orient='records' creates a list of dicts, dates as ISO string
            jobs.to_json(output_file, orient="records", date_format="iso", indent=2)
            logger.info(f"Successfully saved results to {output_file}")
        else:
            logger.warning("No jobs found with the given criteria.")

    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise  # Re-raise to let Airflow know the task failed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape jobs and save to JSON.")

    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory to save the JSON output",
    )
    parser.add_argument(
        "--file_prefix", type=str, required=True, help="Prefix for the filename"
    )
    parser.add_argument("--search_term", type=str, required=True)
    parser.add_argument("--location", type=str, default="European Union")
    parser.add_argument("--results_wanted", type=int, default=3)
    parser.add_argument("--hours_old", type=int, default=24)
    parser.add_argument(
        "--linkedin_fetch_description",
        type=str,
        default="True",
        help="Fetch full description (True/False)",
    )
    parser.add_argument(
        "--verbose",
        type=int,
        default=2,
        help="Verbosity level for scraper logs (0, 1, or 2).",
    )

    args = parser.parse_args()

    fetch_desc = args.linkedin_fetch_description.lower() in ["true", "1", "yes"]

    run_scraper(
        output_dir=args.output_dir,
        file_prefix=args.file_prefix,
        search_term=args.search_term,
        location=args.location,
        results_wanted=args.results_wanted,
        hours_old=args.hours_old,
        linkedin_fetch_description=fetch_desc,
        verbose=args.verbose,
    )

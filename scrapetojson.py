import argparse
import logging
import json
import os
from datetime import datetime
from utils import setup_logging

import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from jobspy import scrape_jobs

# Load environment variables from .env
load_dotenv()

# Use shared logging setup - initialized later with verbose arg
logger = setup_logging("scrapetojson")


def upload_to_s3(content: str, bucket: str, key: str) -> bool:
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=bucket, Key=key, Body=content)
        return True
    except Exception as e:
        logger.error(f"S3 Upload Error: {e}")
        return False


def run_scraper(
    output_dir: str,
    file_prefix: str,
    search_term: str,
    location: str,
    results_wanted: int,
    hours_old: int,
    linkedin_fetch_description: bool,
    verbose: int,
    output_to_s3: bool = False,
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
        "output_to_s3": output_to_s3,
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
            json_str = jobs.to_json(orient="records", date_format="iso", indent=2)
            upload_success = False

            if output_to_s3:
                bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
                if not bucket_name:
                    logger.error(
                        "AWS_S3_BUCKET_NAME not found in environment variables."
                    )
                else:
                    logger.info(f"Attempting upload to S3 bucket: {bucket_name}")
                    if upload_to_s3(json_str, bucket_name, filename):
                        logger.info(
                            f"Successfully uploaded to S3: s3://{bucket_name}/{filename}"
                        )
                        upload_success = True
                    else:
                        logger.warning("S3 upload failed. Falling back to local save.")

            if not upload_success:
                # Ensure output directory exists
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)

                # Save to JSON locally
                with open(output_file, "w") as f:
                    f.write(json_str)
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
    parser.add_argument(
        "--output_to_s3",
        action="store_true",
        help="Upload results to S3 bucket defined in AWS_S3_BUCKET_NAME",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose > 1 else logging.INFO
    logger.setLevel(level)

    # Silence third-party logs (JobSpy, urllib3, boto3) -> ERROR only
    logging.getLogger("JobSpy").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("boto3").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.ERROR)

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
        output_to_s3=args.output_to_s3,
    )

import argparse
import glob
import json
import logging
import os
import re

from utils import setup_logging

# Use shared logging setup
logger = setup_logging("migrate_historic_data")


def extract_date_from_filename(filename: str) -> str:
    """
    Extracts the date in YYYY-MM-DD format from a filename
    expected to have the pattern: prefix_YYYYMMDD_HHMMSS_MMM.json
    Returns the date string if found, else None.
    """
    # Look for 8 consecutive digits followed by an underscore (YYYYMMDD_)
    match = re.search(r"_(\d{8})_", filename)
    if match:
        date_str = match.group(1)
        # Format as YYYY-MM-DD
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return None


def process_file(input_filepath: str, output_filepath: str) -> bool:
    """
    Reads a JSON file, adds scraped_on if missing, and saves to output_filepath.
    """
    filename = os.path.basename(input_filepath)
    scraped_on_date = extract_date_from_filename(filename)

    if not scraped_on_date:
        logger.warning(f"Could not extract date from filename: {filename}. Skipping.")
        return False

    try:
        with open(input_filepath, "r") as f:
            jobs = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read JSON from {input_filepath}: {e}")
        return False

    if not isinstance(jobs, list):
        logger.warning(
            f"Expected a list of jobs in {filename}, found {type(jobs)}. Skipping."
        )
        return False

    modified = False
    for job in jobs:
        if "scraped_on" not in job:
            job["scraped_on"] = scraped_on_date
            modified = True

    # Always save to the new directory, even if no records needed modification,
    # so the output directory contains a complete set of processed files.
    try:
        with open(output_filepath, "w") as f:
            json.dump(jobs, f, indent=2)
        if modified:
            logger.info(f"Processed and updated: {filename} -> {scraped_on_date}")
        else:
            logger.info(f"Processed (no changes needed): {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to write JSON to {output_filepath}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Add scraped_on to historical job JSON files based on their timestamped filenames."
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        required=True,
        help="Directory containing the historical JSON files",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Directory to save the modified JSON files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    input_dir = args.input_dir
    output_dir = args.output_dir

    if not os.path.isdir(input_dir):
        logger.error(f"Input directory does not exist: {input_dir}")
        return

    if not os.path.exists(output_dir):
        logger.info(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir)

    # Use glob to find all json files
    search_pattern = os.path.join(input_dir, "*.json")
    json_files = glob.glob(search_pattern)

    logger.info(f"Found {len(json_files)} JSON files in {input_dir}")

    success_count = 0
    for input_filepath in json_files:
        filename = os.path.basename(input_filepath)
        output_filepath = os.path.join(output_dir, filename)

        if process_file(input_filepath, output_filepath):
            success_count += 1

    logger.info(
        f"Finished processing. Successfully wrote {success_count} / {len(json_files)} files to {output_dir}"
    )


if __name__ == "__main__":
    main()

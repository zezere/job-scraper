import argparse
import glob
import json
import os
import sys
from typing import List, Dict, Any, Tuple
from db_connection import get_connection
from utils import setup_logging

# Define relevant columns exactly as they appear in the database
TABLE_COLUMNS = [
    "id",
    "site",
    "job_url",
    "job_url_direct",
    "title",
    "company",
    "location",
    "date_posted",
    "job_type",
    "salary_source",
    "interval",
    "min_amount",
    "max_amount",
    "currency",
    "is_remote",
    "job_level",
    "job_function",
    "emails",
    "description",
    "company_industry",
    "company_url",
    "company_logo",
    "company_url_direct",
]

# Use shared logging setup
logger = setup_logging("jsontodb")


def insert_jobs(
    cursor, table_name: str, jobs: List[Dict[str, Any]]
) -> Tuple[int, int, List[Dict]]:
    """
    Inserts a list of dicts using simple INSERT (no DB side dedup).
    Assumes jobs is already deduplicated.
    Returns (saved_count, skipped_count, failed_jobs).
    """
    if not jobs:
        return 0, 0, []

    saved_count = 0
    skipped_count = 0
    failed_jobs = []

    # Prepare SQL
    cols_quoted = [f'"{col}"' for col in TABLE_COLUMNS]
    cols_str = ", ".join(cols_quoted)
    placeholders_str = ", ".join(["%s"] * len(TABLE_COLUMNS))

    query = f"INSERT INTO public.{table_name} ({cols_str}) VALUES ({placeholders_str})"

    for idx, item in enumerate(jobs, 1):
        job_id = item.get("id", "unknown")
        job_title = item.get("title", "unknown")

        try:
            values = [item.get(col) for col in TABLE_COLUMNS]

            cursor.execute(query, tuple(values))
            cursor.connection.commit()
            saved_count += 1

            if idx % 50 == 0:
                logger.debug(f"Progress: {idx}/{len(jobs)} jobs processed")

        except Exception as e:
            cursor.connection.rollback()
            logger.error(f"Error saving job '{job_id}' ({job_title}): {e}")
            # We treat DB errors as failures
            failed_jobs.append({"title": f"{job_id} - {job_title}", "error": str(e)})

    return saved_count, skipped_count, failed_jobs


def load_file_content(file_path: str) -> List[Dict]:
    """Helper to safely load a JSON list from a file."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]
        return data or []
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return []


def process_path(path_arg: str):
    """
    1. Finds all files (or single file).
    2. Loads ALL data into memory.
    3. Deduplicates across the ENTIRE dataset.
    4. Inserts the unique set.
    """
    logger.info(f"Target path: {path_arg}")

    # 1. Gather Files
    target_files = []
    if os.path.isdir(path_arg):
        target_files = sorted(glob.glob(os.path.join(path_arg, "*.json")))
        if not target_files:
            logger.warning(f"No .json files found in {path_arg}")
            sys.exit(0)
    elif os.path.exists(path_arg):
        target_files = [path_arg]
    else:
        logger.error(f"Path not found: {path_arg}")
        sys.exit(1)

    logger.info(f"Found {len(target_files)} files to process.")

    # 2. Aggregation & Global Deduplication
    all_jobs = []
    seen_hashes = set()
    total_raw_records = 0

    for fpath in target_files:
        file_data = load_file_content(fpath)
        total_raw_records += len(file_data)

        for item in file_data:
            # Create a stable content hash
            item_hash = json.dumps(item, sort_keys=True)
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                all_jobs.append(item)

    duplicates_removed = total_raw_records - len(all_jobs)
    logger.info("=" * 60)
    logger.info("BATCH AGGREGATION REPORT")
    logger.info(f"  - Total Files:      {len(target_files)}")
    logger.info(f"  - Total Records:    {total_raw_records}")
    logger.info(f"  - Global Duplicates:{duplicates_removed}")
    logger.info(f"  - Unique Payload:   {len(all_jobs)}")
    logger.info("=" * 60)

    if not all_jobs:
        logger.warning("No unique records to ingest.")
        return

    # 3. Insertion Phase
    logger.info("Starting Database Insertion...")
    try:
        with get_connection() as (conn, cursor):
            saved, skipped, failed = insert_jobs(cursor, "jobsli", all_jobs)

            # --- FINAL SUMMARY ---
            logger.info("=" * 60)
            logger.info(f"DB INSERTION COMPLETE")
            logger.info(f"  - Saved:    {saved}")
            logger.info(f"  - Failed:   {len(failed)}")
            if len(failed) > 0:
                logger.warning(f"  - First 5 Failures:")
                for f in failed[:5]:
                    logger.warning(f"    * {f['title']}: {f['error'][:100]}")
            logger.info("=" * 60)

            # Exit 1 if anything failed to save (strict mode as suggested by previous context)
            if failed:
                sys.exit(1)
            else:
                sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal DB Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest JSON to DB (Global Deduplication)"
    )
    parser.add_argument("--path", help="Path to JSON file or directory", required=True)
    args = parser.parse_args()

    process_path(args.path)

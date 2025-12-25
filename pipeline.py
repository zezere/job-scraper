import logging
import os
import psycopg2
import pandas as pd
from datetime import datetime
from config import DATABASE_URL

logger = logging.getLogger(__name__)


def extract_unique_jobs(export_duplicates_csv=True):
    """
    Extracts jobs with unique 'id' values from jobsli table.
    For duplicate IDs, keeps the one with latest 'created_at' timestamp.

    Args:
        export_duplicates_csv: If True, exports duplicate records to CSV

    Returns:
        DataFrame with unique jobs (non-duplicates + latest from each duplicate group)
    """
    logger.info("=" * 60)
    logger.info("Starting unique jobs extraction")
    logger.info("=" * 60)

    try:
        logger.info("Establishing database connection...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}", exc_info=True)
        raise

    try:
        logger.info("Querying all jobs from jobsli table...")
        query = "SELECT * FROM public.jobsli ORDER BY created_at DESC"
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        jobs_df = pd.DataFrame(rows, columns=column_names)
        logger.info(f"Retrieved {len(jobs_df)} total jobs from database")

        if jobs_df.empty:
            logger.warning("No jobs found in database")
            return pd.DataFrame()

        if "id" not in jobs_df.columns:
            logger.error("'id' column not found in jobs table")
            return pd.DataFrame()

        if "created_at" not in jobs_df.columns:
            logger.error("'created_at' column not found in jobs table")
            return pd.DataFrame()

        logger.info("Analyzing duplicates by 'id' field...")
        id_counts = jobs_df["id"].value_counts()
        duplicate_ids = id_counts[id_counts > 1].index.tolist()

        if not duplicate_ids:
            logger.info("No duplicate IDs found. All jobs are unique.")
            return jobs_df

        logger.info(
            f"Found {len(duplicate_ids)} duplicate ID(s) affecting {id_counts[duplicate_ids].sum()} records"
        )

        duplicate_records = jobs_df[jobs_df["id"].isin(duplicate_ids)].copy()
        unique_records = jobs_df[~jobs_df["id"].isin(duplicate_ids)].copy()

        logger.info(f"Records breakdown:")
        logger.info(f"  - Unique (no duplicates): {len(unique_records)}")
        logger.info(f"  - Duplicate records: {len(duplicate_records)}")

        logger.info("Converting created_at to datetime for sorting...")
        duplicate_records["created_at"] = pd.to_datetime(
            duplicate_records["created_at"]
        )

        if export_duplicates_csv:
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            duplicate_count = len(duplicate_records)
            csv_filename = f"logs/duplicates_{timestamp}_{duplicate_count}jobs.csv"

            sorted_duplicates = duplicate_records.sort_values(
                ["id", "created_at"], ascending=[True, False]
            )
            sorted_duplicates.to_csv(csv_filename, index=False)
            logger.info(
                f"Exported {duplicate_count} duplicate records to {csv_filename} (sorted by id, then created_at desc)"
            )

        logger.info("Selecting latest record for each duplicate ID (by created_at)...")
        latest_duplicates = (
            duplicate_records.sort_values("created_at", ascending=False)
            .groupby("id")
            .first()
            .reset_index()
        )

        logger.info(
            f"Selected {len(latest_duplicates)} latest records from duplicate groups"
        )

        unique_jobs = pd.concat([unique_records, latest_duplicates], ignore_index=True)
        logger.info(f"Final unique jobs count: {len(unique_jobs)}")

        logger.info("=" * 60)
        logger.info("Unique jobs extraction completed:")
        logger.info(f"  - Total jobs in database: {len(jobs_df)}")
        logger.info(f"  - Unique jobs extracted: {len(unique_jobs)}")
        logger.info(
            f"  - Duplicate records removed: {len(duplicate_records) - len(latest_duplicates)}"
        )
        logger.info("=" * 60)

        return unique_jobs

    except Exception as e:
        logger.error(f"Error during unique jobs extraction: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")


def move_to_prepared_jobs(unique_jobs_df, export_failed_csv=True):
    """
    Moves unique jobs from DataFrame to preparedjobs table.
    Checks for existing records by comparing all columns except id_primary, created_at, and prepared_at.
    Skips duplicates and logs them.

    Args:
        unique_jobs_df: DataFrame from extract_unique_jobs()
        export_failed_csv: If True, exports failed records to CSV

    Returns:
        dict with stats: {'inserted': count, 'skipped': count, 'errors': count}
    """
    logger.info("=" * 60)
    logger.info("Starting move to preparedjobs table")
    logger.info("=" * 60)

    if unique_jobs_df.empty:
        logger.warning("Received empty DataFrame, nothing to move")
        return {"inserted": 0, "skipped": 0, "errors": 0}

    try:
        logger.info("Establishing database connection...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}", exc_info=True)
        raise

    excluded_from_uniqueness = {"id_primary", "created_at", "prepared_at"}
    excluded_from_insert = {"id_primary", "prepared_at"}

    columns_to_check = [
        col for col in unique_jobs_df.columns if col not in excluded_from_uniqueness
    ]
    columns_to_insert = [
        col for col in unique_jobs_df.columns if col not in excluded_from_insert
    ]

    logger.info(f"Columns to check for uniqueness: {columns_to_check}")
    logger.info(f"Columns to insert: {columns_to_insert}")
    logger.info(f"Excluded from uniqueness check: {excluded_from_uniqueness}")
    logger.info(f"Excluded from insert: {excluded_from_insert}")

    inserted_count = 0
    skipped_count = 0
    error_count = 0
    skipped_records = []
    failed_records = []

    columns_str = ", ".join(
        [f'"{col}"' if col == "interval" else col for col in columns_to_insert]
    )
    placeholders = ", ".join(["%s"] * len(columns_to_insert))

    def get_value(row, key):
        val = row.get(key)
        return val if pd.notna(val) else None

    def check_record_exists(row):
        conditions = []
        values = []
        for col in columns_to_check:
            val = get_value(row, col)
            if val is None:
                conditions.append(
                    f'"{col}" IS NULL' if col == "interval" else f"{col} IS NULL"
                )
            else:
                conditions.append(
                    f'"{col}" = %s' if col == "interval" else f"{col} = %s"
                )
                values.append(val)

        check_query = f"""
        SELECT id_primary, id FROM public.preparedjobs
        WHERE {' AND '.join(conditions)}
        LIMIT 1
        """
        cursor.execute(check_query, values)
        return cursor.fetchone()

    insert_query = f"""
    INSERT INTO public.preparedjobs ({columns_str})
    VALUES ({placeholders})
    """

    logger.info(f"Processing {len(unique_jobs_df)} unique jobs...")
    # Performance note: For large datasets, consider optimizing with batch checks or NOT EXISTS queries

    try:
        for idx, (_, row) in enumerate(unique_jobs_df.iterrows(), 1):
            job_id = row.get("id", "unknown")
            job_title = row.get("title", "unknown")

            try:
                existing = check_record_exists(row)

                if existing:
                    id_primary, existing_id = existing
                    skipped_count += 1
                    skipped_records.append(
                        {
                            "id_primary": id_primary,
                            "id": existing_id,
                            "title": job_title,
                        }
                    )
                    if skipped_count <= 10:
                        logger.info(
                            f"Skipped duplicate: id_primary={id_primary}, id={existing_id}, title={job_title}"
                        )
                else:
                    values = tuple(get_value(row, col) for col in columns_to_insert)
                    cursor.execute(insert_query, values)
                    conn.commit()
                    inserted_count += 1

                    if idx % 10 == 0:
                        logger.debug(
                            f"Progress: {idx}/{len(unique_jobs_df)} processed ({inserted_count} inserted, {skipped_count} skipped, {error_count} errors)"
                        )

            except Exception as e:
                conn.rollback()
                error_count += 1
                failed_records.append(
                    {
                        "id": job_id,
                        "title": job_title,
                        "error": str(e),
                    }
                )
                logger.error(
                    f"Error moving job '{job_title}' (id: {job_id}): {e}",
                    exc_info=True,
                )

        if skipped_records:
            logger.info(f"Skipped {len(skipped_records)} duplicate records:")
            for skipped in skipped_records[:10]:
                logger.info(
                    f"  - id_primary={skipped['id_primary']}, id={skipped['id']}, title={skipped['title']}"
                )
            if len(skipped_records) > 10:
                logger.info(f"  ... and {len(skipped_records) - 10} more")

        if failed_records:
            if export_failed_csv:
                log_dir = "logs"
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                failed_count = len(failed_records)
                csv_filename = (
                    f"logs/failed_prepared_{timestamp}_{failed_count}jobs.csv"
                )
                failed_df = pd.DataFrame(failed_records)
                failed_df.to_csv(csv_filename, index=False)
                logger.warning(
                    f"Exported {failed_count} failed records to {csv_filename}"
                )
            else:
                logger.warning(
                    f"{len(failed_records)} records failed but CSV export is disabled"
                )

        logger.info("=" * 60)
        logger.info("Move to preparedjobs completed:")
        logger.info(f"  - Inserted: {inserted_count} jobs")
        logger.info(f"  - Skipped (duplicates): {skipped_count} jobs")
        logger.info(f"  - Errors: {error_count} jobs")
        logger.info("=" * 60)

        return {
            "inserted": inserted_count,
            "skipped": skipped_count,
            "errors": error_count,
        }

    except Exception as e:
        logger.error(f"Error during move to preparedjobs: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")


def process_jobs(batch_size=1):
    """
    Processes jobs from preparedjobs and moves them to processedjobs.
    Picks oldest jobs first (by prepared_at), processes them, and deletes from preparedjobs.

    Args:
        batch_size: Number of jobs to process (default: 1)

    Returns:
        dict with stats: {'processed': count, 'errors': count}
    """
    logger.info("=" * 60)
    logger.info("Starting job processing")
    logger.info("=" * 60)

    try:
        logger.info("Establishing database connection...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        logger.info("Database connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}", exc_info=True)
        raise

    excluded_from_insert = {"id_primary", "processed_at"}

    processed_count = 0
    error_count = 0
    failed_jobs = []

    try:
        logger.info(f"Querying {batch_size} oldest jobs from preparedjobs...")
        query = f"""
        SELECT * FROM public.preparedjobs 
        ORDER BY prepared_at ASC 
        LIMIT %s
        """
        cursor.execute(query, (batch_size,))
        jobs = cursor.fetchall()

        if not jobs:
            logger.info("No jobs found in preparedjobs to process")
            return {"processed": 0, "errors": 0}

        column_names = [desc[0] for desc in cursor.description]
        logger.info(f"Found {len(jobs)} jobs to process")

        columns_to_insert = [
            col for col in column_names if col not in excluded_from_insert
        ]
        columns_str = ", ".join(
            [f'"{col}"' if col == "interval" else col for col in columns_to_insert]
        )
        placeholders = ", ".join(["%s"] * len(columns_to_insert))

        insert_query = f"""
        INSERT INTO public.processedjobs ({columns_str})
        VALUES ({placeholders})
        """

        delete_query = """
        DELETE FROM public.preparedjobs
        WHERE id_primary = %s
        """

        logger.info(f"Processing {len(jobs)} jobs...")

        for idx, job_row in enumerate(jobs, 1):
            job_dict = dict(zip(column_names, job_row))
            id_primary = job_dict["id_primary"]
            job_id = job_dict.get("id", "unknown")
            job_title = job_dict.get("title", "unknown")

            try:
                values = tuple(job_dict[col] for col in columns_to_insert)

                cursor.execute(insert_query, values)
                cursor.execute(delete_query, (id_primary,))
                conn.commit()

                processed_count += 1
                logger.debug(
                    f"Processed job {idx}/{len(jobs)}: id={job_id}, title={job_title}"
                )

            except Exception as e:
                conn.rollback()
                error_count += 1
                failed_jobs.append(
                    {
                        "id_primary": id_primary,
                        "id": job_id,
                        "title": job_title,
                        "error": str(e),
                    }
                )
                logger.error(
                    f"Error processing job '{job_title}' (id: {job_id}, id_primary: {id_primary}): {e}",
                    exc_info=True,
                )
                continue

        if failed_jobs:
            logger.warning(f"Failed to process {len(failed_jobs)} jobs:")
            for failed in failed_jobs[:5]:
                logger.warning(
                    f"  - id_primary={failed['id_primary']}, id={failed['id']}, title={failed['title']}: {failed['error'][:100]}"
                )
            if len(failed_jobs) > 5:
                logger.warning(f"  ... and {len(failed_jobs) - 5} more")

        logger.info("=" * 60)
        logger.info("Job processing completed:")
        logger.info(f"  - Processed: {processed_count} jobs")
        logger.info(f"  - Errors: {error_count} jobs")
        logger.info("=" * 60)

        return {"processed": processed_count, "errors": error_count}

    except Exception as e:
        logger.error(f"Error during job processing: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed")

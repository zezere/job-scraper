"""
Processor Module Logic Documentation

This module handles the processing of jobs from the `preparedjobs` queue, moving them to the final `processedjobs` table. It manages new job insertions and deduplication updates.

High-Level Process:
1.  **Job Selection**:
    -   The `pick_job_for_processing` function selects a single job from `preparedjobs`.
    -   **Priority**: 'new' jobs are processed before 'duplicate' jobs.
    -   **Ordering**: Oldest `created_at` first, using `id_primary` as a tie-breaker.
    -   **Locking**: The chosen job is atomically updated with a "worker_02 " status prefix to prevent race conditions during parallel processing.

2.  **New Job Processing**:
    -   Handled by `process_new_job`.
    -   **Input**: Jobs with status "new" (locked as "worker_02 new").
    -   **Action**:
        -   Validation: Ensures the job's `id_primary` does not already exist in `processedjobs`.
        -   Insertion: Moves the job to `processedjobs`.
            -   `seen_first_at` is set to the job's `created_at`.
            -   `seen_last_at` is initialized to NULL.
        -   Cleanup: The job is deleted from `preparedjobs` upon success.

3.  **Duplicate Job Processing**:
    -   Handled by `process_duplicate_job`.
    -   **Input**: Jobs with status "duplicate: <id_list>" (locked as "worker_02 duplicate: ...").
    -   **Action**:
        -   Parsing: Extracts potential original IDs from the status string.
        -   **Strict Validation**:
            -   Found Candidates: Must find **exactly one** corresponding original record in `processedjobs`. (Fails if 0 or >1 found).
            -   Content Match: The duplicate must match the original exactly in all content fields (excluding id_primary, status and timestamps).
            -   Timestamp Logic:
                -   Original `seen_first_at` < Duplicate `created_at`.
                -   Original `seen_last_at` (if present) must be between `seen_first_at` and Duplicate `created_at`.
        -   **Outcome (Success)**:
            -   Updates the *original* job's `seen_last_at` to the duplicate's `created_at`.
            -   Deletes the duplicate from `preparedjobs`.
        -   **Outcome (Failure)**:
            -   Marks the job as failed in `preparedjobs` (e.g. "failed_validation ...") and leaves it there for manual inspection.

4.  **Main Execution Loop**:
    -   `run_processor_loop` continuously picks and processes jobs.
    -   It stops gracefully when the queue is empty ("No jobs found").
    -   It provides limits for batch processing if needed.

5.  **Tables Involved**:
    -   `preparedjobs`: The source queue (populated by Matcher).
    -   `processedjobs`: The final destination for unique jobs and the target for deduplication updates.
"""

import re
import pandas as pd
import time
import traceback
from typing import Tuple, Optional, Any
from db_connection import get_connection
from utils import setup_logging, get_value

logger = setup_logging("processor")

# =============================================================================
# CREATE SCRIPT OF TABLE processedjobs
#
# CREATE TABLE IF NOT EXISTS public.processedjobs
# (
#     id_primary bigint NOT NULL,
#     id character varying COLLATE pg_catalog."default" NOT NULL,
#     site character varying COLLATE pg_catalog."default",
#     job_url character varying COLLATE pg_catalog."default",
#     job_url_direct character varying COLLATE pg_catalog."default",
#     title character varying COLLATE pg_catalog."default",
#     company character varying COLLATE pg_catalog."default",
#     location character varying COLLATE pg_catalog."default",
#     date_posted character varying COLLATE pg_catalog."default",
#     job_type character varying COLLATE pg_catalog."default",
#     salary_source character varying COLLATE pg_catalog."default",
#     "interval" character varying COLLATE pg_catalog."default",
#     min_amount double precision,
#     max_amount double precision,
#     currency character varying COLLATE pg_catalog."default",
#     is_remote boolean,
#     job_level character varying COLLATE pg_catalog."default",
#     job_function character varying COLLATE pg_catalog."default",
#     emails character varying COLLATE pg_catalog."default",
#     description character varying COLLATE pg_catalog."default",
#     company_industry character varying COLLATE pg_catalog."default",
#     company_url character varying COLLATE pg_catalog."default",
#     company_logo character varying COLLATE pg_catalog."default",
#     company_url_direct character varying COLLATE pg_catalog."default",
#     seen_first_at timestamp with time zone,
#     seen_last_at timestamp with time zone,
#     CONSTRAINT processedjobs_pkey PRIMARY KEY (id_primary)
# )


def pick_job_for_processing(cursor) -> pd.Series | None:
    """
    Selects and marks a job for processing from preparedjobs using the logic:
    1. Prefer 'new' status, then 'duplicate' status.
    2. Order by created_at ASC, then id_primary ASC.
    3. Locks row by updating status to prefix with "worker_02 ".

    Returns:
        pd.Series with job data, or None if no jobs found.
    """
    worker_id = "worker_02"

    # We update and return in one atomic statement
    # The ORDER BY logic must be inside the subquery to pick the right row
    update_query = f"""
        UPDATE public.preparedjobs
        SET status = '{worker_id} ' || status
        WHERE id_primary = (
            SELECT id_primary
            FROM public.preparedjobs
            WHERE status = 'new' OR status LIKE 'duplicate%'
            ORDER BY 
                CASE WHEN status = 'new' THEN 1 ELSE 2 END ASC, -- Prefer 'new'
                created_at ASC,
                id_primary ASC
            LIMIT 1
        )
        RETURNING *
    """

    cursor.execute(update_query)
    row = cursor.fetchone()

    if row is None:
        return None

    column_names = [desc[0] for desc in cursor.description]
    return pd.Series(row, index=column_names)


def process_new_job(cursor, job_row: pd.Series) -> None:
    """
    Moves a 'new' job from preparedjobs to processedjobs.

    Rules:
    - Target id_primary must NOT exist.
    - Copies all fields except created_at, prepared_at, status.
    - seen_first_at = created_at.
    - seen_last_at = NULL.
    - Deletes from preparedjobs on success.
    """
    id_primary = get_value(job_row, "id_primary")
    title = get_value(job_row, "title")
    created_at = get_value(job_row, "created_at")

    logger.info(f"Processing NEW job: {id_primary} - {title}")

    # 1. Check if exists in processedjobs
    cursor.execute(
        "SELECT 1 FROM public.processedjobs WHERE id_primary = %s", (id_primary,)
    )
    if cursor.fetchone():
        raise ValueError(f"Job {id_primary} already exists in processedjobs table")

    # 2. Dynamic column copy
    # Fetch processedjobs columns
    cursor.execute("SELECT * FROM public.processedjobs LIMIT 0")
    target_columns = [desc[0] for desc in cursor.description]

    # Build insert logic
    insert_cols = []
    insert_vals = []
    placeholders = []

    excluded_source_cols = {
        "created_at",
        "prepared_at",
        "status",
    }

    for col in target_columns:
        if col == "seen_first_at":
            insert_cols.append(col)
            insert_vals.append(created_at)
            placeholders.append("%s")
        elif col == "seen_last_at":
            insert_cols.append(col)
            insert_vals.append(None)
            placeholders.append("%s")
        elif col in excluded_source_cols:
            continue
        else:
            # Map standard columns if they exist in source
            if col in job_row.index:
                insert_cols.append(
                    f'"{col}"' if col == "interval" or " " in col else col
                )
                insert_vals.append(get_value(job_row, col))
                placeholders.append("%s")

    cols_str = ", ".join(insert_cols)
    placeholders_str = ", ".join(placeholders)

    insert_query = (
        f"INSERT INTO public.processedjobs ({cols_str}) VALUES ({placeholders_str})"
    )
    cursor.execute(insert_query, tuple(insert_vals))

    # 3. Delete from preparedjobs
    cursor.execute(
        "DELETE FROM public.preparedjobs WHERE id_primary = %s", (id_primary,)
    )
    logger.info(f"Successfully moved job {id_primary} to processedjobs")


def process_duplicate_job(cursor, job_row: pd.Series, status_msg: str) -> None:
    """
    Handles a 'duplicate' job.

    1. Parses original IDs from status (e.g. "duplicate: 123, 456").
    2. Checks processedjobs for exactly one match (the 'original') that satisfies all strict criteria:
       - Exact duplicate in all fields (except id_primary, status, timestamps).
       - seen_first_at < current job created_at.
       - seen_last_at logic (NULL or within range).
    3. If valid: Updates original's seen_last_at = duplicate's created_at, Deletes duplicate from preparedjobs.
    4. If invalid: Appends "failed" to preparedjobs status.
    """
    duplicate_row = job_row
    duplicate_id = get_value(duplicate_row, "id_primary")
    duplicate_created_at = get_value(duplicate_row, "created_at")

    # 1. Parse IDs (of potential originals)
    # Status format: "worker_02 duplicate: 123, 456"
    match = re.search(r"duplicate:\s*([\d,\s]+)", status_msg)
    if not match:
        logger.warning(f"Could not parse original IDs from status: {status_msg}")
        _mark_failed(cursor, duplicate_id, "failed_parsing")
        return

    original_ids = [
        int(x.strip()) for x in match.group(1).split(",") if x.strip().isdigit()
    ]
    if not original_ids:
        _mark_failed(cursor, duplicate_id, "failed_converting_ids")
        return

    logger.info(
        f"Processing DUPLICATE job {duplicate_id}, claims to be duplicate of: {original_ids}"
    )

    # 2. Fetch originals from processedjobs
    placeholders = ",".join(["%s"] * len(original_ids))
    fetch_query = (
        f"SELECT * FROM public.processedjobs WHERE id_primary IN ({placeholders})"
    )
    cursor.execute(fetch_query, tuple(original_ids))
    originals = cursor.fetchall()

    if not originals:
        logger.warning(f"No originals found in processedjobs for IDs: {original_ids}")
        _mark_failed(cursor, duplicate_id, "failed_finding_original")
        return

    if len(originals) > 1:
        logger.warning(
            f"Ambiguous: found {len(originals)} originals in processedjobs for IDs: {original_ids}"
        )
        _mark_failed(cursor, duplicate_id, "failed_too_many_originals")
        return

    logger.info(f"Fetched {len(originals)} originals.")
    original_cols = [desc[0] for desc in cursor.description]

    # 3. Validation
    # We established len(originals) == 1
    original_row = originals[0]
    original_series = pd.Series(original_row, index=original_cols)
    original_id = get_value(original_series, "id_primary")

    excluded_compare_cols = {
        "id_primary",
        "status",
        "created_at",
        "prepared_at",
        "seen_first_at",
        "seen_last_at",
    }

    # A. Check content equality
    is_exact = True
    for col in duplicate_row.index:
        if col in excluded_compare_cols:
            continue
        if col not in original_series.index:
            continue

        val1 = get_value(duplicate_row, col)
        val2 = get_value(original_series, col)

        # Treat None/Empty as equal
        if (val1 is None or val1 == "") and (val2 is None or val2 == ""):
            continue

        if val1 != val2:
            is_exact = False
            break

    if not is_exact:
        logger.warning(
            f"Duplicate content mismatch for job {duplicate_id} vs original {original_id}"
        )
        _mark_failed(cursor, duplicate_id, "failed_validating_sameness")
        return

    # B. Check timestamp logic
    # Original's seen_first must be older than Duplicate's created_at
    seen_first = get_value(original_series, "seen_first_at")
    seen_last = get_value(original_series, "seen_last_at")

    if seen_first >= duplicate_created_at:
        logger.warning(
            f"Timestamp mismatch: Original {original_id} seen_first ({seen_first}) >= Duplicate {duplicate_id} created ({duplicate_created_at})"
        )
        _mark_failed(cursor, duplicate_id, "failed_validating_timestamps")
        return

    # Original's seen_last (if exists) must be older than Duplicate's created_at
    # AND seen_first < seen_last
    if seen_last is not None:
        if not (seen_first < seen_last < duplicate_created_at):
            logger.warning(
                f"Timestamp mismatch: Original {original_id} seen_last ({seen_last}) not in valid range for Duplicate {duplicate_id} ({duplicate_created_at})"
            )
            _mark_failed(cursor, duplicate_id, "failed_validating_timestamps")
            return

    # 4. Success Action
    logger.info(
        f"Validation passed. Merging duplicate {duplicate_id} into original {original_id}"
    )

    # Update original's seen_last_at using duplicate's created_at
    cursor.execute(
        "UPDATE public.processedjobs SET seen_last_at = %s WHERE id_primary = %s",
        (duplicate_created_at, original_id),
    )

    # Delete duplicate from preparedjobs
    cursor.execute(
        "DELETE FROM public.preparedjobs WHERE id_primary = %s", (duplicate_id,)
    )
    logger.info(f"Successfully processed duplicate {duplicate_id}")


def _mark_failed(cursor, id_primary: int, prefix: str) -> None:
    """Helper to append failure prefix to status in preparedjobs."""

    update_query = """
        UPDATE public.preparedjobs 
        SET status = %s || ' ' || status 
        WHERE id_primary = %s
    """
    cursor.execute(update_query, (prefix, id_primary))


def process_job(cursor) -> bool:
    """
    Picks and processes one job.
    Returns: True if a job was processed, False otherwise.
    """
    job = pick_job_for_processing(cursor)

    if job is None:
        return False

    job_id = get_value(job, "id_primary")
    status = get_value(job, "status")

    logger.info(f"Picked and locked job: {job_id} (Current Status: {status})")

    try:
        if "new" in status:
            process_new_job(cursor, job)
        elif "duplicate" in status:
            process_duplicate_job(cursor, job, status)
        else:
            logger.warning(f"Unknown status '{status}' for job {job_id}")
            _mark_failed(cursor, job_id, "failed_unknown_status")

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")
        traceback.print_exc()
        _mark_failed(cursor, job_id, "failed_exception")

    return True


def run_processor_loop(limit: int = 0) -> None:
    """
    Continually processes jobs.
    limit: Max number of jobs to process (0 for infinite).
    """
    logger.info("Starting processor loop...")
    count = 0

    while True:
        try:
            with get_connection() as (conn, cursor):
                processed = process_job(cursor)
                conn.commit()

            if processed:
                count += 1
                if limit > 0 and count >= limit:
                    logger.info(f"Limit of {limit} jobs reached.")
                    break
            else:
                # No jobs found, stop
                logger.info("No jobs found, stopping processor.")
                break

        except KeyboardInterrupt:
            logger.info("Processor loop stopped by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in processor loop: {e}")
            traceback.print_exc()
            time.sleep(5)
        # Log empty line
        logger.info("")


if __name__ == "__main__":
    run_processor_loop(1)

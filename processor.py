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
from db_ops import fetch_and_lock_job, transfer_job

logger = setup_logging("processor")


def _mark_failed(cursor, id_primary: int, prefix: str) -> None:
    """Helper to append failure prefix to status in preparedjobs."""

    update_query = """
        UPDATE public.preparedjobs 
        SET status = %s || ' ' || status 
        WHERE id_primary = %s
    """
    cursor.execute(update_query, (prefix, id_primary))


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

    # 1. Prefer 'new' status, then 'duplicate', then 'enrichment'.
    # TODO: consider making duplicate and enrichment cases of equal importance.
    # 2. Order by created_at ASC, then id_primary ASC.
    conditions = (
        "status = 'new' OR status LIKE 'duplicate%' OR status LIKE 'enrichment%'"
    )
    order_by = """
        CASE 
            WHEN status = 'new' THEN 1 
            WHEN status LIKE 'duplicate%' OR status LIKE 'enrichment%' THEN 2 
            ELSE 3 -- Fallback (shouldn't happen given WHERE clause)
        END ASC,
        created_at ASC,
        id_primary ASC
    """

    return fetch_and_lock_job(
        cursor=cursor,
        table_name="preparedjobs",
        worker_id=worker_id,
        conditions=conditions,
        order_by=order_by,
        limit=1,
    )


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

    try:
        # seen_first_at/seen_last_at are not in source, so we put them in override values.
        overrides = {"seen_first_at": created_at, "seen_last_at": None}

        transfer_job(
            cursor=cursor,
            job_id=id_primary,
            source_table="preparedjobs",
            target_table="processedjobs",
            target_status=None,
            target_override_values=overrides,
            delete_source=True,
            source_status=None,
            source_status_on_failure="error: target_exists",
        )
        logger.info(f"Successfully moved job {id_primary} to processedjobs")

    except ValueError as e:
        # transfer_job might raise ValueError if job not found or multiple found
        logger.error(f"Error moving job {id_primary}: {e}", exc_info=True)

        raise


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
    # We can use generic update_row, but we need to construct the data dict
    from db_ops import update_row

    update_data = {"seen_last_at": duplicate_created_at}
    update_row(
        cursor=cursor,
        table_name="processedjobs",
        data=update_data,
        id_primary=original_id,
    )

    # Delete duplicate from preparedjobs
    cursor.execute(
        "DELETE FROM public.preparedjobs WHERE id_primary = %s", (duplicate_id,)
    )
    logger.info(f"Successfully processed duplicate {duplicate_id}")


def process_enriched_job(cursor, job_row: pd.Series, status_msg: str) -> None:
    """
    Handles an 'enriched' job.

    1. Parses original IDs from status (e.g. "enrichment: 123, 456").
    2. Checks processedjobs for exactly one match (the 'original') that satisfies all strict criteria:
       - Non-null/non-empty fields are exact duplicates (except id_primary, status, timestamps).
       - seen_first_at < current job created_at.
       - seen_last at is NULL or not older than current job created_at.
    3. If valid: Updates original's NULL/empty fields with current job's values in those fields (enrichment).
    4. If invalid: Appends "failed" to preparedjobs status.
    """
    enrichment_row = job_row
    enrichment_id = get_value(enrichment_row, "id_primary")
    enrichment_created_at = get_value(enrichment_row, "created_at")

    # 1. Parse IDs (of potential originals)
    match = re.search(r"enrichment:\s*([\d,\s]+)", status_msg)
    if not match:
        logger.warning(f"Could not parse original IDs from status: {status_msg}")
        _mark_failed(cursor, enrichment_id, "failed_parsing")
        return

    original_ids = [
        int(x.strip()) for x in match.group(1).split(",") if x.strip().isdigit()
    ]
    if not original_ids:
        _mark_failed(cursor, enrichment_id, "failed_converting_ids")
        return

    logger.info(
        f"Processing ENRICHMENT job {enrichment_id}, claims to enrich: {original_ids}"
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
        _mark_failed(cursor, enrichment_id, "failed_finding_original")
        return

    if len(originals) > 1:
        logger.warning(
            f"Ambiguous: found {len(originals)} originals in processedjobs for IDs: {original_ids}"
        )
        _mark_failed(cursor, enrichment_id, "failed_too_many_originals")
        return

    logger.info(f"Fetched {len(originals)} originals.")

    # 3. Validation
    # We established len(originals) == 1
    original_row = originals[0]
    original_cols = [desc[0] for desc in cursor.description]
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

    # A. Check content consistency (Non-conflicting)
    # Rules:
    # - If Original has data, New Job must match it (Strict duplicate).
    # - If Original is empty, New Job can have data (Enrichment).
    # - If Both are empty, Match.

    update_data = {}
    is_consistent = True

    all_fields = set(enrichment_row.index).union(original_series.index)

    for col in all_fields:
        if col in excluded_compare_cols:
            continue

        # We only care about columns that exist in the DB schema (original_series)
        # If the new job has extra fields not in DB, we can't save them anyway, so ignore?
        # Or if original is missing a column present in enrichment? Ideally we rely on DB schema.
        if col not in original_series.index:
            continue

        # Get values
        val_enrich = (
            get_value(enrichment_row, col) if col in enrichment_row.index else None
        )
        val_orig = get_value(original_series, col)

        is_enrich_empty = val_enrich is None or val_enrich == ""
        is_orig_empty = val_orig is None or val_orig == ""

        # Case 1: Both empty -> OK
        if is_enrich_empty and is_orig_empty:
            continue

        # Case 2: Original has data
        if not is_orig_empty:
            if is_enrich_empty:
                # Original has data, new job doesn't.
                # This technically isn't a conflict, but usually we expect new job to be "better".
                # Logic: "Non-null/non-empty fields are exact duplicates"
                # This implies if New has data, it must match.
                pass
            elif val_enrich != val_orig:
                # Conflict!
                logger.warning(
                    f"Enrichment content conflict for job {enrichment_id} field '{col}': '{val_enrich}' != '{val_orig}'"
                )
                is_consistent = False
                break

        # Case 3: Original is empty, New has data -> ENRICHMENT
        if is_orig_empty and not is_enrich_empty:
            update_data[col] = val_enrich

    if not is_consistent:
        _mark_failed(cursor, enrichment_id, "failed_validating_consistency")
        return

    # B. Check timestamp logic
    # Identical to duplicate logic
    seen_first = get_value(original_series, "seen_first_at")
    seen_last = get_value(original_series, "seen_last_at")

    if seen_first >= enrichment_created_at:
        logger.warning(
            f"Timestamp mismatch: Original {original_id} seen_first ({seen_first}) >= Enrichment {enrichment_id} created ({enrichment_created_at})"
        )
        _mark_failed(cursor, enrichment_id, "failed_validating_timestamps")
        return

    if seen_last is not None:
        if not (seen_first < seen_last < enrichment_created_at):
            # Wait, logic check: "seen_last at is NULL or not older than current job created_at"
            # actually duplicate logic was: seen_first < seen_last.
            # And we want to UPDATE seen_last to current.
            # So effectively we just insure chronology: seen_first < (seen_last) < current
            logger.warning(
                f"Timestamp mismatch: Original {original_id} seen_last ({seen_last}) not in valid range for Enrichment {enrichment_id} ({enrichment_created_at})"
            )
            _mark_failed(cursor, enrichment_id, "failed_validating_timestamps")
            return

    # 4. Success Action
    logger.info(
        f"Validation passed. Enriching original {original_id} with {len(update_data)} new fields from {enrichment_id}"
    )

    # Prepare update
    # Update seen_last_at ONLY if the enrichment is newer
    if seen_last is None or enrichment_created_at > seen_last:
        update_data["seen_last_at"] = enrichment_created_at

    from db_ops import update_row

    # Only run update if we have data to update (content or timestamp)
    if update_data:
        update_row(
            cursor=cursor,
            table_name="processedjobs",
            data=update_data,
            id_primary=original_id,
        )
    else:
        logger.info(f"No fields or timestamp updates needed for original {original_id}")

    # Delete enrichment source
    cursor.execute(
        "DELETE FROM public.preparedjobs WHERE id_primary = %s", (enrichment_id,)
    )
    logger.info(f"Successfully processed enrichment {enrichment_id}")


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
        elif "enrichment" in status:
            process_enriched_job(cursor, job, status)
        else:
            logger.warning(f"Unknown status '{status}' for job {job_id}")
            _mark_failed(cursor, job_id, "failed_unknown_status")

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}", exc_info=True)

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
            logger.error(f"Unexpected error in processor loop: {e}", exc_info=True)

            time.sleep(5)
        # Log empty line
        logger.info("")


if __name__ == "__main__":
    run_processor_loop(40)

# NOTE: duplicates of similar jobs throw these warnings:
# 2026-01-03 13:34:32,759 - processor - INFO - Picked and locked job: 59 (Current Status: worker_02 duplicate: 30)
# 2026-01-03 13:34:32,759 - processor - INFO - Processing DUPLICATE job 59, claims to be duplicate of: [30]
# 2026-01-03 13:34:32,760 - processor - WARNING - No originals found in processedjobs for IDs: [30]
# 2026-01-03 13:34:32,810 - processor - INFO - Picked and locked job: 63 (Current Status: worker_02 duplicate: 34)
# 2026-01-03 13:34:32,810 - processor - INFO - Processing DUPLICATE job 63, claims to be duplicate of: [34]
# 2026-01-03 13:34:32,812 - processor - WARNING - No originals found in processedjobs for IDs: [34]

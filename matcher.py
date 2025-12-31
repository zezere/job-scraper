"""
Matcher Module Logic Documentation

This module handles the core logic for matching incoming scraped jobs (`jobsli` table) against existing jobs to detect duplicates or similar listings.

High-Level Process:
1.  **Job Selection**:
    - The `pick_job_for_matching` function selects a single job from the `jobsli` table.
    - If a specific `id_primary` is provided, it attempts to load that exact job.
    - If not, it selects the oldest job (`created_at ASC`) that hasn't been processed yet (status is NULL or empty).
    - It uses `id_primary ASC` as a tie-breaker for jobs with the same timestamp.
    - The selected job is temporarily marked with status "worker_1" to prevent other workers (if concurrent) from picking it up.

2.  **Candidate Finding (SQL Agnostic)**:
    - The `sql_find_candidates` function queries the `jobsli` table for potential matches.
    - It uses a set of loose criteria to find candidates. A record is considered a candidate if:
        - Exact match on: `id` OR `job_url` OR `description` OR `job_url_direct` OR `emails`.
        - OR Exact match on BOTH: `title` AND `company`.
    - It explicitly requires `created_at` < job's timestamp (or same time with lower `id_primary`) to avoid race conditions and self-matching.

3.  **Assessment & Status Decision**:
    Based on the candidates found (or lack thereof), the `match_job` function determines the job's fate:

    -   **Unique ("unique")**:
        -   Condition: No matching candidates found in the database.
        -   Action: The job is considered new. It is copied to the `preparedjobs` table with status "new".
        -   Outcome: `jobsli` status updated to "unique".

    -   **Duplicate ("candidates_found" + strict check)**:
        -   Condition: At least one candidate is an *exact match* on all significant fields (excluding `id_primary`/timestamps).
        -   Action: The job is flagged as a duplicate. It is copied to `preparedjobs` with status "duplicate: <id_list>".
        -   Outcome: `jobsli` status updated to "duplicate: <id_list>".

    -   **Similar ("candidates_found" + no strict match)**:
        -   Condition: Candidates were found by the loose SQL criteria, but NONE of them were exact text-matches.
        -   Action: The job is flagged as similar (likely a repost or variant). It is copied to `similarjobs` with status "similar: <id_list>".
        -   Outcome: `jobsli` status updated to "similar: <id_list>".

    -   **Special Cases**:
        -   **Unmatchable**: If the job lacks all critical fields (title, company, etc.), it can't be processed. Status -> "unmatchable".
        -   **Abandoned**: If a candidate is currently locked ("worker_..."), we back off and try later. Status -> NULL.
        -   **Error**: Any crash during processing reverts the job status to NULL so it can be retried.

4.  **Tables Involved**:
    -   `jobsli`: The raw intake table. Jobs stay here but get their `status` updated.
    -   `preparedjobs`: The destination for "new" and "duplicate" jobs, ready for downstream processing.
    -   `similarjobs`: The destination for "similar" jobs, kept aside for review or separate logic.
"""

import logging
import re
import sys
import traceback
from typing import Tuple, Any
import pandas as pd
import numpy as np
from db_connection import get_connection
from utils import setup_logging, get_value
import time

logger = setup_logging("matcher")


def _format_row_info(row: pd.Series, identifier: Any, output_func) -> None:
    """Format and output information about a job row."""
    output_func("=" * 80)
    output_func(f"JOB ID: {identifier} - JOB INFORMATION")
    output_func("=" * 80)

    for field in row.index:
        value = get_value(row, field)
        if value is not None:
            display_value = str(value)
            lines = [line for line in display_value.split("\n") if line.strip()]
            if not lines:
                continue
            full_text = "\n".join(lines)
            if len(full_text) > 100:
                full_text = full_text[:100] + "..."
                lines = full_text.split("\n")
            output_func(f"  {field:20s}: {lines[0]}")
            for line in lines[1:]:
                output_func(f"  {'':20s}  {line}")

    output_func("=" * 80)
    output_func("")


def _log_dataframe_rows(df: pd.DataFrame, title: str = "CANDIDATES") -> None:
    """Log each row of a dataframe."""
    if df.empty:
        logger.info(f"No {title.lower()} to display.")
        return

    logger.info(f"{title} ({len(df)} row(s))")
    logger.info("")

    for idx, (_, row) in enumerate(df.iterrows(), 1):
        identifier = row.get("id_primary", f"Row {idx}")
        logger.info(f"CANDIDATE {idx}:")
        _format_row_info(row, identifier, logger.info)


def _copy_job_generic(
    cursor, id_primary: int, target_table: str, status_value: str
) -> None:
    """
    Internal helper to copy a job from jobsli to a target table.
    Used for duplicates and similar jobs (custom status).
    Does NOT affect prepare_new_job.
    """
    logger.debug(
        f"Copying job {id_primary} to {target_table} with status '{status_value}'"
    )

    # Fetch column names dynamically from jobsli
    cursor.execute("SELECT * FROM public.jobsli LIMIT 0")
    columns = [desc[0] for desc in cursor.description]

    # Build column lists for INSERT
    quoted_cols = []
    for col in columns:
        if col == "interval" or " " in col:
            quoted_cols.append(f'"{col}"')
        else:
            quoted_cols.append(col)

    cols_str = ", ".join(quoted_cols)

    # Build SELECT values
    select_parts = []
    for col, safe_col in zip(columns, quoted_cols):
        if col == "status":
            # Escape single quotes in status value just in case
            safe_status = status_value.replace("'", "''")
            select_parts.append(f"'{safe_status}'")
        else:
            select_parts.append(safe_col)
    select_str = ", ".join(select_parts)

    # Insert with explicit columns
    insert_query = f"""
        INSERT INTO public.{target_table} ({cols_str}) 
        SELECT {select_str} 
        FROM public.jobsli 
        WHERE id_primary = %s
    """
    cursor.execute(insert_query, (id_primary,))

    # Update status in jobsli
    update_jobsli_query = "UPDATE public.jobsli SET status = %s WHERE id_primary = %s"
    cursor.execute(update_jobsli_query, (status_value, id_primary))


def _generate_status_string(label: str, ids: list[str]) -> str:
    """
    Constructs the status string for duplicates or similar jobs.
    Format: "label: id1, id2, ..."
    """
    if not ids:
        return label
    return f"{label}: {', '.join(ids)}"


def is_duplicate_job(job_row: pd.Series, candidate_row: pd.Series) -> bool:
    """
    Checks if a candidate is an exact duplicate of the job.
    Compares all fields except: id_primary, created_at, status.
    """
    excluded_fields = {"id_primary", "created_at", "status"}

    # Get common fields to compare
    common_fields = [
        f
        for f in job_row.index
        if f in candidate_row.index and f not in excluded_fields
    ]

    for field in common_fields:
        val1 = get_value(job_row, field)
        val2 = get_value(candidate_row, field)

        # Treat None and empty string as equal
        if (val1 is None or val1 == "") and (val2 is None or val2 == ""):
            continue

        # If values differ, it's not a duplicate
        if val1 != val2:
            return False

    return True


def pick_job_for_matching(cursor, id_primary: int | None = None) -> pd.Series | None:
    """
    Selects and marks a job for matching.

    If id_primary is None, selects the oldest row with null/empty status.
    If id_primary is provided, selects that specific row.
    In both cases, marks the row with the worker ID ("worker_1").
    """
    worker_id = "worker_1"

    if id_primary is None:
        logger.debug("Selecting oldest row with null/empty status")
        update_query = """
            UPDATE public.jobsli
            SET status = %s
            WHERE id_primary = (
                SELECT id_primary
                FROM public.jobsli
                WHERE status IS NULL OR status = ''
                ORDER BY created_at ASC, id_primary ASC
                LIMIT 1
            )
            RETURNING *
        """
        cursor.execute(update_query, (worker_id,))
    else:
        logger.debug(f"Loading and marking row with id_primary={id_primary}")
        update_query = (
            "UPDATE public.jobsli SET status = %s WHERE id_primary = %s RETURNING *"
        )
        cursor.execute(update_query, (worker_id, id_primary))

    row = cursor.fetchone()

    if row is None:
        return None

    column_names = [desc[0] for desc in cursor.description]
    return pd.Series(row, index=column_names)


def sql_find_candidates(
    cursor, job_row: pd.Series
) -> Tuple[pd.Series, str, pd.DataFrame]:
    """
    Step 1: SQL find that finds candidate matches from database.

    Uses SQL WHERE clause to efficiently find potential
    matches without loading entire table into memory.

    Args:
        cursor: Database cursor for executing queries
        job_row: The job row (pd.Series) to match against database.

    Returns:
        Tuple of:
        - job_row: The job row (pd.Series) used for matching
        - assessment: String describing the initial assessment/decision.
          Possible values:
          - "unique": No matching candidates found
          - "candidates_found": Matches found, candidates dataframe returned
          - "unmatchable": Cannot match because all required fields are empty/null
          - "error": An error occurred during SQL query execution
          - "abandoned": Matching interrupted due to some candidates being worked on
        - candidates_df: DataFrame with candidate matches (empty if none found)
    """
    # ==============================
    # LOGIC:
    # 1. if at least one of the following fields is an exact match (unless NULL or empty), proceed to step 4
    #    - id
    #    - job_url
    #    - description
    #    - job_url_direct
    #    - emails
    # 2. if both of the following fields are an exact match (unless NULL or empty), proceed to step 4
    #    - title
    #    - company
    # 3. If no matches, then the job is unique.
    # 4. if field "status" starts with string "worker_" for at least one candidate, abandon matching.
    # 5. otherwise return the candidates dataframe and assessment "candidates_found"
    # ==============================

    logger.debug(
        f"Using provided job_row: id_primary={job_row.get('id_primary')}, title={job_row.get('title', 'N/A')}"
    )

    # Extract values from job_row, handling NULL/empty
    job_id_primary = get_value(job_row, "id_primary")
    job_created_at = get_value(job_row, "created_at")
    job_id = get_value(job_row, "id")
    job_url = get_value(job_row, "job_url")
    job_description = get_value(job_row, "description")
    job_url_direct = get_value(job_row, "job_url_direct")
    job_emails = get_value(job_row, "emails")
    job_title = get_value(job_row, "title")
    job_company = get_value(job_row, "company")

    # Build WHERE conditions for each step
    conditions = []
    params = []

    # Step 1: At least one of (id, job_url, description, job_url_direct, emails) matches
    step1_conditions = []
    if job_id is not None:
        step1_conditions.append("id = %s")
        params.append(job_id)
    if job_url is not None:
        step1_conditions.append("job_url = %s")
        params.append(job_url)
    if job_description is not None:
        step1_conditions.append("description = %s")
        params.append(job_description)
    if job_url_direct is not None:
        step1_conditions.append("job_url_direct = %s")
        params.append(job_url_direct)
    if job_emails is not None:
        step1_conditions.append("emails = %s")
        params.append(job_emails)

    if step1_conditions:
        conditions.append(" OR ".join(step1_conditions))

    # Step 2: Both (title, company) match
    if job_title is not None and job_company is not None:
        conditions.append("(title = %s AND company = %s)")
        params.extend([job_title, job_company])

    # If all fields needed for matching are empty/null, return empty dataframe and assessment "unmatchable"
    if not conditions:
        logger.warning(
            f"Cannot match job id_primary={job_id_primary}: all required fields are empty/null"
        )
        return job_row, "unmatchable", pd.DataFrame()

    # Build SQL query - status field will be character varying
    where_clause = " OR ".join(conditions)

    # id_primary  has not null constraint
    if job_id_primary is None:
        error_msg = "id_primary is required but was None"
        logger.error(error_msg)
        return job_row, "error", pd.DataFrame()

    # Build exclusion clause: exclude the job itself and any newer rows (or same time but higher ID)
    # Logic: Look for candidates that are strictly "older" or "same time but lower ID"
    # This prevents two identical jobs with same timestamp from blocking each other.
    exclusion_clause = " AND (created_at < %s OR (created_at = %s AND id_primary < %s))"
    params.append(job_created_at)
    params.append(job_created_at)
    params.append(job_id_primary)

    query = f"""
        SELECT *
        FROM public.jobsli
        WHERE ({where_clause}){exclusion_clause}
    """

    # Format query with actual parameter values for display
    formatted_query = query
    for param in params:
        if isinstance(param, str):
            # Escape single quotes and wrap in quotes
            escaped_param = param.replace("'", "''")
            # Truncate to 100 characters for display only
            if len(escaped_param) > 100:
                escaped_param = escaped_param[:100] + "..."
            formatted_query = formatted_query.replace("%s", f"'{escaped_param}'", 1)
        elif param is None:
            formatted_query = formatted_query.replace("%s", "NULL", 1)
        else:
            formatted_query = formatted_query.replace("%s", str(param), 1)

    try:
        logger.debug(
            f"Executing SQL query to find candidates for job id_primary={job_id_primary}"
        )
        cursor.execute(query, params)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        candidates_df = pd.DataFrame(rows, columns=column_names)

        logger.debug(f"Found {len(candidates_df)} candidate rows from SQL query")

        # Step 3: If no matches, return empty dataframe and assessment "unique"
        if candidates_df.empty:
            logger.info(
                f"No matching candidates found for job id_primary={job_id_primary}"
            )
            return job_row, "unique", pd.DataFrame()

        # Step 4: If field "status" starts with string "worker_" on at least one row,
        # return empty dataframe and assessment "abandoned"
        if "status" in candidates_df.columns:
            abandoned_mask = (
                candidates_df["status"].fillna("").astype(str).str.startswith("worker_")
            )
            if abandoned_mask.any():
                abandoned_count = abandoned_mask.sum()
                logger.warning(
                    f"Matching abandoned: {abandoned_count} candidate(s) have status starting with 'worker_'"
                )
                return job_row, "abandoned", pd.DataFrame()

        # Step 5: Return the candidates dataframe and assessment "matches_found"
        logger.info(
            f"Found {len(candidates_df)} matching candidate(s) for job id_primary={job_id_primary}"
        )
        return job_row, "candidates_found", candidates_df

    except Exception as e:
        error_msg = f"Error executing SQL query: {e}"
        logger.error(error_msg, exc_info=True)
        traceback.print_exc()
        return job_row, "error", pd.DataFrame()


def match_job(id_primary: int | None = None) -> pd.Series:
    """
    Main matching function that orchestrates the matching process with status management.

    This function:
    1. Selects job or if id not provided - selects one from jobsli table
    2. Marks it with worker ID ("worker_1") in status field
    3. Uses SQL to find candidate matches and give the assessment (unique, candidates_found, etc.)
    4. Updates job status in jobsli table to the assessment value

    Args:
        id_primary: Optional primary ID of the job to match.
                    If None, selects the oldest row with null/empty status.

    Returns:
        Job row that was processed
    """
    worker_id = "worker_1"
    job_row = None

    try:
        with get_connection() as (conn, cursor):
            try:
                # 1. Pick and mark a job
                job_row = pick_job_for_matching(cursor, id_primary)

                if job_row is None:
                    logger.info("No jobs found for matching")
                    return pd.Series(dtype=object)

                job_id_primary = get_value(job_row, "id_primary")
                if job_id_primary is None:
                    logger.error("Picked job row has no id_primary, aborting")
                    return pd.Series(dtype=object)

                logger.info(
                    f"Loaded and marked row: id_primary={job_id_primary}, title={job_row.get('title', 'N/A')}"
                )

                job_row, assessment, candidates_df = sql_find_candidates(
                    cursor, job_row
                )
                logger.debug(
                    f"SQL candidate search completed: {len(candidates_df)} candidates, assessment: {assessment}, job: {job_row.get('title', 'unknown')}"
                )

                if not candidates_df.empty:
                    _log_dataframe_rows(candidates_df, "CANDIDATES")

                # Step 3: Handle the assessment
                if assessment == "unique":
                    try:
                        _copy_job_generic(cursor, job_id_primary, "preparedjobs", "new")
                        logger.info(
                            f"Job {job_id_primary} processed as unique (prepared/new)"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to copy job {job_id_primary} to preparedjobs: {e}"
                        )

                elif assessment == "candidates_found":
                    # Check if any candidate is an exact duplicate
                    duplicate_ids = []
                    all_candidate_ids = []

                    for _, candidate_row in candidates_df.iterrows():
                        cand_id = get_value(candidate_row, "id_primary")
                        if cand_id is not None:
                            all_candidate_ids.append(str(cand_id))

                        if is_duplicate_job(job_row, candidate_row):
                            duplicate_ids.append(str(cand_id))

                    if duplicate_ids:
                        # CASE 1: At least one duplicate found
                        status_msg = _generate_status_string("duplicate", duplicate_ids)
                        logger.info(
                            f"Job {job_id_primary} found to be duplicate of {duplicate_ids}"
                        )

                        try:
                            # Copy to preparedjobs with "duplicate: ..." status
                            # Use our internal generic helper, preserving prepare_new_job logic elsewhere
                            _copy_job_generic(
                                cursor, job_id_primary, "preparedjobs", status_msg
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to process duplicate job {job_id_primary}: {e}"
                            )

                    else:
                        # CASE 2: No duplicates, but candidates exist -> SIMILAR
                        status_msg = _generate_status_string(
                            "similar", all_candidate_ids
                        )
                        logger.info(
                            f"Job {job_id_primary} found to be similar to {all_candidate_ids}"
                        )

                        try:
                            # Copy to similarjobs with "similar: ..." status
                            _copy_job_generic(
                                cursor, job_id_primary, "similarjobs", status_msg
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to process similar job {job_id_primary}: {e}"
                            )

                elif assessment == "abandoned":
                    # Revert status to NULL so it can be picked up later
                    # TODO: Implement better handling for abandoned jobs:
                    # - Add wait/retry delay
                    # - Consider marking with specific status temporarily
                    # - Pick another job immediately
                    logger.info(
                        f"Assessment '{assessment}' for id_primary={job_id_primary} - reverting status to NULL"
                    )
                    update_status_query = (
                        "UPDATE public.jobsli SET status = NULL WHERE id_primary = %s"
                    )
                    cursor.execute(update_status_query, (job_id_primary,))
                    # We are essentially "quitting" this job for now.
                    # The transaction will commit below, saving the NULL status.

                elif assessment == "unmatchable":
                    # Set status to 'unmatchable' (marker that it lacks info)
                    logger.info(
                        f"Assessment '{assessment}' for id_primary={job_id_primary} - setting status to 'unmatchable'"
                    )
                    update_status_query = (
                        "UPDATE public.jobsli SET status = %s WHERE id_primary = %s"
                    )
                    cursor.execute(update_status_query, (assessment, job_id_primary))

                elif assessment == "error":
                    # Revert status to NULL
                    logger.info(
                        f"Assessment '{assessment}' for id_primary={job_id_primary} - reverting status to NULL"
                    )
                    update_status_query = (
                        "UPDATE public.jobsli SET status = NULL WHERE id_primary = %s"
                    )
                    cursor.execute(update_status_query, (job_id_primary,))

                else:
                    logger.warning(
                        f"Unknown assessment '{assessment}' for job {job_id_primary}"
                    )
                conn.commit()
                logger.info("Transaction committed successfully")

                return job_row

            except Exception as e:
                conn.rollback()
                logger.info("Transaction rolled back due to error")
                raise

    except Exception as e:
        logger.error(f"Error during job matching: {e}", exc_info=True)
        traceback.print_exc()
        # NOTE: On error, status remains as worker_id (could be handled differently)
        return job_row if job_row is not None else pd.Series(dtype=object)


def run_matcher_loop(limit: int = 50) -> None:
    """
    Runs the matching process for a batch of jobs.

    Args:
        limit: Maximum number of jobs to process in this run.
    """
    logger.info("=" * 60)
    logger.info(f"Starting matcher loop (limit={limit})")
    logger.info("=" * 60)

    count = 0
    for i in range(limit):
        try:
            result_row = match_job()
            if result_row.empty:
                logger.info("No more jobs to match. Exiting loop.")
                break

            count += 1
            # log empty line for readability between jobs
            logger.info("")

            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error in matcher loop integration: {e}")
            # If catastrophic error, break. If simple job error, match_job catches it.
            # match_job catches its own errors, re-raised ones are serious.
            break

    logger.info(f"Matcher loop completed. Processed {count} jobs.")


if __name__ == "__main__":

    def test_match_job(id_primary: int | None = None) -> None:
        """
        Test the match_job function with a specific row from the database.

        Args:
            id_primary: Optional primary ID of the job to test.
                       If None, match_job will select the oldest row with null/empty status.
        """
        if id_primary is not None:
            logger.info(f"Testing match_job with id_primary={id_primary}")
        else:
            logger.info(
                "Testing match_job (will auto-select oldest row with null/empty status)"
            )

        try:
            result_row = match_job(id_primary)

            if result_row.empty:
                logger.warning("No row was processed (empty result)")
                return

            # log empty line
            logger.info("")

            # logger.info("Matching result:")
            # _format_row_info(
            #     result_row, result_row.get("id_primary", "N/A"), logger.info
            # )

        except Exception as e:
            logger.error(f"Error during matching: {e}", exc_info=True)
            traceback.print_exc()
            return

    # Example usage (hardcode id_primary as needed):
    for i in range(5):
        test_match_job()
        time.sleep(1)

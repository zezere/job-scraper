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


# Also mark the row in jobsli with the worker id - that could be done before this function,
# and cleared after the last function when everything is copied where it needs to be.
def sql_filter_candidates(
    cursor, job_row: pd.Series | None = None
) -> Tuple[pd.Series, str, pd.DataFrame]:
    """
    Step 1: SQL filter that finds candidate matches from database.

    If job_row is not provided, selects the oldest row with null/empty status
    from jobsli table. Then uses SQL WHERE clause to efficiently find potential
    matches without loading entire table into memory.

    Args:
        cursor: Database cursor for executing queries
        job_row: Optional job row (pd.Series) to match against database.
                 If None, selects the oldest row with null/empty status.

    Returns:
        Tuple of:
        - job_row: The job row (pd.Series) used for matching
        - assessment: String describing the initial assessment/decision.
          Possible values:
          - "no_rows_available": No rows with null/empty status found (when job_row not provided)
          - "unique": No matching candidates found (after checking)
          - "matches_found": Matches found, candidates dataframe returned
          - "unmatchable": Cannot match because all required fields are empty/null
          - "error": An error occurred during SQL query execution
          - "abandoned": Matching interrupted due to some candidates being worked on (status starts with "worker_")
        - candidates_df: DataFrame with candidate matches (empty if none found)
    """
    # ==============================
    # LOGIC:
    # 0. If job_row not provided, select the oldest row with null/empty status from jobsli
    # 1. if at least one of the following fields is an exact match (unless NULL or empty), proceed to step 4
    #    - id
    #    - job_url
    #    - description
    #    - job_url_direct
    #    - emails
    # 2. if both of the following fields are an exact match (unless NULL or empty), proceed to step 4
    #    - title
    #    - company
    # 3. If no matches, return empty dataframe and assessment "unique".
    # 4. if field "status" starts with string "worker_" or at least one row, then return empty dataframe and assessment "abandoned".
    # 5. otherwise return the candidates dataframe and assessment "matches_found"
    # ==============================

    # Step 0: Select the oldest row with null/empty status if job_row not provided
    if job_row is None:
        logger.debug("No job_row provided, selecting oldest row with null/empty status")
        try:
            select_query = """
                SELECT *
                FROM public.jobsli
                WHERE status IS NULL OR status = ''
                ORDER BY created_at ASC
                LIMIT 1
            """
            cursor.execute(select_query)
            row = cursor.fetchone()

            if row is None:
                logger.info("No rows available with null/empty status")
                return pd.Series(dtype=object), "no_rows_available", pd.DataFrame()

            column_names = [desc[0] for desc in cursor.description]
            job_row = pd.Series(row, index=column_names)
            logger.info(
                f"Selected row for matching: id_primary={job_row.get('id_primary')}, title={job_row.get('title', 'N/A')}"
            )

        except Exception as e:
            error_msg = f"Error selecting job row from database: {e}"
            logger.error(error_msg, exc_info=True)
            traceback.print_exc()
            return pd.Series(dtype=object), "error", pd.DataFrame()
    else:
        logger.debug(
            f"Using provided job_row: id_primary={job_row.get('id_primary')}, title={job_row.get('title', 'N/A')}"
        )

    # Extract values from job_row, handling NULL/empty
    job_id_primary = get_value(job_row, "id_primary")
    job_created_at = get_value(job_row, "created_at")
    job_id = get_value(job_row, "id")
    job_url = get_value(job_row, "job_url")
    description = get_value(job_row, "description")
    job_url_direct = get_value(job_row, "job_url_direct")
    emails = get_value(job_row, "emails")
    title = get_value(job_row, "title")
    company = get_value(job_row, "company")

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
    if description is not None:
        step1_conditions.append("description = %s")
        params.append(description)
    if job_url_direct is not None:
        step1_conditions.append("job_url_direct = %s")
        params.append(job_url_direct)
    if emails is not None:
        step1_conditions.append("emails = %s")
        params.append(emails)

    if step1_conditions:
        conditions.append(" OR ".join(step1_conditions))

    # Step 2: Both (title, company) match
    if title is not None and company is not None:
        conditions.append("(title = %s AND company = %s)")
        params.extend([title, company])

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

    # Build exclusion clause: exclude the job itself and any newer rows
    exclusion_clause = " AND id_primary != %s"
    params.append(job_id_primary)

    if job_created_at is not None:
        exclusion_clause += " AND created_at <= %s"
        params.append(job_created_at)

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
        return job_row, "matches_found", candidates_df

    except Exception as e:
        error_msg = f"Error executing SQL query: {e}"
        logger.error(error_msg, exc_info=True)
        traceback.print_exc()
        return job_row, "error", pd.DataFrame()


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


def match_job(id_primary: int | None = None) -> pd.Series:
    """
    Main matching function that orchestrates the matching process with status management.

    This function:
    1. Selects or loads a job row
    2. Marks it with worker ID ("worker_1") to prevent other processes from picking it
    3. Performs SQL filter to get candidate matches and initial assessment
    4. Updates status to the assessment value

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
                # Step 1: Select or load the row and mark it with worker ID
                if id_primary is None:
                    # Atomically select and mark the oldest row with null/empty status
                    logger.debug("Selecting oldest row with null/empty status")
                    update_query = """
                        UPDATE public.jobsli
                        SET status = %s
                        WHERE id_primary = (
                            SELECT id_primary
                            FROM public.jobsli
                            WHERE status IS NULL OR status = ''
                            ORDER BY created_at ASC
                            LIMIT 1
                        )
                        RETURNING *
                    """
                    cursor.execute(update_query, (worker_id,))
                    row = cursor.fetchone()

                    if row is None:
                        logger.info("No rows available with null/empty status")
                        return pd.Series(dtype=object)

                    column_names = [desc[0] for desc in cursor.description]
                    job_row = pd.Series(row, index=column_names)
                    logger.info(
                        f"Selected and marked row: id_primary={job_row.get('id_primary')}, title={job_row.get('title', 'N/A')}"
                    )
                else:
                    # Load the specific row and mark it
                    logger.debug(f"Loading row with id_primary={id_primary}")
                    select_query = "SELECT * FROM public.jobsli WHERE id_primary = %s"
                    cursor.execute(select_query, (id_primary,))
                    row = cursor.fetchone()

                    if row is None:
                        logger.error(f"No job found with id_primary={id_primary}")
                        return pd.Series(dtype=object)

                    column_names = [desc[0] for desc in cursor.description]
                    job_row = pd.Series(row, index=column_names)

                    # Mark with worker ID
                    update_query = (
                        "UPDATE public.jobsli SET status = %s WHERE id_primary = %s"
                    )
                    cursor.execute(update_query, (worker_id, id_primary))
                    logger.info(
                        f"Loaded and marked row: id_primary={id_primary}, title={job_row.get('title', 'N/A')}"
                    )

                # Step 2: SQL filter
                job_row, assessment, candidates_df = sql_filter_candidates(
                    cursor, job_row
                )
                logger.debug(
                    f"SQL filter completed: {len(candidates_df)} candidates, assessment: {assessment}, job: {job_row.get('title', 'unknown')}"
                )

                # Log candidate details if matches found
                if not candidates_df.empty:
                    _log_dataframe_rows(candidates_df, "CANDIDATES")

                # Step 3: Update status to assessment value
                job_id_primary = get_value(job_row, "id_primary")
                if job_id_primary is not None:
                    update_status_query = (
                        "UPDATE public.jobsli SET status = %s WHERE id_primary = %s"
                    )
                    cursor.execute(update_status_query, (assessment, job_id_primary))
                    logger.info(
                        f"Updated status to '{assessment}' for id_primary={job_id_primary}"
                    )

                # Commit the transaction
                conn.commit()
                logger.info("Transaction committed successfully")

                return job_row

            except Exception as e:
                # Rollback the transaction on error
                conn.rollback()
                logger.info("Transaction rolled back due to error")
                raise

    except Exception as e:
        logger.error(f"Error during job matching: {e}", exc_info=True)
        traceback.print_exc()
        # On error, status remains as worker_id (could be handled differently)
        return job_row if job_row is not None else pd.Series(dtype=object)


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

            logger.info("Matching result:")
            _format_row_info(
                result_row, result_row.get("id_primary", "N/A"), logger.info
            )

        except Exception as e:
            logger.error(f"Error during matching: {e}", exc_info=True)
            traceback.print_exc()
            return
        # print("Running full matching process...")
        # print()
        # try:
        #     result_row = match_job(job_row)
        #     print_final_result(result_row)
        # except Exception as e:
        #     print(f"ERROR during matching: {e}")
        #     import traceback
        #
        #     traceback.print_exc()

    # Example usage (hardcode id_primary as needed):
    for i in range(30):
        test_match_job()
        time.sleep(1)

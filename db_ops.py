"""
LEGACY MODULE NOTICE:
This module is considered legacy.
Future orchestration will be handled by Apache Airflow.
Refactoring needed to remove dependency on internal locking mechanisms.
"""

import pandas as pd
from typing import Optional, Any, Tuple, List, Dict
from db_connection import get_connection
from utils import setup_logging, get_value

logger = setup_logging("db_ops")


def insert_row(cursor, table_name: str, data: Dict[str, Any] | pd.Series) -> None:
    """
    Generic function to insert a row into a table.

    1. dynamically checks which columns exist in the table.
    2. matches them with keys in 'data'.
    3. executes the INSERT statement.
    """
    # 1. Fetch table columns
    cursor.execute(f"SELECT * FROM public.{table_name} LIMIT 0")
    table_columns = [desc[0] for desc in cursor.description]

    # 2. Match data keys to table columns
    insert_cols = []
    insert_vals = []
    placeholders = []

    for col in table_columns:
        # Handle columns that need quoting (e.g., "interval")
        col_name_safe = f'"{col}"' if col == "interval" or " " in col else col

        # Check if we have data for this column
        # Supports both Dict and pd.Series access
        val = None
        if isinstance(data, pd.Series):
            if col in data.index:
                val = get_value(data, col)
        elif isinstance(data, dict):
            val = data.get(col)

        if val is not None:
            insert_cols.append(col_name_safe)
            insert_vals.append(val)
            placeholders.append("%s")

    if not insert_cols:
        logger.warning(f"No matching columns found to insert into {table_name}")
        return

    # 3. Execute Insert
    cols_str = ", ".join(insert_cols)
    placeholders_str = ", ".join(placeholders)
    query = f"INSERT INTO public.{table_name} ({cols_str}) VALUES ({placeholders_str})"

    cursor.execute(query, tuple(insert_vals))


def update_row(
    cursor, table_name: str, data: Dict[str, Any] | pd.Series, id_primary: int
) -> None:
    """
    Generic function to update a row in a table.
    """
    # 1. Fetch table columns
    cursor.execute(f"SELECT * FROM public.{table_name} LIMIT 0")
    table_columns = [desc[0] for desc in cursor.description]

    # 2. Match data keys to table columns
    update_parts = []
    update_vals = []

    for col in table_columns:
        if col == "id_primary":
            continue  # Don't update the primary key

        # Handle columns that need quoting
        col_name_safe = f'"{col}"' if col == "interval" or " " in col else col

        val = None
        if isinstance(data, pd.Series):
            if col in data.index:
                val = get_value(data, col)
        elif isinstance(data, dict):
            val = data.get(col)

        if val is not None:
            update_parts.append(f"{col_name_safe} = %s")
            update_vals.append(val)

    if not update_parts:
        logger.warning(f"No matching columns found to update in {table_name}")
        return

    # 3. Execute Update
    set_clause = ", ".join(update_parts)
    query = f"UPDATE public.{table_name} SET {set_clause} WHERE id_primary = %s"
    update_vals.append(id_primary)

    cursor.execute(query, tuple(update_vals))


def fetch_and_lock_job(
    cursor,
    table_name: str,
    worker_id: str,
    conditions: str,
    order_by: str,
    limit: int = 1,
) -> pd.Series | None:
    """
    Atomically selects a job matching 'conditions', locks it with 'worker_id',
    and returns it.

    Args:
        conditions: SQL WHERE clause (e.g. "status = 'new'")
        order_by: SQL ORDER BY clause (e.g. "created_at ASC")
    """
    update_query = f"""
        UPDATE public.{table_name}
        SET status = '{worker_id}' || CASE 
            WHEN status IS NOT NULL AND status != '' THEN ' ' || status 
            ELSE '' 
        END
        WHERE id_primary = (
            SELECT id_primary
            FROM public.{table_name}
            WHERE {conditions}
            ORDER BY {order_by}
            LIMIT {limit}
        )
        RETURNING *
    """

    cursor.execute(update_query)
    row = cursor.fetchone()

    if row is None:
        return None

    column_names = [desc[0] for desc in cursor.description]
    return pd.Series(row, index=column_names)


def transfer_job(
    cursor,
    job_id: int,
    source_table: str,
    target_table: str,
    target_status: str | None = None,
    target_override_values: Dict[str, Any] | None = None,
    delete_source: bool = True,
    source_status: str | None = None,
    source_status_on_failure: str | None = None,
) -> None:
    """
    Transfers a job from source_table to target_table.

    1. Checks if job already exists in target.
       - If YES: updates source to 'source_status_on_failure' (if set) and returns.
    2. Reads job from source.
    3. Inserts into target.
    4. Handles Source (delete or update status).

    Args:
        target_status: Status to set in the NEW table.
        target_override_values: Optional dict to overwrite specific fields in NEW table.
        delete_source: If True, deletes the row from source_table (Move).
        source_status: If delete_source is False, updates source_table status to this (Copy).
        source_status_on_failure: Status to set in SOURCE table if job already exists in TARGET.

    """
    # 0. Check if exists in target
    cursor.execute(
        f"SELECT 1 FROM public.{target_table} WHERE id_primary = %s", (job_id,)
    )
    target_exists = cursor.fetchone() is not None

    if target_exists:
        logger.warning(
            f"Job {job_id} already exists in {target_table}. Skipping transfer."
        )
        if source_status_on_failure:
            logger.info(
                f"Updating source status to '{source_status_on_failure}' for existing job {job_id}"
            )
            cursor.execute(
                f"UPDATE public.{source_table} SET status = %s WHERE id_primary = %s",
                (source_status_on_failure, job_id),
            )
        return

    # 1. Fetch Source Job
    cursor.execute(
        f"SELECT * FROM public.{source_table} WHERE id_primary = %s", (job_id,)
    )
    rows = cursor.fetchall()

    if not rows:
        raise ValueError(f"Job {job_id} not found in {source_table}")

    if len(rows) > 1:
        raise ValueError(
            f"Multiple rows found for job {job_id} in {source_table} - Integrity Error"
        )

    row = rows[0]

    col_names = [desc[0] for desc in cursor.description]
    job_data = dict(zip(col_names, row))

    # Apply overrides for TARGET
    if target_status:
        job_data["status"] = target_status
    if target_override_values:
        job_data.update(target_override_values)

    # 2. Insert into Target
    insert_row(cursor, target_table, job_data)

    # 3. Handle Source (Delete or Update)
    if delete_source:
        cursor.execute(
            f"DELETE FROM public.{source_table} WHERE id_primary = %s", (job_id,)
        )
        logger.debug(
            f"Moved job {job_id} from {source_table} to {target_table} (Deleted source)"
        )
    elif source_status:
        cursor.execute(
            f"UPDATE public.{source_table} SET status = %s WHERE id_primary = %s",
            (source_status, job_id),
        )
        logger.debug(
            f"Copied job {job_id} from {source_table} to {target_table} (Updated source status to '{source_status}')"
        )


if __name__ == "__main__":
    # Example 1: Dictionary
    example_dict = {
        "id_primary": 100000,
        "site": "linkedin",
        "title": "Test Job Dict",
        "job_url": "https://example.com/dict_test",
        "company": "Test Company A",
        "location": "Remote",
    }

    # Example 2: Pandas Series
    example_series = pd.Series(
        {
            "id_primary": 100001,
            "site": "linkedin",
            "title": "Test Job Series",
            "job_url": "https://example.com/series_test",
            "company": "Test Company B",
            "location": "Remote",
        }
    )

    # Example 3: Overrides

    def test_db_ops():
        try:
            with get_connection() as (conn, cursor):
                logger.info("Testing...")

                # 1. Fetch the job first to get its 'created_at'
                job_id = 45
                cursor.execute(
                    "SELECT * FROM public.preparedjobs WHERE id_primary = %s", (job_id,)
                )
                row = cursor.fetchone()

                if not row:
                    logger.error(
                        f"Job {job_id} not found in preparedjobs (cannot test transfer)"
                    )
                    return

                col_names = [desc[0] for desc in cursor.description]
                job_data = dict(zip(col_names, row))
                created_at = job_data.get("created_at")

                # 2. Prepare overrides mapping 'created_at' -> 'seen_first_at'
                overrides = {"seen_first_at": created_at, "seen_last_at": None}

                # 3. Transfer
                transfer_job(
                    cursor,
                    job_id=job_id,
                    source_table="preparedjobs",
                    target_table="processedjobs",
                    target_status="new",
                    target_override_values=overrides,
                    delete_source=True,
                    source_status="new",
                )

                conn.commit()
                logger.info("Success!")
        except Exception as e:
            logger.error(f"Test failed: {e}", exc_info=True)

    test_db_ops()

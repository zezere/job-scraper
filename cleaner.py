"""
Cleaner Module Logic Documentation

This module handles the core logic for matching incoming scraped jobs (`jobsli` table) against existing jobs to detect duplicates or similar listings.

Schemas:

CREATE TABLE IF NOT EXISTS public.jobsli
(
    id_primary bigint NOT NULL DEFAULT nextval('jobsli_id_primary_seq'::regclass),
    id character varying,
    site character varying,
    job_url character varying,
    job_url_direct character varying,
    title character varying,
    company character varying,
    location character varying,
    date_posted character varying,
    job_type character varying,
    salary_source character varying,
    "interval" character varying,
    min_amount double precision,
    max_amount double precision,
    currency character varying,
    is_remote boolean,
    job_level character varying,
    job_function character varying,
    emails character varying,
    description character varying,
    company_industry character varying,
    company_url character varying,
    company_logo character varying,
    company_url_direct character varying,
    scraped_on character varying,
    created_at timestamp with time zone DEFAULT now(),
    status character varying,
    CONSTRAINT jobsli_pkey PRIMARY KEY (id_primary)
)

CREATE TABLE IF NOT EXISTS public.jobs
(
    id bigint NOT NULL DEFAULT nextval('jobs_id_seq'::regclass),
    seen_dates date[],
    first_seen_on date,
    last_seen_on date,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone,
    status character varying,
    description_hash uuid,
    title_company_hash uuid,
    jobsli_ids character varying[],
    job_urls character varying[],
    job_urls_direct character varying[],
    title character varying,
    company character varying,
    location character varying,
    date_posted character varying,
    job_type character varying,
    salary_source character varying,
    "interval" character varying,
    min_amount double precision,
    max_amount double precision,
    currency character varying,
    is_remote boolean,
    job_level character varying,
    job_function character varying,
    emails character varying,
    description text,
    company_industry character varying,
    company_url character varying,
    company_logo character varying,
    company_url_direct character varying,
    extra_data jsonb,
    CONSTRAINT jobs_pkey PRIMARY KEY (id)
)

CREATE TABLE IF NOT EXISTS public.review
(
    id bigint NOT NULL DEFAULT nextval('review_id_seq'::regclass),
    similar_job_ids character varying[],
    similarity_reason character varying,
    diff_summary jsonb,
    created_at timestamp with time zone DEFAULT now(),
    resolved_at timestamp with time zone,
    status character varying,
    jobsli_id character varying,
    job_url character varying,
    job_url_direct character varying,
    title character varying,
    company character varying,
    location character varying,
    date_posted character varying,
    job_type character varying,
    salary_source character varying,
    "interval" character varying,
    min_amount double precision,
    max_amount double precision,
    currency character varying,
    is_remote boolean,
    job_level character varying,
    job_function character varying,
    emails character varying,
    description text,
    company_industry character varying,
    company_url character varying,
    company_logo character varying,
    company_url_direct character varying,
    scraped_on character varying,
    extra_data jsonb,
    CONSTRAINT review_pkey PRIMARY KEY (id)
)

CREATE TABLE IF NOT EXISTS public.job_history_log
(
    id bigint NOT NULL DEFAULT nextval('job_history_log_id_seq'::regclass),
    job_id bigint,
    jobsli_id character varying,
    change_source character varying,
    change_type character varying,
    changes jsonb,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT job_history_log_pkey PRIMARY KEY (id)
)

High-Level Process:
1. Row Selection:
    - The `pick_row` function selects a single job from the `jobsli` table.
    - If a specific `id_primary` is provided, it attempts to load that exact job.
    - If not, it selects the oldest job (`scraped_on ASC`) that hasn't been processed yet (status is NULL or empty).
    - It uses `id_primary ASC` as a tie-breaker for jobs with the same timestamp.
    - The selected job is temporarily marked with status "worker_1" to prevent other workers (if concurrent) from picking it up.

2. Basic filter:
    - Check if the row has the minimum amount of data - title, company, description, location, "scraped_on" date.
    - If not, mark the row status as "unmatchable" and exit.

3. Match to existing job:
    - First, check if "id" in table "jobsli" is not empty and matches at least one of "jobsli_ids" in table "jobs".
        - if it finds multiple matches in "jobs", mark row status as "multiple matches: <id_list>" and EXIT
        - if it finds one match in "jobs", proceed to processing the differences
        - if "id" is empty or no matches, continue to next check:
    - Check if "job_url" in jobsli is not empty and matches at least one of "job_urls" in table "jobs.
        - if multiple matches in "jobs", mark row status as "multiple matches: <id_list>" and EXIT
        - if one match, proceed to processing the differences
        - if "job_url" is empty or no matches, continue to next check:
    - Check if hashed "description" in jobsli matches same hash in "jobs".
        - if multiple matches, then also check "title+company" hash in "jobs".
            - if still multiple matches, check "location" in "jobs".
                - if still multiple matches, mark row status as "multiple matches: <id_list>" and EXIT
                - if one match, proceed to processing the differences
                - if no matches, continue to next check:
    - Check if hashed "title+company" hash matches same hash in "jobs".
        - if multiple matches, then check "location" in "jobs".
            - if at least one match, then these could be similar jobs. Add to table "review" (step 6).
        - if no matches, then it's a new job, so continue to next step.

4. Handle a new job:
    - Create new job in table "jobs"
    - Mark the field "status" of the selected row as "new"
    - Exit

5. Process the differences (if any) between our row and the matched job:
    - Compare fields "scraped_on" - the picked row can be either newer, older, or same day. Remember that.
    - If scraped_on date does not exist in table "jobs" field seen_dates, then add it to seen_dates.
    - If scraped_on date is earlier than "first_seen_on", or "last_seen_on", then update those accordingly.
    - Compare remaining fields one-by-one.
      These are the fields to compare:
      - LinkedIn ID field (in table "jobsli" it's "id", in table "jobs" it's in the list "jobsli_ids[]")
      - job_url (in "jobsli" it's "job_url", in "jobs" it's the list "job_urls[])
      - job_url_direct (in "jobs" it's the list "job_urls_direct[]")
      - title
      - company
      - all other fields that are scraped: location, date_posted, job_type, salary_source, interval, min_amount, max_amount, currency, is_remote, job_level, job_function, emails, description, company_industry, company_url, company_logo, company_url_direct
      For non-list fields:
        a) if fields are the same, then fine, nothing to do
        b) if job in "jobs" has that field empty, and our row has data, update job in "jobs" and remember which field was updated and that the change type was "enrichment"
        c) if both fields have values but they differ, then check if our row is newer, older, or same day.
            i) if our row is newer, update job in "jobs" and remember which field was updated and that the change type was "update"
            ii) if our row is older, do nothing
            iii) if our row is same day, overwrite the value in "jobs", remember which field, and that change type was "overwrite"
      For fields that in "jobs" table are lists:
        a) if our field value is in the list, do nothing
        b) if our field value is not in the list, add it to the list and remember which field was added and that the change type was "enrichment"
    - Mark row status as "updated: <id>"
    - In cases b) or c), add record to table "job_history_log"
    - Exit

6. Add to table "review".
    - Get ids of all matches from table "jobs"
    - Add row to "review" and collect all matching ids in field similar_job_ids.
    - In "similarity_reason" add "Same title company and location"
    - Mark row status as "review"
    - Exit

"""

import json
import traceback
import uuid
from datetime import date, datetime
from typing import Any, Literal, Optional, Tuple
import pandas as pd
import time
from db_connection import get_connection
from utils import setup_logging, get_value

from db_ops import fetch_and_lock_job, transfer_job

logger = setup_logging("cleaner")

# ============================================================================
# Tier 1 - data helper functions
# ============================================================================

# Namespace for hashing — a fixed UUID so hashes are reproducible across runs.
_HASH_NAMESPACE = uuid.UUID("e9a7f183-2c4d-4b8a-9f5e-1a3d2e6c8b0f")


def is_empty(value: Any) -> bool:
    """
    Returns True if the value is considered empty.

    Treats the following as empty:
    - None
    - Empty string ""
    - Whitespace-only string "  "
    - Empty list or tuple []
    - NaN / pandas NA / NaT (via pandas.isna, scalars only)

    This complements get_value() and is useful when you already have
    a plain Python value (not a Series field) and want to check emptiness.
    """
    if value is None:
        return True
    # Check list/array types first — pd.isna() on a list returns an array
    # of booleans, making `if pd.isna(value)` ambiguous and causing a warning.
    if isinstance(value, (list, tuple)):
        return len(value) == 0
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def compute_description_hash(description: str) -> Optional[uuid.UUID]:
    """
    Produces a deterministic UUID from a job description string.

    Uses uuid5 with a fixed namespace so the same text always yields
    the same UUID, and different texts always yield different UUIDs.
    Returns None if the description is empty.

    This corresponds to the `description_hash` column in the `jobs` table.
    """
    if is_empty(description):
        return None
    return uuid.uuid5(_HASH_NAMESPACE, description.strip())


def compute_title_company_hash(title: str, company: str) -> Optional[uuid.UUID]:
    """
    Produces a deterministic UUID from title + company as a composite key.

    Concatenates with a separator that is unlikely to appear in real data
    so that ("Foo", "Bar") and ("FooBar", "") do not collide.
    Returns None if either field is empty.

    This corresponds to the `title_company_hash` column in the `jobs` table.
    """
    if is_empty(title) or is_empty(company):
        return None
    composite = f"{title.strip()}||{company.strip()}"
    return uuid.uuid5(_HASH_NAMESPACE, composite)


def parse_scraped_date(scraped_on: Any) -> Optional[date]:
    """
    Converts a `scraped_on` value (varchar from jobsli) into a Python date.

    Tries the most common formats in order: YYYY-MM-DD, then DD/MM/YYYY,
    then MM/DD/YYYY. Returns None if the value is empty or unparseable.

    All downstream date comparisons should go through this function so
    that format inconsistencies across scrapers are handled in one place.
    """
    if is_empty(scraped_on):
        return None
    text = str(scraped_on).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


# Returns: "newer" | "older" | "same"
def classify_date_relation(
    scraped_on: date,
    last_seen_on: date,
) -> Literal["newer", "older", "same"]:
    """
    Classifies how `scraped_on` relates to the existing job's last known date.

    - "newer" : scraped_on is AFTER last_seen_on
    - "older" : scraped_on is BEFORE last_seen_on
    - "same"  : scraped_on equals last_seen_on

    This result drives the field-update rules in step 5 of the cleaner logic:
    newer -> update, older -> skip, same -> overwrite.
    """
    if scraped_on > last_seen_on:
        return "newer"
    if scraped_on < last_seen_on:
        return "older"
    return "same"


# ============================================================================
# Tier 2 — field-level diff helpers
# ============================================================================

# Fields that exist in jobsli or jobs but should never be compared or written to as scalar data.
_EXCLUDED_FIELDS = frozenset(
    {
        "id_primary",
        "site",
        "scraped_on",
        "created_at",
        "status",
        "id",  # bigint PK in jobs, scraper string in jobsli (handled via _LIST_FIELD_MAP)
        "seen_dates",
        "first_seen_on",
        "last_seen_on",
        "updated_at",
        "description_hash",
        "title_company_hash",
        "extra_data",
    }
)

# mapping of fields between tables "jobsli" and "jobs" where "jobs" has a list of values
_LIST_FIELD_MAP = {
    "id": "jobsli_ids",  # LinkedIn / scraper IDs
    "job_url": "job_urls",
    "job_url_direct": "job_urls_direct",
}


# Parameter date_relation can be: "newer" | "older" | "same"
# Returns: {"action": "none" | "enrich" | "update" | "overwrite", "new_value": Any}
def compare_fields(
    incoming_val: Any,
    existing_val: Any,
    date_relation: Literal["newer", "older", "same"],
) -> dict:
    """
    Decides what to do with a single scalar field when merging a jobsli row
    into jobs.

    Returns a dict with:
        "action"    : "none" | "enrich" | "update" | "overwrite"
        "new_value" : the value to write, or None if action is "none"

    Rules (from step 5 of the cleaner logic):
        - Both empty                          -> "none"
        - existing empty, incoming has data   -> "enrich"  (always write, regardless of date)
        - incoming empty, existing has data   -> "none"    (never overwrite with nothing)
        - Both have data, same value          -> "none"
        - Both have data, different, newer    -> "update"
        - Both have data, different, older    -> "none"    (don't regress)
        - Both have data, different, same day -> "overwrite"
    """
    incoming_empty = is_empty(incoming_val)
    existing_empty = is_empty(existing_val)

    if incoming_empty and existing_empty:
        return {"action": "none", "new_value": None}

    if existing_empty and not incoming_empty:
        return {"action": "enrich", "new_value": incoming_val}

    if incoming_empty and not existing_empty:
        return {"action": "none", "new_value": None}

    # Both have data
    if incoming_val == existing_val:
        return {"action": "none", "new_value": None}

    if date_relation == "newer":
        return {"action": "update", "new_value": incoming_val}
    if date_relation == "same":
        return {"action": "overwrite", "new_value": incoming_val}
    # date_relation == "older"
    return {"action": "none", "new_value": None}


# Returns: {"action": "none" | "enrich", "new_value": Any}
def compare_field_to_list(
    incoming_val: Any,
    existing_list: Any,
) -> dict:
    """
    Decides what to do when the incoming scalar value maps to a list field
    in jobs (e.g. jobsli.job_url -> jobs.job_urls[]).

    Returns a dict with:
        "action"    : "none" | "enrich"
        "new_value" : the incoming value to append, or None if action is "none"

    Rules:
        - incoming is empty              -> "none"  (nothing to add)
        - existing list is empty / None  -> "enrich" (start the list)
        - incoming value already in list -> "none"
        - incoming value not in list     -> "enrich" (append it)

    This comparison is always date-agnostic: if the value is new to the list
    we always add it, regardless of whether the row is older or newer.
    """
    if is_empty(incoming_val):
        return {"action": "none", "new_value": None}

    # Normalise the existing list: None / empty -> empty Python list.
    # Postgres arrays can arrive as a Python list or as a string "{v1,v2}".
    if is_empty(existing_list):
        existing = []
    elif isinstance(existing_list, list):
        existing = [str(x) for x in existing_list]
    else:
        raw = str(existing_list).strip("{}")
        existing = [v.strip() for v in raw.split(",")] if raw else []

    if str(incoming_val) in existing:
        return {"action": "none", "new_value": None}

    return {"action": "enrich", "new_value": incoming_val}


# Pass only fields that must be compared. No dates, statuses, etc.
# Parameter date_relation can be: "newer" | "older" | "same"
def build_diff_summary(
    jobsli_row: pd.Series,
    jobs_row: pd.Series,
    date_relation: Literal["newer", "older", "same"],
) -> dict:
    """
    Compares all fields between an incoming jobsli row and the
    matched jobs row and returns a structured summary of what should change.

    Handles two kinds of fields:
    - Scalar fields: direct value comparison via compare_field().
    - List fields: membership check via compare_field_to_list(). The jobsli
      column name may differ from the jobs column name (see _LIST_FIELD_MAP).

    Returns:
        {
            "changes": {
                "<jobs_field>": {
                    "action"   : "enrich" | "update" | "overwrite",
                    "old_value": <existing value>,
                    "new_value": <incoming value>,
                }
            },
            "has_changes": bool   # True if at least one field needs updating
        }

    Keys in "changes" always use the jobs table column name.
    Fields in _EXCLUDED_FIELDS and _LIST_FIELD_MAP source names are
    handled separately and never fall through to the scalar loop.
    """
    changes = {}

    # 1. List fields — route through compare_field_to_list
    for jobsli_col, jobs_col in _LIST_FIELD_MAP.items():
        incoming_val = (
            get_value(jobsli_row, jobsli_col)
            if jobsli_col in jobsli_row.index
            else None
        )
        existing_list = (
            get_value(jobs_row, jobs_col) if jobs_col in jobs_row.index else None
        )

        result = compare_field_to_list(incoming_val, existing_list)

        if result["action"] != "none":
            changes[jobs_col] = {
                "action": result["action"],
                "old_value": existing_list,
                "new_value": result["new_value"],
            }

    # 2. Scalar fields — skip excluded cols and list-mapped source cols
    skip = _EXCLUDED_FIELDS | set(_LIST_FIELD_MAP.keys())
    candidate_fields = (set(jobsli_row.index) | set(jobs_row.index)) - skip

    for field in candidate_fields:
        incoming_val = (
            get_value(jobsli_row, field) if field in jobsli_row.index else None
        )
        existing_val = get_value(jobs_row, field) if field in jobs_row.index else None

        result = compare_fields(incoming_val, existing_val, date_relation)

        if result["action"] != "none":
            changes[field] = {
                "action": result["action"],
                "old_value": existing_val,
                "new_value": result["new_value"],
            }

    return {
        "changes": changes,
        "has_changes": len(changes) > 0,
    }


# ============================================================================
# Tier 3 — DB lookup functions
# ============================================================================

WORKER_ID = "worker_1"


def pick_jobsli_row(cursor) -> pd.Series | None:
    """
    Atomically selects and locks the next unprocessed row from jobsli.

    Selection criteria:
    - Status is NULL or empty string (not yet processed)
    - Oldest scraped_on date first; ties broken by smallest id_primary

    Locking: sets status to "worker_1" so concurrent workers skip it.
    Uses a single UPDATE ... RETURNING statement for atomicity — no
    separate SELECT + UPDATE round-trip.

    Returns the selected row as a pd.Series, or None if no rows are available.
    """
    query = """
        UPDATE public.jobsli
        SET status = %s
        WHERE id_primary = (
            SELECT id_primary
            FROM public.jobsli
            WHERE status IS NULL OR status = ''
            ORDER BY scraped_on::date ASC, id_primary ASC
            LIMIT 1
        )
        RETURNING *
    """
    cursor.execute(query, (WORKER_ID,))
    row = cursor.fetchone()

    if row is None:
        return None

    column_names = [desc[0] for desc in cursor.description]
    return pd.Series(row, index=column_names)


def _query_jobs_by_array_value(cursor, column: str, value: str) -> list[int]:
    """
    Internal helper: returns a list of jobs.id values where `value` is found
    in the given array column using PostgreSQL's = ANY(...) operator.
    Returns an empty list if value is empty or no matches found.
    """
    if is_empty(value):
        return []
    cursor.execute(
        f"SELECT id FROM public.jobs WHERE %s = ANY({column})",
        (str(value),),
    )
    return [row[0] for row in cursor.fetchall()]


def find_jobs_by_jobsli_id(cursor, jobsli_id: str) -> list[int]:
    """
    Returns jobs.id values where jobsli_id appears in the jobsli_ids[] array.
    """
    return _query_jobs_by_array_value(cursor, "jobsli_ids", jobsli_id)


def find_jobs_by_job_url(cursor, job_url: str) -> list[int]:
    """
    Returns jobs.id values where job_url appears in the job_urls[] array.
    """
    return _query_jobs_by_array_value(cursor, "job_urls", job_url)


def find_jobs_by_description_hash(cursor, description_hash: uuid.UUID) -> list[int]:
    """
    Returns jobs.id values where description_hash matches the scalar column.
    """
    if is_empty(description_hash):
        return []
    cursor.execute(
        "SELECT id FROM public.jobs WHERE description_hash = %s",
        (str(description_hash),),
    )
    return [row[0] for row in cursor.fetchall()]


def find_jobs_by_title_company_hash(cursor, title_company_hash: uuid.UUID) -> list[int]:
    """
    Returns jobs.id values where title_company_hash matches the scalar column.
    """
    if is_empty(title_company_hash):
        return []
    cursor.execute(
        "SELECT id FROM public.jobs WHERE title_company_hash = %s",
        (str(title_company_hash),),
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_jobs_by_ids(cursor, job_ids: list[int]) -> pd.DataFrame:
    """
    Fetches full rows from jobs for a list of IDs.
    Returns an empty DataFrame if job_ids is empty.
    """
    if not job_ids:
        return pd.DataFrame()
    cursor.execute(
        "SELECT * FROM public.jobs WHERE id = ANY(%s)",
        (job_ids,),
    )
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=column_names)


# ============================================================================
# Tier 4 — write helpers
# ============================================================================


def update_jobsli_status(cursor, id_primary: int, status: str) -> None:
    """
    Updates the status field of a single row in jobsli.
    Called at the end of every exit path in the cleaner.
    """
    cursor.execute(
        "UPDATE public.jobsli SET status = %s WHERE id_primary = %s",
        (status, id_primary),
    )


def insert_review_record(
    cursor,
    jobsli_row: pd.Series,
    matched_job_ids: list[int],
    reason: str,
) -> None:
    """
    Inserts a row into the review table (step 6 of the cleaner logic).

    Denormalises the incoming jobsli row into review so reviewers have
    full context without joining back to jobsli. The matching job IDs
    are stored as an array in similar_job_ids.
    """
    # Fields to copy directly from jobsli into review (column names match)
    copy_fields = [
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
        "scraped_on",
    ]

    data: dict = {
        "similar_job_ids": sorted(matched_job_ids),
        "similarity_reason": reason,
        "status": "review",
    }

    # jobsli.id (scraper string, e.g. "li-4371835852") maps to review.jobsli_id (varchar)
    jobsli_id_val = get_value(jobsli_row, "id") if "id" in jobsli_row.index else None
    if jobsli_id_val is not None:
        data["jobsli_id"] = jobsli_id_val

    for field in copy_fields:
        val = get_value(jobsli_row, field) if field in jobsli_row.index else None
        if val is not None:
            data[field] = val

    cols = ", ".join(f'"{k}"' if k == "interval" else k for k in data)
    placeholders = ", ".join(["%s"] * len(data))
    cursor.execute(
        f"INSERT INTO public.review ({cols}) VALUES ({placeholders})",
        list(data.values()),
    )


def append_job_history_log(
    cursor,
    job_id: int,
    jobsli_id: str,
    change_source: str,
    change_type: str,
    changes: dict,
) -> None:
    """
    Inserts one record into job_history_log (step 5 of the cleaner logic).

    Args:
        job_id       : jobs.id of the canonical job that was updated
        jobsli_id    : jobsli.id (scraper ID, e.g. "li-4369251916")
        change_source: what triggered the change, e.g. "cleaner"
        change_type  : "enrichment" | "update" | "overwrite"
        changes      : the dict produced by build_diff_summary["changes"]
    """

    cursor.execute(
        """
        INSERT INTO public.job_history_log
            (job_id, jobsli_id, change_source, change_type, changes)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (job_id, jobsli_id, change_source, change_type, json.dumps(changes)),
    )


# ============================================================================
# Tier 5 — Core logic helpers
# ============================================================================


def has_required_data(jobsli_row: pd.Series) -> bool:
    """
    Implements Step 2 of the cleaner logic: Assess how much data was scraped.
    Checks if the row has the minimum amount of data - title, company, description, and "scraped_on" date.
    Returns False if any are missing, True otherwise.
    """
    required_cols = ["title", "company", "description", "scraped_on"]
    missing = [col for col in required_cols if is_empty(get_value(jobsli_row, col))]
    if missing:
        logger.info(f"  unmatchable — missing fields: {', '.join(missing)}")
        return False
    return True


def identify_canonical_job(cursor, jobsli_row: pd.Series) -> dict:
    """
    Implements Step 3 of the cleaner logic: finding a matching job in the jobs table.

    Returns a dict describing the outcome:
        "status"           : "review" | "new" | "single_match"
        "matched_job_id"   : int | None
        "candidate_job_ids": list[int] (only populated if status="review")
        "review_reason"    : str (only populated if status="review")

    Logic flow:
    1. Check if jobsli ID matches any jobsli_ids[].
    2. Check if jobsli job_url matches any job_urls[].
    3. Check if description_hash matches.
       - If multiple hash matches, filter by title_company_hash.
       - If still multiple, filter by location.
    4. Check if title_company_hash matches.
       - If multiple matches, filter by location.

    Returns early on the first successful match layer.
    """
    # 1. Match by jobsli ID
    jobsli_id = get_value(jobsli_row, "id")
    if not is_empty(jobsli_id):
        candidates = find_jobs_by_jobsli_id(cursor, jobsli_id)
        if len(candidates) == 1:
            logger.debug("MATCH: jobsli_id (single match)")
            return {"status": "single_match", "matched_job_id": candidates[0]}
        if len(candidates) > 1:
            ids_str = ", ".join(str(i) for i in sorted(candidates))
            logger.debug(f"REVIEW: jobsli_id matched multiple: {ids_str}")
            return {
                "status": "review",
                "matched_job_id": None,
                "candidate_job_ids": sorted(candidates),
                "review_reason": "Multiple records found with the same jobsli ID",
            }

    # 2. Match by job_url
    job_url = get_value(jobsli_row, "job_url")
    if not is_empty(job_url):
        candidates = find_jobs_by_job_url(cursor, job_url)
        if len(candidates) == 1:
            logger.debug("MATCH: job_url (single match)")
            return {"status": "single_match", "matched_job_id": candidates[0]}
        if len(candidates) > 1:
            ids_str = ", ".join(str(i) for i in sorted(candidates))
            logger.debug(f"REVIEW: job_url matched multiple: {ids_str}")
            return {
                "status": "review",
                "matched_job_id": None,
                "candidate_job_ids": sorted(candidates),
                "review_reason": "Multiple records found with the same job URL",
            }

    # 3. Match by description_hash
    # We assume 'description' exists because Step 2 filters out rows without it
    desc = get_value(jobsli_row, "description")
    desc_hash = compute_description_hash(desc)
    desc_candidates = find_jobs_by_description_hash(cursor, desc_hash)

    title = get_value(jobsli_row, "title")
    company = get_value(jobsli_row, "company")
    tc_hash = compute_title_company_hash(title, company)

    location = get_value(jobsli_row, "location")
    loc_str = str(location).strip().lower() if not is_empty(location) else ""

    if len(desc_candidates) == 1:
        logger.debug("MATCH: description_hash (single match)")
        return {"status": "single_match", "matched_job_id": desc_candidates[0]}

    elif len(desc_candidates) > 1:
        # --- MULTIPLE MATCHES ON DESCRIPTION HASH ---
        # Filter by title_company_hash
        jobs_df = fetch_jobs_by_ids(cursor, desc_candidates)
        tc_matches = jobs_df[jobs_df["title_company_hash"] == str(tc_hash)]

        if tc_matches.empty:
            # Duplicate descriptions, but none share the exact title+company hash.
            # Description is identical, but title/company differ -> Send to review.
            logger.debug(
                "REVIEW: description_hash multiple matches, but 0 matches on title+company"
            )
            return {
                "status": "review",
                "matched_job_id": None,
                "candidate_job_ids": desc_candidates,
                "review_reason": "Same description, but different title/company",
            }

        if len(tc_matches) == 1:
            logger.debug(
                "MATCH: description_hash -> filtered by title_company_hash (single match)"
            )
            return {
                "status": "single_match",
                "matched_job_id": int(tc_matches.iloc[0]["id"]),
            }

        # MULTIPLE MATCHES ON TITLE+COMPANY -> Filter by location
        loc_series = (
            tc_matches["location"].fillna("").astype(str).str.strip().str.lower()
        )
        loc_matches = tc_matches[loc_series == loc_str]

        if loc_matches.empty:
            # Same description, title, and company, but different location -> Send to review
            logger.debug(
                "REVIEW: description_hash -> title+company matched multiple -> location matched 0"
            )
            return {
                "status": "review",
                "matched_job_id": None,
                "candidate_job_ids": tc_matches["id"].tolist(),
                "review_reason": "Same description, title, and company, but different location",
            }

        if len(loc_matches) == 1:
            logger.debug(
                "MATCH: description_hash -> title+company -> location (single match)"
            )
            return {
                "status": "single_match",
                "matched_job_id": int(loc_matches.iloc[0]["id"]),
            }

        # STILL MULTIPLE MATCHES -> Send to review
        logger.debug(
            "REVIEW: description_hash -> title+company -> location matched multiple exactly"
        )
        return {
            "status": "review",
            "matched_job_id": None,
            "candidate_job_ids": loc_matches["id"].tolist(),
            "review_reason": "Same description, title, company, and location",
        }

    # 4. Match by title_company_hash (Fallback if description_hash yielded 0 matches)
    # We assume 'title' and 'company' exist because Step 2 filters out rows without them
    tc_candidates = find_jobs_by_title_company_hash(cursor, tc_hash)

    if not tc_candidates:
        logger.debug(
            "NEW: 0 matches on description_hash, and 0 matches on title_company_hash"
        )
        return {"status": "new", "matched_job_id": None}

    # AT LEAST ONE MATCH ON TITLE+COMPANY -> Filter by location
    jobs_df = fetch_jobs_by_ids(cursor, tc_candidates)
    loc_series = jobs_df["location"].fillna("").astype(str).str.strip().str.lower()
    loc_matches = jobs_df[loc_series == loc_str]

    if loc_matches.empty:
        # Same title and company, but different location -> Treat as new.
        logger.debug(
            "NEW: 0 matches on description_hash, title_company_hash matched, but location matched 0"
        )
        return {"status": "new", "matched_job_id": None}

    # AT LEAST ONE MATCH ON LOCATION (meaning they share Title, Company, and Location but NOT Description) -> Send to review
    logger.debug(
        "REVIEW: 0 matches on description_hash, title_company_hash matched, AND location matched"
    )
    return {
        "status": "review",
        "matched_job_id": None,
        "candidate_job_ids": loc_matches["id"].tolist(),
        "review_reason": "Same title, company, and location (different description)",
    }


# ============================================================================
# Tier 6 — Orchestration routines
# ============================================================================


def create_new_job(cursor, jobsli_row: pd.Series) -> int:
    """
    Implements Step 4 of the cleaner logic: Handle a new job.
    Inserts a completely new record into the jobs table based on jobsli.
    Returns the jobs.id of the inserted row.
    """
    desc = get_value(jobsli_row, "description")
    desc_hash = compute_description_hash(desc) if not is_empty(desc) else None

    title = get_value(jobsli_row, "title")
    company = get_value(jobsli_row, "company")
    tc_hash = (
        compute_title_company_hash(title, company)
        if not is_empty(title) and not is_empty(company)
        else None
    )

    scraped_on_date = parse_scraped_date(get_value(jobsli_row, "scraped_on"))

    data = {
        "description_hash": str(desc_hash) if desc_hash else None,
        "title_company_hash": str(tc_hash) if tc_hash else None,
        "first_seen_on": scraped_on_date,
        "last_seen_on": scraped_on_date,
        "seen_dates": [scraped_on_date] if scraped_on_date else [],
    }

    # Map list fields (e.g. jobsli.job_url -> jobs.job_urls[])
    for jobsli_col, jobs_col in _LIST_FIELD_MAP.items():
        val = get_value(jobsli_row, jobsli_col)
        data[jobs_col] = [val] if not is_empty(val) else []

    # Map scalar fields
    skip = _EXCLUDED_FIELDS | set(_LIST_FIELD_MAP.keys())
    for field in jobsli_row.index:
        if field in skip:
            continue
        val = get_value(jobsli_row, field)
        if not is_empty(val):
            data[field] = val

    cols = ", ".join(f'"{k}"' if k in ("interval",) else k for k in data.keys())
    placeholders = ", ".join(["%s"] * len(data))

    query = f"INSERT INTO public.jobs ({cols}) VALUES ({placeholders}) RETURNING id"
    cursor.execute(query, list(data.values()))

    return cursor.fetchone()[0]


def sync_job_record(
    cursor,
    jobsli_row: pd.Series,
    job_row: pd.Series,
    date_relation: Literal["newer", "older", "same"],
) -> str:
    """
    Implements Step 5: Match Found.
    Merges data from jobsli_row into job_row based on diff rules,
    logs the changes, and updates seen_dates/last_seen_on.
    Returns the string status to assign to the jobsli row
    (one of "duplicate", "update", "overwrite", or "enrich").
    """
    diff = build_diff_summary(jobsli_row, job_row, date_relation)
    changes = diff["changes"]
    has_changes = diff["has_changes"]

    # Track all levels of modification for logging and jobsli status
    change_type = "duplicate"
    if has_changes:
        actions_found = {v["action"] for v in changes.values()}
        change_type = ", ".join(sorted(actions_found))

    jobs_id = int(job_row["id"])
    jobsli_id = str(get_value(jobsli_row, "id"))

    updates = []
    values = []

    if has_changes:
        # Prepare SET clauses for all modifications
        for field, detail in changes.items():
            if detail["action"] == "enrich" and field in _LIST_FIELD_MAP.values():
                # array_append safely adds a single list item
                # Using COALESCE to gracefully handle NULL columns
                updates.append(f"{field} = array_append(COALESCE({field}, '{{}}'), %s)")
                values.append(detail["new_value"])
            else:
                col_name = f'"{field}"' if field == "interval" else field
                updates.append(f"{col_name} = %s")
                values.append(detail["new_value"])

        # Handle hash recomputation if core fields changed
        if "description" in changes:
            desc_hash = compute_description_hash(changes["description"]["new_value"])
            updates.append("description_hash = %s")
            values.append(str(desc_hash) if desc_hash else None)

        if "title" in changes or "company" in changes:
            title = changes.get("title", {}).get(
                "new_value", get_value(job_row, "title")
            )
            company = changes.get("company", {}).get(
                "new_value", get_value(job_row, "company")
            )
            tc_hash = compute_title_company_hash(title, company)
            updates.append("title_company_hash = %s")
            values.append(str(tc_hash) if tc_hash else None)

        # Log to job_history_log (only if fields actually changed)
        append_job_history_log(
            cursor=cursor,
            job_id=jobs_id,
            jobsli_id=jobsli_id,
            change_source="cleaner",
            change_type=change_type,
            changes=changes,
        )
        logger.info(
            f"  → history log: change_type={change_type!r}, "
            f"fields={list(changes.keys())}"
        )

    # Regardless of whether fields changed, we must update date tracking.
    scraped_on_date = parse_scraped_date(get_value(jobsli_row, "scraped_on"))
    if scraped_on_date:
        existing_seen = get_value(job_row, "seen_dates")
        seen_list = []
        if not is_empty(existing_seen):
            # Enforce datetime.date casting for all elements (fixes string vs date sort bugs in test/pandas data)
            seen_list = [
                parsed_d
                for x in existing_seen
                if (parsed_d := parse_scraped_date(x)) is not None
            ]

        if scraped_on_date not in seen_list:
            seen_list.append(scraped_on_date)
            seen_list.sort()
            updates.append("seen_dates = %s")
            values.append(seen_list)

        if date_relation == "newer":
            updates.append("last_seen_on = %s")
            values.append(scraped_on_date)
        elif date_relation == "older":
            # Only move first_seen_on backwards if this row is genuinely earlier
            existing_first = get_value(job_row, "first_seen_on")
            if is_empty(existing_first):
                updates.append("first_seen_on = %s")
                values.append(scraped_on_date)
            else:
                parsed_first = (
                    existing_first
                    if isinstance(existing_first, date)
                    else parse_scraped_date(existing_first)
                )
                if parsed_first is not None and scraped_on_date < parsed_first:
                    updates.append("first_seen_on = %s")
                    values.append(scraped_on_date)

    if updates:
        values.append(jobs_id)
        set_clause = ", ".join(updates)
        cursor.execute(f"UPDATE public.jobs SET {set_clause} WHERE id = %s", values)

    return change_type


def process_job_row(cursor, jobsli_row: pd.Series) -> str:
    """
    The orchestrator for a single jobsli row.
    Implements Steps 2-6 of the cleaner logic.
    Returns the final status string to be assigned to jobsli.status.
    """
    # 1. Step 2: assess missing fields
    if not has_required_data(jobsli_row):
        return "unmatchable"

    # 2. Step 3: Identify canonical job
    match_result = identify_canonical_job(cursor, jobsli_row)
    status = match_result["status"]

    if status == "single_match":
        _summary = f"single_match → job_id={match_result['matched_job_id']}"
    elif status == "review":
        _summary = f"review → {match_result.get('review_reason', '?')} (candidates={match_result.get('candidate_job_ids', [])})"
    else:
        _summary = status
    logger.info(f"  identify: {_summary}")

    if status == "new":
        # Step 4
        new_job_id = create_new_job(cursor, jobsli_row)
        logger.info(f"  → inserted into jobs (id={new_job_id})")
        return "new"

    elif status == "review":
        # Step 6
        candidate_ids = match_result.get("candidate_job_ids", [])
        reason = match_result.get("review_reason", "Manual review required")
        insert_review_record(cursor, jobsli_row, candidate_ids, reason)
        logger.info(f"  → inserted into review (candidates={candidate_ids})")
        return "review"

    elif status == "single_match":
        # Step 5
        matched_id = match_result["matched_job_id"]

        # Fetch the actual job row from DB
        cursor.execute("SELECT * FROM public.jobs WHERE id = %s", (matched_id,))
        job_row_tuple = cursor.fetchone()
        col_names = [desc[0] for desc in cursor.description]
        job_row = pd.Series(job_row_tuple, index=col_names)

        # Determine date relation
        scraped_on = parse_scraped_date(get_value(jobsli_row, "scraped_on"))
        last_seen = get_value(job_row, "last_seen_on")
        date_relation = classify_date_relation(scraped_on, last_seen)

        sync_job_record(cursor, jobsli_row, job_row, date_relation)
        logger.info(
            f"  → synced into jobs (id={matched_id}, date_relation={date_relation})"
        )
        return "done"

    return "unknown"


def run_cleaner(conn, cursor) -> str:
    """
    Processes a single jobsli row through the new pipeline.

    Picks the oldest unprocessed row (status IS NULL or ''), runs it
    through the full matching / creation logic, commits the result, and
    stamps jobsli.status with the outcome.

    Takes both `conn` and `cursor` so it can own commit/rollback entirely,
    which ensures the error stamp is always persisted in its own clean
    transaction even if the main work failed.

    Returns one of:
        "idle"        — no unprocessed rows available; nothing was done
        "done"        — row fully processed
        "unmatchable" — row lacked the required fields; skipped
        "error"       — an unexpected exception occurred; row stamped 'error'
    """
    row = pick_jobsli_row(cursor)
    if row is None:
        logger.info("No unprocessed rows in jobsli — cleaner is idle.")
        conn.commit()
        return "idle"

    id_primary = int(row["id_primary"])
    logger.info(
        f"Processing jobsli id_primary={id_primary} title={row.get('title', 'N/A')!r}"
    )

    try:
        outcome = process_job_row(cursor, row)
        final_status = "unmatchable" if outcome == "unmatchable" else "done"
        update_jobsli_status(cursor, id_primary, final_status)
        conn.commit()
        logger.info(f"  jobsli status for id_primary={id_primary}: {final_status}")
        return final_status

    except Exception as e:
        logger.error(
            f"  jobsli status for id_primary={id_primary}: unhandled error — {e}",
            exc_info=True,
        )
        # The transaction is aborted — rollback first, then stamp 'error' in a clean state.
        conn.rollback()
        try:
            update_jobsli_status(cursor, id_primary, "error")
            conn.commit()
        except Exception:
            logger.error(f"  failed to stamp jobsli status for id_primary={id_primary}")
        return "error"


def run_cleaner_loop(limit: int = 1000) -> None:
    """
    Runs the new pipeline cleaner for up to `limit` rows.

    Each row is processed in its own transaction so that a single bad
    row does not prevent the rest of the batch from being processed.
    The loop stops early when there are no more unprocessed rows.

    Args:
        limit: Maximum number of jobsli rows to process in one call.
    """
    logger.info("=" * 60)
    logger.info(f"Starting cleaner loop (limit={limit})")
    logger.info("=" * 60)

    processed = 0
    for _ in range(limit):
        try:
            with get_connection() as (conn, cursor):
                result = run_cleaner(conn, cursor)
        except Exception as e:
            logger.error(f"Fatal error in cleaner loop: {e}", exc_info=True)
            break

        if result == "idle":
            logger.info("No more rows to process. Exiting loop.")
            break

        processed += 1

    logger.info(f"Cleaner loop finished. Processed {processed} row(s).")


# ============================================================================
# FUNCTIONS THAT WILL BE REUSED
# ============================================================================


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


def _generate_status_string(label: str, ids: list[str]) -> str:
    """
    Constructs the status string for duplicates or similar jobs.
    Format: "label: id1, id2, ..."
    """
    if not ids:
        return label

    # Sort IDs numerically to ensure consistent order (e.g. 2 before 10)
    try:
        sorted_ids = sorted(ids, key=int)
    except ValueError:
        # Fallback to string sort if non-integer IDs exist
        sorted_ids = sorted(ids)

    return f"{label}: {', '.join(sorted_ids)}"


# ============================================================================
# OLD FUNCTIONS WHICH WILL BE DEPRECATED
# ============================================================================


def is_duplicate(job_row: pd.Series, candidate_row: pd.Series) -> bool:
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
        val_job = get_value(job_row, field)
        val_cand = get_value(candidate_row, field)

        # Treat None and empty string as equal
        if (val_job is None or val_job == "") and (val_cand is None or val_cand == ""):
            continue

        # If values differ, it's not a duplicate
        if val_job != val_cand:
            return False

    return True


def has_enrichment(job_row: pd.Series, candidate_row: pd.Series) -> bool:
    """
    Checks if the newer job (`job_row`) has MORE data than the older candidate (`candidate_row`).

    Rules:
    1. Candidate must be older (older create timestamp, or same timestamp but lower id).
    2. All common fields must be either:
       - Exact match.
       - OR Candidate was empty/null, but Job has data (Enrichment).
    3. At least one field MUST be an enrichment to return True.
    4. If any field CONFLICTS (both have data but different), return False.
    """
    # 1. Check strict chronological order (Candidate must be OLDER)
    job_created = get_value(job_row, "created_at")
    cand_created = get_value(candidate_row, "created_at")
    job_id = get_value(job_row, "id_primary")
    cand_id = get_value(candidate_row, "id_primary")

    if not (
        cand_created < job_created or (cand_created == job_created and cand_id < job_id)
    ):
        return False

    excluded_fields = {"id_primary", "created_at", "status"}
    has_enrichment = False

    # Get all potential fields
    all_fields = set(job_row.index).union(candidate_row.index)

    for field in all_fields:
        if field in excluded_fields:
            continue

        val_job = get_value(job_row, field)
        val_cand = get_value(candidate_row, field)

        # Normalize for comparison
        is_job_empty = val_job is None or val_job == ""
        is_cand_empty = val_cand is None or val_cand == ""

        # Case A: Both empty -> Match
        if is_job_empty and is_cand_empty:
            continue

        # Case B: Candidate has data, Job empty -> Loss of data?
        # Requirement says "fields must be exact match or candidate null".
        # Logic implies we only care if we are ADDING data, removing data usually means not a strict enrichment match
        # or just acceptable difference?
        # User prompt: "Fields matches OR if some fields were null/empty for candidate, now they have data"
        # Implies: If Candidate has data, Job MUST match it.
        if not is_cand_empty and is_job_empty:
            # Job is missing data that candidate has. This is NOT "additional data" scenario primarily,
            # but strictly speaking doesn't violate "candidate was null".
            # STRICT interpretation: If candidate has data, job must match.
            # If job is missing it, it's NOT a match.
            return False

        # Case C: Candidate empty, Job has data -> ENRICHMENT
        if is_cand_empty and not is_job_empty:
            has_enrichment = True
            continue

        # Case D: Both have data -> Must MATCH
        if val_job != val_cand:
            return False

    return has_enrichment


def pick_job_for_matching(cursor, id_primary: int | None = None) -> pd.Series | None:
    """
    Selects and marks a job for matching using db_ops.fetch_and_lock_job.
    """
    worker_id = "worker_1"

    if id_primary is not None:
        logger.debug(f"Loading and marking row with id_primary={id_primary}")
        # Build specific condition for ID
        conditions = f"id_primary = {id_primary}"
        # Order doesn't matter much for single ID, but required by API
        order_by = "id_primary"
    else:
        logger.debug("Selecting oldest row with null/empty status")
        conditions = "status IS NULL OR status = ''"
        order_by = "scraped_on::date ASC, id_primary ASC"

    return fetch_and_lock_job(
        cursor=cursor,
        table_name="jobsli",
        worker_id=worker_id,
        conditions=conditions,
        order_by=order_by,
    )


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
        - candidates_df: DataFrame with candidate matches (empty if none found)
    """
    # ==============================
    # LOGIC:
    # 1. if at least one of the following fields is an exact match (unless NULL or empty), proceed to step 4
    #    - id
    #    - job_url
    #    - description
    #    - job_url_direct
    # 2. if both of the following fields are an exact match (unless NULL or empty), proceed to step 4
    #    - title
    #    - company
    # 3. If no matches, then the job is unique.
    # 4. otherwise return the candidates dataframe and assessment "candidates_found"
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

        # Step 4: Return the candidates dataframe and assessment "matches_found"
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
                        transfer_job(
                            cursor=cursor,
                            job_id=job_id_primary,
                            source_table="jobsli",
                            target_table="preparedjobs",
                            target_status="new",
                            delete_source=False,
                            source_status="new",
                            source_status_on_failure="new",
                        )
                        logger.info(
                            f"Job {job_id_primary} processed as unique (prepared/new)"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to copy job {job_id_primary} to preparedjobs: {e}",
                            exc_info=True,
                        )

                elif assessment == "candidates_found":
                    # Check if any candidate is an exact duplicate
                    duplicate_ids = []
                    enrichment_ids = []

                    all_candidate_ids = []
                    for _, candidate_row in candidates_df.iterrows():
                        cand_id = get_value(candidate_row, "id_primary")
                        if cand_id is not None:
                            all_candidate_ids.append(str(cand_id))

                        if is_duplicate(job_row, candidate_row):
                            duplicate_ids.append(str(cand_id))
                        elif has_enrichment(job_row, candidate_row):
                            enrichment_ids.append(str(cand_id))

                    if duplicate_ids:
                        # CASE 1: At least one duplicate found
                        status_msg = _generate_status_string("duplicate", duplicate_ids)
                        logger.info(
                            f"Job {job_id_primary} found to be duplicate of {duplicate_ids}"
                        )

                        try:
                            transfer_job(
                                cursor=cursor,
                                job_id=job_id_primary,
                                source_table="jobsli",
                                target_table="preparedjobs",
                                target_status=status_msg,
                                delete_source=False,
                                source_status=status_msg,
                                source_status_on_failure=status_msg,
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to process duplicate job {job_id_primary}: {e}",
                                exc_info=True,
                            )

                    elif enrichment_ids:
                        # CASE 2: No duplicates, but enrichment found
                        status_msg = _generate_status_string(
                            "enrichment", enrichment_ids
                        )
                        logger.info(
                            f"Job {job_id_primary} found to be enrichment of {enrichment_ids}"
                        )

                        try:
                            transfer_job(
                                cursor=cursor,
                                job_id=job_id_primary,
                                source_table="jobsli",
                                target_table="preparedjobs",
                                target_status=status_msg,
                                delete_source=False,
                                source_status=status_msg,
                                source_status_on_failure=status_msg,
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to process enrichment job {job_id_primary}: {e}",
                                exc_info=True,
                            )

                    else:
                        # CASE 3: No duplicates or enrichment, but candidates exist -> SIMILAR
                        status_msg = _generate_status_string(
                            "similar", all_candidate_ids
                        )
                        logger.info(
                            f"Job {job_id_primary} found to be similar to {all_candidate_ids}"
                        )

                        try:
                            transfer_job(
                                cursor=cursor,
                                job_id=job_id_primary,
                                source_table="jobsli",
                                target_table="similarjobs",
                                target_status=status_msg,
                                delete_source=False,
                                source_status=status_msg,
                                source_status_on_failure=status_msg,
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to process similar job {job_id_primary}: {e}",
                                exc_info=True,
                            )

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
                    # Set status to 'error' to prevent infinite loop
                    logger.info(
                        f"Assessment '{assessment}' for id_primary={job_id_primary} - setting status to 'error'"
                    )
                    update_status_query = "UPDATE public.jobsli SET status = 'error' WHERE id_primary = %s"
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

        # NOTE: On error, status remains as worker_id (could be handled differently)
        return job_row if job_row is not None else pd.Series(dtype=object)


def run_matcher_loop(limit: int = 1000) -> None:
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
            logger.error(f"Error in matcher loop integration: {e}", exc_info=True)
            # If catastrophic error, break. If simple job error, match_job catches it.
            # match_job catches its own errors, re-raised ones are serious.
            break

    logger.info(f"Matcher loop completed. Processed {count} jobs.")


if __name__ == "__main__":
    run_cleaner_loop()

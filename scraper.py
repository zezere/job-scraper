from typing import Dict, Any
import time
import psycopg2
import pandas as pd
from jobspy import scrape_jobs
from db_connection import get_connection
from db_ops import insert_row
from utils import setup_logging, get_value, validate_dataframe
from config import SCRAPER_SETTINGS, SCRAPER_RETRY_ATTEMPTS, SCRAPER_RETRY_DELAY

logger = setup_logging("scraper")

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


def save_jobs(jobs_df: pd.DataFrame) -> None:
    """
    Saves jobs from a DataFrame to the jobsli table in the database.

    Uses standard DB connection and the generic inserts from db_ops.
    Iterates through rows and attempts insert, skipping duplicates gracefully
    via IntegrityError handling.
    """
    logger.info(f"save_jobs called with {len(jobs_df)} jobs to process")

    if jobs_df.empty:
        logger.warning("Received empty DataFrame, nothing to save")
        return

    validate_dataframe(jobs_df, ["id"], save_jobs.__name__)

    try:
        logger.info("Establishing database connection...")
        with get_connection() as (conn, cursor):
            logger.info("Database connection established successfully")

            saved_count = 0
            skipped_count = 0
            failed_jobs = []

            logger.info("Starting to insert jobs into database...")
            for idx, (_, row) in enumerate(jobs_df.iterrows(), 1):
                job_title = row.get("title", "unknown")
                job_url = row.get("job_url", "N/A")

                try:
                    # Use the new generic insert function
                    insert_row(cursor=cursor, table_name="jobsli", data=row)
                    conn.commit()
                    saved_count += 1

                    if idx % 10 == 0:
                        logger.debug(f"Progress: {idx}/{len(jobs_df)} jobs processed")

                except psycopg2.IntegrityError as e:
                    conn.rollback()
                    logger.warning(f"Integrity error for job '{job_title}': {e}")
                    skipped_count += 1
                    failed_jobs.append({"title": job_title, "error": str(e)})

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error saving job '{job_title}': {e}", exc_info=True)
                    skipped_count += 1
                    failed_jobs.append({"title": job_title, "error": str(e)})

            logger.info("=" * 60)
            logger.info(f"Database save operation completed:")
            logger.info(f"  - Successfully saved: {saved_count} jobs")
            logger.info(f"  - Failed/skipped: {skipped_count} jobs")
            logger.info(f"  - Success rate: {(saved_count/len(jobs_df)*100):.1f}%")
            logger.info("=" * 60)

            if failed_jobs:
                logger.warning(f"Details of {len(failed_jobs)} failed jobs:")
                for failed in failed_jobs[:5]:
                    logger.warning(f"  - {failed['title']}: {failed['error'][:100]}")
                if len(failed_jobs) > 5:
                    logger.warning(f"  ... and {len(failed_jobs) - 5} more")

    except Exception as e:
        logger.error(f"Unexpected error during batch insert: {e}", exc_info=True)
        raise


def scrape_linkedin(
    search_term: str = "product analyst",
    location: str = "luxembourg",
    results_wanted: int = SCRAPER_SETTINGS["results_wanted"],
    hours_old: int = SCRAPER_SETTINGS["hours_old"],
    verbose: int = SCRAPER_SETTINGS["verbose"],
    linkedin_fetch_description: bool = SCRAPER_SETTINGS["linkedin_fetch_description"],
) -> None:
    """
    Scrapes jobs from LinkedIn using jobspy and saves them to the database.
    Accepts search parameters as arguments.

    Logs the entire process and handles errors.
    """
    logger.info("=" * 60)
    logger.info("Starting job scraping process")
    logger.info("=" * 60)

    search_params: Dict[str, Any] = {
        "site_name": "linkedin",
        "search_term": search_term,
        "location": location,
        "results_wanted": results_wanted,
        "hours_old": hours_old,
        "verbose": verbose,
        "linkedin_fetch_description": linkedin_fetch_description,
    }

    logger.info(f"Scraping parameters: {search_params}")

    for attempt in range(SCRAPER_RETRY_ATTEMPTS):
        try:
            logger.info(
                f"Initiating job scrape from LinkedIn (Attempt {attempt + 1}/{SCRAPER_RETRY_ATTEMPTS})..."
            )
            jobs = scrape_jobs(**search_params)
            logger.info(f"Scraping completed. Retrieved {len(jobs)} jobs")

            if len(jobs) == 0:
                logger.warning("No jobs found for the given search criteria")
            else:
                logger.info(f"Job data columns: {list(jobs.columns)}")
                logger.info(
                    f"Sample job titles: {jobs['title'].head(3).tolist() if 'title' in jobs.columns else 'N/A'}"
                )

            logger.info("Starting database save operation...")
            save_jobs(jobs)
            logger.info("Job scraping and saving process completed successfully")
            break  # Success, exit loop

        except Exception as e:
            logger.error(
                f"Error during scraping process (Attempt {attempt + 1}/{SCRAPER_RETRY_ATTEMPTS}): {e}",
                exc_info=True,
            )
            if attempt == SCRAPER_RETRY_ATTEMPTS - 1:
                logger.error("Max retries reached. Raising exception.", exc_info=True)
                raise

            delay = SCRAPER_RETRY_DELAY * (2**attempt)
            logger.info(f"Waiting {delay} seconds before retrying...")
            time.sleep(delay)


if __name__ == "__main__":
    scrape_linkedin(results_wanted=10)

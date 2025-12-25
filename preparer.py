from pipeline import extract_unique_jobs, move_to_prepared_jobs
from utils import setup_logging

logger = setup_logging("preparer")

if __name__ == "__main__":
    try:
        unique_jobs = extract_unique_jobs(export_duplicates_csv=True)

        if unique_jobs.empty:
            logger.warning("No unique jobs to process")
        else:
            logger.info(f"Successfully extracted {len(unique_jobs)} unique jobs")
            logger.info("Moving unique jobs to preparedjobs table...")
            stats = move_to_prepared_jobs(unique_jobs)
            logger.info(
                f"Pipeline completed: {stats['inserted']} inserted, {stats['skipped']} skipped, {stats['errors']} errors"
            )

    except Exception as e:
        logger.error(f"Critical error during extraction: {e}", exc_info=True)
        raise

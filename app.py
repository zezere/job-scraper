from scraper import run_scraper
from pipeline import extract_unique_jobs, move_to_prepared_jobs, process_jobs
from utils import setup_logging

logger = setup_logging("app")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting full pipeline: scrape → save → extract → move → process")
    logger.info("=" * 60)

    try:
        logger.info("Step 1: Running scraper...")
        run_scraper()

        logger.info("Step 2: Extracting unique jobs...")
        unique_jobs = extract_unique_jobs(export_duplicates_csv=True)

        if unique_jobs.empty:
            logger.warning("No unique jobs extracted")
        else:
            logger.info(f"Extracted {len(unique_jobs)} unique jobs")

        logger.info("Step 3: Moving unique jobs to preparedjobs...")
        stats = move_to_prepared_jobs(unique_jobs, export_failed_csv=True)
        logger.info(
            f"Move completed: {stats['inserted']} inserted, {stats['skipped']} skipped, {stats['errors']} errors"
        )

        logger.info("Step 4: Processing jobs from preparedjobs...")
        process_stats = process_jobs(batch_size=2)
        logger.info(
            f"Processing completed: {process_stats['processed']} processed, {process_stats['errors']} errors"
        )

        logger.info("=" * 60)
        logger.info("Full pipeline completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Critical error during pipeline execution: {e}", exc_info=True)
        raise

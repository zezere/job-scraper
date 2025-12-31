from scraper import scrape_linkedin
from matcher import run_matcher_loop
from utils import setup_logging

logger = setup_logging("app")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting application: Scraper -> Matcher")
    logger.info("=" * 60)

    try:
        logger.info("Step 1: Running scraper...")
        # Scrape jobs (e.g. 5 jobs as per current config)
        scrape_linkedin()

        logger.info("Step 2: Processing jobs via Matcher...")
        # Run matcher for a reasonable batch (e.g. 50 calls) to clear the queue
        run_matcher_loop(limit=50)

        logger.info("=" * 60)
        logger.info("Application flow completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Critical error during execution: {e}", exc_info=True)
        raise

from jobspy import scrape_jobs
from database import save_jobs
from utils import setup_logging

logger = setup_logging("scraper")


def run_scraper():
    logger.info("=" * 60)
    logger.info("Starting job scraping process")
    logger.info("=" * 60)

    search_params = {
        "site_name": "linkedin",
        "search_term": "postgres",
        "location": "Amsterdam, North Holland, Netherlands",
        "results_wanted": 30,
        "hours_old": 24,
        "verbose": 2,
        "linkedin_fetch_description": True,
    }

    logger.info(f"Scraping parameters: {search_params}")

    try:
        logger.info("Initiating job scrape from LinkedIn...")
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

    except Exception as e:
        logger.error(f"Critical error during scraping process: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_scraper()

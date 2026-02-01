import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", None)

if not DATABASE_URL:
    # Warning instead of Error to allow import even if not set (will fail at usage time if needed)
    pass


# TODO: define scraper interval in .env file
SCRAPER_INTERVAL = int(os.getenv("SCRAPER_INTERVAL", 3600))

# Scraper Settings
SCRAPER_SETTINGS = {
    "results_wanted": int(os.getenv("SCRAPER_RESULTS_WANTED", 5)),
    "hours_old": int(os.getenv("SCRAPER_HOURS_OLD", 24)),
    "verbose": int(os.getenv("SCRAPER_VERBOSE", 2)),
    "linkedin_fetch_description": os.getenv("SCRAPER_FETCH_DESCRIPTION", "True").lower()
    == "true",
}

SCRAPER_RETRY_ATTEMPTS = int(os.getenv("SCRAPER_RETRY_ATTEMPTS", 3))
SCRAPER_RETRY_DELAY = int(os.getenv("SCRAPER_RETRY_DELAY", 10))

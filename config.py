import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", None)
if not DATABASE_URL:
    raise ValueError("DATABASE_URL must be set in .env file")

# TODO: define scraper interval in .env file
SCRAPER_INTERVAL = int(os.getenv("SCRAPER_INTERVAL", 3600))

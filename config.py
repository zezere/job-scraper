import os
from dotenv import load_dotenv

load_dotenv()

try:
    import streamlit as st
except ImportError:
    st = None

DATABASE_URL = os.getenv("DATABASE_URL", None)

if not DATABASE_URL and st is not None:
    try:
        # Check if running in Streamlit Cloud
        DATABASE_URL = st.secrets["DATABASE_URL"]
    except (FileNotFoundError, KeyError):
        # Local streamlit run might not have secrets.toml
        pass

# Fallback for old way if secrets didn't work and env var is missing
DATABASE_URL = DATABASE_URL or os.getenv("DATABASE_URL", None)
if not DATABASE_URL:
    raise ValueError("DATABASE_URL must be set in .env file")

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

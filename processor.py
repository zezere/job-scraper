from pipeline import process_jobs
from utils import setup_logging

logger = setup_logging("processor")

if __name__ == "__main__":
    try:
        stats = process_jobs(batch_size=1)
        logger.info(
            f"Processing completed: {stats['processed']} processed, {stats['errors']} errors"
        )
    except Exception as e:
        logger.error(f"Critical error during processing: {e}", exc_info=True)
        raise

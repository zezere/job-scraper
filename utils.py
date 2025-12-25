import logging
import os
from datetime import datetime


def setup_logging(log_name="app"):
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(
        log_dir, f"{log_name}_{datetime.now().strftime('%Y%m%d')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
    )

    return logging.getLogger(__name__)

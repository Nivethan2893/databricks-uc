import os
import yaml
import logging
from tqdm import tqdm
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime


def setup_logging(name, bu=None,log_dir=None):
    """
    Sets up logging with optional rotating file handler and console output.

    Parameters:
    - name (str): Logger name (e.g., class or function name)
    - log_dir (str or None): If provided, logs are written to this DBFS directory.

    Returns:
    - Tuple[logging.Logger, logging.Handler or None]: Configured logger and optional file handler
    """

    logger = logging.getLogger(name)
    file_handler = None  # Ensure variable exists

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

        # File handler (if log_dir is provided)
        if log_dir:
            try:
                os.makedirs(log_dir, exist_ok=True)
                log_file = os.path.join(log_dir, f"applog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
                file_handler = TimedRotatingFileHandler(log_file, when="m", interval=5, backupCount=5)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
            except Exception as e:
                logger.error(f"Failed to attach file logger: {e}")

        # Console handler (shows up in notebook)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger, file_handler
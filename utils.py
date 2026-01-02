import logging
import os
from datetime import datetime
from typing import Optional, Any, List
import pandas as pd


from logging.handlers import RotatingFileHandler


def setup_logging(log_name: str = "app", log_dir: str = "logs") -> logging.Logger:
    """
    Sets up a logger with a RotatingFileHandler and StreamHandler.
    Ensures that handlers are not duplicated if the logger is already retrieved.
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Get the logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # If handlers exist, return immediately to avoid duplicates
    if logger.hasHandlers():
        return logger

    # Create formatters and handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File Handler - strict separation by log_name
    log_filename = os.path.join(log_dir, f"{log_name}.log")
    file_handler = RotatingFileHandler(
        log_filename, maxBytes=5 * 1024 * 1024, backupCount=5  # 5MB props
    )
    file_handler.setFormatter(formatter)

    # Stream Handler (Console)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Prevent propagation to root logger (avoid double logging if basicConfig was used elsewhere)
    logger.propagate = False

    return logger


def get_value(row: pd.Series, key: str) -> Optional[Any]:
    """
    Extracts a value from a pandas Series, returning None for empty/missing values.

    Handles:
    - NaN, None, pandas NA, NaT -> None
    - Empty strings "" -> None
    - Whitespace-only strings "   " -> None

    Args:
        row: pandas Series to extract value from
        key: column name to extract

    Returns:
        The value from the Series, or None if empty/missing
    """
    val = row.get(key)

    if pd.isna(val):
        return None

    if isinstance(val, str) and not val.strip():
        return None

    return val


def validate_dataframe(
    df: pd.DataFrame, required_columns: List[str], operation_name: str = "operation"
) -> None:
    """
    Validates that a DataFrame has required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must exist
        operation_name: Name of operation for error messages

    Raises:
        ValueError: If df is not a DataFrame or missing required columns
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            f"{operation_name}: Expected DataFrame, got {type(df).__name__}"
        )

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{operation_name}: DataFrame missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )


def validate_query_result(
    df: pd.DataFrame, expected_columns: List[str], operation_name: str = "query"
) -> None:
    """
    Validates that a query result DataFrame has expected columns.

    Args:
        df: DataFrame from query result
        expected_columns: List of column names that should exist
        operation_name: Name of operation for error messages

    Raises:
        ValueError: If df is not a DataFrame or missing expected columns
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            f"{operation_name}: Expected DataFrame, got {type(df).__name__}"
        )

    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{operation_name}: Query result missing expected columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )

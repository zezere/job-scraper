import logging
import os
from datetime import datetime
from typing import Optional, Any, List
import pandas as pd


def setup_logging(log_name: str = "app") -> logging.Logger:
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

    return logging.getLogger(log_name)


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

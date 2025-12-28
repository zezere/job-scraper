from contextlib import contextmanager
from typing import Tuple
import psycopg2
from psycopg2.extensions import connection, cursor
from config import DATABASE_URL


@contextmanager
def get_connection() -> Tuple[connection, cursor]:
    """
    Context manager for database connections.

    Creates a connection and cursor, yields them, and automatically
    closes both when exiting the context (even on errors).

    Usage:
        with get_connection() as (conn, cursor):
            cursor.execute("SELECT ...")
            # Connection and cursor automatically closed here

    Yields:
        Tuple of (connection, cursor)

    Raises:
        Any exceptions from psycopg2.connect() will propagate to the caller,
        allowing the calling function's logger to handle them.
    """
    conn = None
    cursor_obj = None

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor_obj = conn.cursor()
        yield (conn, cursor_obj)
    finally:
        if cursor_obj is not None:
            cursor_obj.close()
        if conn is not None:
            conn.close()


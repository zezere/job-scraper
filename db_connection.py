from contextlib import contextmanager
from typing import Tuple
import psycopg2
from psycopg2.extensions import connection, cursor
@contextmanager
def get_connection(db_url: str | None = None) -> Tuple[connection, cursor]:
    """
    Context manager for database connections.
    
    Args:
        db_url: Optional database URL. If None, tries to fetch from config.DATABASE_URL.
    """
    conn = None
    cursor_obj = None

    connection_string = db_url
    if not connection_string:
        # Lazy import to avoid circular dependency or forcing config load
        try:
            from config import DATABASE_URL
            connection_string = DATABASE_URL
        except ImportError:
            pass

    if not connection_string:
        raise ValueError("No database URL provided or found in config.")

    try:
        conn = psycopg2.connect(connection_string)
        cursor_obj = conn.cursor()
        yield (conn, cursor_obj)
    finally:
        if cursor_obj is not None:
            cursor_obj.close()
        if conn is not None:
            conn.close()



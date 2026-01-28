"""Utility to run SQL scripts from registry."""
import os
import logging
from sqlalchemy import create_engine, text
from utils.utils import build_db_url

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_sql_file(script_path: str, engine=None) -> int:
    """
    Execute a SQL file and return rows affected.

    Args:
        script_path: Relative path from project root to SQL file
        engine: SQLAlchemy engine (created if not provided)

    Returns:
        Number of rows affected (or 0 if not available)
    """
    if engine is None:
        engine = create_engine(build_db_url())

    full_path = os.path.join(BASE_DIR, script_path)

    with open(full_path, 'r') as f:
        sql = f.read()

    logger.info(f"Executing {script_path}...")

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount if result.rowcount >= 0 else 0

    logger.info(f"Completed {script_path}: {rows} rows affected")
    return rows


def run_sql_registry(registry: list, engine=None) -> dict:
    """
    Run all scripts in registry order.

    Args:
        registry: List of registry entries with 'name' and 'script' keys
        engine: SQLAlchemy engine (created if not provided)

    Returns:
        Dict mapping script names to results with 'status' and 'rows'/'error'
    """
    if engine is None:
        engine = create_engine(build_db_url())

    results = {}
    for entry in registry:
        name = entry['name']
        script = entry['script']
        try:
            rows = run_sql_file(script, engine)
            results[name] = {'status': 'success', 'rows': rows}
        except Exception as e:
            logger.error(f"Failed {name}: {e}")
            results[name] = {'status': 'error', 'error': str(e)}
            raise
    return results

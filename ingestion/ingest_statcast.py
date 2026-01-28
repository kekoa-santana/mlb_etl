from pybaseball import statcast_pitcher
from sqlalchemy import create_engine, text
import pandas as pd
import os
import hashlib
import json
import uuid
import logging

from utils.utils import build_db_url
from utils.retry import retry_call

logger = logging.getLogger(__name__)

STATCAST_TIMEOUT = 45
STATCAST_MAX_RETRIES = 3
STATCAST_BACKOFF_FACTOR = 1.5

engine = create_engine(build_db_url(), pool_pre_ping=False)

def get_pitcher_ids() -> []:
    query = text("SELECT DISTINCT pitcher_id FROM raw.pitching_boxscores")

    with engine.begin() as conn:
        result = conn.execute(query)
        pitcher_ids = [row[0] for row in result]

    return pitcher_ids

def extract_statcast(pitcher_ids, start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    failed_ids = []

    for i, pitcher_id in enumerate(pitcher_ids, 1):
        try:
            df = retry_call(
                statcast_pitcher,
                args=(start_date, end_date, pitcher_id),
                max_retries=STATCAST_MAX_RETRIES,
                backoff_factor=STATCAST_BACKOFF_FACTOR,
                timeout=STATCAST_TIMEOUT,
                label=f"pitcher {pitcher_id} [{i}/{len(pitcher_ids)}]"
            )
            if df is not None and not df.empty:
                frames.append(df)
        except Exception as exc:
            failed_ids.append(pitcher_id)
            logger.error(f'Skipping pitcher {pitcher_id} affter all retries: {exc}')
            continue
    
    if failed_ids:
        logger.warning(
            f"Statcast extraction: {len(failed_ids)}/{len(pitcher_ids)} failures: {failed_ids}"
        )

    return pd.concat(frames, ignore_index=True)

def write_and_register_parquet(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    query_params:dict,
    base_folder: str = "E:/data_analytics/dodgers_pitching/data/"
) -> str:
    os.makedirs(base_folder, exist_ok=True)

    run_id = str(uuid.uuid4())
    file_name = f"statcast_pitching_{start_date}_{end_date}_{run_id}.parquet"
    file_path = os.path.join(base_folder, file_name)

    df.to_parquet(file_path, index=False)

    row_count = len(df)
    schema_signature = "|".join(
        f"{col}:{str(dtype)}" for col, dtype in df.dtypes.items()
    )

    schema_hash = hashlib.sha256(schema_signature.encode("utf-8")).hexdigest()

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.landing_statcast_files (
                start_date,
                end_date,
                row_count,
                schema_hash,
                file_path,
                query_params   
            )
            VALUES (
                :start_date,
                :end_date,
                :row_count,
                :schema_hash,
                :file_path,
                :query_params
            )
        """),
        {
            "start_date": start_date,
            "end_date": end_date,
            "row_count": row_count,
            "schema_hash": schema_hash,
            "file_path": file_path,
            "query_params": json.dumps(query_params),
        }
    )

    return file_path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, 'data')


def extract_and_save_statcast(start_date: str, end_date: str, data_dir: str = None, engine=None) -> str:
    """
    Callable entry point for pipeline - extracts statcast data and saves to parquet.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        data_dir: Directory for output parquet file (default: project data/)
        engine: SQLAlchemy engine (uses module default if not provided)

    Returns:
        Path to the saved parquet file
    """
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    print(f"Getting pitcher IDs from raw.pitching_boxscores...")
    pitcher_ids = get_pitcher_ids()
    print(f"Found {len(pitcher_ids)} pitchers")

    print(f"Extracting statcast data from {start_date} to {end_date}...")
    df = extract_statcast(pitcher_ids, start_date, end_date)
    print(f"Extracted {len(df)} pitch records")

    query_params = {
        "type": "statcast_pitcher",
        "start_date": start_date,
        "end_date": end_date,
        "pitcher_ids": pitcher_ids
    }

    file_path = write_and_register_parquet(df, start_date, end_date, query_params, data_dir)
    print(f"Saved parquet to: {file_path}")

    return file_path


def main():
    START_DATE = "2025-03-18"
    END_DATE = "2025-11-01"

    file_path = extract_and_save_statcast(START_DATE, END_DATE)
    print(file_path)


if __name__ == "__main__":
    main()
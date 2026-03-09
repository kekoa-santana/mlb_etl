from pybaseball import statcast, statcast_sprint_speed
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

STATCAST_TIMEOUT = 360
STATCAST_MAX_RETRIES = 3
STATCAST_BACKOFF_FACTOR = 1.5

engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=False)

def extract_statcast(start_date, end_date) -> pd.DataFrame:
    # Fetch league wide statcast data (batters + pitchers)
    df = retry_call(
        statcast,
        args=(start_date, end_date),
        max_retries=STATCAST_MAX_RETRIES,
        timeout=STATCAST_TIMEOUT,
        label=f'statcast {start_date} to {end_date}'
    )

    if df is None or df.empty:
        logger.warning(f"No Statcast data returned for {start_date} to {end_date}")
        return pd.DataFrame()

    return df

def extract_sprint_speed(year, attempts=50):
    df = retry_call(
        statcast_sprint_speed,
        args=(year, attempts),
        max_retries=2,
        timeout=30,
        label=f'statcast_sprint_speed_{year}'
    )

    if df is None or df.empty:
        logger.warning(f'No statcast sprint speed data for {year}')
        return pd.DataFrame()
    
    df['season'] = year
    return df

def write_and_register_parquet(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    query_params:dict,
    base_folder: str = "E:/data_analytics/mlb_pipeline/data/"
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


def extract_and_save_statcast(start_date: str, end_date: str, data_dir: str = None, engine=None) -> tuple:
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

    print(f"Extracting statcast data from {start_date} to {end_date}...")
    df = extract_statcast(start_date, end_date)
    print(f"Extracted {len(df)} pitch records")

    # Extract years for sprint speed
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = range(start_year, end_year + 1)

    frames = []
    for year in years:
        df_year = extract_sprint_speed(year)
        if not df_year.empty:
            frames.append(df_year)
        
    sprint_path=None
    if frames:
        df_run = pd.concat(frames, ignore_index=True)
        sprint_path = os.path.join(data_dir, 'sprint_speed.parquet')
        df_run.to_parquet(sprint_path, index=False)

    query_params = {
        "type": "statcast_pitcher",
        "start_date": start_date,
        "end_date": end_date
    }

    file_path = write_and_register_parquet(df, start_date, end_date, query_params, data_dir)
    print(f"Saved parquet to: {file_path}")

    return file_path, sprint_path
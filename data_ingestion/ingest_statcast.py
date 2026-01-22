from pybaseball import statcast_pitcher
from sqlalchemy import create_engine, text
import pandas as pd
import os
import hashlib
import json
import uuid

from utils.utils import build_db_url

engine = create_engine(build_db_url(), pool_pre_ping=False)

def get_pitcher_ids() -> []:
    query = text("SELECT DISTINCT pitcher_id FROM raw.pitching_boxscores")

    with engine.begin() as conn:
        result = conn.execute(query)
        pitcher_ids = [row[0] for row in result]

    return pitcher_ids

def extract_statcast(pitcher_ids) -> pd.DataFrame:
    frames = []

    for pitcher_id in pitcher_ids:
        df = statcast_pitcher('2025-03-18', '2025-11-01', pitcher_id)
        if df is not None:
            frames.append(df)

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
    file_name = f"statcast_pitching_lad_{start_date}_{end_date}_{run_id}.parquet"
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

def main():
    START_DATE = "2025-03-18"
    END_DATE = "2025-11-01"

    pitcher_ids = get_pitcher_ids()

    query_params = {
        "type": "statcast_pitcher",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "pitcher_ids": pitcher_ids
    }

    df = extract_statcast(pitcher_ids)
    file_path = write_and_register_parquet(
        df,
        START_DATE,
        END_DATE,
        query_params
    )

    print(file_path)

if __name__ == "__main__":
    main()
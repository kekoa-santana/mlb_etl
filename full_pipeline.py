from ingestion.ingest_boxscores import fetch_and_load_boxscores
from ingestion.ingest_team_dim import fetch_team_dim
from ingestion.ingest_statcast import extract_and_save_statcast
from ingestion.ingest_dim_player import extract_and_save_dim_player

from transformation.staging.load_table import load_table
from utils.sql_runner import run_sql_registry
from transformation.production.sql_registry import SQL_REGISTRY

from utils.utils import build_db_url

from sqlalchemy import create_engine
import pandas as pd

import argparse
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

START_DATE = "2025-03-18"
END_DATE = "2025-11-01"

engine = create_engine(build_db_url())

DIM_PLAYER_PARQUET = 'data/dim_player.parquet'
# default data dir is 'data'

def ingestion(start_date: str, end_date: str, data_dir: str) -> str:
    with engine.begin() as conn:
        team_ids = fetch_team_dim()

        fetch_and_load_boxscores(start_date, end_date, team_ids = None, engine=engine)
        return extract_and_save_statcast(start_date, end_date, data_dir=data_dir, engine=engine)
        

def load_staging(parquet: str):
    load_table('statcast_pitches', parquet)
    load_table('statcast_at_bats', parquet)
    load_table('statcast_batted_balls', parquet)

def load_production(parquet: str): 
    extract_and_save_dim_player(parquet)
    load_table('dim_game')
    load_table('dim_player', parquet)
    run_sql_registry(SQL_REGISTRY)


def main():
    parser = argparse.ArgumentParser(description='MLB ETL Pipeline')

    # Date arguments - replace default START_DATE/END_DATE
    parser.add_argument('--start-date', default=START_DATE)
    parser.add_argument('--end-date', default=END_DATE)

    # Phase control - choose which functions to run
    parser.add_argument('--skip-ingestion', action='store_true',
                        help='Skip ingestion(), when parquet already up to date')
    parser.add_argument('--skip-staging', action='store_true',
                        help='skip staging if you already have staging tables in postgresql db')
    parser.add_argument('--skip-production', action='store_true',
                        help='skip load_production() and production SQL')
    
    # Path ovverides
    parser.add_argument('--parquet', type=str,
                        help='existing parquet if skipping ingestion')
    parser.add_argument('--data-dir', default='data')

    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    if args.skip_ingestion:
        if not args.parquet and not args.skip_staging:
            parser.error("--skip-ingestion requires --parquet to specify existing file")
    else:    
        parquet = ingestion(args.start_date, args.end_date, args.data_dir)

    if not args.skip_staging:
        load_staging(parquet)

    if not args.skip_production:
        load_production(DIM_PLAYER_PARQUET)

if __name__ == "__main__":
    main()
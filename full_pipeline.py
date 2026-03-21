from ingestion.ingest_boxscores import fetch_and_load_boxscores
from ingestion.ingest_team_dim import fetch_team_dim
from ingestion.ingest_statcast import extract_and_save_statcast
from ingestion.ingest_dim_player import extract_and_save_dim_player
from ingestion.ingest_transactions import fetch_and_load_transactions
from ingestion.ingest_milb import fetch_and_load_milb
from ingestion.ingest_prospects import fetch_and_load_prospects
from ingestion.ingest_prospect_rankings import fetch_and_load_prospect_rankings
from ingestion.ingest_oaa import fetch_and_load_oaa
from ingestion.ingest_schedule import fetch_and_load_schedule
from ingestion.ingest_rosters import fetch_and_load_rosters

from transformation.staging.load_table import load_table
from utils.sql_runner import run_sql_registry
from transformation.production.sql_registry import SQL_REGISTRY
from transformation.production.load_roster import build_roster

from utils.utils import build_db_url

from sqlalchemy import create_engine
import pandas as pd

import argparse
import logging
from datetime import date
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

START_DATE = "2025-03-18"
END_DATE = "2025-11-01"

engine = create_engine(build_db_url())

DIM_PLAYER_PARQUET = 'data/dim_player.parquet'
# default data dir is 'data'

def ingestion(start_date: str, end_date: str, data_dir: str, skip_statcast: bool = False):
    with engine.begin() as conn:
        team_ids = fetch_team_dim()

        fetch_and_load_boxscores(start_date, end_date)

        # Backfill transactions from Oct 1 of prior season to cover the
        # full offseason (trades, signings, IL, etc.) — not just the
        # pipeline date range.
        season = int(start_date[:4])
        txn_backfill_start = f"{season - 1}-10-01"
        fetch_and_load_transactions(txn_backfill_start, end_date)

        fetch_and_load_milb(start_date, end_date)
        fetch_and_load_prospects(season)
        fetch_and_load_prospect_rankings()
        fetch_and_load_oaa(start_season=season, end_season=season)
        fetch_and_load_schedule(start_date, end_date)

        # Fetch current 40-man + spring training rosters from API
        fetch_and_load_rosters(season=season, engine=engine)

        if skip_statcast:
            return None, None
        return extract_and_save_statcast(start_date, end_date, data_dir=data_dir, engine=engine)
        

def load_staging(parquet: str, sprint_parquet: str = None):
    load_table('statcast_pitches', parquet)
    load_table('statcast_at_bats', parquet)
    load_table('statcast_batted_balls', parquet)
    if sprint_parquet:
        load_table('statcast_sprint_speed', sprint_parquet)

def load_production(parquet: str, season: int = None):
    extract_and_save_dim_player(parquet)
    load_table('dim_player', parquet)
    run_sql_registry(SQL_REGISTRY)
    # Roster rebuild runs last — needs all upstream tables fresh
    # Always build from prior season (has full game data) + overlay current transactions
    roster_season = (season if season else int(START_DATE[:4])) - 1
    build_roster(season=roster_season, engine=engine)


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

    # Standalone modes
    parser.add_argument('--transactions-only', action='store_true',
                        help='Only fetch transactions and load to production')

    parser.add_argument('--spring', action='store_true',
                        help='Spring training mode: ingest boxscores only, skip statcast/staging/production')

    parser.add_argument('--milb-only', action='store_true',
                        help='Only fetch MiLB game logs and prospect rosters')

    parser.add_argument('--live', action='store_true',
                        help='Poll live games and write to live staging tables')
    parser.add_argument('--live-date', default=str(date.today()),
                        help='Date for live polling (default: today)')
    parser.add_argument('--poll-interval', type=int, default=600,
                        help='Seconds between polls (default 600 = 10 min)')

    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    if args.live:
        from ingestion.ingest_live_feed import poll_and_load
        poll_and_load(args.live_date, engine, args.poll_interval)
        return

    if args.transactions_only:
        fetch_and_load_transactions(args.start_date, args.end_date)
        from utils.sql_runner import run_sql_file
        run_sql_file('transformation/production/load_transactions.sql')
        return

    if args.spring:
        ingestion(args.start_date, args.end_date, args.data_dir, skip_statcast=True)
        return

    if args.milb_only:
        season = int(args.start_date[:4])
        fetch_and_load_milb(args.start_date, args.end_date)
        fetch_and_load_prospects(season)
        return

    if args.skip_ingestion:
        if not args.parquet and not args.skip_staging:
            parser.error("--skip-ingestion requires --parquet to specify existing file")
    else:
        statcast_parquet, sprint_parquet = ingestion(args.start_date, args.end_date, args.data_dir)

    if not args.skip_staging:
        load_staging(statcast_parquet, sprint_parquet)

    if not args.skip_production:
        season = int(args.start_date[:4])
        load_production(DIM_PLAYER_PARQUET, season=season)

if __name__ == "__main__":
    main()
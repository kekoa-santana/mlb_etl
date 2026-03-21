"""One-time backfill of delta_run_exp, woba_denom, woba_value into staging
and production from raw parquet files.

Usage:
    python scripts/backfill_run_values.py
"""
import glob
import logging

import pandas as pd
from sqlalchemy import create_engine, text

from utils.utils import build_db_url

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

COLS_TO_BACKFILL = ['game_pk', 'at_bat_number', 'pitch_number',
                    'delta_run_exp', 'woba_denom', 'woba_value']


def backfill_staging():
    """Read parquets and UPDATE staging.statcast_pitches with run value columns."""
    parquets = sorted(glob.glob('data/statcast_pitching_*.parquet'))
    logger.info(f"Found {len(parquets)} parquet files")

    for pq in parquets:
        try:
            df = pd.read_parquet(pq, columns=COLS_TO_BACKFILL)
        except Exception as e:
            logger.warning(f"Skipping unreadable parquet {pq}: {e}")
            continue
        if df.empty:
            logger.info(f"Skipping empty: {pq}")
            continue

        df = df.rename(columns={'at_bat_number': 'game_counter'})

        # Only rows with at least one non-null value to update
        mask = df['delta_run_exp'].notna() | df['woba_denom'].notna() | df['woba_value'].notna()
        df = df[mask].copy()

        if df.empty:
            logger.info(f"No run value data in: {pq}")
            continue

        # Replace NaN with None for SQL
        df = df.where(df.notna(), None)

        # Batch UPDATE via temp table
        with engine.begin() as conn:
            # Write to temp table
            df.to_sql('_tmp_rv_backfill', conn, if_exists='replace', index=False,
                       method='multi', chunksize=5000)

            result = conn.execute(text("""
                UPDATE staging.statcast_pitches sp
                SET delta_run_exp = t.delta_run_exp,
                    woba_denom = t.woba_denom::smallint,
                    woba_value = t.woba_value
                FROM _tmp_rv_backfill t
                WHERE sp.game_pk = t.game_pk
                  AND sp.game_counter = t.game_counter
                  AND sp.pitch_number = t.pitch_number
            """))
            conn.execute(text("DROP TABLE IF EXISTS _tmp_rv_backfill"))

        logger.info(f"Updated staging from {pq}: {result.rowcount} rows")


def backfill_production():
    """Copy delta_run_exp from staging.statcast_pitches to production.fact_pitch."""
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE production.fact_pitch fp
            SET delta_run_exp = sp.delta_run_exp
            FROM staging.statcast_pitches sp
            WHERE fp.game_pk = sp.game_pk
              AND fp.game_counter = sp.game_counter
              AND fp.pitch_number = sp.pitch_number
              AND sp.delta_run_exp IS NOT NULL
              AND fp.delta_run_exp IS NULL
        """))
    logger.info(f"Updated production.fact_pitch: {result.rowcount} rows")


if __name__ == '__main__':
    backfill_staging()
    backfill_production()
    logger.info("Backfill complete")

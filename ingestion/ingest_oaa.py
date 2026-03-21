"""Ingest Outs Above Average (OAA) from Baseball Savant leaderboard.

Fetches the OAA fielding leaderboard CSV for each season and upserts
into production.fact_fielding_oaa.
"""
import logging
import io

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=30)
engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

OAA_URL = (
    "https://baseballsavant.mlb.com/leaderboard/outs_above_average"
    "?type=Fielder&year={season}&position=&team=&min=1&csv=true"
)

COLUMN_MAP = {
    'player_id': 'player_id',
    'year': 'season',
    'primary_pos_formatted': 'position',
    'display_team_name': 'team_name',
    'outs_above_average': 'outs_above_average',
    'fielding_runs_prevented': 'fielding_runs_prevented',
    'outs_above_average_infront': 'oaa_infront',
    'outs_above_average_lateral_toward3bline': 'oaa_lateral_3b',
    'outs_above_average_lateral_toward1bline': 'oaa_lateral_1b',
    'outs_above_average_behind': 'oaa_behind',
    'outs_above_average_rhh': 'oaa_vs_rhh',
    'outs_above_average_lhh': 'oaa_vs_lhh',
    'actual_success_rate_formatted': 'actual_success_rate',
    'adj_estimated_success_rate_formatted': 'expected_success_rate',
    'diff_success_rate_formatted': 'success_rate_diff',
}

INT_COLS = [
    'outs_above_average', 'fielding_runs_prevented',
    'oaa_infront', 'oaa_lateral_3b', 'oaa_lateral_1b', 'oaa_behind',
    'oaa_vs_rhh', 'oaa_vs_lhh',
]

PCT_COLS = ['actual_success_rate', 'expected_success_rate', 'success_rate_diff']


def _parse_pct(val):
    """Convert '73%' or '-3%' to 0.73 / -0.03."""
    if pd.isna(val):
        return None
    s = str(val).strip().replace('%', '')
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def _fetch_oaa(season: int) -> pd.DataFrame:
    """Fetch OAA leaderboard CSV for a single season."""
    url = OAA_URL.format(season=season)
    resp = session.get(url, timeout=session.timeout)
    resp.raise_for_status()

    content = resp.text
    if not content.strip():
        logger.warning(f"Empty response for OAA {season}")
        return pd.DataFrame()

    df = pd.read_csv(io.StringIO(content), encoding='utf-8-sig')
    if df.empty:
        logger.warning(f"No OAA data for {season}")
        return pd.DataFrame()

    # Build player_name from the combined first column
    name_col = df.columns[0]  # "last_name, first_name" (combined with BOM)
    df['player_name'] = df[name_col].str.strip().str.strip('"')
    df = df.drop(columns=[name_col])

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Coerce types
    df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce')
    df = df[df['player_id'].notna()].copy()
    df['player_id'] = df['player_id'].astype(int)
    df['season'] = season

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    for col in PCT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_parse_pct)

    df['position'] = df['position'].astype(str).str.strip().str.strip('"')

    # Keep only mapped columns
    keep = [c for c in COLUMN_MAP.values() if c in df.columns] + ['player_name']
    keep = list(dict.fromkeys(keep))  # dedup preserving order
    df = df[[c for c in keep if c in df.columns]]

    df = df.drop_duplicates(subset=['player_id', 'season', 'position'], keep='first')
    logger.info(f"Fetched {len(df)} OAA rows for {season}")
    return df


def _load_oaa(df: pd.DataFrame):
    """Upsert OAA data to production.fact_fielding_oaa."""
    if df.empty:
        return

    with engine.begin() as conn:
        table = Table(
            'fact_fielding_oaa', MetaData(),
            autoload_with=conn, schema='production'
        )
        pk_cols = {'player_id', 'season', 'position'}
        for chunk_start in range(0, len(df), 50):
            chunk = df.iloc[chunk_start:chunk_start + 50]
            records = chunk.to_dict(orient='records')
            for record in records:
                for k, v in record.items():
                    if pd.isna(v):
                        record[k] = None
            stmt = pg_insert(table).values(records)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in pk_cols
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=list(pk_cols),
                set_=update_cols,
            )
            conn.execute(stmt)

    logger.info(f"Loaded {len(df)} rows to production.fact_fielding_oaa")


def fetch_and_load_oaa(
    start_season: int = 2018,
    end_season: int = 2025,
):
    """Main entry point: fetch OAA for a range of seasons and load."""
    all_dfs = []
    for season in range(start_season, end_season + 1):
        try:
            df = _fetch_oaa(season)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to fetch OAA for {season}: {e}")

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        _load_oaa(combined)
        logger.info(
            f"Finished OAA: {len(combined)} total rows "
            f"across {len(all_dfs)} seasons"
        )
    else:
        logger.warning("No OAA data fetched")

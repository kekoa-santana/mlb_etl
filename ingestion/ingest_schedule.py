"""Ingest MLB schedule from the Stats API.

Fetches the full season schedule including future unplayed games.
Includes probable pitchers when available.
Upserts to production.dim_schedule.
"""
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=15)
engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
    "?sportId=1&startDate={start}&endDate={end}"
    "&hydrate=team,probablePitcher,venue"
    "&gameType=R,F,D,L,W"
)


def _fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    """Fetch schedule from MLB API, chunking by 30-day windows."""
    all_games = []
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while start <= end:
        chunk_end = min(start + timedelta(days=29), end)
        url = SCHEDULE_URL.format(
            start=start.strftime('%Y-%m-%d'),
            end=chunk_end.strftime('%Y-%m-%d')
        )
        resp = session.get(url, timeout=session.timeout)
        resp.raise_for_status()
        data = resp.json()

        for date_obj in data.get('dates', []):
            for game in date_obj.get('games', []):
                teams = game.get('teams', {})
                away = teams.get('away', {})
                home = teams.get('home', {})
                away_team = away.get('team', {})
                home_team = home.get('team', {})
                venue = game.get('venue', {})
                status = game.get('status', {})

                # Probable pitchers
                away_pp = (away.get('probablePitcher') or {}).get('id')
                home_pp = (home.get('probablePitcher') or {}).get('id')

                all_games.append({
                    'game_pk': game['gamePk'],
                    'game_date': date_obj['date'],
                    'season': game.get('seasonDisplay') or game.get('season'),
                    'game_type': game.get('gameType'),
                    'status': status.get('detailedState', status.get('abstractGameState')),
                    'away_team_id': away_team.get('id'),
                    'home_team_id': home_team.get('id'),
                    'away_team_name': away_team.get('name'),
                    'home_team_name': home_team.get('name'),
                    'venue_id': venue.get('id'),
                    'venue_name': venue.get('name'),
                    'day_night': game.get('dayNight'),
                    'series_description': game.get('seriesDescription'),
                    'away_probable_pitcher_id': away_pp,
                    'home_probable_pitcher_id': home_pp,
                })

        start = chunk_end + timedelta(days=1)

    logger.info(f"Fetched {len(all_games)} games from schedule API")
    return all_games


def _load_schedule(df: pd.DataFrame):
    """Upsert schedule to production.dim_schedule."""
    if df.empty:
        return

    with engine.begin() as conn:
        table = Table(
            'dim_schedule', MetaData(),
            autoload_with=conn, schema='production'
        )
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
                if c.name not in ('game_pk',)
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_pk'],
                set_=update_cols,
            )
            conn.execute(stmt)

    logger.info(f"Loaded {len(df)} rows to production.dim_schedule")


def fetch_and_load_schedule(start_date: str, end_date: str):
    """Main entry point: fetch full schedule and load."""
    games = _fetch_schedule(start_date, end_date)
    if not games:
        logger.warning("No games found in schedule")
        return

    df = pd.DataFrame(games)
    df['season'] = pd.to_numeric(df['season'], errors='coerce').astype('Int64')
    df['away_team_id'] = pd.to_numeric(df['away_team_id'], errors='coerce').astype('Int64')
    df['home_team_id'] = pd.to_numeric(df['home_team_id'], errors='coerce').astype('Int64')
    df['venue_id'] = pd.to_numeric(df['venue_id'], errors='coerce').astype('Int64')
    df['away_probable_pitcher_id'] = pd.to_numeric(
        df['away_probable_pitcher_id'], errors='coerce').astype('Int64')
    df['home_probable_pitcher_id'] = pd.to_numeric(
        df['home_probable_pitcher_id'], errors='coerce').astype('Int64')

    df = df.drop_duplicates(subset=['game_pk'], keep='last')
    _load_schedule(df)

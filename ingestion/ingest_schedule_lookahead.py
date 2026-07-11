"""Fetch upcoming MLB schedule with probable pitchers.

Stores the next N days of schedule in raw.schedule_lookahead so the
dashboard has a fallback if the hourly schedule fetch fails.  Probable
pitchers are populated 1-5 days out by MLB and update as announcements
come in — each run upserts, so pitchers fill in progressively.

Usage (standalone):
    python -m ingestion.ingest_schedule_lookahead                     # next 7 days
    python -m ingestion.ingest_schedule_lookahead --days 10
    python -m ingestion.ingest_schedule_lookahead --start-date 2026-04-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow running as script: put repo root on sys.path so utils.* resolves
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import (
    BigInteger, Column, Date, Integer, MetaData, SmallInteger,
    String, Table, Text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=15)

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

_DDL = """\
CREATE TABLE IF NOT EXISTS raw.schedule_lookahead (
    game_pk                     BIGINT PRIMARY KEY,
    game_date                   DATE NOT NULL,
    game_datetime               TEXT,
    game_type                   VARCHAR(2),
    season                      SMALLINT,
    status                      VARCHAR(30),
    day_night                   VARCHAR(10),
    doubleheader                VARCHAR(2),
    games_in_series             SMALLINT,
    series_game_number          SMALLINT,
    home_team_id                INTEGER NOT NULL,
    home_team_name              TEXT,
    away_team_id                INTEGER NOT NULL,
    away_team_name              TEXT,
    venue_id                    INTEGER,
    venue_name                  TEXT,
    home_probable_pitcher_id    BIGINT,
    home_probable_pitcher_name  TEXT,
    away_probable_pitcher_id    BIGINT,
    away_probable_pitcher_name  TEXT,
    home_wins                   SMALLINT,
    home_losses                 SMALLINT,
    away_wins                   SMALLINT,
    away_losses                 SMALLINT,
    fetched_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_lookahead_date
    ON raw.schedule_lookahead (game_date);
"""


def _ensure_table(conn) -> None:
    conn.execute(text(_DDL))


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_schedule(start_date: str, end_date: str) -> list[dict]:
    """Fetch schedule with probable pitchers for a date range."""
    resp = session.get(
        SCHEDULE_URL,
        params={
            "sportId": 1,
            "startDate": start_date,
            "endDate": end_date,
            "hydrate": "probablePitcher,team,venue",
        },
        timeout=session.timeout,
    )
    resp.raise_for_status()

    rows = []
    for day in resp.json().get("dates", []):
        for game in day.get("games", []):
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = home.get("team") or {}
            away_team = away.get("team") or {}
            home_record = home.get("leagueRecord") or {}
            away_record = away.get("leagueRecord") or {}
            venue = game.get("venue") or {}
            status = game.get("status") or {}

            home_pitcher = home.get("probablePitcher") or {}
            away_pitcher = away.get("probablePitcher") or {}

            rows.append({
                "game_pk": game.get("gamePk"),
                "game_date": day.get("date"),
                "game_datetime": game.get("gameDate"),
                "game_type": game.get("gameType"),
                "season": game.get("season"),
                "status": status.get("detailedState"),
                "day_night": game.get("dayNight"),
                "doubleheader": game.get("doubleHeader"),
                "games_in_series": game.get("gamesInSeries"),
                "series_game_number": game.get("seriesGameNumber"),
                "home_team_id": home_team.get("id"),
                "home_team_name": home_team.get("name"),
                "away_team_id": away_team.get("id"),
                "away_team_name": away_team.get("name"),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("name"),
                "home_probable_pitcher_id": home_pitcher.get("id"),
                "home_probable_pitcher_name": home_pitcher.get("fullName"),
                "away_probable_pitcher_id": away_pitcher.get("id"),
                "away_probable_pitcher_name": away_pitcher.get("fullName"),
                "home_wins": home_record.get("wins"),
                "home_losses": home_record.get("losses"),
                "away_wins": away_record.get("wins"),
                "away_losses": away_record.get("losses"),
            })

    return rows


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def _load_raw(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    int_cols = [
        "game_pk", "home_team_id", "away_team_id", "venue_id",
        "home_probable_pitcher_id", "away_probable_pitcher_id",
        "season", "games_in_series", "series_game_number",
        "home_wins", "home_losses", "away_wins", "away_losses",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.where(df.notna(), None)

    table = Table("schedule_lookahead", MetaData(), autoload_with=conn, schema="raw")
    records = df.to_dict(orient="records")
    for rec in records:
        for k, v in rec.items():
            if pd.isna(v):
                rec[k] = None

    stmt = pg_insert(table).values(records)
    # On conflict: update everything EXCEPT game_pk.
    # Crucially, probable pitchers get updated as they become available.
    update_cols = {
        col.name: stmt.excluded[col.name]
        for col in table.columns
        if col.name not in ("game_pk",)
    }
    update_cols["fetched_at"] = text("NOW()")
    stmt = stmt.on_conflict_do_update(
        index_elements=["game_pk"],
        set_=update_cols,
    )
    conn.execute(stmt)
    return len(records)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_and_load_schedule_lookahead(
    start_date: str | None = None,
    days: int = 7,
    engine=None,
) -> int:
    """Fetch upcoming schedule and upsert to raw.schedule_lookahead.

    Args:
        start_date: First date (YYYY-MM-DD). Defaults to today.
        days: Number of days to look ahead.
        engine: SQLAlchemy engine.

    Returns:
        Number of games stored.
    """
    if start_date is None:
        start_date = date.today().isoformat()

    start = date.fromisoformat(start_date)
    end_date = (start + timedelta(days=days)).isoformat()

    if engine is None:
        engine = create_engine(build_db_url())

    logger.info("Fetching schedule %s to %s (%d days)", start_date, end_date, days)
    rows = fetch_schedule(start_date, end_date)
    logger.info("Fetched %d games", len(rows))

    if not rows:
        return 0

    df = pd.DataFrame(rows)
    # Filter to regular season + postseason only (drop spring training / exhibitions)
    if "game_type" in df.columns:
        df = df[df["game_type"].isin(["R", "F", "D", "L", "W", "S"])].copy()

    with engine.begin() as conn:
        _ensure_table(conn)
        count = _load_raw(conn, df)

    pitchers_known = df["home_probable_pitcher_id"].notna().sum() + \
                     df["away_probable_pitcher_id"].notna().sum()
    total_slots = len(df) * 2
    logger.info(
        "Loaded %d games to raw.schedule_lookahead "
        "(%d/%d probable pitcher slots filled)",
        count, pitchers_known, total_slots,
    )
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Fetch upcoming MLB schedule")
    parser.add_argument("--start-date", default=date.today().isoformat())
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    fetch_and_load_schedule_lookahead(start_date=args.start_date, days=args.days)

"""Fetch current MLB 40-man and spring training rosters from the Stats API.

Writes to raw.active_rosters — used by the roster builder as the
authoritative source for current org assignments.  Catches trades,
signings, spring-training NRIs, Rule 5, and anything else that
transaction parsing might miss.

Usage (standalone):
    python -m ingestion.ingest_rosters              # current season
    python -m ingestion.ingest_rosters --season 2025
"""
from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import (
    BigInteger, Column, Date, Integer, MetaData, String, Table, Text,
    create_engine, text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP, insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=15)

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

# Roster types to pull — 40Man includes IL, active is the 26-man,
# and nonRosterInvitees captures spring training invites.
ROSTER_TYPES = ["40Man", "nonRosterInvitees"]

RAW_TABLE = "active_rosters"
RAW_SCHEMA = "raw"


# ---------------------------------------------------------------------------
# DDL — created once, then reused
# ---------------------------------------------------------------------------

_DDL = """\
CREATE TABLE IF NOT EXISTS raw.active_rosters (
    player_id       BIGINT NOT NULL,
    player_name     TEXT,
    team_id         INTEGER NOT NULL,
    team_name       TEXT,
    org_id          INTEGER,
    roster_type     VARCHAR(30),
    status_code     VARCHAR(10),
    status_desc     TEXT,
    position        VARCHAR(4),
    jersey_number   VARCHAR(4),
    season          SMALLINT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (player_id, roster_type, season)
);
"""


def _ensure_table(conn) -> None:
    conn.execute(text(_DDL))


# ---------------------------------------------------------------------------
# Fetch from API
# ---------------------------------------------------------------------------

def _fetch_mlb_teams(season: int) -> list[dict]:
    """Get all 30 MLB teams (AL=103, NL=104)."""
    resp = session.get(
        TEAMS_URL,
        params={"sportId": 1, "season": season},
        timeout=session.timeout,
    )
    resp.raise_for_status()
    teams = []
    for t in resp.json().get("teams", []):
        league = t.get("league") or {}
        if league.get("id") in (103, 104):
            teams.append({
                "team_id": t["id"],
                "team_name": t.get("teamName"),
                "full_name": t.get("name"),
            })
    return teams


def _fetch_roster(team_id: int, season: int, roster_type: str) -> list[dict]:
    """Fetch a single roster type for one team."""
    url = ROSTER_URL.format(team_id=team_id)
    try:
        resp = session.get(
            url,
            params={
                "rosterType": roster_type,
                "season": season,
                "hydrate": "person(currentTeam)",
            },
            timeout=session.timeout,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed roster fetch team=%d type=%s: %s",
                       team_id, roster_type, exc)
        return []

    rows = []
    for entry in resp.json().get("roster", []):
        person = entry.get("person") or {}
        pos = entry.get("position") or {}
        status = entry.get("status") or {}
        current_team = person.get("currentTeam") or {}

        rows.append({
            "player_id": person.get("id"),
            "player_name": person.get("fullName"),
            "team_id": team_id,
            "team_name": current_team.get("name"),
            "org_id": current_team.get("parentOrgId", team_id),
            "roster_type": roster_type,
            "status_code": status.get("code"),
            "status_desc": status.get("description"),
            "position": pos.get("abbreviation"),
            "jersey_number": entry.get("jerseyNumber"),
            "season": season,
        })
    return rows


# ---------------------------------------------------------------------------
# Load to raw
# ---------------------------------------------------------------------------

def _load_raw(conn, df: pd.DataFrame) -> int:
    """Upsert roster rows into raw.active_rosters."""
    if df.empty:
        return 0

    for col in ["player_id", "team_id", "org_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.where(df.notna(), None)

    table = Table(RAW_TABLE, MetaData(), autoload_with=conn, schema=RAW_SCHEMA)
    total = 0
    for start in range(0, len(df), 50):
        chunk = df.iloc[start : start + 50]
        records = chunk.to_dict(orient="records")
        for rec in records:
            for k, v in rec.items():
                if pd.isna(v):
                    rec[k] = None
        stmt = pg_insert(table).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["player_id", "roster_type", "season"],
            set_={
                "player_name": stmt.excluded.player_name,
                "team_id": stmt.excluded.team_id,
                "team_name": stmt.excluded.team_name,
                "org_id": stmt.excluded.org_id,
                "status_code": stmt.excluded.status_code,
                "status_desc": stmt.excluded.status_desc,
                "position": stmt.excluded.position,
                "jersey_number": stmt.excluded.jersey_number,
                "fetched_at": text("NOW()"),
            },
        )
        conn.execute(stmt)
        total += len(records)
    return total


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_and_load_rosters(season: int = None, engine=None) -> int:
    """Fetch 40-man + NRI rosters for all 30 MLB teams and upsert to raw.

    Args:
        season: MLB season year.  Defaults to current year.
        engine: SQLAlchemy engine.

    Returns:
        Total rows upserted.
    """
    if season is None:
        from datetime import date
        season = date.today().year

    if engine is None:
        engine = create_engine(build_db_url())

    teams = _fetch_mlb_teams(season)
    logger.info("Fetching rosters for %d MLB teams (season %d)", len(teams), season)

    all_rows: list[dict] = []
    for i, team in enumerate(teams):
        for rtype in ROSTER_TYPES:
            rows = _fetch_roster(team["team_id"], season, rtype)
            all_rows.extend(rows)
        if (i + 1) % 10 == 0:
            logger.info("  Fetched %d/%d teams (%d players so far)",
                        i + 1, len(teams), len(all_rows))

    logger.info("Fetched %d total roster entries", len(all_rows))

    if not all_rows:
        return 0

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(["player_id", "roster_type", "season"], keep="last")

    with engine.begin() as conn:
        _ensure_table(conn)
        total = _load_raw(conn, df)

    logger.info("Loaded %d roster entries to raw.active_rosters", total)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import date

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Fetch MLB rosters")
    parser.add_argument("--season", type=int, default=date.today().year)
    args = parser.parse_args()
    fetch_and_load_rosters(season=args.season)

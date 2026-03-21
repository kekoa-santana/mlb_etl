"""Build production.dim_roster from production tables + API rosters.

Source-of-truth roster for every player in each MLB organization
(MLB + MiLB levels), with primary/secondary positions derived from
career game data and current org assignments from the MLB API.

Build order:
  1. Base roster from prior-season game appearances (MLB + MiLB)
  2. Positions from career lineup/game data
  3. Pitcher SP/RP classification
  4. Status from transaction-derived timeline
  5. Post-season transaction overlay
  6. API roster overlay (authoritative for current org/team/status)
     — catches trades, signings, spring training NRIs, Rule 5, etc.
  7. New players from API who have no game history yet

Called by full_pipeline.py after the SQL registry finishes so that
all upstream tables contain the freshest data.
"""
from __future__ import annotations

import logging
import re

import pandas as pd
from sqlalchemy import create_engine, text

from utils.utils import build_db_url

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL (used by scripts/build_roster.py for initial creation)
# ---------------------------------------------------------------------------

DDL = """\
CREATE TABLE IF NOT EXISTS production.dim_roster (
    player_id           BIGINT PRIMARY KEY,
    player_name         TEXT NOT NULL,
    org_id              INTEGER NOT NULL,
    roster_status       VARCHAR(20) NOT NULL DEFAULT 'active',
    level               VARCHAR(5),
    primary_position    VARCHAR(4) NOT NULL,
    secondary_positions TEXT[] DEFAULT '{}',
    is_starter          BOOLEAN DEFAULT FALSE,
    team_id             INTEGER,
    team_name           TEXT,
    last_game_date      DATE,
    status_date         DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_roster_org
    ON production.dim_roster (org_id);
CREATE INDEX IF NOT EXISTS idx_roster_status_org
    ON production.dim_roster (roster_status, org_id);
CREATE INDEX IF NOT EXISTS idx_roster_level_org
    ON production.dim_roster (level, org_id);
"""

STATUS_MAP = {
    "active": "active",
    "option_minors": "minors",
    "release": "released",
    "IL-7": "il_7",
    "IL-10": "il_10",
    "IL-15": "il_15",
    "IL-60": "il_60",
    "designate": "restricted",
    "trade": "active",
}

ORG_CHANGE_TYPES = {"TR", "SFA", "SGN", "CLW", "SE", "PUR"}

# MLB Stats API roster status codes → our roster_status values
API_STATUS_MAP = {
    "A": "active",       # Active roster (26-man)
    "RM": "minors",      # Reassigned to minors
    "NRI": "nri",        # Non-roster invitee (spring training)
    "D7": "il_7",
    "D10": "il_10",
    "D15": "il_15",
    "D60": "il_60",
    "SU": "active",      # Bereavement/family emergency
    "PL": "active",      # Paternity list
    "RST": "restricted", # Restricted list
    "DFA": "restricted", # Designated for assignment
    "MIN": "minors",     # Minor league assignment
}

FINAL_COLS = [
    "player_id", "player_name", "org_id", "roster_status", "level",
    "primary_position", "secondary_positions", "is_starter",
    "team_id", "team_name", "last_game_date", "status_date",
]


def _normalize_il_type(il_type: str | None) -> str:
    if not il_type:
        return "il_10"
    m = re.search(r"(\d+)", il_type)
    return f"il_{m.group(1)}" if m else "il_10"


def _read_sql(conn, sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), conn, params=params)


# ---------------------------------------------------------------------------
# Data fetching — all take an open connection
# ---------------------------------------------------------------------------

def _fetch_mlb_appearances(conn, season: int) -> pd.DataFrame:
    return _read_sql(conn, """
        SELECT DISTINCT ON (player_id)
            player_id, team_id, game_date AS last_game_date, player_role
        FROM production.fact_player_game_mlb
        WHERE season = :season
        ORDER BY player_id, game_date DESC
    """, {"season": season})


def _fetch_milb_appearances(conn, season: int) -> pd.DataFrame:
    return _read_sql(conn, """
        SELECT DISTINCT ON (player_id)
            player_id, team_id, team_name,
            parent_org_id AS org_id, level,
            game_date AS last_game_date, player_role
        FROM production.fact_milb_player_game
        WHERE season = :season
        ORDER BY player_id, game_date DESC
    """, {"season": season})


def _fetch_position_counts(conn) -> pd.DataFrame:
    mlb = _read_sql(conn, """
        SELECT player_id, position, COUNT(*) AS games
        FROM production.fact_lineup
        WHERE position NOT IN ('PH', 'PR')
        GROUP BY player_id, position
    """)
    milb = _read_sql(conn, """
        SELECT batter_id AS player_id, position, COUNT(*) AS games
        FROM staging.milb_batting_game_logs
        WHERE position NOT IN ('PH', 'PR')
        GROUP BY batter_id, position
    """)
    combined = pd.concat([mlb, milb], ignore_index=True)
    if combined.empty:
        return combined
    return combined.groupby(["player_id", "position"], as_index=False)["games"].sum()


def _fetch_pitcher_classification(conn, season: int) -> pd.DataFrame:
    mlb = _read_sql(conn, """
        SELECT player_id,
               SUM(CASE WHEN pit_is_starter THEN 1 ELSE 0 END) AS starts,
               COUNT(*) AS appearances
        FROM production.fact_player_game_mlb
        WHERE player_role = 'pitcher' AND season >= :s
        GROUP BY player_id
    """, {"s": season - 1})
    milb = _read_sql(conn, """
        SELECT player_id,
               SUM(CASE WHEN pit_is_starter THEN 1 ELSE 0 END) AS starts,
               COUNT(*) AS appearances
        FROM production.fact_milb_player_game
        WHERE player_role = 'pitcher' AND season >= :s
        GROUP BY player_id
    """, {"s": season - 1})
    combined = pd.concat([mlb, milb], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=["player_id", "is_starter"])
    agg = combined.groupby("player_id", as_index=False).agg(
        starts=("starts", "sum"), appearances=("appearances", "sum")
    )
    agg["is_starter"] = agg["starts"] / agg["appearances"] > 0.5
    return agg[["player_id", "is_starter"]]


def _fetch_current_statuses(conn) -> pd.DataFrame:
    return _read_sql(conn, """
        SELECT DISTINCT ON (player_id)
            player_id, status_type, status_start_date
        FROM production.fact_player_status_timeline
        ORDER BY player_id, status_start_date DESC
    """)


def _fetch_player_names(conn) -> pd.DataFrame:
    return _read_sql(conn, """
        SELECT player_id, player_name, primary_position AS api_position
        FROM production.dim_player
    """)


def _fetch_milb_player_names(conn) -> pd.DataFrame:
    batters = _read_sql(conn, """
        SELECT DISTINCT ON (batter_id)
            batter_id AS player_id, batter_name AS player_name
        FROM staging.milb_batting_game_logs
        ORDER BY batter_id, game_date DESC
    """)
    pitchers = _read_sql(conn, """
        SELECT DISTINCT ON (pitcher_id)
            pitcher_id AS player_id, pitcher_name AS player_name
        FROM staging.milb_pitching_game_logs
        ORDER BY pitcher_id, game_date DESC
    """)
    combined = pd.concat([batters, pitchers], ignore_index=True)
    return combined.drop_duplicates("player_id", keep="first")


def _fetch_post_season_transactions(conn, season: int) -> pd.DataFrame:
    return _read_sql(conn, """
        SELECT *
        FROM production.dim_transaction
        WHERE effective_date > :cutoff
          AND to_team_id BETWEEN 108 AND 158
        ORDER BY effective_date, transaction_id
    """, {"cutoff": f"{season}-10-01"})


def _fetch_team_names(conn) -> pd.DataFrame:
    return _read_sql(conn, "SELECT team_id, team_name FROM production.dim_team")


def _fetch_api_rosters(conn, season: int) -> pd.DataFrame:
    """Get the latest API roster snapshot from raw.active_rosters.

    Prefers 40Man over nonRosterInvitees when a player appears in both.
    """
    try:
        df = _read_sql(conn, """
            SELECT DISTINCT ON (player_id)
                player_id, player_name, team_id, team_name, org_id,
                roster_type, status_code, status_desc, position
            FROM raw.active_rosters
            WHERE season = :season
            ORDER BY player_id,
                     CASE roster_type
                         WHEN '40Man' THEN 1
                         WHEN 'nonRosterInvitees' THEN 2
                         ELSE 3
                     END,
                     fetched_at DESC
        """, {"season": season})
    except Exception:
        logger.warning("raw.active_rosters not available — skipping API overlay")
        return pd.DataFrame()
    return df


def _fetch_current_season_appearances(conn, season: int) -> pd.DataFrame:
    """Get most recent game per player in the current season (spring training + regular)."""
    return _read_sql(conn, """
        SELECT DISTINCT ON (player_id)
            player_id, team_id, game_date AS last_game_date
        FROM production.fact_player_game_mlb
        WHERE season = :season
        ORDER BY player_id, game_date DESC
    """, {"season": season})


# ---------------------------------------------------------------------------
# API roster overlay
# ---------------------------------------------------------------------------

def _apply_api_roster(
    roster: pd.DataFrame,
    api: pd.DataFrame,
    teams: pd.DataFrame,
) -> pd.DataFrame:
    """Overlay API roster as authority for current org/team/status.

    For players already in the roster: update org_id, team_id, team_name,
    roster_status, and level based on the API.

    For players only in the API (new signings, NRIs, etc.): add them.
    """
    if api.empty:
        return roster

    roster = roster.copy()
    team_map = dict(zip(teams["team_id"], teams["team_name"]))

    # Map API status codes to our statuses
    api = api.copy()
    api["api_status"] = api["status_code"].map(API_STATUS_MAP).fillna("active")

    updated = 0
    new_players: list[dict] = []

    for _, row in api.iterrows():
        pid = row["player_id"]
        mask = roster["player_id"] == pid

        new_org = row["org_id"] if pd.notna(row["org_id"]) else row["team_id"]
        new_team = row["team_id"]
        new_name = team_map.get(new_team, row["team_name"])
        new_status = row["api_status"]
        new_level = "MLB" if new_status not in ("minors",) else None

        if mask.any():
            roster.loc[mask, "org_id"] = new_org
            roster.loc[mask, "team_id"] = new_team
            roster.loc[mask, "team_name"] = new_name
            # Only override status if the API is more authoritative
            # (don't downgrade a specific IL status to generic 'active')
            current_status = roster.loc[mask, "roster_status"].iloc[0]
            if not (current_status.startswith("il_") and new_status == "active"):
                roster.loc[mask, "roster_status"] = new_status
            if new_level:
                roster.loc[mask, "level"] = new_level
            updated += 1
        else:
            pos = row["position"] if pd.notna(row["position"]) else "UTIL"
            new_players.append({
                "player_id": pid,
                "player_name": row["player_name"] or "Unknown",
                "org_id": new_org,
                "roster_status": new_status,
                "level": new_level,
                "primary_position": pos,
                "secondary_positions": [],
                "is_starter": False,
                "team_id": new_team,
                "team_name": new_name,
                "last_game_date": None,
                "status_date": pd.Timestamp.now().date(),
            })

    if new_players:
        new_df = pd.DataFrame(new_players)
        new_df = new_df.drop_duplicates("player_id", keep="last")
        roster = pd.concat([roster, new_df], ignore_index=True)
        logger.info("Added %d new players from API rosters", len(new_df))

    logger.info("Updated %d existing players from API rosters", updated)
    return roster


# ---------------------------------------------------------------------------
# Position determination
# ---------------------------------------------------------------------------

def _determine_positions(
    pos_counts: pd.DataFrame, min_secondary: int = 10,
) -> pd.DataFrame:
    if pos_counts.empty:
        return pd.DataFrame(columns=["player_id", "primary_position", "secondary_positions"])

    fielding = pos_counts[~pos_counts["position"].isin(["DH", "P"])].copy()
    has_fielding = set(fielding["player_id"].unique())
    fallback = pos_counts[~pos_counts["player_id"].isin(has_fielding)]
    working = pd.concat([fielding, fallback], ignore_index=True)

    idx_max = working.groupby("player_id")["games"].idxmax()
    primary = working.loc[idx_max, ["player_id", "position"]].rename(
        columns={"position": "primary_position"},
    )

    merged = pos_counts.merge(primary, on="player_id")
    sec = merged[
        (merged["position"] != merged["primary_position"])
        & (merged["games"] >= min_secondary)
        & (~merged["position"].isin(["PH", "PR"]))
    ]
    sec_agg = (
        sec.groupby("player_id")["position"]
        .apply(list)
        .rename("secondary_positions")
    )

    result = primary.merge(sec_agg, on="player_id", how="left")
    result["secondary_positions"] = result["secondary_positions"].apply(
        lambda x: x if isinstance(x, list) else [],
    )
    return result


# ---------------------------------------------------------------------------
# Transaction overlay
# ---------------------------------------------------------------------------

def _apply_transactions(
    roster: pd.DataFrame,
    txns: pd.DataFrame,
    names_df: pd.DataFrame,
) -> pd.DataFrame:
    roster = roster.copy()
    new_players: list[dict] = []

    for _, txn in txns.iterrows():
        pid = txn["player_id"]
        mask = roster["player_id"] == pid
        eff = txn["effective_date"]

        if txn["type_code"] in ORG_CHANGE_TYPES:
            new_org = int(txn["to_team_id"])
            is_minor = "minor league" in str(txn.get("description", "")).lower()
            new_status = "minors" if is_minor else "active"
            new_level = None if is_minor else "MLB"

            if mask.any():
                roster.loc[mask, "org_id"] = new_org
                roster.loc[mask, "team_id"] = new_org
                roster.loc[mask, "team_name"] = txn["to_team_name"]
                roster.loc[mask, "roster_status"] = new_status
                if new_level:
                    roster.loc[mask, "level"] = new_level
                roster.loc[mask, "status_date"] = eff
            else:
                name_row = names_df[names_df["player_id"] == pid]
                name = (
                    name_row["player_name"].iloc[0]
                    if len(name_row) > 0
                    else txn.get("player_name", "Unknown")
                )
                api_pos = (
                    name_row["api_position"].iloc[0]
                    if len(name_row) > 0 and "api_position" in name_row.columns
                    else "UTIL"
                )
                new_players.append({
                    "player_id": pid,
                    "player_name": name,
                    "org_id": new_org,
                    "roster_status": new_status,
                    "level": new_level,
                    "primary_position": api_pos or "UTIL",
                    "secondary_positions": [],
                    "is_starter": False,
                    "team_id": new_org,
                    "team_name": txn["to_team_name"],
                    "last_game_date": None,
                    "status_date": eff,
                    "player_role": None,
                    "api_position": api_pos,
                    "status_type": None,
                    "status_start_date": None,
                })

        elif txn["type_code"] == "OPT" and mask.any():
            roster.loc[mask, "roster_status"] = "minors"
            roster.loc[mask, "level"] = "AAA"
            roster.loc[mask, "status_date"] = eff

        elif txn["type_code"] == "CU" and mask.any():
            roster.loc[mask, "roster_status"] = "active"
            roster.loc[mask, "level"] = "MLB"
            roster.loc[mask, "status_date"] = eff

        elif txn["type_code"] == "DES" and mask.any():
            roster.loc[mask, "roster_status"] = "restricted"
            roster.loc[mask, "status_date"] = eff

        elif txn["type_code"] == "REL" and mask.any():
            roster.loc[mask, "roster_status"] = "released"
            roster.loc[mask, "status_date"] = eff

        if txn.get("is_il_placement") and mask.any():
            roster.loc[mask, "roster_status"] = _normalize_il_type(txn.get("il_type"))
            roster.loc[mask, "status_date"] = eff
        elif txn.get("is_il_activation") and mask.any():
            roster.loc[mask, "roster_status"] = "active"
            roster.loc[mask, "status_date"] = eff
        elif txn.get("is_il_transfer") and txn.get("il_type") and mask.any():
            roster.loc[mask, "roster_status"] = _normalize_il_type(txn["il_type"])
            roster.loc[mask, "status_date"] = eff

    if new_players:
        new_df = pd.DataFrame(new_players)
        new_df = new_df.drop_duplicates("player_id", keep="last")
        new_df = new_df[~new_df["player_id"].isin(roster["player_id"])]
        roster = pd.concat([roster, new_df], ignore_index=True)
        logger.info("Added %d new players from transactions", len(new_df))

    return roster


# ---------------------------------------------------------------------------
# Write to database
# ---------------------------------------------------------------------------

def _write_roster(conn, df: pd.DataFrame) -> int:
    out = df[FINAL_COLS].copy()

    out["secondary_positions"] = out["secondary_positions"].apply(
        lambda x: "{" + ",".join(x) + "}" if isinstance(x, list) and x else "{}",
    )
    out["player_id"] = out["player_id"].astype("int64")
    out["org_id"] = out["org_id"].astype("int64")
    out["is_starter"] = out["is_starter"].astype(bool)
    out["team_id"] = pd.to_numeric(out["team_id"], errors="coerce").astype("Int64")

    out.to_sql("_tmp_roster", conn, schema="production",
               if_exists="replace", index=False)

    conn.execute(text("TRUNCATE TABLE production.dim_roster"))
    conn.execute(text("""
        INSERT INTO production.dim_roster
            (player_id, player_name, org_id, roster_status, level,
             primary_position, secondary_positions, is_starter,
             team_id, team_name, last_game_date, status_date)
        SELECT
            player_id, player_name, org_id::integer, roster_status, level,
            primary_position, secondary_positions::text[], is_starter::boolean,
            team_id::integer, team_name, last_game_date::date, status_date::date
        FROM production._tmp_roster
    """))
    conn.execute(text("DROP TABLE IF EXISTS production._tmp_roster"))

    count = len(out)
    logger.info("Wrote %d players to production.dim_roster", count)
    return count


# ---------------------------------------------------------------------------
# Core build — used by both pipeline and standalone script
# ---------------------------------------------------------------------------

def build_roster(season: int, engine=None) -> int:
    """Full rebuild of dim_roster from upstream production tables + API rosters.

    Args:
        season: Prior season year to build base roster from (e.g. 2025).
            The API roster overlay uses season+1 (the current season).
        engine: SQLAlchemy engine. Created from build_db_url() if not provided.

    Returns:
        Number of players written.
    """
    if engine is None:
        engine = create_engine(build_db_url())

    current_season = season + 1

    with engine.begin() as conn:
        # Ensure table exists
        conn.execute(text(DDL))

        logger.info("Building roster from %d season data...", season)

        mlb = _fetch_mlb_appearances(conn, season)
        milb = _fetch_milb_appearances(conn, season)
        pos_counts = _fetch_position_counts(conn)
        pitcher_cls = _fetch_pitcher_classification(conn, season)
        statuses = _fetch_current_statuses(conn)
        dim_names = _fetch_player_names(conn)
        milb_names = _fetch_milb_player_names(conn)
        teams = _fetch_team_names(conn)

        logger.info(
            "Fetched: %d MLB, %d MiLB, %d position records",
            len(mlb), len(milb), len(pos_counts),
        )

        # ---- Build player universe ----
        mlb_df = mlb.copy()
        mlb_df["org_id"] = mlb_df["team_id"]
        mlb_df["level"] = "MLB"

        milb_df = milb[~milb["player_id"].isin(mlb_df["player_id"])].copy()

        roster = pd.concat(
            [
                mlb_df[["player_id", "org_id", "team_id", "level",
                         "last_game_date", "player_role"]],
                milb_df[["player_id", "org_id", "team_id", "team_name",
                          "level", "last_game_date", "player_role"]],
            ],
            ignore_index=True,
        )

        # ---- Player names ----
        all_names = pd.concat(
            [dim_names[["player_id", "player_name"]], milb_names],
            ignore_index=True,
        ).drop_duplicates("player_id", keep="first")
        roster = roster.merge(all_names, on="player_id", how="left")
        roster["player_name"] = roster["player_name"].fillna("Unknown")

        roster = roster.merge(
            dim_names[["player_id", "api_position"]], on="player_id", how="left",
        )

        # ---- Team names for MLB teams ----
        roster = roster.merge(
            teams.rename(columns={"team_name": "mlb_team_name"}),
            on="team_id", how="left",
        )
        roster["team_name"] = roster["team_name"].fillna(roster["mlb_team_name"])
        roster.drop(columns=["mlb_team_name"], inplace=True, errors="ignore")

        # ---- Positions ----
        positions = _determine_positions(pos_counts)
        roster = roster.merge(positions, on="player_id", how="left")

        # ---- Pitcher handling ----
        pitcher_mask = roster["player_role"] == "pitcher"
        roster = roster.merge(pitcher_cls, on="player_id", how="left")
        roster["is_starter"] = roster["is_starter"].fillna(False)

        needs_pos = pitcher_mask & roster["primary_position"].isna()
        roster.loc[needs_pos & roster["is_starter"], "primary_position"] = "SP"
        roster.loc[needs_pos & ~roster["is_starter"], "primary_position"] = "RP"

        p_primary = pitcher_mask & (roster["primary_position"] == "P")
        roster.loc[p_primary & roster["is_starter"], "primary_position"] = "SP"
        roster.loc[p_primary & ~roster["is_starter"], "primary_position"] = "RP"

        api_pitcher = roster["api_position"] == "P"
        misclassified = api_pitcher & roster["primary_position"].isin(["DH", "UTIL"])
        roster.loc[misclassified & roster["is_starter"], "primary_position"] = "SP"
        roster.loc[misclassified & ~roster["is_starter"], "primary_position"] = "RP"

        # ---- Fallback positions ----
        no_pos = roster["primary_position"].isna()
        roster.loc[no_pos, "primary_position"] = roster.loc[no_pos, "api_position"]
        roster.loc[roster["primary_position"].isna(), "primary_position"] = "UTIL"
        roster["secondary_positions"] = roster["secondary_positions"].apply(
            lambda x: x if isinstance(x, list) else [],
        )

        # ---- Status from timeline ----
        roster = roster.merge(
            statuses[["player_id", "status_type", "status_start_date"]],
            on="player_id", how="left",
        )
        roster["roster_status"] = roster["status_type"].map(STATUS_MAP).fillna("active")
        milb_mask = roster["level"] != "MLB"
        roster.loc[milb_mask & roster["status_type"].isna(), "roster_status"] = "minors"

        roster["status_date"] = roster["status_start_date"].fillna(roster["last_game_date"])
        roster["status_date"] = roster["status_date"].fillna(
            pd.Timestamp(f"{season}-04-01")
        )

        # ---- Post-season transaction overlay ----
        txns = _fetch_post_season_transactions(conn, season)
        logger.info("Applying %d post-season transactions", len(txns))
        roster = _apply_transactions(roster, txns, dim_names)

        # ---- Update last_game_date from current season (spring training) ----
        current_games = _fetch_current_season_appearances(conn, current_season)
        if not current_games.empty:
            logger.info("Updating %d players with %d-season game data",
                        len(current_games), current_season)
            for _, row in current_games.iterrows():
                mask = roster["player_id"] == row["player_id"]
                if mask.any():
                    roster.loc[mask, "last_game_date"] = row["last_game_date"]
                    roster.loc[mask, "team_id"] = row["team_id"]

        # ---- API roster overlay (authoritative for current org/team) ----
        api_rosters = _fetch_api_rosters(conn, current_season)
        logger.info("Applying API roster overlay: %d entries", len(api_rosters))
        roster = _apply_api_roster(roster, api_rosters, teams)

        # ---- Final cleanup ----
        roster = roster[roster["org_id"].between(108, 158)].copy()
        roster = roster.drop_duplicates("player_id", keep="last")
        roster.drop(
            columns=["player_role", "api_position", "status_type",
                     "status_start_date", "mlb_team_name"],
            inplace=True, errors="ignore",
        )

        # ---- Write ----
        count = _write_roster(conn, roster)

        by_level = roster["level"].value_counts()
        logger.info(
            "Done: %d players across %d orgs",
            len(roster), roster["org_id"].nunique(),
        )
        for lvl, cnt in sorted(by_level.items(), key=lambda x: x[1], reverse=True):
            logger.info("  %-5s %d", lvl, cnt)

        return count

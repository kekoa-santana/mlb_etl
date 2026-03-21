"""
Live game feed ingestion — polls MLB Stats API and writes to staging.live_pitches
and staging.live_batted_balls tables.

Ported from gamefeed/gamefeed.py (data fetching/parsing only, no card generation).
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from sqlalchemy import create_engine

from utils.retry import build_retry_session
from utils.utils import build_db_url
from transformation.staging.transform_load_table import transform_and_load
from schema.staging.live_pitches import LIVE_PITCHES_SPEC
from schema.staging.live_batted_balls import LIVE_BATTED_BALLS_SPEC

logger = logging.getLogger(__name__)

session = build_retry_session(timeout=15)

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
MLB_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"

# Call codes for pitch classification
_WHIFF_CODES = {"S", "W", "M"}
_SWING_CODES = {"S", "W", "M", "F", "T", "L", "X", "D", "E"}
_CALLED_STRIKE_CODES = {"C"}
_FOUL_CODES = {"F", "T", "L"}
_BIP_CODES = {"X", "D", "E"}


# ── Data classes ─────────────────────────────────────────────────────

@dataclass
class GameState:
    game_pk: int
    home_team_id: int
    away_team_id: int
    home_team: str
    away_team: str
    status: str = "Preview"


# ── API fetching ─────────────────────────────────────────────────────

def fetch_schedule(game_date: str, sport_id: int = 1) -> list[dict]:
    url = f"{MLB_SCHEDULE_URL}?sportId={sport_id}&date={game_date}"
    resp = session.get(url, timeout=session.timeout)
    resp.raise_for_status()
    data = resp.json()
    dates = data.get("dates", [])
    if not dates:
        return []
    return dates[0].get("games", [])


def fetch_game_feed(game_pk: int) -> dict:
    url = MLB_FEED_URL.format(game_pk=game_pk)
    resp = session.get(url, timeout=session.timeout)
    resp.raise_for_status()
    return resp.json()


# ── Pitch extraction ─────────────────────────────────────────────────

def extract_all_pitches(feed: dict, game_pk: int, game_date: str) -> list[dict]:
    """Parse ALL pitches from a live game feed into flat rows for live_pitches table."""
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    rows = []

    for play in plays:
        about = play.get("about", {})
        if not about.get("isComplete", False):
            continue

        at_bat_index = about.get("atBatIndex")
        if at_bat_index is None:
            continue

        matchup = play.get("matchup", {})
        result = play.get("result", {})
        batter = matchup.get("batter", {})
        pitcher = matchup.get("pitcher", {})
        bat_side = matchup.get("batSide", {}).get("code", "")
        pitch_hand = matchup.get("pitchHand", {}).get("code", "")

        play_events = play.get("playEvents", [])
        pitch_events = [e for e in play_events if e.get("isPitch")]

        for i, ev in enumerate(pitch_events):
            details = ev.get("details", {})
            ptype = details.get("type", {})
            call = details.get("call", {})
            pitch_data = ev.get("pitchData", {})
            coords = pitch_data.get("coordinates", {})
            breaks = pitch_data.get("breaks", {})
            count = ev.get("count", {})

            call_code = call.get("code", "")
            is_final = i == len(pitch_events) - 1

            # Hit data only on final pitch if BIP
            hit_data = ev.get("hitData") or {}
            launch_speed = None
            launch_angle = None
            hit_distance = None
            if is_final and hit_data.get("launchSpeed") is not None:
                launch_speed = hit_data.get("launchSpeed")
                launch_angle = hit_data.get("launchAngle")
                hit_distance = hit_data.get("totalDistance")

            row = {
                "game_pk": game_pk,
                "at_bat_index": at_bat_index,
                "pitch_number": i + 1,
                "game_date": game_date,
                "pitcher_id": pitcher.get("id"),
                "batter_id": batter.get("id"),
                "inning": about.get("inning"),
                "half_inning": about.get("halfInning"),
                "pitch_type": ptype.get("code"),
                "pitch_name": ptype.get("description"),
                "release_speed": pitch_data.get("startSpeed"),
                "spin_rate": breaks.get("spinRate"),
                "pfx_x": coords.get("pfxX"),
                "pfx_z": coords.get("pfxZ"),
                "plate_x": coords.get("pX"),
                "plate_z": coords.get("pZ"),
                "zone": pitch_data.get("zone"),
                "sz_top": pitch_data.get("strikeZoneTop"),
                "sz_bot": pitch_data.get("strikeZoneBottom"),
                "call_code": call_code,
                "call_description": call.get("description"),
                "is_whiff": call_code in _WHIFF_CODES,
                "is_called_strike": call_code in _CALLED_STRIKE_CODES,
                "is_swing": call_code in _SWING_CODES,
                "is_bip": call_code in _BIP_CODES,
                "is_foul": call_code in _FOUL_CODES,
                "balls": count.get("balls"),
                "strikes": count.get("strikes"),
                "outs_when_up": count.get("outs"),
                "bat_side": bat_side,
                "pitch_hand": pitch_hand,
                "launch_speed": launch_speed,
                "launch_angle": launch_angle,
                "hit_distance": hit_distance,
                "event_type": result.get("eventType") if is_final else None,
                "event": result.get("event") if is_final else None,
            }
            rows.append(row)

    return rows


def extract_batted_balls(feed: dict, game_pk: int, game_date: str) -> list[dict]:
    """Extract batted ball events from a live game feed for live_batted_balls table."""
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    rows = []

    game_data = feed.get("gameData", {})
    teams = game_data.get("teams", {})
    home_team_id = teams.get("home", {}).get("id")
    away_team_id = teams.get("away", {}).get("id")

    for play in plays:
        about = play.get("about", {})
        if not about.get("isComplete", False):
            continue

        at_bat_index = about.get("atBatIndex")
        if at_bat_index is None:
            continue

        matchup = play.get("matchup", {})
        result = play.get("result", {})
        batter = matchup.get("batter", {})
        pitcher = matchup.get("pitcher", {})

        # Get hit data from last pitch event
        play_events = play.get("playEvents", [])
        if not play_events:
            continue

        last_event = play_events[-1]
        hit_data = last_event.get("hitData") or {}
        if hit_data.get("launchSpeed") is None:
            continue

        pitch_data = last_event.get("pitchData", {})
        ptype = last_event.get("details", {}).get("type", {})
        half = about.get("halfInning", "")
        batter_team_id = away_team_id if half == "top" else home_team_id

        rows.append({
            "game_pk": game_pk,
            "at_bat_index": at_bat_index,
            "game_date": game_date,
            "batter_id": batter.get("id"),
            "pitcher_id": pitcher.get("id"),
            "batter_name": batter.get("fullName"),
            "pitcher_name": pitcher.get("fullName"),
            "team_id": batter_team_id,
            "inning": about.get("inning"),
            "half_inning": half,
            "launch_speed": hit_data.get("launchSpeed"),
            "launch_angle": hit_data.get("launchAngle"),
            "hit_distance": hit_data.get("totalDistance"),
            "event_type": result.get("eventType"),
            "event": result.get("event"),
            "pitch_speed": pitch_data.get("startSpeed"),
            "pitch_type": ptype.get("code"),
            "bat_side": matchup.get("batSide", {}).get("code", ""),
            "pitch_hand": matchup.get("pitchHand", {}).get("code", ""),
        })

    return rows


# ── Load helpers ─────────────────────────────────────────────────────

def _load_pitches(engine, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    n, report = transform_and_load(
        engine=engine,
        df_raw=df,
        spec=LIVE_PITCHES_SPEC,
        schema='staging',
        table='live_pitches',
        constraint='live_pitches_pkey',
    )
    logger.info(f"  live_pitches: {n} rows loaded ({report['rows_in']} in)")


def _load_batted_balls(engine, rows: list[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    n, report = transform_and_load(
        engine=engine,
        df_raw=df,
        spec=LIVE_BATTED_BALLS_SPEC,
        schema='staging',
        table='live_batted_balls',
        constraint='live_batted_balls_pkey',
    )
    logger.info(f"  live_batted_balls: {n} rows loaded ({report['rows_in']} in)")


# ── Main poll loop ───────────────────────────────────────────────────

def poll_and_load(game_date: str, engine, poll_interval: int = 600):
    """Poll live games and write to live staging tables.

    Fetches schedule, then polls each active game every poll_interval seconds.
    Full game re-parse each poll (idempotent via ON CONFLICT DO UPDATE).
    """
    logger.info(f"Live feed: fetching schedule for {game_date}...")
    games = fetch_schedule(game_date)

    if not games:
        logger.info("No games scheduled.")
        return

    states = {}
    for g in games:
        pk = g["gamePk"]
        teams = g.get("teams", {})
        home = teams.get("home", {}).get("team", {})
        away = teams.get("away", {}).get("team", {})
        states[pk] = GameState(
            game_pk=pk,
            home_team_id=home.get("id", 0),
            away_team_id=away.get("id", 0),
            home_team=home.get("name", "HOME"),
            away_team=away.get("name", "AWAY"),
            status=g.get("status", {}).get("abstractGameState", "Preview"),
        )

    logger.info(f"Found {len(states)} games")
    for pk, st in states.items():
        logger.info(f"  {pk}: {st.away_team} @ {st.home_team} ({st.status})")

    all_final_at_start = all(s.status == "Final" for s in states.values())

    while True:
        active = {
            pk: st for pk, st in states.items()
            if st.status != "Final"
        }

        # Even if all final, do one pass to capture data
        targets = active if active else states

        for pk, state in targets.items():
            try:
                feed = fetch_game_feed(pk)
            except Exception as e:
                logger.warning(f"Error fetching {pk}: {e}")
                continue

            # Update status
            game_data = feed.get("gameData", {})
            state.status = game_data.get("status", {}).get("abstractGameState", "Preview")

            logger.info(f"Processing {pk} ({state.away_team} @ {state.home_team}) — {state.status}")

            pitch_rows = extract_all_pitches(feed, pk, game_date)
            bb_rows = extract_batted_balls(feed, pk, game_date)

            _load_pitches(engine, pitch_rows)
            _load_batted_balls(engine, bb_rows)

        if not active or all_final_at_start:
            logger.info("All games final. Done.")
            break

        logger.info(f"Sleeping {poll_interval}s before next poll...")
        time.sleep(poll_interval)

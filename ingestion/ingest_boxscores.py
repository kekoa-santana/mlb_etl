import logging
import json
import pandas as pd
from sqlalchemy import create_engine, Text, Table, Column, BigInteger, MetaData
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger=logging.getLogger(__name__)
session=build_retry_session(timeout=15)

engine = create_engine(build_db_url(), pool_pre_ping=True)

def _fetch_game_pks(start_date: str, end_date: str, team_id: int = None) -> list:
    """
    Fetch game primary keys from MLB Stats API schedule endpoint.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        team_id: Optional team ID to filter games (None = all teams)

    Returns:
        List of game_pk integers
    """
    game_pks = []

    schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}"
    if team_id is not None:
        schedule_url += f"&teamId={team_id}"

    response = session.get(schedule_url, timeout=session.timeout)
    response.raise_for_status()
    data = response.json()

    schedule = data.get("dates") or {}
    for day in schedule:
        games = day.get("games") or []
        for game in games:
            if game.get("gameType", " ").lower() != "s":
                game_pk = game.get("gamePk")
                if game_pk not in game_pks:
                    game_pks.append(game_pk)

    return game_pks


metadata = MetaData()
landing = Table(
    "landing_boxscores", metadata,
    Column("source", Text, nullable=False),
    Column("game_pk", BigInteger, primary_key=True),
    Column("payload", JSONB, nullable=False),
    schema="raw",
)

def insert_raw_payload(game_pk, data_payload):
    with engine.begin() as conn:
        stmt = pg_insert(landing).values(
            source="MLB_stats_api",
            game_pk=game_pk,
            payload=data_payload,
        ).on_conflict_do_nothing(
            index_elements=["game_pk"]
        )
        conn.execute(stmt)

def fetch_boxscores(game_pks: list, desired_team_id: int = None) -> list:
    boxscore_base_url = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
    boxscore_rows = []
    row = 0

    for game_pk in game_pks:
        try:
            url = boxscore_base_url.format(game_pk)
            response = session.get(url, timeout=session.timeout)
            response.raise_for_status()
            data = response.json()

            insert_raw_payload(game_pk, data)

            teams = data.get("teams") or {}
            for side in ['away', 'home']:
                s = teams.get(side) or {}
                team_data = s.get("team") or {}
                player_data = s.get("players") or {}

                team_id = team_data.get("id")
                # If desired_team_id is None, include all teams
                if desired_team_id is None or team_id == desired_team_id:
                    for player_idx, player in player_data.items():
                        player_stats = player.get("stats") or {}
                        person_data = player.get("person") or {}

                        pitching_stats = player_stats.get("pitching") or {}

                        if pitching_stats:
                            boxscore_rows.append({
                                "row_num": row,
                                "pitcher_id": person_data.get("id"),
                                "pitcher_name": person_data.get("fullName"),
                                "game_pk": game_pk,
                                "team_id": team_id,
                                "team_name": team_data.get("name"),
                                "is_starter_text": pitching_stats.get("gamesStarted"),
                                "fly_outs_text": pitching_stats.get("flyOuts"),
                                "ground_outs_text": pitching_stats.get("groundOuts"),
                                "air_outs_text": pitching_stats.get("airOuts"),
                                "runs_text": pitching_stats.get("runs"),
                                "doubles_text": pitching_stats.get("doubles"),
                                "triples_text": pitching_stats.get("triples"),
                                "home_runs_text": pitching_stats.get("homeRuns"),
                                "strike_outs_text": pitching_stats.get("strikeOuts"),
                                "walks_text": pitching_stats.get("baseOnBalls"),
                                "intentional_walks_text": pitching_stats.get("intentionalWalks"),
                                "hits_text": pitching_stats.get("hits"),
                                "hit_by_pitch_text": pitching_stats.get("hitByPitch"),
                                "at_bats_text": pitching_stats.get("atBats"),
                                "caught_stealing_text": pitching_stats.get("caughtStealing"),
                                "stolen_bases_text": pitching_stats.get("stolenBases"),
                                "stolen_base_percentage_text": pitching_stats.get("stolenBasePercentage"),
                                "number_of_pitches_text": pitching_stats.get("numberOfPitches"),
                                "innings_pitched_text": pitching_stats.get("inningsPitched"),
                                "wins_text": pitching_stats.get("wins"),
                                "losses_text": pitching_stats.get("losses"),
                                "saves_text": pitching_stats.get("saves"),
                                "save_opportunities_text": pitching_stats.get("saveOpportunities"),
                                "holds_text": pitching_stats.get("holds"),
                                "blown_saves_text": pitching_stats.get("blownSaves"),
                                "earned_runs_text": pitching_stats.get("earnedRuns"),
                                "batters_faced_text": pitching_stats.get("battersFaced"),
                                "outs_text": pitching_stats.get("outs"),
                                "complete_game_text": pitching_stats.get("completeGames"),
                                "shutout_text": pitching_stats.get("shutouts"),
                                "pitches_thrown_text": pitching_stats.get("pitchesThrown"),
                                "balls_text": pitching_stats.get("balls"),
                                "strikes_text": pitching_stats.get("strikes"),
                                "strike_percentage_text": pitching_stats.get("strikePercentage"),
                                "hit_batsmen_text": pitching_stats.get("hitBatsmen"),
                                "balks_text": pitching_stats.get("balks"),
                                "wild_pitches_text": pitching_stats.get("wildPitches"),
                                "pickoffs_text": pitching_stats.get("pickoffs"),
                                "rbi_text": pitching_stats.get("rbi"),
                                "games_finished_text": pitching_stats.get("gamesFinished"),
                                "runs_scored_per_9_text": pitching_stats.get("runsScoredPer9"),
                                "home_runs_per_9_text": pitching_stats.get("homeRunsPer9"),
                                "inherited_runners_text": pitching_stats.get("inheritedRunners"),
                                "inherited_runners_scored_text": pitching_stats.get("inheritedRunnersScored"),
                                "catchers_interference_text": pitching_stats.get("catchersInterference"),
                                "sac_bunts_text": pitching_stats.get("sacBunts"),
                                "sac_flies_text": pitching_stats.get("sacFlies"),
                                "passed_ball_text": pitching_stats.get("passedBall"),
                                "pop_outs_text": pitching_stats.get("popOuts"),
                                "line_outs_text": pitching_stats.get("lineOuts"),
                                "source": "MLB_stats_api"
                            })

                            row += 1       
        except Exception as exc:
            logger.error(f"Skipping game_pk {game_pk}: {exc}")
            continue
        
    return boxscore_rows

def load_to_psql(df: pd.DataFrame):
    with engine.begin() as conn:
        df.to_sql(
            'pitching_boxscores',
            conn,
            schema = 'raw',
            if_exists = 'append',
            index = False,
            method="multi",
            chunksize=50
        )


def fetch_and_load_boxscores(start_date: str, end_date: str, team_ids: list[int]=None, engine=None):
    game_pks = []
    boxscores = []

    if team_ids:
        for team_id in team_ids:
            print(f"Fetching game_pks from {start_date} to {end_date} for team: {team_id}")
            single_team = _fetch_game_pks(start_date, end_date, team_id)
            game_pks.extend(single_team)

            print(f"Found {len(game_pks)} games")

            rows = fetch_boxscores(single_team, team_id)
            boxscores.extend(rows)
            print(f"Extracted {len(rows)} boxscore rows")

        if rows:
            df = pd.DataFrame(rows)
            load_to_psql(df)
            print(f"Loaded {len(df)} rows to raw.pitching_boxscores")
        else:
            print("No boxscore rows to load")
    
    else:
        print(f"Fetching game_pks from {start_date} to {end_date} for team all teams")
        game_pks = _fetch_game_pks(start_date, end_date)
        print(f'Found {len(game_pks)} games')

        boxscores = fetch_boxscores(game_pks)
        print(f'Extracted {len(boxscores)} boxscore rows')

        if len(boxscores) > 0:
            df = pd.DataFrame(boxscores)
            load_to_psql(df)
            print(f'loaded {len(df)} rows to raw.pitching_boxscores')
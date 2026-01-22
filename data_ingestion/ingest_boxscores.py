import requests
import json
import pandas as pd
from sqlalchemy import create_engine, Text, Table, Column, BigInteger, MetaData
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from utils.utils import build_db_url

engine = create_engine(build_db_url(), pool_pre_ping=True)

def _fetch_game_pks() -> []:
    game_pks = []

    schedule_url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId=119&startDate=2025-03-18&endDate=2025-11-02"
    response = requests.get(schedule_url, timeout=15)
    response.raise_for_status()
    data = response.json() 

    schedule = data.get("dates") or {}
    for day in schedule:
        games = day.get("games") or []
        for game in games:
            if game.get("gameType", " ").lower() != "s":
                game_pk = game.get("gamePk")
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

def fetch_boxscores(game_pks: [], desired_team_id: int) -> []:
    boxscore_base_url = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
    boxscore_rows = []
    row = 0

    for game_pk in game_pks:
        url = boxscore_base_url.format(game_pk)
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        insert_raw_payload(game_pk, data)

        teams = data.get("teams") or {}
        for side in ['away', 'home']:
            s = teams.get(side) or {}
            team_data = s.get("team") or {}
            player_data = s.get("players") or {}

            team_id = team_data.get("id")
            if team_id == desired_team_id:
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
            chunksize=5000
        )

def main():
    print("fetching game_pks")
    game_pks = _fetch_game_pks()
    rows = fetch_boxscores(game_pks, 119)

    df = pd.DataFrame(rows)
    load_to_psql(df)

if __name__ == "__main__":
    main()
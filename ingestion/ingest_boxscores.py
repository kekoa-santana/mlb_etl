import logging
import json
import pandas as pd
from sqlalchemy import create_engine, Text, Table, Column, BigInteger, MetaData
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger=logging.getLogger(__name__)
session=build_retry_session(timeout=15)

engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

def _fetch_game_table(start_date: str, end_date: str):
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
    rows = []

    schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}"

    response = session.get(schedule_url, timeout=session.timeout)
    response.raise_for_status()
    data = response.json()

    schedule = data.get("dates") or {}
    for day in schedule:
        games = day.get("games") or []
        for game in games:
            team_info = game.get('teams') or {}
            home = team_info.get('home')
            home_team = home.get('team')
            home_record = home.get('leagueRecord')

            away = team_info.get('away')
            away_team = away.get('team')
            away_record = away.get('leagueRecord')

            venue = game.get('venue')

            if game.get("gameType", " ").lower() != "s":
                game_pk = game.get("gamePk")
                rows.append({
                    'game_date': day.get('date'),
                    'game_pk': game_pk,
                    'game_type': game.get('gameType'),
                    'season': game.get('season'),
                    'home_team_id': home_team.get('id'),
                    'home_team_name': home_team.get('name'),
                    'away_team_id': away_team.get('id'),
                    'away_team_name': away_team.get('name'),
                    'home_wins_text': home_record.get('wins'),
                    'home_losses_text': home_record.get('losses'),
                    'away_wins_text': away_record.get('wins'),
                    'away_losses_text': away_record.get('losses'),
                    'venue_id_text': venue.get('id'),
                    'doubleheader_text': game.get('doubleheader'),
                    'day_night_text': game.get('dayNight'),
                    'games_in_series_text': game.get('gamesInSeries'),
                    'series_in_game_number_text': game.get('seriesGameNumber')
                })
                if game_pk not in game_pks:
                    game_pks.append(game_pk)

    return game_pks, pd.DataFrame(rows)


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

def fetch_boxscores(game_pks: list) -> list:
    boxscore_base_url = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
    pitching_rows = []
    batting_rows = []
    pitching_row = 0
    batting_row = 0

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
                for player_idx, player in player_data.items():
                    batter_stats = player.get('stats') or {}
                    batter_data = player.get('person') or {}
                    position = player.get('position') or {}

                    batting_stats = batter_stats.get('batting') or {}
                    fielding_stats =batter_stats.get('fielding') or {}

                    pitcher_stats = player.get("stats") or {}
                    pitcher_data = player.get("person") or {}

                    pitching_stats = pitcher_stats.get("pitching") or {}

                    if pitching_stats:
                        pitching_rows.append({
                            "row_num": pitching_row,
                            "pitcher_id": pitcher_data.get("id"),
                            "pitcher_name": pitcher_data.get("fullName"),
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

                        pitching_row += 1

                    if batting_stats:
                        batting_rows.append({
                            'row_num': batting_row,
                            'batter_id': batter_data.get('id'),
                            'batter_name': batter_data.get('fullName'),
                            'game_pk': game_pk,
                            'team_id': team_id,
                            'team_name': team_data.get('name'),
                            'position': position.get('abbreviation'),
                            'ground_outs_text': batting_stats.get('groundOuts'),
                            'air_outs_text': batting_stats.get('airOuts'),
                            'runs_text': batting_stats.get('runs'),
                            'doubles_text': batting_stats.get('doubles'),
                            'triples_text': batting_stats.get('triples'),
                            'home_runs_text': batting_stats.get('homeRuns'),
                            'strikeouts_text': batting_stats.get('strikeOuts'),
                            'walks_text': batting_stats.get('baseOnBalls'),
                            'intentional_walks_text': batting_stats.get('intentionalWalks'),
                            'hits_text': batting_stats.get('hits'),
                            'hit_by_pitch_text': batting_stats.get('hitByPitch'),
                            'at_bats_text': batting_stats.get('atBats'),
                            'caught_stealing_text': batting_stats.get('caughtStealing'),
                            'sb_text': batting_stats.get('stolenBases'),
                            'sb_pct_text': batting_stats.get('stolenBasePercentage'),
                            'plate_appearances_text': batting_stats.get('plateAppearances'),
                            'total_bases_text': batting_stats.get('totalBases'),
                            'rbi_text': batting_stats.get('rbi') ,
                            'errors_text': fielding_stats.get('errors'),
                            "source": "MLB_stats_api"
                        })
                        
                        batting_row += 1
        except Exception as exc:
            logger.error(f"Skipping game_pk {game_pk}: {exc}")
            continue
        
    return pitching_rows, batting_rows

def load_to_psql(df: pd.DataFrame, table_name: str):
    with engine.begin() as conn:
        df.to_sql(
            table_name,
            conn,
            schema = 'raw',
            if_exists = 'append',
            index = False,
            method="multi",
            chunksize=50
        )


def fetch_and_load_boxscores(start_date: str, end_date: str):
    game_pks = []

    game_pks, dim_game_df = _fetch_game_table(start_date, end_date)
    if not dim_game_df.empty:
        load_to_psql(dim_game_df, 'dim_game')
    else:
        print('No dim_game dataframe to load')

    print(f'Found {len(game_pks)} games')

    pitching_boxscore, batting_boxscore = fetch_boxscores(game_pks)

    if len(pitching_boxscore) > 0:
        df_pitch = pd.DataFrame(pitching_boxscore)
        load_to_psql(df_pitch, 'pitching_boxscores')
    else:
        print('No pitching boxscores to load')
    
    if len(batting_boxscore) > 0:
        df_bat = pd.DataFrame(batting_boxscore)
        load_to_psql(df_bat, 'batting_boxscores')
    else:
        print('No batting boxscores to load')
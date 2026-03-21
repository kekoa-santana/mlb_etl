import logging
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=15)
engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

SPORT_IDS = {
    11: 'AAA',
    12: 'AA',
    13: 'A+',
    14: 'A',
    16: 'ROK',
}

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"
TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"


def _build_team_org_map(season: int) -> dict[int, dict]:
    """Build mapping of MiLB team_id -> {parent_org_id, parent_org_name}."""
    team_map = {}
    for sport_id in SPORT_IDS:
        url = f"{TEAMS_URL}?sportId={sport_id}&season={season}"
        resp = session.get(url, timeout=session.timeout)
        resp.raise_for_status()
        for team in resp.json().get('teams', []):
            parent = team.get('parentOrgId') or team.get('parentOrg', {}).get('id')
            parent_name = team.get('parentOrgName') or team.get('parentOrg', {}).get('name')
            team_map[team['id']] = {
                'parent_org_id': parent,
                'parent_org_name': parent_name,
            }
    return team_map


def _fetch_milb_schedule(start_date: str, end_date: str) -> list[dict]:
    """Fetch MiLB schedule across all levels for a date range."""
    games = []
    for sport_id, level in SPORT_IDS.items():
        url = f"{SCHEDULE_URL}?sportId={sport_id}&startDate={start_date}&endDate={end_date}"
        resp = session.get(url, timeout=session.timeout)
        resp.raise_for_status()
        data = resp.json()

        for day in data.get('dates') or []:
            for game in day.get('games') or []:
                status = game.get('status', {}).get('abstractGameState', '')
                if status != 'Final':
                    continue
                games.append({
                    'game_pk': game['gamePk'],
                    'sport_id': sport_id,
                    'level': level,
                    'game_date': day.get('date'),
                    'season': int(float(game.get('season', 0))),
                    'home_team_id': game.get('teams', {}).get('home', {}).get('team', {}).get('id'),
                    'away_team_id': game.get('teams', {}).get('away', {}).get('team', {}).get('id'),
                })
    logger.info(f"Found {len(games)} completed MiLB games from {start_date} to {end_date}")
    return games


def _fetch_milb_boxscores(games: list[dict], team_org_map: dict) -> tuple[list, list]:
    """Fetch boxscores for MiLB games and extract batting/pitching rows."""
    batting_rows = []
    pitching_rows = []

    for i, game in enumerate(games):
        game_pk = game['game_pk']
        try:
            url = BOXSCORE_URL.format(game_pk)
            resp = session.get(url, timeout=session.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error(f"Skipping game_pk {game_pk}: {exc}")
            continue

        teams = data.get('teams') or {}
        for side in ['away', 'home']:
            s = teams.get(side) or {}
            team_data = s.get('team') or {}
            player_data = s.get('players') or {}
            team_id = team_data.get('id')

            org_info = team_org_map.get(team_id, {})

            for _, player in player_data.items():
                person = player.get('person') or {}
                position = player.get('position') or {}
                stats = player.get('stats') or {}

                batting_stats = stats.get('batting') or {}
                pitching_stats = stats.get('pitching') or {}
                fielding_stats = stats.get('fielding') or {}

                if batting_stats:
                    batting_rows.append({
                        'batter_id': person.get('id'),
                        'batter_name': person.get('fullName'),
                        'game_pk': game_pk,
                        'team_id': team_id,
                        'team_name': team_data.get('name'),
                        'position': position.get('abbreviation'),
                        'sport_id': game['sport_id'],
                        'level': game['level'],
                        'parent_org_id': org_info.get('parent_org_id'),
                        'game_date': pd.to_datetime(game['game_date']).date(),
                        'season': game['season'],
                        'ground_outs': batting_stats.get('groundOuts'),
                        'air_outs': batting_stats.get('airOuts'),
                        'runs': batting_stats.get('runs'),
                        'doubles': batting_stats.get('doubles'),
                        'triples': batting_stats.get('triples'),
                        'home_runs': batting_stats.get('homeRuns'),
                        'strikeouts': batting_stats.get('strikeOuts'),
                        'walks': batting_stats.get('baseOnBalls'),
                        'intentional_walks': batting_stats.get('intentionalWalks'),
                        'hits': batting_stats.get('hits'),
                        'hit_by_pitch': batting_stats.get('hitByPitch'),
                        'at_bats': batting_stats.get('atBats'),
                        'caught_stealing': batting_stats.get('caughtStealing'),
                        'sb': batting_stats.get('stolenBases'),
                        'sb_pct': batting_stats.get('stolenBasePercentage'),
                        'plate_appearances': batting_stats.get('plateAppearances'),
                        'total_bases': batting_stats.get('totalBases'),
                        'rbi': batting_stats.get('rbi'),
                        'errors': fielding_stats.get('errors'),
                        'source': 'MLB_stats_api',
                    })

                if pitching_stats:
                    pitching_rows.append({
                        'pitcher_id': person.get('id'),
                        'pitcher_name': person.get('fullName'),
                        'game_pk': game_pk,
                        'team_id': team_id,
                        'team_name': team_data.get('name'),
                        'sport_id': game['sport_id'],
                        'level': game['level'],
                        'parent_org_id': org_info.get('parent_org_id'),
                        'game_date': pd.to_datetime(game['game_date']).date(),
                        'season': game['season'],
                        'is_starter': pitching_stats.get('gamesStarted'),
                        'fly_outs': pitching_stats.get('flyOuts'),
                        'ground_outs': pitching_stats.get('groundOuts'),
                        'air_outs': pitching_stats.get('airOuts'),
                        'runs': pitching_stats.get('runs'),
                        'doubles': pitching_stats.get('doubles'),
                        'triples': pitching_stats.get('triples'),
                        'home_runs': pitching_stats.get('homeRuns'),
                        'strike_outs': pitching_stats.get('strikeOuts'),
                        'walks': pitching_stats.get('baseOnBalls'),
                        'intentional_walks': pitching_stats.get('intentionalWalks'),
                        'hits': pitching_stats.get('hits'),
                        'hit_by_pitch': pitching_stats.get('hitByPitch'),
                        'at_bats': pitching_stats.get('atBats'),
                        'caught_stealing': pitching_stats.get('caughtStealing'),
                        'stolen_bases': pitching_stats.get('stolenBases'),
                        'stolen_base_pct': pitching_stats.get('stolenBasePercentage'),
                        'number_of_pitches': pitching_stats.get('numberOfPitches'),
                        'innings_pitched': pitching_stats.get('inningsPitched'),
                        'wins': pitching_stats.get('wins'),
                        'losses': pitching_stats.get('losses'),
                        'saves': pitching_stats.get('saves'),
                        'save_opportunities': pitching_stats.get('saveOpportunities'),
                        'holds': pitching_stats.get('holds'),
                        'blown_saves': pitching_stats.get('blownSaves'),
                        'earned_runs': pitching_stats.get('earnedRuns'),
                        'batters_faced': pitching_stats.get('battersFaced'),
                        'outs': pitching_stats.get('outs'),
                        'complete_game': pitching_stats.get('completeGames'),
                        'shutout': pitching_stats.get('shutouts'),
                        'balls': pitching_stats.get('balls'),
                        'strikes': pitching_stats.get('strikes'),
                        'strike_pct': pitching_stats.get('strikePercentage'),
                        'hit_batsmen': pitching_stats.get('hitBatsmen'),
                        'balks': pitching_stats.get('balks'),
                        'wild_pitches': pitching_stats.get('wildPitches'),
                        'pickoffs': pitching_stats.get('pickoffs'),
                        'rbi': pitching_stats.get('rbi'),
                        'games_finished': pitching_stats.get('gamesFinished'),
                        'inherited_runners': pitching_stats.get('inheritedRunners'),
                        'inherited_runners_scored': pitching_stats.get('inheritedRunnersScored'),
                        'catchers_interference': pitching_stats.get('catchersInterference'),
                        'sac_bunts': pitching_stats.get('sacBunts'),
                        'sac_flies': pitching_stats.get('sacFlies'),
                        'passed_ball': pitching_stats.get('passedBall'),
                        'source': 'MLB_stats_api',
                    })

        if (i + 1) % 100 == 0:
            logger.info(f"Processed {i + 1}/{len(games)} MiLB boxscores "
                        f"({len(batting_rows)} batting, {len(pitching_rows)} pitching rows)")

    logger.info(f"Finished: {len(batting_rows)} batting rows, {len(pitching_rows)} pitching rows")
    return batting_rows, pitching_rows


def _clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce columns with bad API values like '.---' to proper types."""
    # Columns that should be numeric but API may return string placeholders
    numeric_cols = [c for c in df.columns if c not in (
        'batter_id', 'batter_name', 'game_pk', 'team_id', 'team_name',
        'position', 'sport_id', 'level', 'parent_org_id', 'game_date',
        'season', 'source', 'pitcher_id', 'pitcher_name', 'is_starter',
        'complete_game', 'shutout', 'games_finished',
    )]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def _load_to_staging(df: pd.DataFrame, table_name: str):
    """Upsert DataFrame to staging table in 50-row chunks."""
    if df.empty:
        logger.info(f"No rows to load for staging.{table_name}")
        return

    df = _clean_numeric_columns(df)

    with engine.begin() as conn:
        table = Table(table_name, MetaData(), autoload_with=conn, schema='staging')
        for chunk_start in range(0, len(df), 50):
            chunk = df.iloc[chunk_start:chunk_start + 50]
            records = chunk.where(chunk.notna(), None).to_dict(orient='records')
            for record in records:
                for k, v in record.items():
                    try:
                        if pd.isna(v):
                            record[k] = None
                    except (TypeError, ValueError):
                        pass
            stmt = pg_insert(table).values(records).on_conflict_do_nothing()
            conn.execute(stmt)

    logger.info(f"Loaded {len(df)} rows to staging.{table_name}")


def fetch_and_load_milb(start_date: str, end_date: str):
    """Main entry point: fetch MiLB schedule + boxscores, load to staging."""
    season = int(start_date[:4])
    team_org_map = _build_team_org_map(season)
    logger.info(f"Built org map for {len(team_org_map)} MiLB teams")

    games = _fetch_milb_schedule(start_date, end_date)
    if not games:
        logger.info("No MiLB games found for date range")
        return

    batting_rows, pitching_rows = _fetch_milb_boxscores(games, team_org_map)

    if batting_rows:
        df_bat = pd.DataFrame(batting_rows)
        df_bat = df_bat.dropna(subset=['batter_id'])
        _load_to_staging(df_bat, 'milb_batting_game_logs')

    if pitching_rows:
        df_pitch = pd.DataFrame(pitching_rows)
        df_pitch = df_pitch.dropna(subset=['pitcher_id'])
        _load_to_staging(df_pitch, 'milb_pitching_game_logs')

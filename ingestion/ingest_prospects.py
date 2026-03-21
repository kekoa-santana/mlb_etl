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

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL = "https://statsapi.mlb.com/api/v1/teams/{}/roster"


def _fetch_milb_teams(season: int) -> list[dict]:
    """Fetch all MiLB teams across all levels with parent org info."""
    teams = []
    for sport_id, level in SPORT_IDS.items():
        url = f"{TEAMS_URL}?sportId={sport_id}&season={season}"
        resp = session.get(url, timeout=session.timeout)
        resp.raise_for_status()
        for team in resp.json().get('teams', []):
            parent_org = team.get('parentOrgId') or team.get('parentOrg', {}).get('id')
            parent_name = team.get('parentOrgName') or team.get('parentOrg', {}).get('name')
            teams.append({
                'team_id': team['id'],
                'team_name': team.get('name'),
                'sport_id': sport_id,
                'level': level,
                'parent_org_id': parent_org,
                'parent_org_name': parent_name,
            })
    logger.info(f"Found {len(teams)} MiLB teams across {len(SPORT_IDS)} levels")
    return teams


def _fetch_roster(team: dict, season: int) -> list[dict]:
    """Fetch hydrated roster for a single MiLB team."""
    url = (f"{ROSTER_URL.format(team['team_id'])}"
           f"?rosterType=active&season={season}"
           f"&hydrate=person(currentTeam)")
    try:
        resp = session.get(url, timeout=session.timeout)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"Failed to fetch roster for {team['team_name']}: {exc}")
        return []

    rows = []
    for entry in resp.json().get('roster', []):
        person = entry.get('person') or {}
        position = entry.get('position') or {}
        status = entry.get('status') or {}

        rows.append({
            'player_id': person.get('id'),
            'full_name': person.get('fullName'),
            'first_name': person.get('firstName'),
            'last_name': person.get('lastName'),
            'primary_position': position.get('abbreviation'),
            'bat_side': (person.get('batSide') or {}).get('code'),
            'pitch_hand': (person.get('pitchHand') or {}).get('code'),
            'birth_date': person.get('birthDate'),
            'current_age': person.get('currentAge'),
            'height': person.get('height'),
            'weight': person.get('weight'),
            'milb_team_id': team['team_id'],
            'milb_team_name': team['team_name'],
            'parent_org_id': team['parent_org_id'],
            'parent_org_name': team['parent_org_name'],
            'sport_id': team['sport_id'],
            'level': team['level'],
            'status_code': status.get('code'),
            'status_description': status.get('description'),
            'mlb_debut_date': person.get('mlbDebutDate'),
            'draft_year': person.get('draftYear'),
            'season': season,
            'jersey_number': entry.get('jerseyNumber'),
        })
    return rows


def _load_prospects(df: pd.DataFrame):
    """Upsert prospects to production.dim_prospects (update on conflict)."""
    if df.empty:
        logger.info("No prospect rows to load")
        return

    with engine.begin() as conn:
        table = Table('dim_prospects', MetaData(), autoload_with=conn, schema='production')
        for chunk_start in range(0, len(df), 50):
            chunk = df.iloc[chunk_start:chunk_start + 50]
            records = chunk.to_dict(orient='records')
            for record in records:
                for k, v in record.items():
                    if pd.isna(v):
                        record[k] = None
            stmt = pg_insert(table).values(records)
            update_cols = {c.name: stmt.excluded[c.name]
                          for c in table.columns if c.name not in ('player_id', 'season')}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id', 'season'],
                set_=update_cols,
            )
            conn.execute(stmt)

    logger.info(f"Loaded {len(df)} rows to production.dim_prospects")


def fetch_and_load_prospects(season: int):
    """Main entry point: fetch all MiLB rosters and load dim_prospects."""
    teams = _fetch_milb_teams(season)
    all_rows = []
    for i, team in enumerate(teams):
        rows = _fetch_roster(team, season)
        all_rows.extend(rows)
        if (i + 1) % 20 == 0:
            logger.info(f"Fetched rosters for {i + 1}/{len(teams)} teams "
                        f"({len(all_rows)} players so far)")

    logger.info(f"Total: {len(all_rows)} prospect rows from {len(teams)} teams")

    if all_rows:
        df = pd.DataFrame(all_rows)
        # Dedup by player_id+season, keep latest entry (highest level if on multiple rosters)
        df = df.drop_duplicates(subset=['player_id', 'season'], keep='last')
        _load_prospects(df)

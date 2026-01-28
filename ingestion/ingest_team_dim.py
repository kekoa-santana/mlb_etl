import pandas as pd
import json

from sqlalchemy import create_engine, text
from utils.utils import build_db_url
from utils.retry import build_retry_session

session=build_retry_session()

def fetch_team_dim() -> list[int]:
    teams_url = 'https://statsapi.mlb.com/api/v1/teams'

    response = session.get(teams_url, timeout=session.timeout)
    response.raise_for_status()

    data = response.json()

    teams = data.get('teams') or []
    team_list = []
    team_ids = []

    for team in teams:
        league = team.get('league') or {}
        venue = team.get('venue') or {}
        division = team.get('division') or {}

        if league:
            if league.get('id') in [103, 104]:
                team_list.append({
                    'team_id': team.get('id'),
                    'team_name': team.get('teamName'),
                    'full_name': team.get('name'),
                    'abbreviation': team.get('abbreviation'),
                    'venue': venue.get('name'),
                    'division': division.get('name'),
                    'divsion_id': division.get('id'),
                    'location': team.get('locationName')
                })

                team_ids.append(team.get('id'))
    
    df=pd.DataFrame(team_list)
    
    engine = create_engine(build_db_url())

    with engine.begin() as conn:
        df.to_sql('dim_team', conn, 'production', if_exists='replace', index=False)

    return team_ids
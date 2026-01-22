import json
import requests
import pandas as pd
from datetime import datetime, date

from sqlalchemy import create_engine, text
from utils.utils import build_db_url

players_url = 'https://statsapi.mlb.com/api/v1/sports/1/players'

engine = create_engine(build_db_url())

sql_query = '''
    SELECT DISTINCT pitcher AS player_id
    FROM staging.statcast_pitches
    WHERE pitcher IS NOT NULL
    UNION
    SELECT DISTINCT batter AS player_id
    FROM staging.statcast_pitches
    WHERE batter IS NOT NULL;
'''

def main():
    player_ids = []
    showing_ids = []
    
    with engine.begin() as conn: 
        rows = conn.execute(text(sql_query))
        
        id_list = rows.fetchall()
        
        if id_list:
            player_ids = [x[0] for x in id_list]
        else:
            print("no id_list")

    response = requests.get(players_url)
    response.raise_for_status()

    data = response.json()

    players = data.get('people') or []

    player_list = []

    for player in players:
        if player.get('id') not in player_ids:
            continue

        current_team = player.get('currentTeam')
        primary_position = player.get('primaryPosition')
        bat_side = player.get('batSide')
        pitch_hand = player.get('pitchHand')

        if player.get('draftYear') is None:
            draft_year = 0
        else:
            draft_year = player.get('draftYear')

        today = date.today()
        birth_date = player.get('birthDate')
        birth_date_dt = datetime.strptime(birth_date, '%Y-%m-%d').date()
        age = today.year - birth_date_dt.year - ((today.month, today.day) < (birth_date_dt.month, birth_date_dt.day))

        player_list.append({
            'player_id': player.get('id'),
            'full_name': player.get('fullName'),
            'team_id': current_team.get('id'),
            'first_name': player.get('useName'),
            'last_name': player.get('useLastName'),
            'birth_date': birth_date_dt,
            'age': age,
            'height': player.get('height'),
            'weight': player.get('weight'),
            'active': player.get('active'),
            'primary_position_code': primary_position.get('code'),
            'primary_position': primary_position.get('abbreviation'),
            'draft_year' : draft_year,
            'mlb_debut_date': player.get('mlbDebutDate'),
            'bat_side': bat_side.get('code'),
            'pitch_hand': pitch_hand.get('code'),
            'sz_top': player.get('strikeZoneTop'),
            'sz_bot': player.get('strikeZoneBottom')
        })

        showing_ids.append(player.get('id'))

    df = pd.DataFrame(player_list)
    print(df.head())
    
    df.to_parquet('data/dim_player.parquet')

if __name__ == "__main__":
    main()
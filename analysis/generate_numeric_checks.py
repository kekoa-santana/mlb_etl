from sqlalchemy import create_engine, text
from utils.utils import build_db_url

engine = create_engine(build_db_url())

INT_COLUMNS = [
    'fly_outs_text', 'ground_outs_text', 'air_outs_text', 'runs_text', 
    'doubles_text', 'triples_text', 'home_runs_text', 'strike_outs_text', 
    'walks_text', 'intentional_walks_text', 'hits_text', 'hit_by_pitch_text', 
    'at_bats_text', 'caught_stealing_text', 'stolen_bases_text',
    'number_of_pitches_text', 'wins_text', 'losses_text', 'saves_text', 
    'save_opportunities_text', 'holds_text', 'blown_saves_text', 
    'earned_runs_text', 'batters_faced_text', 'outs_text', 'pitches_thrown_text', 
    'balls_text', 'strikes_text', 'hit_batsmen_text', 'balks_text', 'wild_pitches_text', 
    'pickoffs_text', 'rbi_text', 'games_finished_text', 'inherited_runners_text', 
    'inherited_runners_scored_text', 'catchers_interference_text', 'sac_bunts_text', 
    'sac_flies_text', 'passed_ball_text', 'pop_outs_text', 'line_outs_text'
    ]

FLOAT_COLUMNS = [
    'stolen_base_percentage_text', 'innings_pitched_text', 'strike_percentage_text',
    'runs_scored_per_9_text', 'home_runs_per_9_text'
]

SPECIAL_NULL_TOKENS = (".---", "-.--")

def run_numeric_checks():
    for int_col in INT_COLUMNS:
        print(f"\n---------- Checking column: {int_col} ----------")

        q = f"""
            SELECT *
            FROM raw.pitching_boxscores
            WHERE {int_col} IS NOT NULL
                AND TRIM({int_col}) <> ''
                AND {int_col} !~ '^[0-9]+$'
            LIMIT 50;
        """

        with engine.connect() as conn:
            result = conn.execute(text(q)).fetchall()

            if len(result) == 0:
                print("OK")
            else:
                print(f"Found {len(result)} invalid values:")
                for row in result:
                    print(row)

    for dec_col in FLOAT_COLUMNS:
        print(f"\n---------- Checking column: {dec_col} ----------")

        # Allow SPECIAL_TOKEN_VALUES, they will be NULL in the staging phase
        q = f"""
            SELECT *
            FROM raw.pitching_boxscores
            WHERE {dec_col} IS NOT NULL
                AND TRIM({dec_col}) <> ''
                AND TRIM({dec_col}) NOT IN ('{SPECIAL_NULL_TOKENS[0]}', '{SPECIAL_NULL_TOKENS[1]}')
                AND {dec_col} !~ '^([0-9]+(\.[0-9]+)?|\.[0-9]+)$'
            LIMIT 50;
        """

        q_special = f"""
            SELECT TRIM({dec_col}) AS token, COUNT(*) AS cnt
            FROM raw.pitching_boxscores
            WHERE TRIM({dec_col}) IN ('{SPECIAL_NULL_TOKENS[0]}', '{SPECIAL_NULL_TOKENS[1]}')
            GROUP BY TRIM({dec_col});
        """

        with engine.connect() as conn:
            result = conn.execute(text(q)).fetchall()
            null_row_count = conn.execute(text(q_special)).fetchall()

            if len(result) == 0:
                print("OK")
            else:
                print(f"Found {len(result)} invalid values:")
                for row in result:
                    print(row)

if __name__ == "__main__":
    run_numeric_checks()
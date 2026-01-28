"""Builder for dim_game - derives unique games from staging.statcast_pitches."""
import pandas as pd
from sqlalchemy import create_engine, text
from utils.utils import build_db_url


def build_dim_game(df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Extract unique games from staging.statcast_pitches.

    Args:
        df: Ignored - data is sourced from staging table

    Returns:
        DataFrame with unique games: game_pk, game_date, game_type, home_team, away_team
    """
    engine = create_engine(build_db_url())

    query = text("""
        SELECT DISTINCT
            game_pk,
            game_date,
            game_type,
            home_team,
            away_team
        FROM staging.statcast_pitches
    """)

    with engine.connect() as conn:
        return pd.read_sql(query, conn)

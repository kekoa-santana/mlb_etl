import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from utils.utils import build_db_url
from transformation.staging.transform_load_table import transform_and_load

from schema.staging.statcast_pitches import STATCAST_PITCHES_SPEC
from schema.staging.statcast_batted_balls import STATCAST_BATTED_BALLS_SPEC
from schema.staging.statcast_at_bats import STATCAST_AT_BATS_SPEC
from schema.production.dim_tables import DIM_PLAYER_SPEC, DIM_TEAM_SPEC, DIM_GAME_SPEC
from schema.production.sat_tables import SAT_BATTED_BALLS_SPEC, SAT_PITCH_SHAPE_SPEC
from transformation.builders.build_at_bats import build_statcast_at_bats
from transformation.builders.build_dim_game import build_dim_game

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PARQUET_PATH = os.path.join(BASE_DIR, 'data', 'statcast_pitching_lad_2025-03-18_2025-11-01_f008ac3a-0f27-4843-b345-95059ed956bf.parquet')

REGISTRY = {
    'statcast_pitches': {
        'spec': STATCAST_PITCHES_SPEC,
        'schema': 'staging',
        'table': 'statcast_pitches',
        'constraint': 'statcast_pitches_pkey',
        'source': 'parquet',
        'builder': None
    },
    'statcast_batted_balls': {
        'spec': STATCAST_BATTED_BALLS_SPEC,
        'schema': 'staging',
        'table': 'statcast_batted_balls',
        'constraint': 'statcast_batted_balls_pkey',
        'source': 'parquet',
        'builder': None
    },
    'statcast_at_bats': {
        'spec': STATCAST_AT_BATS_SPEC,
        'schema': 'staging',
        'table': 'statcast_at_bats',
        'constraint': 'statcast_at_bats_pkey',
        'source': 'parquet',
        'builder': build_statcast_at_bats
    },
    'dim_player': {
        'spec': DIM_PLAYER_SPEC,
        'schema': 'production',
        'table': 'dim_player',
        'constraint': 'dim_player_pkey',
        'source': 'parquet',
        'builder': None
    },
    'dim_game': {
        'spec': DIM_GAME_SPEC,
        'schema': 'production',
        'table': 'dim_game',
        'constraint': 'dim_game_pkey',
        'source': 'staging',
        'builder': build_dim_game
    }
}

def load_table(table_key: str, parquet_path: str = None):
    if table_key not in REGISTRY:
        raise ValueError(f"Unknown table '{table_key}'. Options: {list(REGISTRY)}")

    cfg = REGISTRY[table_key]
    source = cfg.get('source', 'parquet')

    engine = create_engine(build_db_url())

    builder = cfg.get("builder")

    if source == 'staging':
        # Builder fetches data from staging tables
        if builder is None:
            raise ValueError(f"Table '{table_key}' has source='staging' but no builder")
        df_raw = builder(None)
    else:
        # Source is parquet
        if parquet_path is None:
            parquet_path = PARQUET_PATH
        df_raw = pd.read_parquet(parquet_path)
        if builder is not None:
            df_raw = builder(df_raw)

    n, report = transform_and_load(
        engine,
        df_raw,
        spec=cfg['spec'],
        schema=cfg['schema'],
        table=cfg['table'],
        constraint=cfg['constraint']
    )

    print(report)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("table", help=f"One of: {', '.join(REGISTRY.keys())}")
    parser.add_argument("--parquet", default=PARQUET_PATH)
    args = parser.parse_args()

    load_table(args.table, parquet_path=args.parquet)

if __name__ == "__main__":
    main()
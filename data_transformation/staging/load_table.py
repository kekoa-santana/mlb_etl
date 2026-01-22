import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from utils.utils import build_db_url
from data_transformation.staging.transform_load_table import transform_and_load

from data_quality.specs.staging.statcast_pitches import STATCAST_PITCHES_SPEC
from data_quality.specs.staging.statcast_batted_balls import STATCAST_BATTED_BALLS_SPEC
from data_quality.specs.staging.statcast_at_bats import STATCAST_AT_BATS_SPEC
from data_quality.specs.production.dim_player import DIM_PLAYER_SPEC
from data_quality.specs.staging.builders.build_at_bats import build_statcast_at_bats

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
        'constraint': 'dim_player_pkey1',
        'source': 'parquet',
        'builder': None
    }
}

def load_table(table_key: str, parquet_path: str | str = PARQUET_PATH):
    if table_key not in REGISTRY:
        raise ValueError(f"Unknown table '{table_key}'. Options: {list(REGISTRY)}")

    cfg = REGISTRY[table_key]

    engine = create_engine(build_db_url())

    df_raw = pd.read_parquet(parquet_path)

    builder = cfg.get("builder")
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
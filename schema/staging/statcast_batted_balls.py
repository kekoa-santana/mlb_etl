from schema.shared.statcast_common import COMMON_PITCH_STAGING_COLUMNS
from schema.spec_engine import ColumnSpec, TableSpec
from schema.shared.helpers import merge_columns
from utils.statcast_utils import is_homerun, is_bip

import pandas as pd

STATCAST_BATTED_BALLS_ONLY: dict[str, ColumnSpec] = {
    'pitch_number': ColumnSpec(
        name='pitch_number',
        dtype='SmallInteger',
        nullable=False,
        bounds=(1, 130),
        primary_key=True
    ),
    'pitch_type': ColumnSpec(
        name='pitch_type',
        dtype='String(10)'
    ),
    'bb_type': ColumnSpec(
        name='bb_type',
        dtype='Text'
    ),
    'launch_speed': ColumnSpec(
        name='launch_speed',
        dtype='REAL',
        bounds=(30, 135)
    ),
    'launch_angle': ColumnSpec(
        name='launch_angle',
        dtype='REAL',
        bounds=(-90, 100)
    ),
    'hit_distance_sc': ColumnSpec(
        name='hit_distance_sc',
        dtype='REAL',
        bounds=(0, 550)
    ),
    'estimated_ba_using_speedangle': ColumnSpec(
        name='estimated_ba_using_speedangle',
        dtype='REAL',
        bounds=(0, 1)
    ),
    'estimated_woba_using_speedangle': ColumnSpec(
        name='estimated_woba_using_speedangle',
        dtype='REAL',
        bounds=(0, 2.2)
    ),
    'estimated_slg_using_speedangle': ColumnSpec(
        name='estimated_slg_using_speedangle',
        dtype='REAL',
        bounds=(0, 4)
    ),
    'babip_value': ColumnSpec(
        name='babip_value',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'iso_value': ColumnSpec(
        name='iso_value',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'woba_value': ColumnSpec(
        name='woba_value',
        dtype='REAL',
        bounds=(0, 2.5)
    ),
    'hit_location': ColumnSpec(
        name='hit_location',
        dtype='SmallInteger',
        bounds=(1, 9)
    ),
    'hc_x': ColumnSpec(
        name='hc_x',
        dtype='REAL',
        bounds=(0, 275)
    ),
    'hc_y': ColumnSpec(
        name='hc_y',
        dtype='REAL',
        bounds=(0, 275)
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
    ),
    'events': ColumnSpec(
        name='events',
        dtype='Text'
    ),

    # Derived Columns
    'is_homerun': ColumnSpec(
        name='is_homerun',
        dtype='Boolean',
        derive=lambda df: df['events'].map(is_homerun)
    )
}

def rule_in_play(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["description"].map(is_bip).fillna(False)
    return df[mask]

STATCAST_BATTED_BALLS_COLUMNS = merge_columns(
    COMMON_PITCH_STAGING_COLUMNS,
    STATCAST_BATTED_BALLS_ONLY
)

STATCAST_BATTED_BALLS_SPEC = TableSpec(
    name='statcast_batted_balls',
    pk = ['game_pk', 'game_counter', 'pitch_number'],
    columns = STATCAST_BATTED_BALLS_COLUMNS,
    row_filters = [
        rule_in_play
    ]
)
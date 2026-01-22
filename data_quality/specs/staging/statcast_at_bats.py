from data_quality.specs.spec_engine import ColumnSpec, TableSpec
from data_quality.specs.shared.statcast_common import COMMON_PITCH_STAGING_COLUMNS
from data_quality.specs.shared.helpers import merge_columns

import numpy as np
import pandas as pd

from utils.statcast_utils import (is_called_strike, is_whiff, is_swing, is_foul)

AT_BAT_CALC: dict[str, ColumnSpec] = {
    'pitch_number': ColumnSpec(
        name='pitch_number',
        dtype='Int64',
        bounds=(1, 135)
    ),
    'bat_score': ColumnSpec(
        name='bat_score',
        dtype='Int64', 
        bounds=(0, 30)
    ),
    'fld_score': ColumnSpec(
        name='fld_score',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'on_1b': ColumnSpec(
        name='on_1b',
        dtype='Int64'
    ),
    'on_2b': ColumnSpec(
        name='on_2b',
        dtype='Int64'
    ),
    'on_3b': ColumnSpec(
        name='on_3b',
        dtype='Int64'
    ),
    'stand': ColumnSpec(
        name='stand',
        dtype='string'
    ),
    'p_throws': ColumnSpec(
        name='p_throws',
        dtype='string'
    ),
    'post_bat_score': ColumnSpec(
        name='post_bat_score',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'is_whiff': ColumnSpec(
        name='is_whiff',
        dtype='boolean',
        derive = lambda df: df['description'].map(is_whiff)
    ),
    'is_called_strike': ColumnSpec(
        name='is_called_strike',
        dtype='boolean',
        derive = lambda df: df['description'].map(is_called_strike)
    ),
    'is_swing': ColumnSpec(
        name='is_swing',
        dtype='boolean',
        derive = lambda df: df['description'].map(is_swing)
    ),
    'is_foul': ColumnSpec(
        name='is_foul',
        dtype='boolean',
        derive=lambda df: df['description'].map(is_foul)
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='Int64',
        bounds=(-30, 30)
    ),
}

STATCAST_AT_BATS_ONLY: dict[str, ColumnSpec] = {
    'inning': ColumnSpec(
        name='inning',
        dtype='Int64',
        bounds=(1, 30)
    ),
    'inning_topbot': ColumnSpec(
        name='inning_topbot',
        dtype='string',
    ),
    'last_pitch_number': ColumnSpec(
        name='last_pitch_number',
        dtype='Int64'
    ),
    'events': ColumnSpec(
        name='events',
        dtype='string'
    ),
    'balls': ColumnSpec(
        name='balls',
        dtype='Int64',
        bounds=(0, 3)
    ),
    'strikes': ColumnSpec(
        name='strikes',
        dtype='Int64',
        bounds=(0, 2)
    ),
    'outs_when_up': ColumnSpec(
        name='outs_when_up',
        dtype='Int64',
        bounds=(0, 2)
    ),
    'times_through_order': ColumnSpec(
        name='times_through_order',
        dtype='Int64',
        bounds=(1, 10),
        original_name='n_thruorder_pitcher'
    ),
    'bat_score': ColumnSpec(
        name='bat_score',
        dtype='Int64', 
        bounds=(0, 30)
    ),
    'fld_score': ColumnSpec(
        name='fld_score',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='Int64',
        bounds=(-30, 30)
    ),
    'post_bat_score': ColumnSpec(
        name='post_bat_score',
        dtype='Int64',
        bounds=(0, 30)
    ),
}

DERIVED_COLUMNS: dict[str, ColumnSpec] = {
    # derived in build_at_bats
    'outs_on_ab': ColumnSpec(
        name='outs_on_ab',
        dtype='Int64',
        bounds=(0, 3)
    ),
    'is_bip': ColumnSpec(
        name='is_bip',
        dtype='boolean'
    ),
    'is_walk': ColumnSpec(
        name='is_walk',
        dtype='boolean'
    ),
    'is_strikeout': ColumnSpec(
        name='is_strikeout',
        dtype='boolean'
    ),
    'rbi': ColumnSpec(
        name='rbi',
        dtype='Int64',
        bounds=(0, 4)
    ),
    'total_whiffs': ColumnSpec(
        name='total_whiffs',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'total_pitches': ColumnSpec(
        name='total_pitches',
        dtype='Int64',
        bounds=(1, 30)
    ),
    'total_called_strikes': ColumnSpec(
        name='total_called_strikes',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'total_swings': ColumnSpec(
        name='total_swings',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'total_fouls': ColumnSpec(
        name='total_fouls',
        dtype='Int64',
        bounds=(0, 30)
    ),
    'pitcher_pa_number': ColumnSpec(
        name='pitcher_pa_number',
        dtype='Int64',
        bounds=(1, 100)
    )
}

STATCAST_AT_BATS_COLUMNS = merge_columns(COMMON_PITCH_STAGING_COLUMNS, STATCAST_AT_BATS_ONLY, DERIVED_COLUMNS)
STATCAST_AT_BATS_INPUT = merge_columns(COMMON_PITCH_STAGING_COLUMNS, STATCAST_AT_BATS_ONLY, AT_BAT_CALC)

STATCAST_AT_BATS_INPUT_SPEC = TableSpec(
    'statcast_at_bats_input',
    pk=['game_pk', 'game_counter', 'pitch_number'],
    columns = STATCAST_AT_BATS_INPUT
)

STATCAST_AT_BATS_SPEC = TableSpec(
    'statcast_at_bats',
    pk = ['game_pk', 'game_counter'],
    columns=STATCAST_AT_BATS_COLUMNS
)
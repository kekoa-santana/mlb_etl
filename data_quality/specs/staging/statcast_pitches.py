from data_quality.specs.spec_engine import ColumnSpec, TableSpec
from data_quality.specs.shared.helpers import merge_columns
from data_quality.specs.shared.statcast_common import COMMON_PITCH_STAGING_COLUMNS

from utils.statcast_utils import (is_bip, is_whiff, is_swing, is_called_strike, is_ball, map_pitch_result, is_foul)

import pandas as pd
import numpy as np

STATCAST_PITCH_ONLY: dict[str, ColumnSpec] = {
    'pitch_number': ColumnSpec(
        name='pitch_number',
        dtype='Int64',
        nullable=False,
        bounds=(1, 130)
    ),
    'pitch_type': ColumnSpec(
        name='pitch_type',
        dtype='string'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='string'
    ),
    'release_speed': ColumnSpec(
        name='release_speed',
        dtype='float64',
        bounds=(20, 108)
    ),
    'release_pos_x': ColumnSpec(
        name='release_pos_x',
        dtype='float64',
        bounds=(-6, 6)
    ),
    'release_pos_y': ColumnSpec(
        name='release_pos_y', 
        dtype='float64',
        bounds=(46, 60)
    ),
    'release_pos_z': ColumnSpec(
        name='release_pos_z',
        dtype='float64',
        bounds=(2.5, 9)
    ),
    'release_spin_rate': ColumnSpec(
        name='release_spin_rate',
        dtype='float64',
        bounds=(500, 3500)
    ),
    'release_extension': ColumnSpec(
        name='release_extension',
        dtype='float64',
        bounds=(3.5, 9)
    ),
    'spin_axis': ColumnSpec(
        name='spin_axis',
        dtype='float64',
        bounds=(0, 360)
    ),
    'effective_speed': ColumnSpec(
        name='effective_speed',
        dtype='float64',
        bounds=(60, 110)
    ),
    'pfx_x': ColumnSpec(
        name='pfx_x',
        dtype='float64',
        bounds=(-5, 5)
    ),
    'pfx_z': ColumnSpec(
        name='pfx_z',
        dtype='float64',
        bounds=(-4.5, 4.5)
    ),
    'vy0': ColumnSpec(
        name='vy0',
        dtype='float64',
        bounds=(-180, -50)
    ),
    'vx0': ColumnSpec(
        name='vx0',
        dtype='float64',
        bounds=(-30, 30)
    ),
    'vz0': ColumnSpec(
        name='vz0',
        dtype='float64',
        bounds=(-30, 30)
    ),
    'ax': ColumnSpec(
        name='ax',
        dtype='float64',
        bounds=(-50, 50)
    ),
    'ay': ColumnSpec(
        name='ay',
        dtype='float64',
        bounds=(0, 50)
    ),
    'az': ColumnSpec(
        name='az',
        dtype='float64',
        bounds=(-60, 20)
    ),
    'zone': ColumnSpec(
        name='zone',
        dtype='Int64',
        bounds=(1, 14)
    ),
    'plate_x': ColumnSpec(
        name='plate_x',
        dtype='float64',
        bounds=(-3, 3)
    ),
    'plate_z': ColumnSpec(
        name='plate_z',
        dtype='float64',
        bounds=(0, 7)
    ),
    'sz_top': ColumnSpec(
        name='sz_top',
        dtype='float64'
    ),
    'sz_bot': ColumnSpec(
        name='sz_bot',
        dtype='float64'
    ),
    'p_throws': ColumnSpec(
        name='p_throws',
        dtype='string'
    ),
    'stand': ColumnSpec(
        name='stand',
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
    'inning': ColumnSpec(
        name='inning',
        dtype='Int64'
    ),
    'on_3b': ColumnSpec(
        name='on_3b',
        dtype='Int64'
    ),
    'on_2b': ColumnSpec(
        name='on_2b',
        dtype='Int64'
    ),
    'on_1b': ColumnSpec(
        name='on_1b',
        dtype='Int64'
    ),
    'outs_when_up': ColumnSpec(
        name='outs_when_up',
        dtype='Int64',
        bounds=(0, 2)
    ),
    'home_score': ColumnSpec(
        name='home_score',
        dtype='Int64'
    ),
    'away_score': ColumnSpec(
        name='away_score',
        dtype='Int64'
    ),
    'bat_score': ColumnSpec(
        name='bat_score',
        dtype='Int64'
    ),
    'fld_score': ColumnSpec(
        name='fld_score',
        dtype='Int64'
    ),
    'home_score_diff': ColumnSpec(
        name='home_score_diff',
        dtype='Int64'
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='Int64'
    ),
    'if_fielding_alignment': ColumnSpec(
        name='if_fielding_alignment',
        dtype='string'
    ),
    'of_fielding_alignment': ColumnSpec(
        name='of_fielding_alignment',
        dtype='string'
    ),
    'arm_angle': ColumnSpec(
        name='arm_angle',
        dtype='float64',
        bounds=(-30, 90)
    ),
    'home_team': ColumnSpec(
        name='home_team',
        dtype='string'
    ),
    'away_team': ColumnSpec(
        name='away_team',
        dtype='string'
    ),
    'game_type': ColumnSpec(
        name='game_type',
        dtype='string'
    ),
    
    # Derived Columns
    'pitch_result_type': ColumnSpec(
        name='pitch_result_type',
        dtype='string',
        derive= lambda df: df['description'].map(map_pitch_result)
    ),
    'is_bip': ColumnSpec(
        name='is_bip',
        dtype='boolean',
        derive = lambda df: df['description'].map(is_bip)
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
    'is_ball': ColumnSpec(
        name='is_ball',
        dtype='boolean',
        derive = lambda df: df['description'].map(is_ball)
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
    )
}

STATCAST_PITCH_COLUMNS = merge_columns(COMMON_PITCH_STAGING_COLUMNS, STATCAST_PITCH_ONLY)

def rule_strike_zone(df: pd.DataFrame) -> dict[str, int]:
    invalid = {}

    if "sz_bot" in df.columns and "sz_top" in df.columns:
        sz_bot = pd.to_numeric(df['sz_bot'], errors="coerce")
        sz_top = pd.to_numeric(df['sz_top'], errors='coerce')

        # impossible inversion
        mask_inv = sz_bot.notna() & sz_top.notna() & (sz_bot > sz_top)
        n_inv = int(mask_inv.sum())
        if n_inv:
            df.loc[mask_inv, ['sz_bot', 'sz_top']] = np.nan
        invalid['sz_inverted'] = n_inv

        # implausible absolute values (broad, low false positives)
        mask_abs = (
            (sz_top.notna() & ((sz_top < 2.0) | (sz_top > 5.5))) |
            (sz_bot.notna() & ((sz_bot < 0.5) | (sz_bot > 3.5)))
        )
        n_abs = int(mask_abs.sum())
        if n_abs:
            df.loc[mask_abs, ['sz_top', 'sz_bot']] = np.nan
        invalid['sz_abs_outliers'] = n_abs

        # implausible zone height
        height = sz_top - sz_bot
        mask_h = height.notna() & ((height < 0.5) | (height > 5))
        n_h = int(mask_h.sum())
        if n_h:
            df.loc[mask_h, ['sz_top', 'sz_bot']] = np.nan
        invalid['sz_height_outliers'] = n_h

    return invalid

def rule_effective_speed_delta(df: pd.DataFrame) -> dict[str, int]:
    invalid = {}

    if ('effective_speed' in df.columns) and ('release_speed' in df.columns):
        eff = pd.to_numeric(df['effective_speed'], errors='coerce')
        rel = pd.to_numeric(df['release_speed'], errors = 'coerce')
        delta_speed = eff - rel
        mask_del = eff.notna() & rel.notna() & delta_speed.abs() > 6
        n_del = int(mask_del.sum())
        if n_del:
            df.loc[mask_del, 'effective_speed'] = np.nan
        invalid['effective_speed_invalid'] = n_del
    
    return invalid

STATCAST_PITCHES_SPEC = TableSpec(
    name='statcast_pitches',
    pk=['game_pk', 'game_counter', 'pitch_number'],
    columns = STATCAST_PITCH_COLUMNS,
    table_rules = [
        rule_strike_zone,
        rule_effective_speed_delta
    ]
)
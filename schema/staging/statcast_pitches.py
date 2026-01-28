from schema.spec_engine import ColumnSpec, TableSpec
from schema.shared.helpers import merge_columns
from schema.shared.statcast_common import COMMON_PITCH_STAGING_COLUMNS

from utils.statcast_utils import (is_bip, is_whiff, is_swing, is_called_strike, is_ball, map_pitch_result, is_foul)

import pandas as pd
import numpy as np

STATCAST_PITCH_ONLY: dict[str, ColumnSpec] = {
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
    'pitch_name': ColumnSpec(
        name='pitch_name',
        dtype='Text'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
    ),
    'release_speed': ColumnSpec(
        name='release_speed',
        dtype='REAL',
        bounds=(20, 108)
    ),
    'release_pos_x': ColumnSpec(
        name='release_pos_x',
        dtype='REAL',
        bounds=(-6, 6)
    ),
    'release_pos_y': ColumnSpec(
        name='release_pos_y', 
        dtype='REAL',
        bounds=(46, 60)
    ),
    'release_pos_z': ColumnSpec(
        name='release_pos_z',
        dtype='REAL',
        bounds=(2.5, 9)
    ),
    'release_spin_rate': ColumnSpec(
        name='release_spin_rate',
        dtype='REAL',
        bounds=(500, 3500)
    ),
    'release_extension': ColumnSpec(
        name='release_extension',
        dtype='REAL',
        bounds=(3.5, 9)
    ),
    'spin_axis': ColumnSpec(
        name='spin_axis',
        dtype='REAL',
        bounds=(0, 360)
    ),
    'effective_speed': ColumnSpec(
        name='effective_speed',
        dtype='REAL',
        bounds=(60, 110)
    ),
    'pfx_x': ColumnSpec(
        name='pfx_x',
        dtype='REAL',
        bounds=(-5, 5)
    ),
    'pfx_z': ColumnSpec(
        name='pfx_z',
        dtype='REAL',
        bounds=(-4.5, 4.5)
    ),
    'vy0': ColumnSpec(
        name='vy0',
        dtype='REAL',
        bounds=(-180, -50)
    ),
    'vx0': ColumnSpec(
        name='vx0',
        dtype='REAL',
        bounds=(-30, 30)
    ),
    'vz0': ColumnSpec(
        name='vz0',
        dtype='REAL',
        bounds=(-30, 30)
    ),
    'ax': ColumnSpec(
        name='ax',
        dtype='REAL',
        bounds=(-50, 50)
    ),
    'ay': ColumnSpec(
        name='ay',
        dtype='REAL',
        bounds=(0, 50)
    ),
    'az': ColumnSpec(
        name='az',
        dtype='REAL',
        bounds=(-60, 20)
    ),
    'zone': ColumnSpec(
        name='zone',
        dtype='SmallInteger',
        bounds=(1, 14)
    ),
    'plate_x': ColumnSpec(
        name='plate_x',
        dtype='REAL',
        bounds=(-3, 3)
    ),
    'plate_z': ColumnSpec(
        name='plate_z',
        dtype='REAL',
        bounds=(0, 7)
    ),
    'sz_top': ColumnSpec(
        name='sz_top',
        dtype='REAL'
    ),
    'sz_bot': ColumnSpec(
        name='sz_bot',
        dtype='REAL'
    ),
    'p_throws': ColumnSpec(
        name='p_throws',
        dtype='String(1)'
    ),
    'stand': ColumnSpec(
        name='stand',
        dtype='String(1)'
    ),
    'balls': ColumnSpec(
        name='balls',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'strikes': ColumnSpec(
        name='strikes',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'inning': ColumnSpec(
        name='inning',
        dtype='SmallInteger'
    ),
    'on_3b': ColumnSpec(
        name='on_3b',
        dtype='BigInteger'
    ),
    'on_2b': ColumnSpec(
        name='on_2b',
        dtype='BigInteger'
    ),
    'on_1b': ColumnSpec(
        name='on_1b',
        dtype='BigInteger'
    ),
    'outs_when_up': ColumnSpec(
        name='outs_when_up',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'home_score': ColumnSpec(
        name='home_score',
        dtype='SmallInteger'
    ),
    'away_score': ColumnSpec(
        name='away_score',
        dtype='SmallInteger'
    ),
    'bat_score': ColumnSpec(
        name='bat_score',
        dtype='SmallInteger'
    ),
    'fld_score': ColumnSpec(
        name='fld_score',
        dtype='SmallInteger'
    ),
    'home_score_diff': ColumnSpec(
        name='home_score_diff',
        dtype='SmallInteger'
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='SmallInteger'
    ),
    'if_fielding_alignment': ColumnSpec(
        name='if_fielding_alignment',
        dtype='Text'
    ),
    'of_fielding_alignment': ColumnSpec(
        name='of_fielding_alignment',
        dtype='Text'
    ),
    'arm_angle': ColumnSpec(
        name='arm_angle',
        dtype='REAL',
        bounds=(-30, 90)
    ),
    'home_team': ColumnSpec(
        name='home_team',
        dtype='String(3)'
    ),
    'away_team': ColumnSpec(
        name='away_team',
        dtype='String(3)'
    ),
    'game_type': ColumnSpec(
        name='game_type',
        dtype='String(1)'
    ),
    
    # Derived Columns
    'pitch_result_type': ColumnSpec(
        name='pitch_result_type',
        dtype='Text',
        derive= lambda df: df['description'].map(map_pitch_result)
    ),
    'is_bip': ColumnSpec(
        name='is_bip',
        dtype='Boolean',
        derive = lambda df: df['description'].map(is_bip)
    ),
    'is_whiff': ColumnSpec(
        name='is_whiff',
        dtype='Boolean',
        derive = lambda df: df['description'].map(is_whiff)
    ),
    'is_called_strike': ColumnSpec(
        name='is_called_strike',
        dtype='Boolean',
        derive = lambda df: df['description'].map(is_called_strike)
    ),
    'is_ball': ColumnSpec(
        name='is_ball',
        dtype='Boolean',
        derive = lambda df: df['description'].map(is_ball)
    ),
    'is_swing': ColumnSpec(
        name='is_swing',
        dtype='Boolean',
        derive = lambda df: df['description'].map(is_swing)
    ),
    'is_foul': ColumnSpec(
        name='is_foul',
        dtype='Boolean',
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
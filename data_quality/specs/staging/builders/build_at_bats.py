import pandas as pd
import numpy as np

from utils.statcast_utils import (is_bip, is_walk, is_strikeout)

from data_quality.specs.spec_engine import apply_table_spec
from data_quality.specs.staging.statcast_at_bats import STATCAST_AT_BATS_INPUT_SPEC, STATCAST_AT_BATS_SPEC

OUTS_BY_EVENT = {
    'field_out': 1,
    'strikeout': 1,
    'double_play': 2,
    'force_out': 1,
    'strikeout_double_play': 2,
    'sac_bunt': 1,
    'fielders_choice_out': 1,
    'sac_fly': 1,
    'grounded_into_double_play': 2,
    'fielders_choice': 1,
    'triple_play': 3
}

PITCH_FLAG_TOTALS = {
    'is_whiff': 'total_whiffs', 
    'is_called_strike': 'total_called_strikes',
    'is_swing': 'total_swings',
    'is_foul': 'total_fouls'
    }
    

def build_statcast_at_bats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df, _ = apply_table_spec(df, STATCAST_AT_BATS_INPUT_SPEC)

    required = {"game_pk", "game_counter", "pitch_number", "bat_score", "post_bat_score"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for at-bat build: {sorted(missing)}")

    df_sorted = df.sort_values(by = ['game_pk', 'game_counter', 'pitch_number'])

    flag_sums = (
        df_sorted
            .assign(**{c: df_sorted[c].astype('Int64') for c in PITCH_FLAG_TOTALS.keys()})
            .groupby(['game_pk', 'game_counter'], sort=False)
            .agg(
                **{
                    total_col: (pitch_col, "sum")
                    for pitch_col, total_col in PITCH_FLAG_TOTALS.items()
                }
            )
            .reset_index()
    )

    end = df_sorted.groupby(by=['game_pk', 'game_counter'], sort=False).tail(1)
    
    ab_level = end.sort_values(by=['game_pk', 'game_counter']).copy()
    ab_level = ab_level.sort_values(by=['game_pk', 'pitcher', 'game_counter'])

    ab_level['pitcher_pa_number'] = (
        ab_level.groupby(['game_pk', 'pitcher'], sort=False).cumcount() + 1
    ).astype("Int64")

    ab_level['last_pitch_number'] = ab_level['pitch_number']

    ab_level = ab_level.merge(
        flag_sums,
        on=['game_pk', 'game_counter'],
        how='left',
        validate='one_to_one'
    )

    ab_level['rbi'] = ab_level['post_bat_score'] - ab_level['bat_score']
    ab_level['rbi'] = (
        pd.to_numeric(ab_level['rbi'], errors='coerce')
        .fillna(0)
        .clip(lower=0)
    )

    pitch_counts = (
        df_sorted
            .groupby(['game_pk', 'game_counter'], sort=False)
            .size()
            .rename('total_pitches')
            .reset_index()
    )

    ab_level = ab_level.merge(
        pitch_counts,
        on=['game_pk', 'game_counter'],
        how='left',
        validate='one_to_one'
    )

    ab_level['outs_on_ab'] = ab_level['events'].map(OUTS_BY_EVENT).fillna(0).astype("Int64")

    ab_level['is_bip'] = ab_level['description'].map(is_bip).fillna(False)
    ab_level['is_strikeout'] = ab_level['events'].map(is_strikeout).fillna(False)
    ab_level['is_walk'] = ab_level['events'].map(is_walk).fillna(False)
    
    ab_level, _ = apply_table_spec(ab_level, STATCAST_AT_BATS_SPEC)

    return ab_level
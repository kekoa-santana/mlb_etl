import pandas as pd
import numpy as np

# ----------------------------
#    MAP PITCH RESULT
# ----------------------------

def map_pitch_result(description: str) -> str:
    if pd.isna(description):
        return None

    desc = description.lower()

    if desc in ('swinging_strike', 'swinging_strike_blocked', 'foul_tip'):
        return "whiff"
    if desc == 'called_strike':
        return "called_strike"
    if desc == 'automatic_strike':
        return "automatic_strike"
    if desc in ('ball', 'blocked_ball', 'automatic_ball'):
        return "ball"
    if desc == "hit_into_play":
        return "in_play"
    if desc == 'foul':
        return 'foul'
    if desc == 'hit_by_pitch':
        return 'hit_by_pitch'
    if desc in ('bunt_foul_tip', 'foul_bunt', 'missed_bunt'):
        return 'bunt_strike'
    return 'others'

# ---------------------
#   FLAG HELPERS
# ---------------------

# BIP detection
def is_bip(description: str) -> bool:
    return isinstance(description, str) and description.lower() == 'hit_into_play'

# whiff detection
def is_whiff(description:str) -> bool:
    return isinstance(description, str) and description.lower() in ('swinging_strike', 'swinging_strike_blocked', 'foul_tip')

# called strike detection
def is_called_strike(description: str) -> bool:
    return isinstance(description, str) and description.lower() == 'called_strike'

# ball detection
def is_ball(description: str) -> bool:
    return isinstance(description, str) and description.lower() in ('ball', 'blocked_ball', 'automatic_ball')

def is_swing(description: str) -> bool:
    return isinstance(description, str) and description.lower() in (
        'swinging_strike', 'hit_into_play', 'swinging_strike_blocked', 'foul_tip',
        'foul', 'foul_bunt', 'bunt_foul_tip', 'missed_bunt'
        )

def is_walk(events: str) -> bool:
    return isinstance(events, str) and events.lower() in (
        'intent_walk', 'walk'
    )

def is_strikeout(events: str) -> bool:
    return isinstance(events, str) and events.lower() in (
        'strikeout_double_play', 'strikeout'
    )

def is_homerun(events: str) -> bool:
    return isinstance(events, str) and events.lower() == 'home_run'

def is_foul(description: str) -> bool:
    return isinstance(description, str) and description.lower() == 'foul'

# ---------------------
#    PK ASSURANCE
# ---------------------
def assert_pk_unique(
    df: pd.DataFrame,
    pk_cols: list[str]
    ) -> pd.DataFrame:
    # Find PK duplicates and drop them

    missing = [c for c in pk_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing PK columns: {missing}")

    dup_mask = df.duplicated(subset=pk_cols, keep=False)
    if dup_mask.any():
        n = int(dup_mask.sum())
        sample = df.loc[dup_mask, pk_cols].head(25)

        print(f"[DQ] Dropping {n} duplicate rows based on PK {pk_cols}. Sample keys:\n{sample}")
        sort_cols = [col for col in ['game_pk', 'at_bat_number', 'pitch_number', 'game_date'] if col in df.columns]
        df = df.sort_values(by = sort_cols)
        
        df = df.drop_duplicates(subset=pk_cols, keep="last").copy()

    return df
import os
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
FILE_PATH = os.path.join(BASE_DIR, "data", "statcast_pitching_lad_2025-03-18_2025-11-01_f008ac3a-0f27-4843-b345-95059ed956bf.parquet")

def run_statcast_dq(parquet_path: str):
    df = pd.read_parquet(parquet_path)
    print("\nRunning Data Quality Checks on RAW Statcast Parquet\n")

    # DQ1: Duplicate primary keys
    dupes = df.duplicated(subset=["game_pk", "at_bat_number", "pitch_number"], keep = False)
    if dupes.any():
        print("DQ1 FAIL: Duplicate primary keys detected:")
        print(df.loc[dupes, ["game_pk", "at_bat_number", "pitch_number"]])
    else:
        print("DQ1 PASS: No duplicate primary keys")

    # DQ2: Missing critical identifiers
    null_keys = df[
        df["game_pk"].isna() |
        df["at_bat_number"].isna() |
        df["pitch_number"].isna() |
        df["pitcher"].isna() |
        df["batter"].isna()
    ]

    if not null_keys.empty:
        print("\nDQ2 FAIL: Missing key identifiers:")
        print(null_keys)
    else:
        print("\nDQ2 PASS: All critical identifying fields present")

    # DQ3: Non-sequential pitch numbers
    df_sorted = df.sort_values(["game_pk", "at_bat_number", "pitch_number"])
    df_sorted["next_pitch"] = (
        df_sorted.groupby(["game_pk", "at_bat_number"])["pitch_number"].shift(-1)
    )
    bad_seq = df_sorted[
        (df_sorted["next_pitch"].notna())
        & ((df_sorted["next_pitch"] - df_sorted["pitch_number"])!= 1)
    ]

    if not bad_seq.empty:
        print("\nDQ3 FAIL: Non-sequential pitch_numbers:")
        print(bad_seq[['game_pk', 'at_bat_number', 'pitch_number', 'next_pitch']])
    else:
        print("\nDQ3 PASS: Pitch sequencing valid.")

    # DQ4: Physical constraints for EV & LA
    bad_physics = df[
        ((df["launch_speed"] < 0) | (df["launch_speed"] > 130)) |
        ((df["launch_angle"] < -90) | (df["launch_angle"] > 90))
    ]

    if not bad_physics.empty:
        print("\nDQ4 FAIL: Impossible EV/LA values detected:")
        print(bad_physics[['launch_angle', 'lauch_speed']])
    else:
        print("\nDQ4 PASS: EV/LA values within physical limits.")

    # DQ5: Spin metrics sanity
    bad_spin = df[
        ((df["spin_axis"] < 0) | df["spin_axis"] > 360) |
        (df["release_spin_rate"] < 0) |
        (df["release_spin_rate"] > 4500)
    ]

    if not bad_spin.empty:
        print("\nDQ5 FAIL: Invalid spin metrics detected:")
        print(bad_spin[["spin_axis", "release_spin_rate"]])
    else:
        print("\nDQ5 PASS: Spin metrics valid.")

    #DQ6: Invalid pitch location
    bad_loc = df[
        ((df["plate_x"] < -3) | df["plate_x"] > 3) |
        ((df["plate_z"] < 0) | (df["plate_z"] > 7))
    ]

    if not bad_loc.empty:
        print("\nDQ6 FAIL: Invalid plate_x or plate_z values:")
        print(bad_loc[["plate_x", "plate_z"]])
    else:
        print("\nDQ6 PASS: Realistic pitch locations.")

    #DQ7: BIP Consistency
    bip_missing = df[
        df["description"].str.startswith("hit_into_play", na=False)
        & (df["launch_speed"].isna() | df["launch_angle"].isna())
    ]

    if not bip_missing.empty:
        print("\nDQ7 FAIL: Inconsistent BIP rows found:")
        print(bip_missing[["description", "launch_speed", "launch_angle"]])
    else:
        print("\nDQ7 PASS: BIP rows complete.")

    #DQ8: Events vs description accuracy
    desc_bip = df["description"].str.startswith("hit_into_play", na=False)
    desc_strike = df["description"].str.contains("strike", na=False) | df["description"].eq("foul_tip") | df["description"].eq("missed_bunt")
    desc_ball = df["description"].eq("ball") | df["description"].eq("blocked_ball") | df["description"].eq("automatic_ball")

    event_hit = df["events"].isin(["single", "double", "triple", "home_run"])
    event_walk = df["events"].eq("walk")
    event_strikeout = df["events"].eq("strikeout")

    bad_events = df[
        (event_hit & ~desc_bip) |
        (event_walk & ~desc_ball) |
        (event_strikeout & ~desc_strike)
    ]

    if not bad_events.empty:
        print("\nDQ8 FAIL: events inconsistent with description:")
        print(bad_events[['events', 'description']])
    else:
        print("\nDQ8 PASS: Event/description consistent.")

    #DQ9: Monitering null % for Hawkeye swing percentage
    print("\nDQ9 INFO: Null Percentages")
    print(
        df[['bat_speed', 'swing_length', 'release_spin_rate', 'spin_axis']]
        .isna()
        .mean()
        .rename("pct_null")
    )

    print("\nAll raw Statcast DQ checks complete.\n")


def main():
    run_statcast_dq(FILE_PATH)

if __name__ == "__main__":
    main()
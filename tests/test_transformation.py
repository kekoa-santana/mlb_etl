"""Unit tests for transformation modules — builders, transform_load, staging registry."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from transformation.builders.build_at_bats import build_statcast_at_bats, OUTS_BY_EVENT


# ── OUTS_BY_EVENT mapping ──

class TestOutsByEvent:
    def test_single_outs(self):
        for event in ["field_out", "strikeout", "force_out", "sac_bunt",
                       "fielders_choice_out", "sac_fly", "fielders_choice"]:
            assert OUTS_BY_EVENT[event] == 1, f"{event} should be 1 out"

    def test_double_outs(self):
        for event in ["double_play", "strikeout_double_play",
                       "grounded_into_double_play"]:
            assert OUTS_BY_EVENT[event] == 2, f"{event} should be 2 outs"

    def test_triple_play(self):
        assert OUTS_BY_EVENT["triple_play"] == 3


# ── build_statcast_at_bats ──

class TestBuildStatcastAtBats:
    def _make_pitch_df(self):
        """Create minimal pitch-level DataFrame for at-bat building."""
        return pd.DataFrame({
            "game_pk": [1, 1, 1, 1, 1, 1],
            "at_bat_number": [1, 1, 1, 2, 2, 2],
            "pitch_number": [1, 2, 3, 1, 2, 3],
            "pitcher": [100, 100, 100, 100, 100, 100],
            "batter": [200, 200, 200, 201, 201, 201],
            "game_date": ["2025-06-15"] * 6,
            "description": [
                "called_strike", "ball", "hit_into_play",
                "swinging_strike", "foul", "swinging_strike",
            ],
            "events": [
                None, None, "single",
                None, None, "strikeout",
            ],
            "bat_score": [0, 0, 0, 0, 0, 0],
            "post_bat_score": [0, 0, 0, 0, 0, 0],
            "inning": [1, 1, 1, 1, 1, 1],
            "inning_topbot": ["Top"] * 6,
            "balls": [0, 1, 1, 0, 0, 0],
            "strikes": [1, 1, 1, 1, 1, 2],
            "outs_when_up": [0, 0, 0, 0, 0, 0],
            "on_1b": [None] * 6,
            "on_2b": [None] * 6,
            "on_3b": [None] * 6,
            "fld_score": [0] * 6,
            "stand": ["R"] * 6,
            "p_throws": ["R"] * 6,
            "pitch_type": ["FF"] * 6,
            "release_speed": [95.0] * 6,
            "zone": [5] * 6,
            "plate_x": [0.1] * 6,
            "plate_z": [2.5] * 6,
        })

    def test_produces_one_row_per_at_bat(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        assert len(result) == 2

    def test_total_pitches(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        assert (result["total_pitches"] == 3).all()

    def test_outs_on_ab_mapping(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        single_row = result[result["events"] == "single"]
        strikeout_row = result[result["events"] == "strikeout"]
        assert single_row["outs_on_ab"].iloc[0] == 0
        assert strikeout_row["outs_on_ab"].iloc[0] == 1

    def test_pitcher_pa_number(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        result = result.sort_values("game_counter")
        assert list(result["pitcher_pa_number"]) == [1, 2]

    def test_rbi_clipped_to_zero(self):
        df = self._make_pitch_df()
        # bat_score > post_bat_score (shouldn't happen, but tests clipping)
        df.loc[df["at_bat_number"] == 1, "bat_score"] = 5
        df.loc[df["at_bat_number"] == 1, "post_bat_score"] = 3
        result = build_statcast_at_bats(df)
        assert (result["rbi"] >= 0).all()

    def test_missing_required_columns_raises(self):
        df = pd.DataFrame({"game_pk": [1], "pitch_number": [1]})
        with pytest.raises((ValueError, KeyError)):
            build_statcast_at_bats(df)

    def test_is_bip_flag(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        single_row = result[result["events"] == "single"]
        assert single_row["is_bip"].iloc[0] is True or single_row["is_bip"].iloc[0] == True

    def test_is_strikeout_flag(self):
        df = self._make_pitch_df()
        result = build_statcast_at_bats(df)
        k_row = result[result["events"] == "strikeout"]
        assert k_row["is_strikeout"].iloc[0] is True or k_row["is_strikeout"].iloc[0] == True


# ── transform_load_table helpers ──

class TestAlignDfToTable:
    def test_adds_missing_columns(self):
        from transformation.staging.transform_load_table import align_df_to_table
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = align_df_to_table(df, ["a", "b", "c"])
        assert "c" in result.columns
        assert pd.isna(result.loc[0, "c"])

    def test_drops_extra_columns(self):
        from transformation.staging.transform_load_table import align_df_to_table
        df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
        result = align_df_to_table(df, ["a", "b"])
        assert "extra" not in result.columns

    def test_reorders_columns(self):
        from transformation.staging.transform_load_table import align_df_to_table
        df = pd.DataFrame({"b": [2], "a": [1]})
        result = align_df_to_table(df, ["a", "b"])
        assert list(result.columns) == ["a", "b"]


class TestPrepareForPostgres:
    def test_replaces_empty_strings(self):
        from transformation.staging.transform_load_table import prepare_for_postgres
        from schema.spec_engine import TableSpec, ColumnSpec
        spec = TableSpec(
            name="test", pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer"),
                "val": ColumnSpec(name="val", dtype="Text"),
            },
        )
        df = pd.DataFrame({"id": [1, 2], "val": ["hello", ""]})
        result = prepare_for_postgres(df, spec)
        # Empty string should be replaced (None or pd.NA)
        assert result.loc[1, "val"] is None or pd.isna(result.loc[1, "val"])

    def test_empty_string_not_preserved(self):
        from transformation.staging.transform_load_table import prepare_for_postgres
        from schema.spec_engine import TableSpec, ColumnSpec
        spec = TableSpec(
            name="test", pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer"),
                "val": ColumnSpec(name="val", dtype="Text"),
            },
        )
        df = pd.DataFrame({"id": [1], "val": [""]})
        result = prepare_for_postgres(df, spec)
        # Empty string should become None or NA, not remain as ""
        val = result.loc[0, "val"]
        assert val is None or pd.isna(val)


# ── staging load_table REGISTRY ──

class TestStagingRegistry:
    def test_registry_has_required_keys(self):
        from transformation.staging.load_table import REGISTRY
        required_keys = {"spec", "schema", "table", "constraint", "source"}
        for name, cfg in REGISTRY.items():
            missing = required_keys - set(cfg.keys())
            assert not missing, f"Registry entry '{name}' missing keys: {missing}"

    def test_known_tables(self):
        from transformation.staging.load_table import REGISTRY
        expected = {
            "statcast_pitches", "statcast_batted_balls",
            "statcast_at_bats", "dim_player", "statcast_sprint_speed",
        }
        assert expected == set(REGISTRY.keys())

    def test_unknown_table_raises(self):
        from transformation.staging.load_table import load_table
        with pytest.raises(ValueError, match="Unknown table"):
            load_table("nonexistent_table")

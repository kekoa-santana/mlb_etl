"""Unit tests for utils/statcast_utils.py — pitch classification and PK enforcement."""
import pytest
import pandas as pd
import numpy as np

from utils.statcast_utils import (
    map_pitch_result, is_bip, is_whiff, is_called_strike,
    is_ball, is_swing, is_walk, is_strikeout, is_homerun,
    is_foul, assert_pk_unique,
)


# ── map_pitch_result ──

class TestMapPitchResult:
    @pytest.mark.parametrize("desc,expected", [
        ("swinging_strike", "whiff"),
        ("swinging_strike_blocked", "whiff"),
        ("foul_tip", "whiff"),
        ("called_strike", "called_strike"),
        ("automatic_strike", "automatic_strike"),
        ("ball", "ball"),
        ("blocked_ball", "ball"),
        ("automatic_ball", "ball"),
        ("hit_into_play", "in_play"),
        ("foul", "foul"),
        ("hit_by_pitch", "hit_by_pitch"),
        ("bunt_foul_tip", "bunt_strike"),
        ("foul_bunt", "bunt_strike"),
        ("missed_bunt", "bunt_strike"),
        ("pitchout", "others"),
    ])
    def test_known_descriptions(self, desc, expected):
        assert map_pitch_result(desc) == expected

    def test_case_insensitive(self):
        assert map_pitch_result("SWINGING_STRIKE") == "whiff"
        assert map_pitch_result("Called_Strike") == "called_strike"

    def test_nan_returns_none(self):
        assert map_pitch_result(np.nan) is None
        assert map_pitch_result(None) is None

    def test_unknown_returns_others(self):
        assert map_pitch_result("some_unknown_type") == "others"


# ── Flag helpers ──

class TestFlagHelpers:
    def test_is_bip(self):
        assert is_bip("hit_into_play") is True
        assert is_bip("HIT_INTO_PLAY") is True
        assert is_bip("foul") is False
        assert is_bip(np.nan) is False
        assert is_bip(None) is False
        assert is_bip(42) is False

    def test_is_whiff(self):
        assert is_whiff("swinging_strike") is True
        assert is_whiff("swinging_strike_blocked") is True
        assert is_whiff("foul_tip") is True
        assert is_whiff("called_strike") is False

    def test_is_called_strike(self):
        assert is_called_strike("called_strike") is True
        assert is_called_strike("ball") is False

    def test_is_ball(self):
        assert is_ball("ball") is True
        assert is_ball("blocked_ball") is True
        assert is_ball("automatic_ball") is True
        assert is_ball("called_strike") is False

    def test_is_swing(self):
        assert is_swing("swinging_strike") is True
        assert is_swing("hit_into_play") is True
        assert is_swing("foul") is True
        assert is_swing("foul_bunt") is True
        assert is_swing("called_strike") is False
        assert is_swing("ball") is False

    def test_is_walk(self):
        assert is_walk("walk") is True
        assert is_walk("intent_walk") is True
        assert is_walk("strikeout") is False
        assert is_walk(np.nan) is False

    def test_is_strikeout(self):
        assert is_strikeout("strikeout") is True
        assert is_strikeout("strikeout_double_play") is True
        assert is_strikeout("walk") is False

    def test_is_homerun(self):
        assert is_homerun("home_run") is True
        assert is_homerun("single") is False

    def test_is_foul(self):
        assert is_foul("foul") is True
        assert is_foul("foul_tip") is False


# ── assert_pk_unique ──

class TestAssertPkUnique:
    def test_no_duplicates(self):
        df = pd.DataFrame({"game_pk": [1, 2, 3], "val": ["a", "b", "c"]})
        result = assert_pk_unique(df, ["game_pk"])
        assert len(result) == 3

    def test_drops_duplicates_keeps_last(self):
        df = pd.DataFrame({
            "game_pk": [1, 1, 2],
            "val": ["first", "second", "third"],
        })
        result = assert_pk_unique(df, ["game_pk"])
        assert len(result) == 2
        # Should keep last occurrence
        row_1 = result[result["game_pk"] == 1]
        assert row_1["val"].iloc[0] == "second"

    def test_composite_pk(self):
        df = pd.DataFrame({
            "game_pk": [1, 1, 1],
            "pitch_number": [1, 2, 1],
            "val": ["a", "b", "c"],
        })
        result = assert_pk_unique(df, ["game_pk", "pitch_number"])
        assert len(result) == 2

    def test_missing_pk_column_raises(self):
        df = pd.DataFrame({"game_pk": [1, 2]})
        with pytest.raises(ValueError, match="Missing PK columns"):
            assert_pk_unique(df, ["game_pk", "nonexistent"])

    def test_empty_dataframe(self):
        df = pd.DataFrame({"game_pk": pd.Series(dtype="int64")})
        result = assert_pk_unique(df, ["game_pk"])
        assert len(result) == 0

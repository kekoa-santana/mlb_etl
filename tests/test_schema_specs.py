"""Unit tests for schema spec definitions — validates all TableSpec/ColumnSpec are well-formed."""
import pytest

from schema.spec_engine import TableSpec, ColumnSpec


def _get_all_specs():
    """Collect all TableSpec objects across the project."""
    specs = {}

    from schema.staging.statcast_pitches import STATCAST_PITCHES_SPEC
    specs["statcast_pitches"] = STATCAST_PITCHES_SPEC

    from schema.staging.statcast_at_bats import STATCAST_AT_BATS_SPEC
    specs["statcast_at_bats"] = STATCAST_AT_BATS_SPEC

    from schema.staging.statcast_batted_balls import STATCAST_BATTED_BALLS_SPEC
    specs["statcast_batted_balls"] = STATCAST_BATTED_BALLS_SPEC

    from schema.staging.statcast_sprint_speed import STATCAST_SPRINT_SPEC
    specs["statcast_sprint_speed"] = STATCAST_SPRINT_SPEC

    from schema.staging.milb_game_logs import (
        MILB_BATTING_GAME_LOGS_SPEC, MILB_PITCHING_GAME_LOGS_SPEC,
    )
    specs["milb_batting_game_logs"] = MILB_BATTING_GAME_LOGS_SPEC
    specs["milb_pitching_game_logs"] = MILB_PITCHING_GAME_LOGS_SPEC

    from schema.production.dim_tables import DIM_PLAYER_SPEC, DIM_TEAM_SPEC, DIM_GAME_SPEC
    specs["dim_player"] = DIM_PLAYER_SPEC
    specs["dim_team"] = DIM_TEAM_SPEC
    specs["dim_game"] = DIM_GAME_SPEC

    from schema.production.fact_tables import FACT_PA_SPEC, FACT_PITCH_SPEC
    specs["fact_pa"] = FACT_PA_SPEC
    specs["fact_pitch"] = FACT_PITCH_SPEC

    from schema.production.sat_tables import SAT_PITCH_SHAPE_SPEC, SAT_BATTED_BALLS_SPEC
    specs["sat_pitch_shape"] = SAT_PITCH_SHAPE_SPEC
    specs["sat_batted_balls"] = SAT_BATTED_BALLS_SPEC

    from schema.production.dim_prospects import DIM_PROSPECTS_SPEC
    specs["dim_prospects"] = DIM_PROSPECTS_SPEC

    from schema.production.transaction_tables import DIM_TRANSACTION_SPEC
    specs["dim_transaction"] = DIM_TRANSACTION_SPEC

    from schema.raw.landing_statcast_files import LANDING_STATCAST_FILES_SPEC
    specs["landing_statcast_files"] = LANDING_STATCAST_FILES_SPEC

    from schema.raw.transactions import RAW_TRANSACTIONS_SPEC
    specs["raw_transactions"] = RAW_TRANSACTIONS_SPEC

    return specs


ALL_SPECS = _get_all_specs()

VALID_DTYPES = {
    "BigInteger", "SmallInteger", "Integer", "Text", "REAL", "Float",
    "DATE", "DateTime", "Boolean", "UUID", "JSONB",
    "TIMESTAMP(timezone=True)",
    None,
}

# Some dtypes use patterns like VARCHAR(3), String(N) — handle dynamically
def _is_valid_dtype(dtype):
    if dtype in VALID_DTYPES:
        return True
    if dtype is not None and (dtype.startswith("String(") or dtype.startswith("VARCHAR(")):
        return True
    return False


class TestAllSpecs:
    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_spec_is_tablespec(self, name):
        assert isinstance(ALL_SPECS[name], TableSpec)

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_spec_has_name(self, name):
        assert ALL_SPECS[name].name, f"{name} has empty name"

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_spec_has_pk(self, name):
        assert len(ALL_SPECS[name].pk) > 0, f"{name} has no PK defined"

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_pk_columns_exist_in_spec(self, name):
        spec = ALL_SPECS[name]
        col_names = {cs.name for cs in spec.columns.values()}
        for pk in spec.pk:
            assert pk in col_names, f"{name}: PK '{pk}' not found in columns"

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_pk_columns_are_not_nullable(self, name):
        spec = ALL_SPECS[name]
        for pk in spec.pk:
            colspec = spec.columns.get(pk)
            if colspec:
                assert not colspec.nullable, f"{name}: PK '{pk}' should not be nullable"

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_column_dtypes_are_valid(self, name):
        spec = ALL_SPECS[name]
        for col_name, colspec in spec.columns.items():
            assert _is_valid_dtype(colspec.dtype), (
                f"{name}.{col_name}: dtype '{colspec.dtype}' not in valid set"
            )

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_column_names_are_strings(self, name):
        """Verify all column names are non-empty strings."""
        spec = ALL_SPECS[name]
        for key, colspec in spec.columns.items():
            assert isinstance(colspec.name, str) and len(colspec.name) > 0, (
                f"{name}: column '{key}' has invalid name"
            )

    @pytest.mark.parametrize("name", list(ALL_SPECS.keys()))
    def test_bounds_are_tuples(self, name):
        spec = ALL_SPECS[name]
        for col_name, colspec in spec.columns.items():
            if colspec.bounds is not None:
                assert isinstance(colspec.bounds, tuple), (
                    f"{name}.{col_name}: bounds must be tuple"
                )
                assert len(colspec.bounds) == 2, (
                    f"{name}.{col_name}: bounds must have exactly 2 elements"
                )
                assert colspec.bounds[0] <= colspec.bounds[1], (
                    f"{name}.{col_name}: bounds lo > hi"
                )


class TestDimProspectsSpec:
    def test_composite_pk(self):
        from schema.production.dim_prospects import DIM_PROSPECTS_SPEC
        assert DIM_PROSPECTS_SPEC.pk == ["player_id", "season"]

    def test_has_bio_columns(self):
        from schema.production.dim_prospects import DIM_PROSPECTS_SPEC
        cols = set(DIM_PROSPECTS_SPEC.columns.keys())
        bio_cols = {"birth_date", "bat_side", "pitch_hand", "height", "weight"}
        assert bio_cols.issubset(cols)

    def test_has_org_columns(self):
        from schema.production.dim_prospects import DIM_PROSPECTS_SPEC
        cols = set(DIM_PROSPECTS_SPEC.columns.keys())
        org_cols = {"parent_org_id", "parent_org_name", "milb_team_id", "level", "sport_id"}
        assert org_cols.issubset(cols)


class TestMilbGameLogSpecs:
    def test_batting_pk(self):
        from schema.staging.milb_game_logs import MILB_BATTING_GAME_LOGS_SPEC
        assert "batter_id" in MILB_BATTING_GAME_LOGS_SPEC.pk
        assert "game_pk" in MILB_BATTING_GAME_LOGS_SPEC.pk

    def test_pitching_pk(self):
        from schema.staging.milb_game_logs import MILB_PITCHING_GAME_LOGS_SPEC
        assert "pitcher_id" in MILB_PITCHING_GAME_LOGS_SPEC.pk
        assert "game_pk" in MILB_PITCHING_GAME_LOGS_SPEC.pk

    def test_batting_has_level_columns(self):
        from schema.staging.milb_game_logs import MILB_BATTING_GAME_LOGS_SPEC
        cols = set(MILB_BATTING_GAME_LOGS_SPEC.columns.keys())
        assert {"sport_id", "level", "parent_org_id", "season"}.issubset(cols)


class TestFactSpecs:
    def test_fact_pa_has_identity_pk(self):
        from schema.production.fact_tables import FACT_PA_SPEC
        pa_id_col = FACT_PA_SPEC.columns.get("pa_id")
        assert pa_id_col is not None
        assert pa_id_col.identity is True

    def test_fact_pitch_has_identity_pk(self):
        from schema.production.fact_tables import FACT_PITCH_SPEC
        pitch_id_col = FACT_PITCH_SPEC.columns.get("pitch_id")
        assert pitch_id_col is not None
        assert pitch_id_col.identity is True

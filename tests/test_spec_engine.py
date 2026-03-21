"""Unit tests for schema/spec_engine.py — type coercion, bounds, and table spec application."""
import pytest
import pandas as pd
import numpy as np

from schema.spec_engine import (
    ColumnSpec, TableSpec, _coerce_series, _apply_bounds_one, apply_table_spec,
)


# ── _coerce_series ──

class TestCoerceSeries:
    def test_integer_coercion(self):
        s = pd.Series(["1", "2", "bad", None])
        result = _coerce_series(s, "Integer")
        assert result.dtype == "Int64"
        assert result.iloc[0] == 1
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])

    def test_smallint_coercion(self):
        s = pd.Series([10, 20, 30])
        result = _coerce_series(s, "SmallInteger")
        assert result.dtype == "Int64"

    def test_bigint_coercion(self):
        s = pd.Series([1_000_000_000, 2_000_000_000])
        result = _coerce_series(s, "BigInteger")
        assert result.dtype == "Int64"

    def test_real_coercion(self):
        s = pd.Series(["1.5", "2.7", "bad"])
        result = _coerce_series(s, "REAL")
        assert result.dtype == "float64"
        assert np.isclose(result.iloc[0], 1.5)
        assert np.isnan(result.iloc[2])

    def test_text_coercion(self):
        s = pd.Series([1, None, "hello"])
        result = _coerce_series(s, "Text")
        assert result.dtype == "string"

    def test_string_n_coercion(self):
        s = pd.Series(["abc", "def"])
        result = _coerce_series(s, "String(10)")
        assert result.dtype == "string"

    def test_boolean_coercion(self):
        s = pd.Series([True, False, None])
        result = _coerce_series(s, "Boolean")
        assert result.dtype == "boolean"

    def test_date_coercion(self):
        s = pd.Series(["2025-01-01", "bad", None])
        result = _coerce_series(s, "DATE")
        assert pd.api.types.is_datetime64_any_dtype(result)
        assert pd.isna(result.iloc[1])

    def test_datetime_coercion(self):
        s = pd.Series(["2025-01-01 12:00:00"])
        result = _coerce_series(s, "DateTime")
        assert pd.api.types.is_datetime64_any_dtype(result)


# ── _apply_bounds_one ──

class TestApplyBounds:
    def test_within_bounds(self):
        df = pd.DataFrame({"speed": [90.0, 95.0, 100.0]})
        n = _apply_bounds_one(df, "speed", (80.0, 110.0))
        assert n == 0
        assert df["speed"].notna().all()

    def test_out_of_bounds_set_nan(self):
        df = pd.DataFrame({"speed": [50.0, 95.0, 120.0]})
        n = _apply_bounds_one(df, "speed", (80.0, 110.0))
        assert n == 2
        assert pd.isna(df.loc[0, "speed"])
        assert df.loc[1, "speed"] == 95.0
        assert pd.isna(df.loc[2, "speed"])

    def test_non_numeric_coerced(self):
        df = pd.DataFrame({"val": ["5", "bad", "15"]})
        n = _apply_bounds_one(df, "val", (0, 10))
        assert n == 1  # 15 is out of bounds
        assert pd.isna(df.loc[2, "val"])

    def test_boundary_values(self):
        df = pd.DataFrame({"val": [0.0, 10.0]})
        n = _apply_bounds_one(df, "val", (0.0, 10.0))
        assert n == 0  # boundary inclusive


# ── apply_table_spec ──

class TestApplyTableSpec:
    def _make_spec(self, **kwargs):
        defaults = {
            "name": "test_table",
            "pk": ["id"],
            "columns": {
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "value": ColumnSpec(name="value", dtype="REAL"),
            },
        }
        defaults.update(kwargs)
        return TableSpec(**defaults)

    def test_basic_coercion_and_report(self):
        spec = self._make_spec()
        df = pd.DataFrame({"id": ["1", "2"], "value": ["3.14", "2.72"]})
        result, report = apply_table_spec(df, spec)

        assert result["id"].dtype == "Int64"
        assert result["value"].dtype == "float64"
        assert report["rows_in"] == 2
        assert report["rows_out"] == 2

    def test_column_rename(self):
        spec = TableSpec(
            name="test",
            pk=["new_name"],
            columns={
                "new_name": ColumnSpec(
                    name="new_name", dtype="Integer",
                    nullable=False, original_name="old_name"
                ),
            },
        )
        df = pd.DataFrame({"old_name": [1, 2, 3]})
        result, _ = apply_table_spec(df, spec)
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_derived_column(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "doubled": ColumnSpec(
                    name="doubled", dtype="Integer",
                    derive=lambda df: df["id"] * 2,
                ),
            },
        )
        df = pd.DataFrame({"id": [1, 2, 3]})
        result, report = apply_table_spec(df, spec)
        assert list(result["doubled"]) == [2, 4, 6]
        assert report["derived_columns"]["doubled"] is True

    def test_derived_column_missing_dependency(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "bad_derive": ColumnSpec(
                    name="bad_derive", dtype="Integer",
                    derive=lambda df: df["nonexistent"] * 2,
                ),
            },
        )
        df = pd.DataFrame({"id": [1, 2]})
        result, report = apply_table_spec(df, spec)
        assert "failed_missing_dep" in report["derived_columns"]["bad_derive"]

    def test_bounds_enforcement(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "speed": ColumnSpec(name="speed", dtype="REAL", bounds=(80.0, 110.0)),
            },
        )
        df = pd.DataFrame({"id": [1, 2, 3], "speed": [50.0, 95.0, 120.0]})
        result, report = apply_table_spec(df, spec)
        assert pd.isna(result.loc[0, "speed"])
        assert result.loc[1, "speed"] == 95.0
        assert pd.isna(result.loc[2, "speed"])
        assert report["invalid_bounds"]["speed"] == 2

    def test_not_nullable_violations(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "required": ColumnSpec(name="required", dtype="Text", nullable=False),
            },
        )
        df = pd.DataFrame({"id": [1, 2], "required": ["ok", None]})
        _, report = apply_table_spec(df, spec)
        assert report["not_nullable_violations"]["required"] == 1

    def test_pk_dedup(self):
        spec = self._make_spec()
        df = pd.DataFrame({"id": [1, 1, 2], "value": [1.0, 2.0, 3.0]})
        result, report = apply_table_spec(df, spec)
        assert report["rows_in"] == 3
        assert report["rows_out"] == 2

    def test_missing_pk_raises(self):
        spec = self._make_spec()
        df = pd.DataFrame({"value": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Missing PK columns"):
            apply_table_spec(df, spec)

    def test_row_filters(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "keep": ColumnSpec(name="keep", dtype="Boolean"),
            },
            row_filters=[lambda df: df[df["keep"] == True]],
        )
        df = pd.DataFrame({"id": [1, 2, 3], "keep": [True, False, True]})
        result, report = apply_table_spec(df, spec)
        assert report["rows_out"] == 2

    def test_table_rules(self):
        def my_rule(df):
            return {"negative_values": int((df["value"] < 0).sum())}

        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "value": ColumnSpec(name="value", dtype="REAL"),
            },
            table_rules=[my_rule],
        )
        df = pd.DataFrame({"id": [1, 2, 3], "value": [-1.0, 5.0, -2.0]})
        _, report = apply_table_spec(df, spec)
        assert report["rule_violations"]["negative_values"] == 2

    def test_missing_optional_column_skipped(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "optional_col": ColumnSpec(name="optional_col", dtype="Text", nullable=True),
            },
        )
        df = pd.DataFrame({"id": [1, 2]})
        result, report = apply_table_spec(df, spec)
        assert len(result) == 2
        assert "optional_col" not in report["missing_required_columns"]

    def test_missing_required_column_reported(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="Integer", nullable=False),
                "required_col": ColumnSpec(name="required_col", dtype="Text", nullable=False),
            },
        )
        df = pd.DataFrame({"id": [1, 2]})
        _, report = apply_table_spec(df, spec)
        assert "required_col" in report["missing_required_columns"]

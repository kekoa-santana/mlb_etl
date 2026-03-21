"""Unit tests for schema/table_factory.py — dtype parsing and column generation."""
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from schema.table_factory import parse_dtype, spec_to_cols, DTYPE_MAP
from schema.spec_engine import ColumnSpec, TableSpec


class TestParseDtype:
    def test_none_returns_text(self):
        result = parse_dtype(None)
        assert isinstance(result, sa.Text)

    @pytest.mark.parametrize("dtype_str,sa_type", [
        ("BigInteger", sa.BigInteger),
        ("SmallInteger", sa.SmallInteger),
        ("Integer", sa.Integer),
        ("Text", sa.Text),
        ("REAL", sa.REAL),
        ("Float", sa.Float),
        ("DATE", sa.Date),
        ("DateTime", sa.DateTime),
        ("Boolean", sa.Boolean),
        ("UUID", UUID),
    ])
    def test_known_dtypes(self, dtype_str, sa_type):
        result = parse_dtype(dtype_str)
        assert isinstance(result, sa_type)

    def test_string_n(self):
        result = parse_dtype("String(50)")
        assert isinstance(result, sa.String)
        assert result.length == 50

    def test_string_different_lengths(self):
        for n in [1, 10, 255]:
            result = parse_dtype(f"String({n})")
            assert result.length == n

    def test_timestamp_with_timezone(self):
        result = parse_dtype("TIMESTAMP(timezone=True)")
        assert isinstance(result, sa.TIMESTAMP)
        assert result.timezone is True

    def test_unknown_dtype_returns_text(self):
        result = parse_dtype("UnknownType")
        assert isinstance(result, sa.Text)


class TestSpecToCols:
    def test_basic_columns(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="BigInteger", nullable=False, primary_key=True),
                "name": ColumnSpec(name="name", dtype="Text"),
            },
        )
        cols = spec_to_cols(spec)
        assert len(cols) == 2
        assert cols[0].name == "id"
        assert cols[0].primary_key is True
        assert cols[0].nullable is False
        assert cols[1].name == "name"

    def test_identity_column(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="BigInteger", identity=True, nullable=False),
            },
        )
        cols = spec_to_cols(spec)
        assert len(cols) == 1

    def test_server_default(self):
        spec = TableSpec(
            name="test",
            pk=["id"],
            columns={
                "id": ColumnSpec(name="id", dtype="BigInteger", nullable=False, primary_key=True),
                "created_at": ColumnSpec(
                    name="created_at",
                    dtype="TIMESTAMP(timezone=True)",
                    server_default="now()",
                ),
            },
        )
        cols = spec_to_cols(spec)
        assert cols[1].server_default is not None

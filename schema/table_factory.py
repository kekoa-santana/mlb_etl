from schema.spec_engine import TableSpec

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

DTYPE_MAP = {
    'BigInteger': sa.BigInteger,
    'SmallInteger': sa.SmallInteger,
    'Integer': sa.Integer,
    'Text': sa.Text,
    'REAL': sa.REAL,
    'Float': sa.Float,
    'DATE': sa.Date,
    'DateTime': sa.DateTime,
    'Boolean': sa.Boolean,
    'UUID': UUID,
}

def parse_dtype(dtype: str | None):
    if dtype is None:
        return sa.Text()
    if dtype.startswith('String('):
        length=int(dtype[7:-1]) # extract number from String(N)
        return sa.String(length)
    if dtype == 'TIMESTAMP(timezone=True)':
        return sa.TIMESTAMP(timezone=True)
    return DTYPE_MAP.get(dtype, sa.Text)()

def spec_to_cols(spec: TableSpec) -> []:
    column_list = []

    for _, colspec in spec.columns.items():
        col = sa.Column(
            colspec.name,
            parse_dtype(colspec.dtype),
            sa.Identity() if colspec.identity else None,
            nullable=colspec.nullable,
            primary_key=colspec.primary_key,
            server_default=sa.text(colspec.server_default) if colspec.server_default else None
        )
        column_list.append(col)

    return column_list

def create_table_from_schema(schema: str, spec: TableSpec):
    constraints = []
    if spec.unique_constraints:
        for name, columns in spec.unique_constraints:
            constraints.append(sa.UniqueConstraint(*columns, name=name))

    op.create_table(
        spec.name,
        *spec_to_cols(spec),
        *constraints,
        schema=schema
    )
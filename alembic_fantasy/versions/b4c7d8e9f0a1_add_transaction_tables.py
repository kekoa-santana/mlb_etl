"""add raw.transactions and production.dim_transaction tables

Revision ID: b4c7d8e9f0a1
Revises: a1b2c3d4e5f6
Create Date: 2026-03-09 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import create_table_from_schema
from schema.raw.transactions import RAW_TRANSACTIONS_SPEC
from schema.production.transaction_tables import DIM_TRANSACTION_SPEC

# revision identifiers, used by Alembic.
revision: str = 'b4c7d8e9f0a1'
down_revision: Union[str, Sequence[str]] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    create_table_from_schema('raw', RAW_TRANSACTIONS_SPEC)
    create_table_from_schema('production', DIM_TRANSACTION_SPEC)


def downgrade():
    op.drop_table('dim_transaction', schema='production')
    op.drop_table('transactions', schema='raw')

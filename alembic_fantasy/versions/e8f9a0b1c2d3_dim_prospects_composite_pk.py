"""change dim_prospects PK from player_id to (player_id, season)

Revision ID: e8f9a0b1c2d3
Revises: d7e8f9a0b1c2
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'e8f9a0b1c2d3'
down_revision: Union[str, Sequence[str]] = 'd7e8f9a0b1c2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.drop_constraint('dim_prospects_pkey', 'dim_prospects', schema='production')
    op.create_primary_key(
        'dim_prospects_pkey', 'dim_prospects',
        ['player_id', 'season'],
        schema='production',
    )


def downgrade():
    op.drop_constraint('dim_prospects_pkey', 'dim_prospects', schema='production')
    op.create_primary_key(
        'dim_prospects_pkey', 'dim_prospects',
        ['player_id'],
        schema='production',
    )

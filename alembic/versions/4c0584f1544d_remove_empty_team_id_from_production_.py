"""remove empty team_id from production.fact_pitch

Revision ID: 4c0584f1544d
Revises: 3138c09fbe06
Create Date: 2026-02-11 11:25:45.959156

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4c0584f1544d'
down_revision: Union[str, Sequence[str], None] = '3138c09fbe06'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE production.fact_pitch DROP COLUMN team_id CASCADE')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('fact_pitch', sa.Column('team_id', sa.SmallInteger), schema='production')

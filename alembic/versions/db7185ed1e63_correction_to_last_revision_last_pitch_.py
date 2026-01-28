"""correction to last revision, last_pitch_number missed in production.fact_pa not staging.statcast_at_bats

Revision ID: db7185ed1e63
Revises: 27adfd417580
Create Date: 2026-01-26 11:20:44.665507

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'db7185ed1e63'
down_revision: Union[str, Sequence[str], None] = '27adfd417580'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE production.fact_pa ADD COLUMN last_pitch_number SMALLINT')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('fact_pa', 'last_pitch_number', schema='production')

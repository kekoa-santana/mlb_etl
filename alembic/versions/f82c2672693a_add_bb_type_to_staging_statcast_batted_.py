"""add bb_type to staging.statcast_batted_balls

Revision ID: f82c2672693a
Revises: 8b6c04d72a6c
Create Date: 2026-01-26 11:45:27.397512

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f82c2672693a'
down_revision: Union[str, Sequence[str], None] = '8b6c04d72a6c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE staging.statcast_batted_balls ADD COLUMN bb_type TEXT')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('statcast_batted_balls', 'bb_type', schema='staging')
    
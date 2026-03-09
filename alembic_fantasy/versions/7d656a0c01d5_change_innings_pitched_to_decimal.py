"""change innings_pitched to decimal

Revision ID: 7d656a0c01d5
Revises: bf315ab50925
Create Date: 2026-02-25 11:57:06.879377

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d656a0c01d5'
down_revision: Union[str, Sequence[str], None] = 'bf315ab50925'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE staging.pitching_boxscores ALTER COLUMN innings_pitched TYPE REAL')


def downgrade() -> None:
    """Downgrade schema."""
    pass

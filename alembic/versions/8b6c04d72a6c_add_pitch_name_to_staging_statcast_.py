"""add pitch_name to staging.statcast_pitches, missed in original spec

Revision ID: 8b6c04d72a6c
Revises: b2ac8abca607
Create Date: 2026-01-26 11:42:49.825315

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8b6c04d72a6c'
down_revision: Union[str, Sequence[str], None] = 'b2ac8abca607'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

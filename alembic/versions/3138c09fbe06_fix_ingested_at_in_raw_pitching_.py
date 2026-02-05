"""fix ingested_at in raw.pitching_boxscores

Revision ID: 3138c09fbe06
Revises: c44043547519
Create Date: 2026-01-28 17:02:45.851307

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision: str = '3138c09fbe06'
down_revision: Union[str, Sequence[str], None] = 'c44043547519'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
"""change staging.pitching_boxscores columns: strike_out to strike_outs and complete_game to bool instead of smallint

Revision ID: c44043547519
Revises: fd7acfeeb82a
Create Date: 2026-01-28 15:00:56.212837

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c44043547519'
down_revision: Union[str, Sequence[str], None] = 'fd7acfeeb82a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass

def downgrade() -> None:
    """Downgrade schema."""
    pass

"""fix naming of home/away team wins

Revision ID: bf315ab50925
Revises: ca3831ae2b67
Create Date: 2026-02-23 22:00:09.187343

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bf315ab50925'
down_revision: Union[str, Sequence[str], None] = 'ca3831ae2b67'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN home_wins TO home_team_wins;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN home_losses TO home_team_losses;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN away_wins TO away_team_wins;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN away_losses TO away_team_losses;')


def downgrade() -> None:
    """Downgrade schema."""
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN home_team_wins TO home_wins;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN home_team_losses TO home_losses;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN away_team_wins TO away_wins;')
    op.execute('ALTER TABLE production.dim_game RENAME COLUMN away_team_losses TO away_losses;')

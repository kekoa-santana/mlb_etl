"""fix primary and constraint key

Revision ID: c3bdd819d2ea
Revises: 804422e8cd2e
Create Date: 2026-01-24 19:42:08.878130

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3bdd819d2ea'
down_revision: Union[str, Sequence[str], None] = '804422e8cd2e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint(
        'landing_boxscores_game_pk_unique',
        'landing_boxscores',
        ['game_pk'],
        schema='raw'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        'landing_boxscores_game_pk_unique',
        'landing_boxscores',
        schema='raw',
        type_='unique'
    )
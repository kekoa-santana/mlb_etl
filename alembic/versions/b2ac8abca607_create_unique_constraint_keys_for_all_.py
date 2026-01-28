"""create unique constraint keys for all of my production tables

Revision ID: b2ac8abca607
Revises: db7185ed1e63
Create Date: 2026-01-26 11:34:54.394761

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2ac8abca607'
down_revision: Union[str, Sequence[str], None] = 'db7185ed1e63'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint(
        'uq_fact_pa_natural', 
        'fact_pa', 
        columns=['game_pk', 'game_counter'],
        schema='production')
    op.create_unique_constraint(
        'uq_fact_pitch_natural', 
        'fact_pitch', 
        columns=['game_pk', 'game_counter', 'pitch_number'],
        schema='production')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_fact_pa_natural', 'fact_pa', type_='unique', schema='production')
    op.drop_constraint('uq_fact_pitch_natural', 'fact_pitch', type_='unique', schema='production')

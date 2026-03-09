"""add fact_lineup table

Revision ID: a1b2c3d4e5f6
Revises: 7d656a0c01d5
Create Date: 2026-03-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '7d656a0c01d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'fact_lineup',
        sa.Column('game_pk', sa.BigInteger, nullable=False),
        sa.Column('player_id', sa.BigInteger, nullable=False),
        sa.Column('team_id', sa.Integer, nullable=False),
        sa.Column('batting_order', sa.SmallInteger, nullable=False),
        sa.Column('is_starter', sa.Boolean, nullable=False),
        sa.Column('position', sa.String(4)),
        sa.Column('home_away', sa.String(4)),
        sa.Column('season', sa.Integer),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('game_pk', 'player_id'),
        schema='production',
    )
    op.create_index(
        'ix_fact_lineup_player_season',
        'fact_lineup',
        ['player_id', 'season'],
        schema='production',
    )
    op.create_index(
        'ix_fact_lineup_season_order',
        'fact_lineup',
        ['season', 'batting_order'],
        schema='production',
    )


def downgrade() -> None:
    op.drop_index('ix_fact_lineup_season_order', table_name='fact_lineup', schema='production')
    op.drop_index('ix_fact_lineup_player_season', table_name='fact_lineup', schema='production')
    op.drop_table('fact_lineup', schema='production')

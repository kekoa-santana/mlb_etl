"""add live_pitches and live_batted_balls staging tables

Revision ID: h3c4d5e6f7g8
Revises: g2b3c4d5e6f7
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import spec_to_cols
from schema.staging.live_pitches import LIVE_PITCHES_SPEC
from schema.staging.live_batted_balls import LIVE_BATTED_BALLS_SPEC

# revision identifiers, used by Alembic.
revision: str = 'h3c4d5e6f7g8'
down_revision: Union[str, Sequence[str]] = 'g2b3c4d5e6f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_with_pk(schema, spec):
    """Create table from spec with composite PK."""
    op.create_table(
        spec.name,
        *spec_to_cols(spec),
        sa.PrimaryKeyConstraint(*spec.pk),
        schema=schema,
    )


def upgrade():
    _create_with_pk('staging', LIVE_PITCHES_SPEC)
    _create_with_pk('staging', LIVE_BATTED_BALLS_SPEC)

    op.create_index(
        'ix_live_pitches_game_pitcher',
        'live_pitches',
        ['game_pk', 'pitcher_id'],
        schema='staging',
    )
    op.create_index(
        'ix_live_batted_balls_game',
        'live_batted_balls',
        ['game_pk'],
        schema='staging',
    )


def downgrade():
    op.drop_index('ix_live_batted_balls_game', table_name='live_batted_balls', schema='staging')
    op.drop_index('ix_live_pitches_game_pitcher', table_name='live_pitches', schema='staging')
    op.drop_table('live_batted_balls', schema='staging')
    op.drop_table('live_pitches', schema='staging')

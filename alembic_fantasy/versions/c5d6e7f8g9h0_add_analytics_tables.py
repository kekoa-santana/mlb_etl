"""add analytics tables (rolling, platoon, streak, matchup)

Revision ID: c5d6e7f8g9h0
Revises: e8f9a0b1c2d3
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import create_table_from_schema
from schema.production.analytics_tables import (
    FACT_PLAYER_FORM_ROLLING_SPEC,
    FACT_PLATOON_SPLITS_SPEC,
    FACT_STREAK_INDICATOR_SPEC,
    FACT_MATCHUP_HISTORY_SPEC,
)

# revision identifiers, used by Alembic.
revision: str = 'c5d6e7f8g9h0'
down_revision: Union[str, Sequence[str]] = 'e8f9a0b1c2d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_with_pk(schema, spec):
    """Create table from spec and add composite PK from spec.pk."""
    from schema.table_factory import spec_to_cols
    op.create_table(
        spec.name,
        *spec_to_cols(spec),
        sa.PrimaryKeyConstraint(*spec.pk),
        schema=schema,
    )


def upgrade():
    _create_with_pk('production', FACT_PLAYER_FORM_ROLLING_SPEC)
    _create_with_pk('production', FACT_PLATOON_SPLITS_SPEC)
    _create_with_pk('production', FACT_STREAK_INDICATOR_SPEC)
    _create_with_pk('production', FACT_MATCHUP_HISTORY_SPEC)

    # indexes for common query patterns
    op.create_index(
        'ix_rolling_player_date',
        'fact_player_form_rolling',
        ['player_id', 'game_date'],
        schema='production',
    )
    op.create_index(
        'ix_streak_player_date',
        'fact_streak_indicator',
        ['player_id', 'game_date'],
        schema='production',
    )
    op.create_index(
        'ix_platoon_season_role',
        'fact_platoon_splits',
        ['season', 'player_role'],
        schema='production',
    )
    op.create_index(
        'ix_matchup_pitcher',
        'fact_matchup_history',
        ['pitcher_id'],
        schema='production',
    )


def downgrade():
    op.drop_index('ix_matchup_pitcher', table_name='fact_matchup_history', schema='production')
    op.drop_index('ix_platoon_season_role', table_name='fact_platoon_splits', schema='production')
    op.drop_index('ix_streak_player_date', table_name='fact_streak_indicator', schema='production')
    op.drop_index('ix_rolling_player_date', table_name='fact_player_form_rolling', schema='production')
    op.drop_table('fact_matchup_history', schema='production')
    op.drop_table('fact_streak_indicator', schema='production')
    op.drop_table('fact_platoon_splits', schema='production')
    op.drop_table('fact_player_form_rolling', schema='production')

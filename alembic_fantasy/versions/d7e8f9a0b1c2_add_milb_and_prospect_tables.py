"""add milb game log and dim_prospects tables

Revision ID: d7e8f9a0b1c2
Revises: b4c7d8e9f0a1
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import create_table_from_schema
from schema.staging.milb_game_logs import (
    MILB_BATTING_GAME_LOGS_SPEC,
    MILB_PITCHING_GAME_LOGS_SPEC,
)
from schema.production.dim_prospects import DIM_PROSPECTS_SPEC

# revision identifiers, used by Alembic.
revision: str = 'd7e8f9a0b1c2'
down_revision: Union[str, Sequence[str]] = 'b4c7d8e9f0a1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    create_table_from_schema('staging', MILB_BATTING_GAME_LOGS_SPEC)
    create_table_from_schema('staging', MILB_PITCHING_GAME_LOGS_SPEC)
    create_table_from_schema('production', DIM_PROSPECTS_SPEC)

    # Indexes for common query patterns
    op.create_index(
        'ix_milb_batting_player_season',
        'milb_batting_game_logs',
        ['batter_id', 'season'],
        schema='staging',
    )
    op.create_index(
        'ix_milb_batting_level_date',
        'milb_batting_game_logs',
        ['level', 'game_date'],
        schema='staging',
    )
    op.create_index(
        'ix_milb_pitching_player_season',
        'milb_pitching_game_logs',
        ['pitcher_id', 'season'],
        schema='staging',
    )
    op.create_index(
        'ix_milb_pitching_level_date',
        'milb_pitching_game_logs',
        ['level', 'game_date'],
        schema='staging',
    )
    op.create_index(
        'ix_dim_prospects_org_level',
        'dim_prospects',
        ['parent_org_id', 'level'],
        schema='production',
    )


def downgrade():
    op.drop_index('ix_dim_prospects_org_level', table_name='dim_prospects', schema='production')
    op.drop_index('ix_milb_pitching_level_date', table_name='milb_pitching_game_logs', schema='staging')
    op.drop_index('ix_milb_pitching_player_season', table_name='milb_pitching_game_logs', schema='staging')
    op.drop_index('ix_milb_batting_level_date', table_name='milb_batting_game_logs', schema='staging')
    op.drop_index('ix_milb_batting_player_season', table_name='milb_batting_game_logs', schema='staging')
    op.drop_table('dim_prospects', schema='production')
    op.drop_table('milb_pitching_game_logs', schema='staging')
    op.drop_table('milb_batting_game_logs', schema='staging')

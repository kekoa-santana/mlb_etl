"""add player game, status timeline, milb, prospect, and projection tables

Revision ID: g2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from schema.table_factory import spec_to_cols
from schema.production.player_game_tables import (
    FACT_PLAYER_GAME_MLB_SPEC,
    FACT_PLAYER_STATUS_TIMELINE_SPEC,
)
from schema.production.milb_prospect_tables import (
    FACT_MILB_PLAYER_GAME_SPEC,
    FACT_PROSPECT_SNAPSHOT_SPEC,
    FACT_PROSPECT_TRANSITION_SPEC,
)
from schema.production.projection_tables import (
    DIM_MODEL_RUN_SPEC,
    FACT_PLAYER_PROJECTION_SPEC,
    FACT_PROJECTION_BACKTEST_SPEC,
)

# revision identifiers, used by Alembic.
revision: str = 'g2b3c4d5e6f7'
down_revision: Union[str, Sequence[str]] = 'f1a2b3c4d5e6'
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


def _create_projection_table(schema, name, pk_cols, columns):
    """Create projection table with UUID run_id columns."""
    op.create_table(
        name,
        *columns,
        sa.PrimaryKeyConstraint(*pk_cols),
        schema=schema,
    )


def upgrade():
    # -- player game + status timeline --
    _create_with_pk('production', FACT_PLAYER_GAME_MLB_SPEC)
    _create_with_pk('production', FACT_PLAYER_STATUS_TIMELINE_SPEC)

    # -- milb + prospect tables --
    _create_with_pk('production', FACT_MILB_PLAYER_GAME_SPEC)
    _create_with_pk('production', FACT_PROSPECT_SNAPSHOT_SPEC)
    _create_with_pk('production', FACT_PROSPECT_TRANSITION_SPEC)

    # -- projection tables (UUID columns need special handling) --
    op.create_table(
        'dim_model_run',
        sa.Column('run_id', UUID(as_uuid=True), nullable=False, server_default=sa.text('gen_random_uuid()')),
        sa.Column('model_name', sa.Text(), nullable=False),
        sa.Column('model_version', sa.Text()),
        sa.Column('feature_cutoff_date', sa.Date()),
        sa.Column('train_start_date', sa.Date()),
        sa.Column('train_end_date', sa.Date()),
        sa.Column('target_variable', sa.Text()),
        sa.Column('hyperparameters', sa.Text()),
        sa.Column('notes', sa.Text()),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('run_id'),
        schema='production',
    )
    op.create_table(
        'fact_player_projection',
        sa.Column('run_id', UUID(as_uuid=True), nullable=False),
        sa.Column('player_id', sa.BigInteger(), nullable=False),
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('horizon', sa.String(10), nullable=False),
        sa.Column('scenario', sa.String(10), nullable=False),
        sa.Column('player_role', sa.String(7)),
        sa.Column('projected_pts_p10', sa.REAL()),
        sa.Column('projected_pts_p50', sa.REAL()),
        sa.Column('projected_pts_p90', sa.REAL()),
        sa.Column('projected_pa', sa.REAL()),
        sa.Column('projected_ip', sa.REAL()),
        sa.Column('projected_avg', sa.REAL()),
        sa.Column('projected_obp', sa.REAL()),
        sa.Column('projected_slg', sa.REAL()),
        sa.Column('projected_era', sa.REAL()),
        sa.Column('projected_whip', sa.REAL()),
        sa.Column('projected_k9', sa.REAL()),
        sa.Column('confidence_score', sa.REAL()),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('run_id', 'player_id', 'as_of_date', 'horizon', 'scenario'),
        schema='production',
    )
    op.create_table(
        'fact_projection_backtest',
        sa.Column('run_id', UUID(as_uuid=True), nullable=False),
        sa.Column('player_id', sa.BigInteger(), nullable=False),
        sa.Column('target_date', sa.Date(), nullable=False),
        sa.Column('metric', sa.String(20), nullable=False),
        sa.Column('predicted_value', sa.REAL()),
        sa.Column('actual_value', sa.REAL()),
        sa.Column('error', sa.REAL()),
        sa.Column('abs_error', sa.REAL()),
        sa.Column('pct_error', sa.REAL()),
        sa.Column('within_p10_p90', sa.Boolean()),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('run_id', 'player_id', 'target_date', 'metric'),
        schema='production',
    )

    # -- indexes --
    op.create_index('ix_player_game_mlb_date', 'fact_player_game_mlb', ['player_id', 'game_date'], schema='production')
    op.create_index('ix_status_timeline_player', 'fact_player_status_timeline', ['player_id', 'season'], schema='production')
    op.create_index('ix_milb_player_game_date', 'fact_milb_player_game', ['player_id', 'game_date'], schema='production')
    op.create_index('ix_milb_player_game_level', 'fact_milb_player_game', ['sport_id', 'season'], schema='production')
    op.create_index('ix_prospect_snapshot_org', 'fact_prospect_snapshot', ['parent_org_id', 'season'], schema='production')
    op.create_index('ix_prospect_transition_season', 'fact_prospect_transition', ['season'], schema='production')
    op.create_index('ix_projection_player', 'fact_player_projection', ['player_id', 'as_of_date'], schema='production')
    op.create_index('ix_backtest_run', 'fact_projection_backtest', ['run_id', 'metric'], schema='production')


def downgrade():
    op.drop_index('ix_backtest_run', table_name='fact_projection_backtest', schema='production')
    op.drop_index('ix_projection_player', table_name='fact_player_projection', schema='production')
    op.drop_index('ix_prospect_transition_season', table_name='fact_prospect_transition', schema='production')
    op.drop_index('ix_prospect_snapshot_org', table_name='fact_prospect_snapshot', schema='production')
    op.drop_index('ix_milb_player_game_level', table_name='fact_milb_player_game', schema='production')
    op.drop_index('ix_milb_player_game_date', table_name='fact_milb_player_game', schema='production')
    op.drop_index('ix_status_timeline_player', table_name='fact_player_status_timeline', schema='production')
    op.drop_index('ix_player_game_mlb_date', table_name='fact_player_game_mlb', schema='production')
    op.drop_table('fact_projection_backtest', schema='production')
    op.drop_table('fact_player_projection', schema='production')
    op.drop_table('dim_model_run', schema='production')
    op.drop_table('fact_prospect_transition', schema='production')
    op.drop_table('fact_prospect_snapshot', schema='production')
    op.drop_table('fact_milb_player_game', schema='production')
    op.drop_table('fact_player_status_timeline', schema='production')
    op.drop_table('fact_player_game_mlb', schema='production')

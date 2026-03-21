"""add advanced stats tables (batting, pitching, pitch type run values)

Revision ID: k6f7g8h9i0j1
Revises: j5e6f7g8h9i0
Create Date: 2026-03-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import spec_to_cols
from schema.production.advanced_stats_tables import (
    FACT_BATTING_ADVANCED_SPEC,
    FACT_PITCHING_ADVANCED_SPEC,
    FACT_PITCH_TYPE_RV_SPEC,
)

# revision identifiers, used by Alembic.
revision: str = 'k6f7g8h9i0j1'
down_revision: Union[str, Sequence[str]] = 'j5e6f7g8h9i0'
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


def _column_exists(schema, table, column):
    """Check if a column already exists."""
    from alembic import context
    conn = context.get_bind()
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table AND column_name = :col"
    ), {'schema': schema, 'table': table, 'col': column})
    return result.fetchone() is not None


def upgrade():
    _create_with_pk('production', FACT_BATTING_ADVANCED_SPEC)
    _create_with_pk('production', FACT_PITCHING_ADVANCED_SPEC)
    _create_with_pk('production', FACT_PITCH_TYPE_RV_SPEC)

    # Add delta_run_exp to existing tables (skip if already added manually)
    for col, dtype in [('delta_run_exp', sa.REAL()),
                       ('woba_denom', sa.SmallInteger()),
                       ('woba_value', sa.REAL())]:
        if not _column_exists('staging', 'statcast_pitches', col):
            op.add_column('statcast_pitches', sa.Column(col, dtype), schema='staging')

    if not _column_exists('production', 'fact_pitch', 'delta_run_exp'):
        op.add_column('fact_pitch', sa.Column('delta_run_exp', sa.REAL()), schema='production')

    # Indexes
    op.create_index('ix_batting_advanced_season', 'fact_batting_advanced',
                     ['season'], schema='production')
    op.create_index('ix_pitching_advanced_season', 'fact_pitching_advanced',
                     ['season'], schema='production')
    op.create_index('ix_pitch_type_rv_season', 'fact_pitch_type_run_value',
                     ['pitcher_id', 'season'], schema='production')


def downgrade():
    op.drop_index('ix_pitch_type_rv_season', table_name='fact_pitch_type_run_value', schema='production')
    op.drop_index('ix_pitching_advanced_season', table_name='fact_pitching_advanced', schema='production')
    op.drop_index('ix_batting_advanced_season', table_name='fact_batting_advanced', schema='production')
    op.drop_table('fact_pitch_type_run_value', schema='production')
    op.drop_table('fact_pitching_advanced', schema='production')
    op.drop_table('fact_batting_advanced', schema='production')

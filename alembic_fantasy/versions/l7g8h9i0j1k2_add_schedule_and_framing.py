"""add schedule and catcher framing tables

Revision ID: l7g8h9i0j1k2
Revises: k6f7g8h9i0j1
Create Date: 2026-03-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import spec_to_cols
from schema.production.schedule_tables import DIM_SCHEDULE_SPEC
from schema.production.catcher_framing_tables import FACT_CATCHER_FRAMING_SPEC

revision: str = 'l7g8h9i0j1k2'
down_revision: Union[str, Sequence[str]] = 'k6f7g8h9i0j1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_with_pk(schema, spec):
    op.create_table(
        spec.name,
        *spec_to_cols(spec),
        sa.PrimaryKeyConstraint(*spec.pk),
        schema=schema,
    )


def upgrade():
    _create_with_pk('production', DIM_SCHEDULE_SPEC)
    _create_with_pk('production', FACT_CATCHER_FRAMING_SPEC)

    op.create_index('ix_schedule_date', 'dim_schedule',
                     ['game_date'], schema='production')
    op.create_index('ix_schedule_team', 'dim_schedule',
                     ['season', 'home_team_id'], schema='production')
    op.create_index('ix_catcher_framing_season', 'fact_catcher_framing',
                     ['season'], schema='production')


def downgrade():
    op.drop_index('ix_catcher_framing_season', table_name='fact_catcher_framing', schema='production')
    op.drop_index('ix_schedule_team', table_name='dim_schedule', schema='production')
    op.drop_index('ix_schedule_date', table_name='dim_schedule', schema='production')
    op.drop_table('fact_catcher_framing', schema='production')
    op.drop_table('dim_schedule', schema='production')

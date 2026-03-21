"""add prospect ranking table and org depth materialized view

Revision ID: i4d5e6f7g8h9
Revises: h3c4d5e6f7g8
Create Date: 2026-03-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import spec_to_cols
from schema.production.prospect_ranking_tables import DIM_PROSPECT_RANKING_SPEC

# revision identifiers, used by Alembic.
revision: str = 'i4d5e6f7g8h9'
down_revision: Union[str, Sequence[str]] = 'h3c4d5e6f7g8'
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
    # -- dim_prospect_ranking table --
    _create_with_pk('production', DIM_PROSPECT_RANKING_SPEC)

    op.create_index(
        'ix_prospect_ranking_org',
        'dim_prospect_ranking',
        ['season'],
        schema='production',
    )

    # -- mv_org_depth materialized view --
    # Created via SQL script (load_org_depth.sql) in the pipeline.
    # Run it here so the view exists after migration.
    with open('transformation/production/load_org_depth.sql', 'r') as f:
        sql = f.read()
    op.execute(sa.text(sql))


def downgrade():
    op.execute(sa.text('DROP MATERIALIZED VIEW IF EXISTS production.mv_org_depth'))
    op.drop_index('ix_prospect_ranking_org', table_name='dim_prospect_ranking', schema='production')
    op.drop_table('dim_prospect_ranking', schema='production')

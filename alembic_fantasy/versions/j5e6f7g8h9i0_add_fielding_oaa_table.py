"""add fielding OAA table

Revision ID: j5e6f7g8h9i0
Revises: i4d5e6f7g8h9
Create Date: 2026-03-16

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import spec_to_cols
from schema.production.fielding_tables import FACT_FIELDING_OAA_SPEC

# revision identifiers, used by Alembic.
revision: str = 'j5e6f7g8h9i0'
down_revision: Union[str, Sequence[str]] = 'i4d5e6f7g8h9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        FACT_FIELDING_OAA_SPEC.name,
        *spec_to_cols(FACT_FIELDING_OAA_SPEC),
        sa.PrimaryKeyConstraint(*FACT_FIELDING_OAA_SPEC.pk),
        schema='production',
    )
    op.create_index(
        'ix_fielding_oaa_season',
        'fact_fielding_oaa',
        ['season', 'position'],
        schema='production',
    )


def downgrade():
    op.drop_index('ix_fielding_oaa_season', table_name='fact_fielding_oaa', schema='production')
    op.drop_table('fact_fielding_oaa', schema='production')

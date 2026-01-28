"""initial migration

Revision ID: 964c9688d37d
Revises: 
Create Date: 2026-01-23 21:20:35.254229

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from schema.table_factory import create_table_from_schema
from schema.raw.boxscores import LANDING_BOXSCORES_SPEC, PITCHING_BOXSCORES_SPEC
from schema.raw.landing_statcast_files import LANDING_STATCAST_FILES_SPEC
from schema.staging.pitching_boxscores import STAGING_BOXSCORE_SPEC
from schema.staging.statcast_at_bats import STATCAST_AT_BATS_SPEC
from schema.staging.statcast_batted_balls import STATCAST_BATTED_BALLS_SPEC
from schema.staging.statcast_pitches import STATCAST_PITCHES_SPEC


# revision identifiers, used by Alembic.
revision: str = '964c9688d37d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Upgrade schema."""
    op.execute('CREATE SCHEMA raw')
    op.execute('CREATE SCHEMA staging')
    op.execute('CREATE SCHEMA production')
    create_table_from_schema('raw', LANDING_BOXSCORES_SPEC)
    create_table_from_schema('raw', LANDING_STATCAST_FILES_SPEC)
    create_table_from_schema('raw', PITCHING_BOXSCORES_SPEC)
    create_table_from_schema('staging', STAGING_BOXSCORE_SPEC)
    create_table_from_schema('staging', STATCAST_AT_BATS_SPEC)
    create_table_from_schema('staging', STATCAST_BATTED_BALLS_SPEC)
    create_table_from_schema('staging', STATCAST_PITCHES_SPEC)

def downgrade():
    """Downgrade schema."""
    op.drop_table('statcast_at_bats', schema='staging')
    op.drop_table('pitching_boxscores', schema='staging')
    op.drop_table('statcast_batted_balls', schema='staging')
    op.drop_table('statcast_pitches', schema='staging')
    op.drop_table('landing_boxscores', schema='raw')
    op.drop_table('landing_statcast_files', schema='raw')
    op.drop_table('pitching_boxscores', schema='raw')
    op.execute('DROP SCHEMA raw')
    op.execute('DROP SCHEMA staging')
    op.execute('DROP SCHEMA production')
"""create raw staging production schemas and tables

Revision ID: ca3831ae2b67
Revises: 
Create Date: 2026-02-23 14:38:10.628699

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.table_factory import create_table_from_schema
from schema.raw.boxscores import (
    LANDING_BOXSCORES_SPEC, RAW_GAME_SPEC, 
    RAW_PITCHING_BOXSCORES_SPEC, RAW_BATTING_BOXSCORES_SPEC
)
from schema.raw.landing_statcast_files import LANDING_STATCAST_FILES_SPEC
from schema.staging.batting_boxscores import BATTING_BOXSCORE_SPEC
from schema.staging.pitching_boxscores import PITCHING_BOXSCORE_SPEC
from schema.staging.statcast_at_bats import STATCAST_AT_BATS_SPEC
from schema.staging.statcast_batted_balls import STATCAST_BATTED_BALLS_SPEC
from schema.staging.statcast_pitches import STATCAST_PITCHES_SPEC

# revision identifiers, used by Alembic.
revision: str = 'ca3831ae2b67'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    """Upgrade schema."""
    op.execute('CREATE SCHEMA raw')
    op.execute('CREATE SCHEMA staging')
    op.execute('CREATE SCHEMA production')

    # Raw
    create_table_from_schema('raw', LANDING_BOXSCORES_SPEC)
    create_table_from_schema('raw', RAW_GAME_SPEC)
    create_table_from_schema('raw', RAW_PITCHING_BOXSCORES_SPEC)
    create_table_from_schema('raw', RAW_BATTING_BOXSCORES_SPEC)
    create_table_from_schema('raw', LANDING_STATCAST_FILES_SPEC)

    # Staging
    create_table_from_schema('staging', BATTING_BOXSCORE_SPEC)
    create_table_from_schema('staging', PITCHING_BOXSCORE_SPEC)
    create_table_from_schema('staging', STATCAST_AT_BATS_SPEC)
    create_table_from_schema('staging', STATCAST_BATTED_BALLS_SPEC)
    create_table_from_schema('staging', STATCAST_PITCHES_SPEC)

    # Production

def downgrade() -> None:
    """Downgrade schema."""
    pass

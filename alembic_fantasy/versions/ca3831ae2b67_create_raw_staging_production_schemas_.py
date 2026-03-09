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
from schema.production.fact_tables import FACT_PA_SPEC, FACT_PITCH_SPEC
from schema.production.dim_tables import DIM_GAME_SPEC, DIM_PLAYER_SPEC, DIM_TEAM_SPEC
from schema.production.sat_tables import SAT_BATTED_BALLS_SPEC, SAT_PITCH_SHAPE_SPEC
from schema.staging.statcast_sprint_speed import STATCAST_SPRINT_SPEC

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
    create_table_from_schema('staging', STATCAST_SPRINT_SPEC)

    # Production
    create_table_from_schema('production', FACT_PITCH_SPEC)
    create_table_from_schema('production', FACT_PA_SPEC)
    create_table_from_schema('production', DIM_GAME_SPEC)
    create_table_from_schema('production', DIM_TEAM_SPEC)
    create_table_from_schema('production', DIM_PLAYER_SPEC)
    create_table_from_schema('production', SAT_PITCH_SHAPE_SPEC)
    create_table_from_schema('production', SAT_BATTED_BALLS_SPEC)

def downgrade():
    """Downgrade schema."""
    # Drop production
    op.drop_table('sat_pitch_shape', schema='production')
    op.drop_table('sat_batted_balls', schema='production')
    op.drop_table('fact_pitch', schema='production')
    op.drop_table('fact_pa', schema='production')
    op.drop_table('dim_game', schema='production')
    op.drop_table('dim_team', schema='production')
    op.drop_table('dim_player', schema='production')

    # Drop Staging
    op.drop_table('statcast_at_bats', schema='staging')
    op.drop_table('statcast_batted_balls', schema='staging')
    op.drop_table('statcast_pitches', schema='staging')
    op.drop_table('statcast_sprint_speed', schema='staging')
    op.drop_table('pitching_boxscores', schema='staging')
    op.drop_table('batting_boxscores', schema='staging')

    # Drop Raw
    op.drop_table('landing_boxscores', schema='raw')
    op.drop_table('landing_statcast_files', schema='raw')
    op.drop_table('pitching_boxscores', schema='raw')
    op.drop_table('batting_boxscores', schema='raw')
    op.drop_table('dim_game', schema='raw')

    # Drop Schemas
    op.execute('DROP SCHEMA production')
    op.execute('DROP SCHEMA staging')
    op.execute('DROP SCHEMA raw')
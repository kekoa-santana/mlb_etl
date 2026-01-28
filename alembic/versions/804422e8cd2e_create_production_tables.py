"""create production tables

Revision ID: 804422e8cd2e
Revises: 964c9688d37d
Create Date: 2026-01-24 13:29:54.396364

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from schema.production.dim_tables import DIM_PLAYER_SPEC, DIM_GAME_SPEC, DIM_TEAM_SPEC
from schema.production.fact_tables import FACT_PITCH_SPEC, FACT_PA_SPEC
from schema.production.sat_tables import SAT_BATTED_BALLS_SPEC, SAT_PITCH_SHAPE_SPEC
from schema.table_factory import create_table_from_schema


# revision identifiers, used by Alembic.
revision: str = '804422e8cd2e'
down_revision: Union[str, Sequence[str], None] = '964c9688d37d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    create_table_from_schema('production', DIM_PLAYER_SPEC)
    create_table_from_schema('production', DIM_TEAM_SPEC)
    create_table_from_schema('production', DIM_GAME_SPEC)
    create_table_from_schema('production', FACT_PA_SPEC)
    create_table_from_schema('production', FACT_PITCH_SPEC)
    create_table_from_schema('production', SAT_BATTED_BALLS_SPEC)
    create_table_from_schema('production', SAT_PITCH_SHAPE_SPEC)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('dim_player', schema='production')
    op.drop_table('dim_team', schema='production')
    op.drop_table('dim_game', schema='production')
    op.drop_table('sat_batted_balls', schema='production')
    op.drop_table('sat_pitch_shape', schema='production')
    op.drop_table('fact_pitch', schema='production')
    op.drop_table('fact_pa', schema='production')
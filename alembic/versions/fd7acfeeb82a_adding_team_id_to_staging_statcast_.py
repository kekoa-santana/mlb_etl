"""adding team_id to staging.statcast_pitches, production.fact_pitch and production.dim_team


Revision ID: fd7acfeeb82a
Revises: f82c2672693a
Create Date: 2026-01-28 12:20:42.292058

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fd7acfeeb82a'
down_revision: Union[str, Sequence[str], None] = 'f82c2672693a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass



def downgrade() -> None:
    """Downgrade schema."""
    pass
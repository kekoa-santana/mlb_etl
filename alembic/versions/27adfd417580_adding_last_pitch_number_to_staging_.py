"""adding last_pitch_number to staging.statcast_at_bats, missed it in the original spec

Revision ID: 27adfd417580
Revises: ffd5fecf3564
Create Date: 2026-01-26 11:14:41.188461

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '27adfd417580'
down_revision: Union[str, Sequence[str], None] = 'ffd5fecf3564'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass

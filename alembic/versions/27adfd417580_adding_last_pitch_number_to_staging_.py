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
    op.execute('''
        ALTER TABLE staging.statcast_at_bats
        ADD COLUMN last_pitch_number SMALLINT;
    ''')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('statcast_at_bats', 'last_pitch_number', schema='staging')

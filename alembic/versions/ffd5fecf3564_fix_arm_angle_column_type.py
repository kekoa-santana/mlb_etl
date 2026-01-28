"""fix arm_angle column type

Revision ID: ffd5fecf3564
Revises: c3bdd819d2ea
Create Date: 2026-01-26 10:00:41.757959

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ffd5fecf3564'
down_revision: Union[str, Sequence[str], None] = 'c3bdd819d2ea'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('ALTER TABLE staging.statcast_pitches ALTER COLUMN arm_angle TYPE REAL USING arm_angle::real')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('statcast_pitches', 'arm_angle', type_=sa.Text, schema='staging')

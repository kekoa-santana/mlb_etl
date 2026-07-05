"""add is_barrel column to sat_batted_balls

Adds the Statcast barrel classification as a boolean column on
production.sat_batted_balls. Prior to this migration, barrel was
computed inline via a fixed CASE WHEN (EV>=98 AND LA BETWEEN 26 AND 30)
in 11 SQL sites in the projection repo, which matches the Statcast
window only at exactly 98 mph and under-counts every high-EV barrel
outside 26-30 degrees.

The barrel window is the piecewise table published on Baseball Savant:
at 98 mph LA is 26-30, expanding to 8-50 at >= 116 mph.

Revision ID: n9i0j1k2l3m4
Revises: m8h9i0j1k2l3
Create Date: 2026-07-04

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "n9i0j1k2l3m4"
down_revision: str | Sequence[str] = "m8h9i0j1k2l3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Piecewise Statcast barrel classification. NULL when either EV or LA is NULL.
# Table matches Baseball Savant's published EV/LA windows exactly.
_IS_BARREL_EXPR = """
CASE
    WHEN launch_speed IS NULL OR launch_angle IS NULL THEN NULL
    WHEN launch_speed < 98 THEN FALSE
    WHEN launch_speed >= 116 AND launch_angle BETWEEN 8 AND 50 THEN TRUE
    WHEN launch_speed >= 115 AND launch_angle BETWEEN 9 AND 48 THEN TRUE
    WHEN launch_speed >= 114 AND launch_angle BETWEEN 10 AND 47 THEN TRUE
    WHEN launch_speed >= 113 AND launch_angle BETWEEN 11 AND 46 THEN TRUE
    WHEN launch_speed >= 112 AND launch_angle BETWEEN 12 AND 45 THEN TRUE
    WHEN launch_speed >= 111 AND launch_angle BETWEEN 13 AND 44 THEN TRUE
    WHEN launch_speed >= 110 AND launch_angle BETWEEN 14 AND 43 THEN TRUE
    WHEN launch_speed >= 109 AND launch_angle BETWEEN 15 AND 42 THEN TRUE
    WHEN launch_speed >= 108 AND launch_angle BETWEEN 16 AND 41 THEN TRUE
    WHEN launch_speed >= 107 AND launch_angle BETWEEN 17 AND 40 THEN TRUE
    WHEN launch_speed >= 106 AND launch_angle BETWEEN 18 AND 39 THEN TRUE
    WHEN launch_speed >= 105 AND launch_angle BETWEEN 19 AND 38 THEN TRUE
    WHEN launch_speed >= 104 AND launch_angle BETWEEN 20 AND 37 THEN TRUE
    WHEN launch_speed >= 103 AND launch_angle BETWEEN 21 AND 36 THEN TRUE
    WHEN launch_speed >= 102 AND launch_angle BETWEEN 22 AND 35 THEN TRUE
    WHEN launch_speed >= 101 AND launch_angle BETWEEN 23 AND 34 THEN TRUE
    WHEN launch_speed >= 100 AND launch_angle BETWEEN 24 AND 33 THEN TRUE
    WHEN launch_speed >= 99  AND launch_angle BETWEEN 25 AND 31 THEN TRUE
    WHEN launch_speed >= 98  AND launch_angle BETWEEN 26 AND 30 THEN TRUE
    ELSE FALSE
END
"""


def upgrade():
    op.add_column(
        "sat_batted_balls",
        sa.Column("is_barrel", sa.Boolean(), nullable=True),
        schema="production",
    )
    op.execute(f"UPDATE production.sat_batted_balls SET is_barrel = ({_IS_BARREL_EXPR})")
    op.create_index(
        "ix_sat_batted_balls_is_barrel",
        "sat_batted_balls",
        ["is_barrel"],
        schema="production",
        postgresql_where=sa.text("is_barrel = TRUE"),
    )


def downgrade():
    op.drop_index(
        "ix_sat_batted_balls_is_barrel",
        table_name="sat_batted_balls",
        schema="production",
    )
    op.drop_column("sat_batted_balls", "is_barrel", schema="production")

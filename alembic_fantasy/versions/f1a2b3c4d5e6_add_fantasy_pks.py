"""add primary key constraints to fantasy scoring tables

Revision ID: f1a2b3c4d5e6
Revises: c5d6e7f8g9h0
Create Date: 2026-03-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str]] = 'c5d6e7f8g9h0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _dedup(schema, table, pk_cols):
    """Remove duplicate rows before adding PK constraint."""
    pk = ', '.join(pk_cols)
    op.execute(sa.text(f"""
        DELETE FROM {schema}.{table} a
        USING (
            SELECT ctid, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY ctid) AS rn
            FROM {schema}.{table}
        ) b
        WHERE a.ctid = b.ctid AND b.rn > 1
    """))


def upgrade():
    # dedup before adding PKs
    _dedup('fantasy', 'dk_batter_game_scores', ['batter_id', 'game_pk'])
    _dedup('fantasy', 'dk_pitcher_game_scores', ['pitcher_id', 'game_pk'])
    _dedup('fantasy', 'espn_batter_game_scores', ['batter_id', 'game_pk'])
    _dedup('fantasy', 'espn_pitcher_game_scores', ['pitcher_id', 'game_pk'])

    op.create_primary_key(
        'pk_dk_batter_game_scores',
        'dk_batter_game_scores',
        ['batter_id', 'game_pk'],
        schema='fantasy',
    )
    op.create_primary_key(
        'pk_dk_pitcher_game_scores',
        'dk_pitcher_game_scores',
        ['pitcher_id', 'game_pk'],
        schema='fantasy',
    )
    op.create_primary_key(
        'pk_espn_batter_game_scores',
        'espn_batter_game_scores',
        ['batter_id', 'game_pk'],
        schema='fantasy',
    )
    op.create_primary_key(
        'pk_espn_pitcher_game_scores',
        'espn_pitcher_game_scores',
        ['pitcher_id', 'game_pk'],
        schema='fantasy',
    )


def downgrade():
    op.drop_constraint('pk_espn_pitcher_game_scores', 'espn_pitcher_game_scores', schema='fantasy', type_='primary')
    op.drop_constraint('pk_espn_batter_game_scores', 'espn_batter_game_scores', schema='fantasy', type_='primary')
    op.drop_constraint('pk_dk_pitcher_game_scores', 'dk_pitcher_game_scores', schema='fantasy', type_='primary')
    op.drop_constraint('pk_dk_batter_game_scores', 'dk_batter_game_scores', schema='fantasy', type_='primary')

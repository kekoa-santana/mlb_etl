"""add pipeline performance indexes

Indexes targeting the actual query access patterns in production SQL
scripts. The biggest gaps are:

- staging.batting/pitching_boxscores: PK leads with player_id but
  every production join uses game_pk alone -> full scans on 458k/173k rows
- production.fact_pitch (5.4M rows): no secondary indexes despite
  constant lookups by pa_id, pitcher_id, and batter_id
- production.fact_pa (1.4M rows): no secondary indexes despite
  GROUP BY batter_id / pitcher_id in advanced stats, platoon, matchup
- staging.statcast_batted_balls: PK is pitch_id but load_batted_balls
  joins on (game_pk, game_counter, pitch_number)
- production.dim_game: universal WHERE game_type NOT IN ('E','S') and
  ORDER BY game_date have no index support

Revision ID: m8h9i0j1k2l3
Revises: l7g8h9i0j1k2
Create Date: 2026-03-30

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'm8h9i0j1k2l3'
down_revision: Union[str, Sequence[str]] = 'l7g8h9i0j1k2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # ── TIER 1: Critical ─────────────────────────────────────────────
    # staging boxscores: PK leads with player_id, but every production
    # script joins on game_pk alone (fantasy scores, game totals,
    # player game, rolling window, streak, park factors)
    op.create_index('ix_stg_batting_game_pk', 'batting_boxscores',
                     ['game_pk'], schema='staging')
    op.create_index('ix_stg_pitching_game_pk', 'pitching_boxscores',
                     ['game_pk'], schema='staging')

    # fact_pitch (5.4M rows): surrogate PK + UK on natural key, but
    # queries constantly look up by pa_id, pitcher_id, batter_id
    op.create_index('ix_fact_pitch_pa_id', 'fact_pitch',
                     ['pa_id'], schema='production')
    op.create_index('ix_fact_pitch_pitcher_game', 'fact_pitch',
                     ['pitcher_id', 'game_pk'], schema='production')
    op.create_index('ix_fact_pitch_batter_game', 'fact_pitch',
                     ['batter_id', 'game_pk'], schema='production')

    # fact_pa (1.4M rows): surrogate PK + UK on (game_pk, game_counter),
    # but GROUP BY batter_id / pitcher_id in advanced stats, platoon,
    # matchup, park factors
    op.create_index('ix_fact_pa_batter_game', 'fact_pa',
                     ['batter_id', 'game_pk'], schema='production')
    op.create_index('ix_fact_pa_pitcher_game', 'fact_pa',
                     ['pitcher_id', 'game_pk'], schema='production')

    # ── TIER 2: High ─────────────────────────────────────────────────
    # statcast_batted_balls: PK is pitch_id but load_batted_balls.sql
    # joins on (game_pk, game_counter, pitch_number)
    op.create_index('ix_stg_batted_balls_game', 'statcast_batted_balls',
                     ['game_pk', 'game_counter', 'pitch_number'],
                     schema='staging')

    # dim_game: nearly every query filters WHERE game_type NOT IN ('E','S')
    op.create_index('ix_dim_game_season_type', 'dim_game',
                     ['season', 'game_type'], schema='production')
    # rolling window + streak queries ORDER BY game_date within windows
    op.create_index('ix_dim_game_date', 'dim_game',
                     ['game_date'], schema='production')
    # park factors GROUP BY venue_id, season
    op.create_index('ix_dim_game_venue_season', 'dim_game',
                     ['venue_id', 'season'], schema='production')


def downgrade():
    # tier 2
    op.drop_index('ix_dim_game_venue_season', table_name='dim_game', schema='production')
    op.drop_index('ix_dim_game_date', table_name='dim_game', schema='production')
    op.drop_index('ix_dim_game_season_type', table_name='dim_game', schema='production')
    op.drop_index('ix_stg_batted_balls_game', table_name='statcast_batted_balls', schema='staging')
    # tier 1
    op.drop_index('ix_fact_pa_pitcher_game', table_name='fact_pa', schema='production')
    op.drop_index('ix_fact_pa_batter_game', table_name='fact_pa', schema='production')
    op.drop_index('ix_fact_pitch_batter_game', table_name='fact_pitch', schema='production')
    op.drop_index('ix_fact_pitch_pitcher_game', table_name='fact_pitch', schema='production')
    op.drop_index('ix_fact_pitch_pa_id', table_name='fact_pitch', schema='production')
    op.drop_index('ix_stg_pitching_game_pk', table_name='pitching_boxscores', schema='staging')
    op.drop_index('ix_stg_batting_game_pk', table_name='batting_boxscores', schema='staging')

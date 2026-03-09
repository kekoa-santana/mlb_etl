"""SQL script registry for production table loads.

This registry defines the order and configuration for SQL scripts that
transform data from staging tables to production tables.
"""

SQL_REGISTRY = [
    {
        'name': 'transform_dim_game',
        'script': 'transformation/production/transform_dim_game.sql',
        'tables': ['production.dim_game'],
        'depends_on': ['raw.dim_game']
    },
    {
        'name': 'transform_pitching_boxscores',
        'script': 'transformation/staging/transform_pitching_boxscores.sql',
        'tables': ['staging.pitching_boxscores'],
        'depends_on': ['raw.pitching_boxscores']
    },
    {
        'name': 'transform_batting_boxscores',
        'script': 'transformation/staging/transform_batting_boxscores.sql',
        'tables': ['staging.batting_boxscores'],
        'depends_on': ['raw.batting_boxscores']
    },
    {
        'name': 'load_facts',
        'script': 'transformation/production/production_load_facts.sql',
        'tables': ['production.fact_pa', 'production.fact_pitch'],
        'depends_on': ['staging.statcast_at_bats', 'staging.statcast_pitches', 'production.dim_game']
    },
    {
        'name': 'load_pitch_shape',
        'script': 'transformation/production/load_pitch_shape.sql',
        'tables': ['production.sat_pitch_shape'],
        'depends_on': ['production.fact_pitch']
    },
    {
        'name': 'load_batted_balls',
        'script': 'transformation/production/load_batted_balls.sql',
        'tables': ['production.sat_batted_balls'],
        'depends_on': ['production.fact_pitch', 'production.dim_player', 'staging.statcast_batted_balls']
    },
    {
        'name': 'load_lineup',
        'script': 'transformation/production/load_lineup.sql',
        'tables': ['production.fact_lineup'],
        'depends_on': ['raw.landing_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_weather',
        'script': 'transformation/production/load_weather.sql',
        'tables': ['production.dim_weather'],
        'depends_on': ['raw.landing_boxscores']
    },
    {
        'name': 'load_park_factors',
        'script': 'transformation/production/load_park_factors.sql',
        'tables': ['production.dim_park_factor'],
        'depends_on': ['production.fact_pa', 'production.fact_pitch', 'production.dim_game']
    },
    {
        'name': 'load_umpire',
        'script': 'transformation/production/load_umpire.sql',
        'tables': ['production.dim_umpire'],
        'depends_on': ['raw.landing_boxscores']
    },
    {
        'name': 'load_game_totals',
        'script': 'transformation/production/load_game_totals.sql',
        'tables': ['production.fact_game_totals'],
        'depends_on': ['staging.batting_boxscores', 'production.dim_game']
    },
]

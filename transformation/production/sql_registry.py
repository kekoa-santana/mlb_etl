"""SQL script registry for production table loads.

This registry defines the order and configuration for SQL scripts that
transform data from staging tables to production tables.
"""

SQL_REGISTRY = [
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
        'name': 'transform_pitching_boxscores',
        'script': 'transformation/staging/transform_pitching_boxscores.sql',
        'tables': ['staging.pitching_boxscores'],
        'depends_on': ['raw.pitching_boxscores']
    }
]

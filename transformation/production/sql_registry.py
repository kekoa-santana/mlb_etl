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
    {
        'name': 'load_transactions',
        'script': 'transformation/production/load_transactions.sql',
        'tables': ['production.dim_transaction'],
        'depends_on': ['raw.transactions']
    },
    {
        'name': 'load_rolling_window',
        'script': 'transformation/production/load_rolling_window.sql',
        'tables': ['production.fact_player_form_rolling'],
        'depends_on': ['staging.batting_boxscores', 'staging.pitching_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_streak_indicator',
        'script': 'transformation/production/load_streak_indicator.sql',
        'tables': ['production.fact_streak_indicator'],
        'depends_on': ['staging.batting_boxscores', 'staging.pitching_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_platoon_splits',
        'script': 'transformation/production/load_platoon_splits.sql',
        'tables': ['production.fact_platoon_splits'],
        'depends_on': ['production.fact_pa', 'production.fact_pitch', 'production.sat_batted_balls', 'production.dim_player', 'production.dim_game']
    },
    {
        'name': 'load_matchup_history',
        'script': 'transformation/production/load_matchup_history.sql',
        'tables': ['production.fact_matchup_history'],
        'depends_on': ['production.fact_pa', 'production.fact_pitch', 'production.sat_batted_balls', 'production.dim_game']
    },
    # -- fantasy scoring --
    {
        'name': 'load_dk_batter_scores',
        'script': 'transformation/production/load_dk_batter_scores.sql',
        'tables': ['fantasy.dk_batter_game_scores'],
        'depends_on': ['staging.batting_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_dk_pitcher_scores',
        'script': 'transformation/production/load_dk_pitcher_scores.sql',
        'tables': ['fantasy.dk_pitcher_game_scores'],
        'depends_on': ['staging.pitching_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_espn_batter_scores',
        'script': 'transformation/production/load_espn_batter_scores.sql',
        'tables': ['fantasy.espn_batter_game_scores'],
        'depends_on': ['staging.batting_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_espn_pitcher_scores',
        'script': 'transformation/production/load_espn_pitcher_scores.sql',
        'tables': ['fantasy.espn_pitcher_game_scores'],
        'depends_on': ['staging.pitching_boxscores', 'production.dim_game']
    },
    # -- player game logs + status --
    {
        'name': 'load_player_game_mlb',
        'script': 'transformation/production/load_player_game_mlb.sql',
        'tables': ['production.fact_player_game_mlb'],
        'depends_on': ['staging.batting_boxscores', 'staging.pitching_boxscores', 'production.dim_game']
    },
    {
        'name': 'load_player_status_timeline',
        'script': 'transformation/production/load_player_status_timeline.sql',
        'tables': ['production.fact_player_status_timeline'],
        'depends_on': ['production.dim_transaction']
    },
    # -- milb + prospects --
    {
        'name': 'load_milb_player_game',
        'script': 'transformation/production/load_milb_player_game.sql',
        'tables': ['production.fact_milb_player_game'],
        'depends_on': ['staging.milb_batting_game_logs', 'staging.milb_pitching_game_logs']
    },
    {
        'name': 'load_prospect_snapshot',
        'script': 'transformation/production/load_prospect_snapshot.sql',
        'tables': ['production.fact_prospect_snapshot'],
        'depends_on': ['production.dim_prospects', 'staging.milb_batting_game_logs', 'staging.milb_pitching_game_logs']
    },
    {
        'name': 'load_prospect_transition',
        'script': 'transformation/production/load_prospect_transition.sql',
        'tables': ['production.fact_prospect_transition'],
        'depends_on': ['production.dim_prospects']
    },
    # -- org depth materialized view --
    {
        'name': 'load_org_depth',
        'script': 'transformation/production/load_org_depth.sql',
        'tables': ['production.mv_org_depth'],
        'depends_on': ['production.fact_milb_player_game', 'production.dim_prospect_ranking']
    },
    # -- advanced stats --
    {
        'name': 'load_batting_advanced',
        'script': 'transformation/production/load_batting_advanced.sql',
        'tables': ['production.fact_batting_advanced'],
        'depends_on': ['production.fact_pa', 'production.sat_batted_balls', 'production.dim_game', 'production.fact_game_totals']
    },
    {
        'name': 'load_pitching_advanced',
        'script': 'transformation/production/load_pitching_advanced.sql',
        'tables': ['production.fact_pitching_advanced'],
        'depends_on': ['production.fact_pitch', 'production.fact_pa', 'production.sat_batted_balls', 'production.dim_game']
    },
    {
        'name': 'load_pitch_type_run_value',
        'script': 'transformation/production/load_pitch_type_run_value.sql',
        'tables': ['production.fact_pitch_type_run_value'],
        'depends_on': ['production.fact_pitch', 'production.sat_batted_balls', 'production.dim_game']
    },
    {
        'name': 'load_catcher_framing',
        'script': 'transformation/production/load_catcher_framing.sql',
        'tables': ['production.fact_catcher_framing'],
        'depends_on': ['staging.statcast_pitches', 'production.fact_pitch', 'production.dim_game']
    },
]

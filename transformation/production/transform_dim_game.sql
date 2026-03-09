INSERT INTO production.dim_game(
    game_pk, home_team_id, away_team_id, game_date, season, game_type,
    home_team_name, away_team_name, home_team_wins, home_team_losses, 
    away_team_wins, away_team_losses, venue_id, doubleheader, 
    day_night, games_in_series, series_in_game_number
)
SELECT
    game_pk,
    home_team_id,
    away_team_id,
    game_date,
    season,
    game_type,
    home_team_name,
    away_team_name,
    CAST(NULLIF(home_wins_text, 'NaN') AS SMALLINT),
    CAST(NULLIF(home_losses_text, 'NaN') AS SMALLINT),
    CAST(NULLIF(away_wins_text, 'NaN') AS SMALLINT),
    CAST(NULLIF(away_losses_text, 'NaN') AS SMALLINT),
    CAST(NULLIF(venue_id_text, 'NaN') AS SMALLINT),
    CAST(LOWER(doubleheader_text) AS TEXT),
    CAST(LOWER(day_night_text) AS TEXT),
    CAST(NULLIF(games_in_series_text, 'NaN') AS SMALLINT),
    CAST(NULLIF(series_in_game_number_text, 'NaN') AS SMALLINT)
FROM raw.dim_game
ON CONFLICT (game_pk) DO UPDATE SET
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    game_date = EXCLUDED.game_date,
    season = EXCLUDED.season,
    game_type = EXCLUDED.game_type,
    home_team_name = EXCLUDED.home_team_name,
    away_team_name = EXCLUDED.away_team_name,
    home_team_wins = EXCLUDED.home_team_wins,
    home_team_losses = EXCLUDED.home_team_losses,
    away_team_wins = EXCLUDED.away_team_wins,
    away_team_losses = EXCLUDED.away_team_losses,
    venue_id = EXCLUDED.venue_id,
    doubleheader = EXCLUDED.doubleheader,
    day_night = EXCLUDED.day_night,
    games_in_series = EXCLUDED.games_in_series,
    series_in_game_number = EXCLUDED.series_in_game_number
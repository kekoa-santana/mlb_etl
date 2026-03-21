INSERT INTO production.fact_prospect_snapshot (
    player_id, season,
    full_name, primary_position, bat_side, pitch_hand,
    birth_date, age_at_season_start,
    level, sport_id,
    parent_org_id, parent_org_name,
    milb_team_id, milb_team_name,
    status_code, mlb_debut_date, draft_year,
    games_played
)
WITH game_counts AS (
    SELECT
        batter_id AS player_id,
        season,
        COUNT(DISTINCT game_pk) AS games
    FROM staging.milb_batting_game_logs
    GROUP BY batter_id, season

    UNION ALL

    SELECT
        pitcher_id AS player_id,
        season,
        COUNT(DISTINCT game_pk) AS games
    FROM staging.milb_pitching_game_logs
    GROUP BY pitcher_id, season
),
player_season_games AS (
    SELECT
        player_id,
        season,
        SUM(games)::int AS games_played
    FROM game_counts
    GROUP BY player_id, season
)
SELECT
    dp.player_id,
    dp.season,
    dp.full_name,
    dp.primary_position,
    dp.bat_side,
    dp.pitch_hand,
    dp.birth_date,
    (dp.season - EXTRACT(YEAR FROM dp.birth_date))::smallint AS age_at_season_start,
    dp.level,
    dp.sport_id,
    dp.parent_org_id,
    dp.parent_org_name,
    dp.milb_team_id,
    dp.milb_team_name,
    dp.status_code,
    dp.mlb_debut_date,
    dp.draft_year,
    COALESCE(psg.games_played, 0) AS games_played
FROM production.dim_prospects dp
LEFT JOIN player_season_games psg
    ON psg.player_id = dp.player_id
    AND psg.season = dp.season
ON CONFLICT (player_id, season) DO UPDATE
SET full_name            = EXCLUDED.full_name,
    primary_position     = EXCLUDED.primary_position,
    bat_side             = EXCLUDED.bat_side,
    pitch_hand           = EXCLUDED.pitch_hand,
    birth_date           = EXCLUDED.birth_date,
    age_at_season_start  = EXCLUDED.age_at_season_start,
    level                = EXCLUDED.level,
    sport_id             = EXCLUDED.sport_id,
    parent_org_id        = EXCLUDED.parent_org_id,
    parent_org_name      = EXCLUDED.parent_org_name,
    milb_team_id         = EXCLUDED.milb_team_id,
    milb_team_name       = EXCLUDED.milb_team_name,
    status_code          = EXCLUDED.status_code,
    mlb_debut_date       = EXCLUDED.mlb_debut_date,
    draft_year           = EXCLUDED.draft_year,
    games_played         = EXCLUDED.games_played;

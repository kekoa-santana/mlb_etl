INSERT INTO production.fact_prospect_transition (
    player_id, event_date, from_level, to_level,
    from_sport_id, to_sport_id,
    from_team_name, to_team_name,
    transition_type, season
)
WITH ordered AS (
    SELECT
        player_id,
        season,
        level,
        sport_id,
        milb_team_name,
        LAG(level)          OVER (PARTITION BY player_id ORDER BY season) AS prev_level,
        LAG(sport_id)       OVER (PARTITION BY player_id ORDER BY season) AS prev_sport_id,
        LAG(milb_team_name) OVER (PARTITION BY player_id ORDER BY season) AS prev_team_name,
        LAG(season)         OVER (PARTITION BY player_id ORDER BY season) AS prev_season
    FROM production.dim_prospects
)
SELECT
    o.player_id,
    -- Use Jan 1 of the new season as the event date
    make_date(o.season, 1, 1)  AS event_date,
    o.prev_level               AS from_level,
    o.level                    AS to_level,
    o.prev_sport_id            AS from_sport_id,
    o.sport_id                 AS to_sport_id,
    o.prev_team_name           AS from_team_name,
    o.milb_team_name           AS to_team_name,
    CASE
        WHEN o.sport_id < o.prev_sport_id AND o.sport_id = 1  THEN 'callup'
        WHEN o.sport_id < o.prev_sport_id                      THEN 'promotion'
        WHEN o.sport_id > o.prev_sport_id AND o.prev_sport_id = 1 THEN 'option'
        WHEN o.sport_id > o.prev_sport_id                      THEN 'demotion'
        ELSE 'lateral'
    END                        AS transition_type,
    o.season
FROM ordered o
WHERE o.prev_level IS NOT NULL          -- skip first appearance
  AND o.level IS DISTINCT FROM o.prev_level  -- only actual level changes
ON CONFLICT (player_id, event_date, from_level, to_level) DO UPDATE
SET from_sport_id    = EXCLUDED.from_sport_id,
    to_sport_id      = EXCLUDED.to_sport_id,
    from_team_name   = EXCLUDED.from_team_name,
    to_team_name     = EXCLUDED.to_team_name,
    transition_type  = EXCLUDED.transition_type,
    season           = EXCLUDED.season;

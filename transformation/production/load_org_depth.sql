-- Refreshes production.mv_org_depth materialized view.
-- Shows organizational depth per MLB parent org, per month, per minor league level.
-- Combines player counts, performance aggregates, and ranked prospect counts.

DROP MATERIALIZED VIEW IF EXISTS production.mv_org_depth;

CREATE MATERIALIZED VIEW production.mv_org_depth AS
WITH player_months AS (
    SELECT
        parent_org_id,
        season,
        date_trunc('month', game_date)::date  AS month_start,
        level,
        sport_id,
        player_id,
        player_role,
        count(*)                               AS games,
        -- batter stats
        sum(bat_h)                             AS total_h,
        sum(bat_ab)                            AS total_ab,
        sum(bat_hr)                            AS total_hr,
        sum(bat_bb)                            AS total_bb,
        sum(bat_k)                             AS total_k,
        sum(bat_sb)                            AS total_sb,
        sum(bat_pa)                            AS total_pa,
        -- pitcher stats
        sum(pit_k)                             AS total_pit_k,
        sum(pit_bb)                            AS total_pit_bb,
        sum(pit_bf)                            AS total_pit_bf,
        sum(pit_ip)                            AS total_pit_ip,
        sum(pit_er)                            AS total_pit_er
    FROM production.fact_milb_player_game
    WHERE parent_org_id IS NOT NULL
      AND game_date IS NOT NULL
    GROUP BY parent_org_id, season,
             date_trunc('month', game_date)::date,
             level, sport_id, player_id, player_role
),
-- Pre-join rankings to player-months to avoid correlated subqueries
player_ranked AS (
    SELECT
        pm.parent_org_id,
        pm.season,
        pm.month_start,
        pm.level,
        pm.player_id,
        pr.future_value
    FROM (
        SELECT DISTINCT parent_org_id, season, month_start, level, player_id
        FROM player_months
    ) pm
    LEFT JOIN production.dim_prospect_ranking pr
        ON pr.player_id = pm.player_id
       AND pr.season = pm.season
),
-- Org-wide ranked prospect totals per season
org_season_ranked AS (
    SELECT
        parent_org_id,
        season,
        count(DISTINCT player_id) AS total_ranked_prospects
    FROM player_ranked
    WHERE future_value IS NOT NULL
    GROUP BY parent_org_id, season
),
-- Ranked prospects per org + month + level
level_ranked AS (
    SELECT
        parent_org_id,
        season,
        month_start,
        level,
        count(DISTINCT player_id) AS ranked_prospects_at_level,
        avg(future_value)::real   AS avg_future_value
    FROM player_ranked
    WHERE future_value IS NOT NULL
    GROUP BY parent_org_id, season, month_start, level
),
org_level_summary AS (
    SELECT
        parent_org_id,
        season,
        month_start,
        level,
        min(sport_id)                                                    AS sport_id,
        count(DISTINCT player_id)                                        AS total_players,
        count(DISTINCT player_id) FILTER (WHERE player_role = 'batter')  AS batters,
        count(DISTINCT player_id) FILTER (WHERE player_role = 'pitcher') AS pitchers,
        sum(games)                                                       AS total_games,
        -- aggregate batter stats
        sum(total_h)::real  / NULLIF(sum(total_ab), 0)                   AS level_avg,
        sum(total_bb)::real / NULLIF(sum(total_pa), 0)                   AS level_bb_rate,
        sum(total_k)::real  / NULLIF(sum(total_pa), 0)                   AS level_k_rate,
        sum(total_hr)                                                    AS level_total_hr,
        sum(total_sb)                                                    AS level_total_sb,
        -- aggregate pitcher stats
        CASE WHEN sum(total_pit_ip) > 0
             THEN (sum(total_pit_er)::real / sum(total_pit_ip)) * 9
             ELSE NULL END                                               AS level_era,
        sum(total_pit_k)::real  / NULLIF(sum(total_pit_bf), 0)          AS level_pit_k_rate,
        sum(total_pit_bb)::real / NULLIF(sum(total_pit_bf), 0)          AS level_pit_bb_rate
    FROM player_months
    GROUP BY parent_org_id, season, month_start, level
)
SELECT
    o.parent_org_id,
    dt.team_name                                       AS org_name,
    o.season,
    o.month_start,
    o.level,
    o.sport_id,
    o.total_players,
    o.batters,
    o.pitchers,
    o.total_games,
    o.level_avg,
    o.level_bb_rate,
    o.level_k_rate,
    o.level_total_hr,
    o.level_total_sb,
    o.level_era,
    o.level_pit_k_rate,
    o.level_pit_bb_rate,
    COALESCE(lr.ranked_prospects_at_level, 0)         AS ranked_prospects_at_level,
    lr.avg_future_value,
    COALESCE(osr.total_ranked_prospects, 0)           AS org_total_ranked_prospects
FROM org_level_summary o
LEFT JOIN production.dim_team dt
    ON dt.team_id = o.parent_org_id
LEFT JOIN level_ranked lr
    ON lr.parent_org_id = o.parent_org_id
   AND lr.season = o.season
   AND lr.month_start = o.month_start
   AND lr.level = o.level
LEFT JOIN org_season_ranked osr
    ON osr.parent_org_id = o.parent_org_id
   AND osr.season = o.season;

-- Indexes for common query patterns
CREATE UNIQUE INDEX ix_mv_org_depth_pk
    ON production.mv_org_depth (parent_org_id, season, month_start, level);

CREATE INDEX ix_mv_org_depth_season
    ON production.mv_org_depth (season, month_start);

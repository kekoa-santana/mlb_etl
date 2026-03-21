INSERT INTO production.dim_park_factor (
    venue_id, season, batter_stand, venue_name,
    hr_pf_season, pa_season, hr_season,
    hr_pf_3yr, pa_3yr, hr_3yr
)
WITH pa_hand AS (
    SELECT DISTINCT ON (pa_id) pa_id, batter_stand
    FROM production.fact_pitch
    WHERE batter_stand IS NOT NULL
),
venue_season AS (
    SELECT
        g.venue_id,
        g.season,
        ph.batter_stand,
        count(*) AS pa,
        count(*) FILTER (WHERE fp.events = 'home_run') AS hr
    FROM production.fact_pa fp
    JOIN production.dim_game g ON g.game_pk = fp.game_pk
    JOIN pa_hand ph ON ph.pa_id = fp.pa_id
    WHERE g.game_type != 'E'
    GROUP BY g.venue_id, g.season, ph.batter_stand
),
lg_season AS (
    SELECT season, batter_stand,
        sum(hr)::numeric / NULLIF(sum(pa), 0) AS lg_hr_rate
    FROM venue_season
    GROUP BY season, batter_stand
),
-- 3-year rolling aggregates
rolling_venue AS (
    SELECT
        v1.venue_id, v1.season, v1.batter_stand,
        sum(v2.pa) AS pa_3yr,
        sum(v2.hr) AS hr_3yr
    FROM (SELECT DISTINCT venue_id, season, batter_stand FROM venue_season) v1
    JOIN venue_season v2
        ON v2.venue_id = v1.venue_id
        AND v2.batter_stand = v1.batter_stand
        AND v2.season BETWEEN v1.season - 2 AND v1.season
    GROUP BY v1.venue_id, v1.season, v1.batter_stand
),
rolling_lg AS (
    SELECT
        ls1.season, ls1.batter_stand,
        -- average league rate over the 3-year window
        sum(vs.hr)::numeric / NULLIF(sum(vs.pa), 0) AS lg_hr_rate_3yr
    FROM (SELECT DISTINCT season, batter_stand FROM lg_season) ls1
    JOIN venue_season vs
        ON vs.batter_stand = ls1.batter_stand
        AND vs.season BETWEEN ls1.season - 2 AND ls1.season
    GROUP BY ls1.season, ls1.batter_stand
),
-- venue names from boxscore info
venue_names AS (
    SELECT DISTINCT ON (g.venue_id)
        g.venue_id,
        rtrim(
            (SELECT elem->>'value'
             FROM jsonb_array_elements(lb.payload::jsonb->'info') elem
             WHERE elem->>'label' = 'Venue'),
            '.'
        ) AS venue_name
    FROM production.dim_game g
    JOIN raw.landing_boxscores lb ON lb.game_pk = g.game_pk
    WHERE g.game_type != 'E'
    ORDER BY g.venue_id, g.season DESC
)
SELECT
    vs.venue_id,
    vs.season,
    vs.batter_stand,
    vn.venue_name,
    -- single-season PF
    round((vs.hr::numeric / NULLIF(vs.pa, 0)) / NULLIF(ls.lg_hr_rate, 0), 3) AS hr_pf_season,
    vs.pa AS pa_season,
    vs.hr AS hr_season,
    -- 3-year rolling PF
    round((rv.hr_3yr::numeric / NULLIF(rv.pa_3yr, 0)) / NULLIF(rl.lg_hr_rate_3yr, 0), 3) AS hr_pf_3yr,
    rv.pa_3yr,
    rv.hr_3yr
FROM venue_season vs
JOIN lg_season ls ON ls.season = vs.season AND ls.batter_stand = vs.batter_stand
JOIN rolling_venue rv ON rv.venue_id = vs.venue_id AND rv.season = vs.season AND rv.batter_stand = vs.batter_stand
JOIN rolling_lg rl ON rl.season = vs.season AND rl.batter_stand = vs.batter_stand
LEFT JOIN venue_names vn ON vn.venue_id = vs.venue_id
WHERE vs.pa >= 100
ON CONFLICT (venue_id, season, batter_stand) DO UPDATE
SET venue_name     = EXCLUDED.venue_name,
    hr_pf_season   = EXCLUDED.hr_pf_season,
    pa_season      = EXCLUDED.pa_season,
    hr_season      = EXCLUDED.hr_season,
    hr_pf_3yr      = EXCLUDED.hr_pf_3yr,
    pa_3yr         = EXCLUDED.pa_3yr,
    hr_3yr         = EXCLUDED.hr_3yr;

-- production.fact_batting_advanced
-- Player-season batting: counting stats, rate stats, wOBA, wRC+, Statcast quality
-- Sources: fact_pa + sat_batted_balls + dim_game + fact_game_totals

INSERT INTO production.fact_batting_advanced (
    batter_id, season,
    pa, ab, hits, singles, doubles, triples, hr, bb, ibb, hbp, k, sf,
    avg, obp, slg, ops, k_pct, bb_pct,
    woba, wrc_plus,
    xba, xslg, xwoba, barrel_pct, hard_hit_pct, sweet_spot_pct, bip_count
)
WITH pa_events AS (
    SELECT
        fp.batter_id,
        dg.season,
        dg.venue_id,
        fp.events,
        -- classify events
        CASE WHEN fp.events IN ('single','double','triple','home_run') THEN 1 ELSE 0 END AS is_hit,
        CASE WHEN fp.events = 'single' THEN 1 ELSE 0 END AS is_1b,
        CASE WHEN fp.events = 'double' THEN 1 ELSE 0 END AS is_2b,
        CASE WHEN fp.events = 'triple' THEN 1 ELSE 0 END AS is_3b,
        CASE WHEN fp.events = 'home_run' THEN 1 ELSE 0 END AS is_hr,
        CASE WHEN fp.events = 'walk' THEN 1 ELSE 0 END AS is_bb,
        CASE WHEN fp.events = 'intent_walk' THEN 1 ELSE 0 END AS is_ibb,
        CASE WHEN fp.events = 'hit_by_pitch' THEN 1 ELSE 0 END AS is_hbp,
        CASE WHEN fp.events IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END AS is_k,
        CASE WHEN fp.events = 'sac_fly' THEN 1 ELSE 0 END AS is_sf,
        CASE WHEN fp.events = 'sac_bunt' THEN 1 ELSE 0 END AS is_sac_bunt,
        -- wOBA weights (approximate FanGraphs linear weights)
        CASE
            WHEN fp.events = 'walk' THEN 0.69
            WHEN fp.events = 'hit_by_pitch' THEN 0.72
            WHEN fp.events = 'single' THEN 0.88
            WHEN fp.events = 'double' THEN 1.24
            WHEN fp.events = 'triple' THEN 1.56
            WHEN fp.events = 'home_run' THEN 2.00
            ELSE 0
        END AS woba_weight,
        -- wOBA denom: exclude IBB and sac bunts
        CASE WHEN fp.events NOT IN ('intent_walk','sac_bunt','catcher_interf') THEN 1 ELSE 0 END AS woba_denom
    FROM production.fact_pa fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
),
-- Batted ball quality from statcast
batter_bip AS (
    SELECT
        fp.batter_id,
        dg.season,
        count(*)                                        AS bip_count,
        avg(NULLIF(sb.xba, 'NaN'::real))               AS xba,
        avg(NULLIF(sb.xslg, 'NaN'::real))              AS xslg,
        avg(NULLIF(sb.xwoba, 'NaN'::real))             AS xwoba,
        avg(CASE WHEN sb.launch_speed >= 98
                  AND sb.launch_angle BETWEEN 26 AND 30
             THEN 1.0 ELSE 0.0 END)                    AS barrel_pct,
        avg(sb.hard_hit::int)                           AS hard_hit_pct,
        avg(sb.sweet_spot::int)                         AS sweet_spot_pct
    FROM production.sat_batted_balls sb
    JOIN production.fact_pitch fpi ON fpi.pitch_id = sb.pitch_id
    JOIN production.fact_pa fp ON fp.pa_id = fpi.pa_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    GROUP BY fp.batter_id, dg.season
),
-- League wOBA by season (for wRC+ calculation)
league_season AS (
    SELECT
        season,
        sum(woba_weight)::real / NULLIF(sum(woba_denom), 0)   AS lg_woba,
        count(*)                                                AS lg_pa
    FROM pa_events
    GROUP BY season
),
-- League runs per PA from fact_game_totals
league_runs AS (
    SELECT
        dg.season,
        sum(gt.runs)::real / NULLIF(sum(gt.plate_appearances), 0) AS lg_r_per_pa
    FROM production.fact_game_totals gt
    JOIN production.dim_game dg ON dg.game_pk = gt.game_pk
    GROUP BY dg.season
),
-- Run park factor from fact_game_totals (runs at venue vs league avg)
park_run_factor AS (
    SELECT
        dg.venue_id,
        dg.season,
        CASE WHEN count(*) >= 20 THEN
            (sum(gt.runs)::real / count(*)) /
            NULLIF(lr.lg_r_per_pa * (SELECT avg(plate_appearances)::real FROM production.fact_game_totals), 0)
        ELSE 1.0 END AS pf
    FROM production.fact_game_totals gt
    JOIN production.dim_game dg ON dg.game_pk = gt.game_pk
    JOIN league_runs lr ON lr.season = dg.season
    GROUP BY dg.venue_id, dg.season, lr.lg_r_per_pa
),
-- Player season aggregates
player_season AS (
    SELECT
        pe.batter_id,
        pe.season,
        count(*)                                               AS pa,
        sum(CASE WHEN pe.events NOT IN ('walk','intent_walk','hit_by_pitch','sac_fly','sac_bunt','catcher_interf')
                 THEN 1 ELSE 0 END)                            AS ab,
        sum(is_hit)                                            AS hits,
        sum(is_1b)                                             AS singles,
        sum(is_2b)                                             AS doubles,
        sum(is_3b)                                             AS triples,
        sum(is_hr)                                             AS hr,
        sum(is_bb)                                             AS bb,
        sum(is_ibb)                                            AS ibb,
        sum(is_hbp)                                            AS hbp,
        sum(is_k)                                              AS k,
        sum(is_sf)                                             AS sf,
        sum(woba_weight)::real / NULLIF(sum(woba_denom), 0)    AS woba,
        -- Weighted average park factor for this batter's home games
        avg(COALESCE(pf.pf, 1.0))                             AS avg_pf
    FROM pa_events pe
    LEFT JOIN park_run_factor pf
        ON pf.venue_id = pe.venue_id AND pf.season = pe.season
    GROUP BY pe.batter_id, pe.season
)
SELECT
    ps.batter_id,
    ps.season,
    ps.pa,
    ps.ab,
    ps.hits,
    ps.singles,
    ps.doubles,
    ps.triples,
    ps.hr,
    ps.bb,
    ps.ibb,
    ps.hbp,
    ps.k,
    ps.sf,
    -- rate stats
    ps.hits::real / NULLIF(ps.ab, 0)                           AS avg,
    (ps.hits + ps.bb + ps.hbp)::real /
        NULLIF(ps.ab + ps.bb + ps.hbp + ps.sf, 0)             AS obp,
    (ps.singles + 2*ps.doubles + 3*ps.triples + 4*ps.hr)::real
        / NULLIF(ps.ab, 0)                                     AS slg,
    (ps.hits::real / NULLIF(ps.ab, 0)) +
    ((ps.hits + ps.bb + ps.hbp)::real /
        NULLIF(ps.ab + ps.bb + ps.hbp + ps.sf, 0))            AS ops,
    ps.k::real / NULLIF(ps.pa, 0)                              AS k_pct,
    ps.bb::real / NULLIF(ps.pa, 0)                             AS bb_pct,
    -- wOBA
    ps.woba,
    -- wRC+ = (((wOBA - lgwOBA) / 1.15 + lgR/PA) / (lgR/PA * PF)) * 100
    CASE WHEN lr.lg_r_per_pa > 0 AND ps.avg_pf > 0 THEN
        (((ps.woba - ls.lg_woba) / 1.15 + lr.lg_r_per_pa) /
         (lr.lg_r_per_pa * ps.avg_pf)) * 100
    ELSE NULL END                                               AS wrc_plus,
    -- statcast quality of contact
    bb.xba,
    bb.xslg,
    bb.xwoba,
    bb.barrel_pct,
    bb.hard_hit_pct,
    bb.sweet_spot_pct,
    bb.bip_count
FROM player_season ps
JOIN league_season ls ON ls.season = ps.season
JOIN league_runs lr ON lr.season = ps.season
LEFT JOIN batter_bip bb ON bb.batter_id = ps.batter_id AND bb.season = ps.season
WHERE ps.pa >= 1
ON CONFLICT (batter_id, season) DO UPDATE SET
    pa = EXCLUDED.pa,
    ab = EXCLUDED.ab,
    hits = EXCLUDED.hits,
    singles = EXCLUDED.singles,
    doubles = EXCLUDED.doubles,
    triples = EXCLUDED.triples,
    hr = EXCLUDED.hr,
    bb = EXCLUDED.bb,
    ibb = EXCLUDED.ibb,
    hbp = EXCLUDED.hbp,
    k = EXCLUDED.k,
    sf = EXCLUDED.sf,
    avg = EXCLUDED.avg,
    obp = EXCLUDED.obp,
    slg = EXCLUDED.slg,
    ops = EXCLUDED.ops,
    k_pct = EXCLUDED.k_pct,
    bb_pct = EXCLUDED.bb_pct,
    woba = EXCLUDED.woba,
    wrc_plus = EXCLUDED.wrc_plus,
    xba = EXCLUDED.xba,
    xslg = EXCLUDED.xslg,
    xwoba = EXCLUDED.xwoba,
    barrel_pct = EXCLUDED.barrel_pct,
    hard_hit_pct = EXCLUDED.hard_hit_pct,
    sweet_spot_pct = EXCLUDED.sweet_spot_pct,
    bip_count = EXCLUDED.bip_count;

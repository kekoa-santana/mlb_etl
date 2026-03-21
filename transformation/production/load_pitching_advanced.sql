-- production.fact_pitching_advanced
-- Player-season pitching: K%, BB%, SwStr%, CSW%, Zone%, Chase%, batted ball quality against
-- Sources: fact_pitch + fact_pa + sat_batted_balls + dim_game

INSERT INTO production.fact_pitching_advanced (
    pitcher_id, season,
    total_pitches, batters_faced, pa_against,
    k_pct, bb_pct, swstr_pct, csw_pct, zone_pct, chase_pct, contact_pct,
    xwoba_against, barrel_pct_against, hard_hit_pct_against, bip_against,
    woba_against
)
WITH pitch_agg AS (
    SELECT
        fp.pitcher_id,
        dg.season,
        count(*)                                                     AS total_pitches,
        sum(fp.is_whiff::int)                                        AS whiffs,
        sum(fp.is_called_strike::int)                                AS called_strikes,
        sum(fp.is_swing::int)                                        AS swings,
        sum(fp.is_bip::int)                                          AS bip,
        sum(CASE WHEN fp.zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END)   AS in_zone,
        sum(CASE WHEN fp.zone > 9 THEN 1 ELSE 0 END)                AS out_zone,
        sum(CASE WHEN fp.zone > 9 AND fp.is_swing THEN 1 ELSE 0 END) AS chase
    FROM production.fact_pitch fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    GROUP BY fp.pitcher_id, dg.season
),
pa_outcomes AS (
    SELECT
        pa.pitcher_id,
        dg.season,
        count(*)                                                           AS pa_against,
        sum(CASE WHEN pa.events IN ('strikeout','strikeout_double_play')
            THEN 1 ELSE 0 END)                                            AS k_against,
        sum(CASE WHEN pa.events = 'walk' THEN 1 ELSE 0 END)              AS bb_against,
        sum(CASE
            WHEN pa.events = 'walk' THEN 0.69
            WHEN pa.events = 'hit_by_pitch' THEN 0.72
            WHEN pa.events = 'single' THEN 0.88
            WHEN pa.events = 'double' THEN 1.24
            WHEN pa.events = 'triple' THEN 1.56
            WHEN pa.events = 'home_run' THEN 2.00
            ELSE 0
        END)::real /
        NULLIF(sum(CASE WHEN pa.events NOT IN ('intent_walk','sac_bunt','catcher_interf')
                   THEN 1 ELSE 0 END), 0)                                AS woba_against
    FROM production.fact_pa pa
    JOIN production.dim_game dg ON dg.game_pk = pa.game_pk
    GROUP BY pa.pitcher_id, dg.season
),
pitcher_bip AS (
    SELECT
        fpi.pitcher_id,
        dg.season,
        count(*)                                                    AS bip_against,
        avg(NULLIF(sb.xwoba, 'NaN'::real))                            AS xwoba_against,
        avg(CASE WHEN sb.launch_speed >= 98
                  AND sb.launch_angle BETWEEN 26 AND 30
             THEN 1.0 ELSE 0.0 END)                                AS barrel_pct_against,
        avg(sb.hard_hit::int)                                       AS hard_hit_pct_against
    FROM production.sat_batted_balls sb
    JOIN production.fact_pitch fpi ON fpi.pitch_id = sb.pitch_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    GROUP BY fpi.pitcher_id, dg.season
)
SELECT
    po.pitcher_id,
    po.season,
    pa.total_pitches,
    po.pa_against                                                   AS batters_faced,
    po.pa_against,
    po.k_against::real / NULLIF(po.pa_against, 0)                  AS k_pct,
    po.bb_against::real / NULLIF(po.pa_against, 0)                 AS bb_pct,
    pa.whiffs::real / NULLIF(pa.total_pitches, 0)                   AS swstr_pct,
    (pa.called_strikes + pa.whiffs)::real /
        NULLIF(pa.total_pitches, 0)                                 AS csw_pct,
    pa.in_zone::real / NULLIF(pa.total_pitches, 0)                  AS zone_pct,
    pa.chase::real / NULLIF(pa.out_zone, 0)                         AS chase_pct,
    (pa.swings - pa.whiffs)::real / NULLIF(pa.swings, 0)            AS contact_pct,
    bb.xwoba_against,
    bb.barrel_pct_against,
    bb.hard_hit_pct_against,
    bb.bip_against,
    po.woba_against
FROM pa_outcomes po
JOIN pitch_agg pa ON pa.pitcher_id = po.pitcher_id AND pa.season = po.season
LEFT JOIN pitcher_bip bb ON bb.pitcher_id = po.pitcher_id AND bb.season = po.season
WHERE po.pa_against >= 1
ON CONFLICT (pitcher_id, season) DO UPDATE SET
    total_pitches = EXCLUDED.total_pitches,
    batters_faced = EXCLUDED.batters_faced,
    pa_against = EXCLUDED.pa_against,
    k_pct = EXCLUDED.k_pct,
    bb_pct = EXCLUDED.bb_pct,
    swstr_pct = EXCLUDED.swstr_pct,
    csw_pct = EXCLUDED.csw_pct,
    zone_pct = EXCLUDED.zone_pct,
    chase_pct = EXCLUDED.chase_pct,
    contact_pct = EXCLUDED.contact_pct,
    xwoba_against = EXCLUDED.xwoba_against,
    barrel_pct_against = EXCLUDED.barrel_pct_against,
    hard_hit_pct_against = EXCLUDED.hard_hit_pct_against,
    bip_against = EXCLUDED.bip_against,
    woba_against = EXCLUDED.woba_against;

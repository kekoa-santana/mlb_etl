-- production.fact_pitch_type_run_value
-- Per-pitcher, per-season, per-pitch-type: run values, usage, outcome rates, pitch characteristics
-- Sources: fact_pitch + sat_batted_balls + sat_pitch_shape + dim_game
-- Requires: delta_run_exp column on fact_pitch (backfilled from parquet)

INSERT INTO production.fact_pitch_type_run_value (
    pitcher_id, season, pitch_type, pitch_name,
    pitches_thrown, usage_pct,
    run_value_total, run_value_per_100,
    whiff_pct, csw_pct, zone_pct, chase_pct, put_away_pct,
    xwoba_contact, avg_launch_speed, avg_launch_angle,
    avg_velo, avg_spin, avg_pfx_x, avg_pfx_z
)
WITH pitcher_totals AS (
    SELECT
        fp.pitcher_id,
        dg.season,
        count(*) AS total_pitches
    FROM production.fact_pitch fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    GROUP BY fp.pitcher_id, dg.season
),
pitch_type_agg AS (
    SELECT
        fp.pitcher_id,
        dg.season,
        fp.pitch_type,
        -- Most common pitch_name for this type
        mode() WITHIN GROUP (ORDER BY fp.pitch_name)               AS pitch_name,
        count(*)                                                    AS pitches_thrown,
        -- Run values (from delta_run_exp - negative = good for pitcher)
        sum(fp.delta_run_exp)                                       AS run_value_total,
        -- Outcome counts
        sum(fp.is_whiff::int)                                       AS whiffs,
        sum(fp.is_swing::int)                                       AS swings,
        sum((fp.is_whiff OR fp.is_called_strike)::int)             AS csw,
        sum(CASE WHEN fp.zone BETWEEN 1 AND 9 THEN 1 ELSE 0 END)  AS in_zone,
        sum(CASE WHEN fp.zone > 9 THEN 1 ELSE 0 END)               AS out_zone,
        sum(CASE WHEN fp.zone > 9 AND fp.is_swing
            THEN 1 ELSE 0 END)                                     AS chase,
        -- Put-away: K on 2-strike counts
        sum(CASE WHEN fp.strikes = 2
                  AND fp.is_whiff
            THEN 1 ELSE 0 END)                                     AS put_away,
        sum(CASE WHEN fp.strikes = 2 THEN 1 ELSE 0 END)           AS two_strike_pitches,
        -- Pitch characteristics
        avg(fp.release_speed)                                       AS avg_velo,
        avg(fp.release_spin_rate)                                   AS avg_spin,
        avg(fp.pfx_x)                                               AS avg_pfx_x,
        avg(fp.pfx_z)                                               AS avg_pfx_z
    FROM production.fact_pitch fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    WHERE fp.pitch_type IS NOT NULL
    GROUP BY fp.pitcher_id, dg.season, fp.pitch_type
),
-- Batted ball quality per pitch type
bip_quality AS (
    SELECT
        fpi.pitcher_id,
        dg.season,
        fpi.pitch_type,
        avg(NULLIF(sb.xwoba, 'NaN'::real))                            AS xwoba_contact,
        avg(sb.launch_speed)                                        AS avg_launch_speed,
        avg(sb.launch_angle)                                        AS avg_launch_angle
    FROM production.sat_batted_balls sb
    JOIN production.fact_pitch fpi ON fpi.pitch_id = sb.pitch_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    WHERE fpi.pitch_type IS NOT NULL
    GROUP BY fpi.pitcher_id, dg.season, fpi.pitch_type
)
SELECT
    pta.pitcher_id,
    pta.season,
    pta.pitch_type,
    pta.pitch_name,
    pta.pitches_thrown,
    pta.pitches_thrown::real / NULLIF(pt.total_pitches, 0)          AS usage_pct,
    -- Run values (negative = good for pitcher in Statcast convention)
    pta.run_value_total,
    CASE WHEN pta.pitches_thrown >= 1 THEN
        (pta.run_value_total / pta.pitches_thrown) * 100
    ELSE NULL END                                                   AS run_value_per_100,
    -- Outcome rates
    pta.whiffs::real / NULLIF(pta.swings, 0)                       AS whiff_pct,
    pta.csw::real / NULLIF(pta.pitches_thrown, 0)                   AS csw_pct,
    pta.in_zone::real / NULLIF(pta.pitches_thrown, 0)               AS zone_pct,
    pta.chase::real / NULLIF(pta.out_zone, 0)                       AS chase_pct,
    pta.put_away::real / NULLIF(pta.two_strike_pitches, 0)          AS put_away_pct,
    -- Batted ball quality
    bq.xwoba_contact,
    bq.avg_launch_speed,
    bq.avg_launch_angle,
    -- Pitch characteristics
    pta.avg_velo,
    pta.avg_spin,
    pta.avg_pfx_x,
    pta.avg_pfx_z
FROM pitch_type_agg pta
JOIN pitcher_totals pt ON pt.pitcher_id = pta.pitcher_id AND pt.season = pta.season
LEFT JOIN bip_quality bq
    ON bq.pitcher_id = pta.pitcher_id
   AND bq.season = pta.season
   AND bq.pitch_type = pta.pitch_type
WHERE pta.pitches_thrown >= 10
ON CONFLICT (pitcher_id, season, pitch_type) DO UPDATE SET
    pitch_name = EXCLUDED.pitch_name,
    pitches_thrown = EXCLUDED.pitches_thrown,
    usage_pct = EXCLUDED.usage_pct,
    run_value_total = EXCLUDED.run_value_total,
    run_value_per_100 = EXCLUDED.run_value_per_100,
    whiff_pct = EXCLUDED.whiff_pct,
    csw_pct = EXCLUDED.csw_pct,
    zone_pct = EXCLUDED.zone_pct,
    chase_pct = EXCLUDED.chase_pct,
    put_away_pct = EXCLUDED.put_away_pct,
    xwoba_contact = EXCLUDED.xwoba_contact,
    avg_launch_speed = EXCLUDED.avg_launch_speed,
    avg_launch_angle = EXCLUDED.avg_launch_angle,
    avg_velo = EXCLUDED.avg_velo,
    avg_spin = EXCLUDED.avg_spin,
    avg_pfx_x = EXCLUDED.avg_pfx_x,
    avg_pfx_z = EXCLUDED.avg_pfx_z;

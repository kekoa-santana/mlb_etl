-- production.fact_catcher_framing
-- Catcher framing metrics computed from pitch-level data.
-- Shadow zone: pitches within ~1 ball width of the strike zone edge.
-- Compares each catcher's called strike rate to league average for the
-- same pitch location bucket, then converts to run value (~0.125 per extra strike).
-- Sources: staging.statcast_pitches + fact_pitch + dim_game + dim_player

INSERT INTO production.fact_catcher_framing (
    player_id, season, player_name, team_name,
    pitches_called, runs_extra_strikes,
    strike_rate, expected_strike_rate, strike_rate_diff,
    shadow_zone_pitches, shadow_zone_strike_rate
)
WITH called_pitches AS (
    SELECT
        sp.fielder_2                                          AS catcher_id,
        dg.season,
        sp.plate_x,
        sp.plate_z,
        sp.sz_top,
        sp.sz_bot,
        fp.is_called_strike,
        -- Shadow zone: within ~0.33 ft of zone edge
        CASE WHEN sp.plate_x IS NOT NULL AND sp.plate_z IS NOT NULL
              AND sp.sz_top IS NOT NULL AND sp.sz_bot IS NOT NULL
              AND (
                  (abs(sp.plate_x) BETWEEN 0.5 AND 1.16)
                  OR (sp.plate_z BETWEEN sp.sz_bot - 0.33 AND sp.sz_bot + 0.33)
                  OR (sp.plate_z BETWEEN sp.sz_top - 0.33 AND sp.sz_top + 0.33)
              )
             THEN true ELSE false END                        AS is_shadow_zone,
        -- Location bucket (5x5 grid)
        width_bucket(sp.plate_x::float8, -1.5::float8, 1.5::float8, 5)  AS x_bucket,
        width_bucket(sp.plate_z::float8,
                     LEAST(sp.sz_bot - 0.5, 1.0)::float8,
                     GREATEST(sp.sz_top + 0.5, 4.5)::float8, 5)         AS z_bucket
    FROM staging.statcast_pitches sp
    JOIN production.dim_game dg ON dg.game_pk = sp.game_pk
    JOIN production.fact_pitch fp
        ON fp.game_pk = sp.game_pk
       AND fp.game_counter = sp.game_counter
       AND fp.pitch_number = sp.pitch_number
    WHERE sp.fielder_2 IS NOT NULL
      AND fp.is_called_strike IS NOT NULL
      AND (fp.description = 'called_strike' OR fp.description = 'ball'
           OR fp.description = 'blocked_ball')
      AND sp.plate_x IS NOT NULL AND sp.plate_x != 'NaN'::real
      AND sp.plate_z IS NOT NULL AND sp.plate_z != 'NaN'::real
      AND sp.sz_top IS NOT NULL AND sp.sz_top != 'NaN'::real
      AND sp.sz_bot IS NOT NULL AND sp.sz_bot != 'NaN'::real
),
league_bucket_rates AS (
    SELECT
        season, x_bucket, z_bucket,
        avg(is_called_strike::int)::real AS expected_cs_rate
    FROM called_pitches
    GROUP BY season, x_bucket, z_bucket
    HAVING count(*) >= 50
),
catcher_agg AS (
    SELECT
        cp.catcher_id,
        cp.season,
        count(*)                                                        AS pitches_called,
        avg(cp.is_called_strike::int)::real                            AS strike_rate,
        count(*) FILTER (WHERE cp.is_shadow_zone)                       AS shadow_zone_pitches,
        avg(cp.is_called_strike::int) FILTER (WHERE cp.is_shadow_zone)::real
                                                                        AS shadow_zone_strike_rate,
        avg(lb.expected_cs_rate)::real                                  AS expected_strike_rate
    FROM called_pitches cp
    LEFT JOIN league_bucket_rates lb
        ON lb.season = cp.season
       AND lb.x_bucket = cp.x_bucket
       AND lb.z_bucket = cp.z_bucket
    GROUP BY cp.catcher_id, cp.season
    HAVING count(*) >= 500
)
SELECT
    ca.catcher_id                                              AS player_id,
    ca.season,
    dp.player_name,
    dt.team_name,
    ca.pitches_called,
    ((ca.strike_rate - ca.expected_strike_rate)
        * ca.pitches_called * 0.125)::real                     AS runs_extra_strikes,
    ca.strike_rate,
    ca.expected_strike_rate,
    (ca.strike_rate - ca.expected_strike_rate)::real           AS strike_rate_diff,
    ca.shadow_zone_pitches,
    ca.shadow_zone_strike_rate
FROM catcher_agg ca
LEFT JOIN production.dim_player dp ON dp.player_id = ca.catcher_id
LEFT JOIN LATERAL (
    SELECT dt2.team_name
    FROM production.fact_lineup fl
    JOIN production.dim_game dg2 ON dg2.game_pk = fl.game_pk AND dg2.season = ca.season
    JOIN production.dim_team dt2 ON dt2.team_id = fl.team_id
    WHERE fl.player_id = ca.catcher_id
    GROUP BY dt2.team_name
    ORDER BY count(*) DESC
    LIMIT 1
) dt ON TRUE
ON CONFLICT (player_id, season) DO UPDATE SET
    player_name = EXCLUDED.player_name,
    team_name = EXCLUDED.team_name,
    pitches_called = EXCLUDED.pitches_called,
    runs_extra_strikes = EXCLUDED.runs_extra_strikes,
    strike_rate = EXCLUDED.strike_rate,
    expected_strike_rate = EXCLUDED.expected_strike_rate,
    strike_rate_diff = EXCLUDED.strike_rate_diff,
    shadow_zone_pitches = EXCLUDED.shadow_zone_pitches,
    shadow_zone_strike_rate = EXCLUDED.shadow_zone_strike_rate;

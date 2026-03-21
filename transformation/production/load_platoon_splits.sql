INSERT INTO production.fact_platoon_splits (
    player_id, season, player_role, platoon_side,
    pa, ab, h, doubles, triples, hr, bb, k, hbp, sf,
    avg, obp, slg, ops, woba, k_pct, bb_pct,
    total_pitches, whiff_rate, chase_rate,
    hard_hit_pct, sweet_spot_pct, xwoba_avg
)
WITH batter_pa AS (
    SELECT
        fp.batter_id,
        dg.season,
        CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        COUNT(*) AS pa,
        COUNT(*) FILTER (WHERE fp.events NOT IN ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf')) AS ab,
        COUNT(*) FILTER (WHERE fp.events IN ('single', 'double', 'triple', 'home_run')) AS h,
        COUNT(*) FILTER (WHERE fp.events = 'double') AS doubles,
        COUNT(*) FILTER (WHERE fp.events = 'triple') AS triples,
        COUNT(*) FILTER (WHERE fp.events = 'home_run') AS hr,
        COUNT(*) FILTER (WHERE fp.events = 'walk') AS bb,
        COUNT(*) FILTER (WHERE fp.events IN ('strikeout', 'strikeout_double_play')) AS k,
        COUNT(*) FILTER (WHERE fp.events = 'hit_by_pitch') AS hbp,
        COUNT(*) FILTER (WHERE fp.events = 'sac_fly') AS sf
    FROM production.fact_pa fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    JOIN production.dim_player dp ON dp.player_id = fp.pitcher_id
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fp.batter_id, dg.season, CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END
),
batter_pitch_agg AS (
    SELECT
        fpi.batter_id,
        dg.season,
        CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        COUNT(*) AS total_pitches,
        COUNT(*) FILTER (WHERE fpi.is_whiff) AS whiffs,
        COUNT(*) FILTER (WHERE fpi.is_swing AND fpi.zone > 9) AS chases,
        COUNT(*) FILTER (WHERE fpi.zone > 9) AS out_of_zone
    FROM production.fact_pitch fpi
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    JOIN production.dim_player dp ON dp.player_id = fpi.pitcher_id
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fpi.batter_id, dg.season, CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END
),
batter_bb AS (
    SELECT
        fpi.batter_id,
        dg.season,
        CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        AVG(CASE WHEN sb.hard_hit THEN 1.0 ELSE 0.0 END) AS hard_hit_pct,
        AVG(CASE WHEN sb.sweet_spot THEN 1.0 ELSE 0.0 END) AS sweet_spot_pct,
        AVG(sb.xwoba) AS xwoba_avg
    FROM production.fact_pitch fpi
    JOIN production.sat_batted_balls sb ON sb.pitch_id = fpi.pitch_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    JOIN production.dim_player dp ON dp.player_id = fpi.pitcher_id
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fpi.batter_id, dg.season, CASE WHEN dp.pitch_hand = 'L' THEN 'vLH' ELSE 'vRH' END
),
batter_splits AS (
    SELECT
        bpa.batter_id AS player_id,
        bpa.season,
        'batter' AS player_role,
        bpa.platoon_side,
        bpa.pa, bpa.ab, bpa.h, bpa.doubles, bpa.triples, bpa.hr,
        bpa.bb, bpa.k, bpa.hbp, bpa.sf,
        bpa.h::numeric / NULLIF(bpa.ab, 0) AS avg,
        (bpa.h + bpa.bb + bpa.hbp)::numeric / NULLIF(bpa.ab + bpa.bb + bpa.hbp + bpa.sf, 0) AS obp,
        (bpa.h - bpa.doubles - bpa.triples - bpa.hr
            + bpa.doubles * 2 + bpa.triples * 3 + bpa.hr * 4)::numeric / NULLIF(bpa.ab, 0) AS slg,
        bpa.h::numeric / NULLIF(bpa.ab, 0)
            + (bpa.h + bpa.bb + bpa.hbp)::numeric / NULLIF(bpa.ab + bpa.bb + bpa.hbp + bpa.sf, 0) AS ops,
        NULL::real AS woba,
        bpa.k::numeric / NULLIF(bpa.pa, 0) AS k_pct,
        bpa.bb::numeric / NULLIF(bpa.pa, 0) AS bb_pct,
        bpia.total_pitches,
        bpia.whiffs::numeric / NULLIF(bpia.total_pitches, 0) AS whiff_rate,
        bpia.chases::numeric / NULLIF(bpia.out_of_zone, 0) AS chase_rate,
        bbb.hard_hit_pct,
        bbb.sweet_spot_pct,
        bbb.xwoba_avg
    FROM batter_pa bpa
    LEFT JOIN batter_pitch_agg bpia
        ON bpia.batter_id = bpa.batter_id
        AND bpia.season = bpa.season
        AND bpia.platoon_side = bpa.platoon_side
    LEFT JOIN batter_bb bbb
        ON bbb.batter_id = bpa.batter_id
        AND bbb.season = bpa.season
        AND bbb.platoon_side = bpa.platoon_side
),
pitcher_pa AS (
    SELECT
        fp.pitcher_id,
        dg.season,
        CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        COUNT(DISTINCT fp.pa_id) AS pa,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events NOT IN ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf')) AS ab,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events IN ('single', 'double', 'triple', 'home_run')) AS h,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'double') AS doubles,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'triple') AS triples,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'home_run') AS hr,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'walk') AS bb,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events IN ('strikeout', 'strikeout_double_play')) AS k,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'hit_by_pitch') AS hbp,
        COUNT(DISTINCT fp.pa_id) FILTER (WHERE fp.events = 'sac_fly') AS sf
    FROM production.fact_pa fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    JOIN production.fact_pitch fpi ON fpi.pa_id = fp.pa_id
    WHERE dg.game_type NOT IN ('E', 'S')
      AND fpi.batter_stand IS NOT NULL
    GROUP BY fp.pitcher_id, dg.season, CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END
),
pitcher_pitch_agg AS (
    SELECT
        fpi.pitcher_id,
        dg.season,
        CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        COUNT(*) AS total_pitches,
        COUNT(*) FILTER (WHERE fpi.is_whiff) AS whiffs,
        COUNT(*) FILTER (WHERE fpi.is_swing AND fpi.zone > 9) AS chases,
        COUNT(*) FILTER (WHERE fpi.zone > 9) AS out_of_zone
    FROM production.fact_pitch fpi
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
      AND fpi.batter_stand IS NOT NULL
    GROUP BY fpi.pitcher_id, dg.season, CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END
),
pitcher_bb AS (
    SELECT
        fpi.pitcher_id,
        dg.season,
        CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END AS platoon_side,
        AVG(CASE WHEN sb.hard_hit THEN 1.0 ELSE 0.0 END) AS hard_hit_pct,
        AVG(CASE WHEN sb.sweet_spot THEN 1.0 ELSE 0.0 END) AS sweet_spot_pct,
        AVG(sb.xwoba) AS xwoba_avg
    FROM production.fact_pitch fpi
    JOIN production.sat_batted_balls sb ON sb.pitch_id = fpi.pitch_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
      AND fpi.batter_stand IS NOT NULL
    GROUP BY fpi.pitcher_id, dg.season, CASE WHEN fpi.batter_stand = 'L' THEN 'vLH' ELSE 'vRH' END
),
pitcher_splits AS (
    SELECT
        ppa.pitcher_id AS player_id,
        ppa.season,
        'pitcher' AS player_role,
        ppa.platoon_side,
        ppa.pa, ppa.ab, ppa.h, ppa.doubles, ppa.triples, ppa.hr,
        ppa.bb, ppa.k, ppa.hbp, ppa.sf,
        ppa.h::numeric / NULLIF(ppa.ab, 0) AS avg,
        (ppa.h + ppa.bb + ppa.hbp)::numeric / NULLIF(ppa.ab + ppa.bb + ppa.hbp + ppa.sf, 0) AS obp,
        (ppa.h - ppa.doubles - ppa.triples - ppa.hr
            + ppa.doubles * 2 + ppa.triples * 3 + ppa.hr * 4)::numeric / NULLIF(ppa.ab, 0) AS slg,
        ppa.h::numeric / NULLIF(ppa.ab, 0)
            + (ppa.h + ppa.bb + ppa.hbp)::numeric / NULLIF(ppa.ab + ppa.bb + ppa.hbp + ppa.sf, 0) AS ops,
        NULL::real AS woba,
        ppa.k::numeric / NULLIF(ppa.pa, 0) AS k_pct,
        ppa.bb::numeric / NULLIF(ppa.pa, 0) AS bb_pct,
        ppia.total_pitches,
        ppia.whiffs::numeric / NULLIF(ppia.total_pitches, 0) AS whiff_rate,
        ppia.chases::numeric / NULLIF(ppia.out_of_zone, 0) AS chase_rate,
        pbb.hard_hit_pct,
        pbb.sweet_spot_pct,
        pbb.xwoba_avg
    FROM pitcher_pa ppa
    LEFT JOIN pitcher_pitch_agg ppia
        ON ppia.pitcher_id = ppa.pitcher_id
        AND ppia.season = ppa.season
        AND ppia.platoon_side = ppa.platoon_side
    LEFT JOIN pitcher_bb pbb
        ON pbb.pitcher_id = ppa.pitcher_id
        AND pbb.season = ppa.season
        AND pbb.platoon_side = ppa.platoon_side
)
SELECT * FROM batter_splits
UNION ALL
SELECT * FROM pitcher_splits

ON CONFLICT (player_id, season, player_role, platoon_side) DO UPDATE
SET pa             = EXCLUDED.pa,
    ab             = EXCLUDED.ab,
    h              = EXCLUDED.h,
    doubles        = EXCLUDED.doubles,
    triples        = EXCLUDED.triples,
    hr             = EXCLUDED.hr,
    bb             = EXCLUDED.bb,
    k              = EXCLUDED.k,
    hbp            = EXCLUDED.hbp,
    sf             = EXCLUDED.sf,
    avg            = EXCLUDED.avg,
    obp            = EXCLUDED.obp,
    slg            = EXCLUDED.slg,
    ops            = EXCLUDED.ops,
    woba           = EXCLUDED.woba,
    k_pct          = EXCLUDED.k_pct,
    bb_pct         = EXCLUDED.bb_pct,
    total_pitches  = EXCLUDED.total_pitches,
    whiff_rate     = EXCLUDED.whiff_rate,
    chase_rate     = EXCLUDED.chase_rate,
    hard_hit_pct   = EXCLUDED.hard_hit_pct,
    sweet_spot_pct = EXCLUDED.sweet_spot_pct,
    xwoba_avg      = EXCLUDED.xwoba_avg;

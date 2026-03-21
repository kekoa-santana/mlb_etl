INSERT INTO production.fact_matchup_history (
    batter_id, pitcher_id,
    pa, ab, h, doubles, triples, hr, bb, k, hbp, sf,
    avg, obp, slg, ops,
    total_pitches, whiff_rate, chase_rate, zone_contact_rate,
    avg_exit_velo, avg_launch_angle, hard_hit_pct, xwoba_avg,
    first_matchup_date, last_matchup_date
)
WITH matchup_pa AS (
    SELECT
        fp.batter_id,
        fp.pitcher_id,
        COUNT(*) AS pa,
        COUNT(*) FILTER (WHERE fp.events NOT IN ('walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt', 'sac_fly_double_play', 'catcher_interf')) AS ab,
        COUNT(*) FILTER (WHERE fp.events IN ('single', 'double', 'triple', 'home_run')) AS h,
        COUNT(*) FILTER (WHERE fp.events = 'double') AS doubles,
        COUNT(*) FILTER (WHERE fp.events = 'triple') AS triples,
        COUNT(*) FILTER (WHERE fp.events = 'home_run') AS hr,
        COUNT(*) FILTER (WHERE fp.events = 'walk') AS bb,
        COUNT(*) FILTER (WHERE fp.events IN ('strikeout', 'strikeout_double_play')) AS k,
        COUNT(*) FILTER (WHERE fp.events = 'hit_by_pitch') AS hbp,
        COUNT(*) FILTER (WHERE fp.events = 'sac_fly') AS sf,
        MIN(dg.game_date) AS first_matchup_date,
        MAX(dg.game_date) AS last_matchup_date
    FROM production.fact_pa fp
    JOIN production.dim_game dg ON dg.game_pk = fp.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fp.batter_id, fp.pitcher_id
),
matchup_pitches AS (
    SELECT
        fpi.batter_id,
        fpi.pitcher_id,
        COUNT(*) AS total_pitches,
        COUNT(*) FILTER (WHERE fpi.is_whiff) AS whiffs,
        COUNT(*) FILTER (WHERE fpi.is_swing AND fpi.zone > 9) AS chases,
        COUNT(*) FILTER (WHERE fpi.zone > 9) AS out_of_zone,
        COUNT(*) FILTER (WHERE fpi.zone <= 9 AND fpi.zone IS NOT NULL AND NOT COALESCE(fpi.is_whiff, false) AND COALESCE(fpi.is_swing, false)) AS zone_contact,
        COUNT(*) FILTER (WHERE fpi.zone <= 9 AND fpi.zone IS NOT NULL AND COALESCE(fpi.is_swing, false)) AS zone_swings
    FROM production.fact_pitch fpi
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fpi.batter_id, fpi.pitcher_id
),
matchup_bb AS (
    SELECT
        fpi.batter_id,
        fpi.pitcher_id,
        AVG(sb.launch_speed) AS avg_exit_velo,
        AVG(sb.launch_angle) AS avg_launch_angle,
        AVG(CASE WHEN sb.hard_hit THEN 1.0 ELSE 0.0 END) AS hard_hit_pct,
        AVG(sb.xwoba) AS xwoba_avg
    FROM production.fact_pitch fpi
    JOIN production.sat_batted_balls sb ON sb.pitch_id = fpi.pitch_id
    JOIN production.dim_game dg ON dg.game_pk = fpi.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY fpi.batter_id, fpi.pitcher_id
)
SELECT
    mpa.batter_id,
    mpa.pitcher_id,
    mpa.pa, mpa.ab, mpa.h, mpa.doubles, mpa.triples, mpa.hr,
    mpa.bb, mpa.k, mpa.hbp, mpa.sf,
    mpa.h::numeric / NULLIF(mpa.ab, 0) AS avg,
    (mpa.h + mpa.bb + mpa.hbp)::numeric / NULLIF(mpa.ab + mpa.bb + mpa.hbp + mpa.sf, 0) AS obp,
    (mpa.h - mpa.doubles - mpa.triples - mpa.hr
        + mpa.doubles * 2 + mpa.triples * 3 + mpa.hr * 4)::numeric / NULLIF(mpa.ab, 0) AS slg,
    mpa.h::numeric / NULLIF(mpa.ab, 0)
        + (mpa.h + mpa.bb + mpa.hbp)::numeric / NULLIF(mpa.ab + mpa.bb + mpa.hbp + mpa.sf, 0) AS ops,
    mp.total_pitches,
    mp.whiffs::numeric / NULLIF(mp.total_pitches, 0) AS whiff_rate,
    mp.chases::numeric / NULLIF(mp.out_of_zone, 0) AS chase_rate,
    mp.zone_contact::numeric / NULLIF(mp.zone_swings, 0) AS zone_contact_rate,
    mbb.avg_exit_velo,
    mbb.avg_launch_angle,
    mbb.hard_hit_pct,
    mbb.xwoba_avg,
    mpa.first_matchup_date,
    mpa.last_matchup_date
FROM matchup_pa mpa
LEFT JOIN matchup_pitches mp
    ON mp.batter_id = mpa.batter_id AND mp.pitcher_id = mpa.pitcher_id
LEFT JOIN matchup_bb mbb
    ON mbb.batter_id = mpa.batter_id AND mbb.pitcher_id = mpa.pitcher_id

ON CONFLICT (batter_id, pitcher_id) DO UPDATE
SET pa                 = EXCLUDED.pa,
    ab                 = EXCLUDED.ab,
    h                  = EXCLUDED.h,
    doubles            = EXCLUDED.doubles,
    triples            = EXCLUDED.triples,
    hr                 = EXCLUDED.hr,
    bb                 = EXCLUDED.bb,
    k                  = EXCLUDED.k,
    hbp                = EXCLUDED.hbp,
    sf                 = EXCLUDED.sf,
    avg                = EXCLUDED.avg,
    obp                = EXCLUDED.obp,
    slg                = EXCLUDED.slg,
    ops                = EXCLUDED.ops,
    total_pitches      = EXCLUDED.total_pitches,
    whiff_rate         = EXCLUDED.whiff_rate,
    chase_rate         = EXCLUDED.chase_rate,
    zone_contact_rate  = EXCLUDED.zone_contact_rate,
    avg_exit_velo      = EXCLUDED.avg_exit_velo,
    avg_launch_angle   = EXCLUDED.avg_launch_angle,
    hard_hit_pct       = EXCLUDED.hard_hit_pct,
    xwoba_avg          = EXCLUDED.xwoba_avg,
    first_matchup_date = EXCLUDED.first_matchup_date,
    last_matchup_date  = EXCLUDED.last_matchup_date;

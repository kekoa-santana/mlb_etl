INSERT INTO production.sat_batted_balls (
    pitch_id, bb_type, events, launch_speed, launch_angle,
    hit_distance_sc, hc_x, hc_y, is_homerun, pa_id, xba,
    xslg, xwoba, woba_value, babip_value, iso_value,
    hit_location, hard_hit, sweet_spot, ideal_contact, is_barrel,
    la_band, ev_band, hc_x_centered, spray_bucket
)
SELECT
    p.pitch_id,
    bb.bb_type,
    bb.events,
    bb.launch_speed,
    bb.launch_angle,
    bb.hit_distance_sc,
    bb.hc_x,
    bb.hc_y,
    bb.is_homerun,
    p.pa_id,
    bb.estimated_ba_using_speedangle AS xba,
    bb.estimated_slg_using_speedangle AS xslg,
    bb.estimated_woba_using_speedangle AS xwoba,
    bb.woba_value,
    bb.babip_value,
    bb.iso_value,
    bb.hit_location,
    
    CASE 
        WHEN bb.launch_speed IS NULL THEN NULL
        WHEN bb.launch_speed >= 95 THEN TRUE ELSE FALSE END AS hard_hit,
        
    CASE 
        WHEN bb.launch_angle IS NULL THEN NULL
        WHEN bb.launch_angle BETWEEN 8 AND 32 THEN TRUE ELSE FALSE END AS sweet_spot,

    CASE
        WHEN bb.launch_angle IS NULL OR bb.launch_speed IS NULL THEN NULL
        WHEN bb.launch_speed >= 95 AND bb.launch_angle BETWEEN 8 AND 32 THEN TRUE ELSE FALSE END AS ideal_contact,

    CASE
        WHEN bb.launch_speed IS NULL OR bb.launch_angle IS NULL THEN NULL
        WHEN bb.launch_speed < 98 THEN FALSE
        WHEN bb.launch_speed >= 116 AND bb.launch_angle BETWEEN 8 AND 50 THEN TRUE
        WHEN bb.launch_speed >= 115 AND bb.launch_angle BETWEEN 9 AND 48 THEN TRUE
        WHEN bb.launch_speed >= 114 AND bb.launch_angle BETWEEN 10 AND 47 THEN TRUE
        WHEN bb.launch_speed >= 113 AND bb.launch_angle BETWEEN 11 AND 46 THEN TRUE
        WHEN bb.launch_speed >= 112 AND bb.launch_angle BETWEEN 12 AND 45 THEN TRUE
        WHEN bb.launch_speed >= 111 AND bb.launch_angle BETWEEN 13 AND 44 THEN TRUE
        WHEN bb.launch_speed >= 110 AND bb.launch_angle BETWEEN 14 AND 43 THEN TRUE
        WHEN bb.launch_speed >= 109 AND bb.launch_angle BETWEEN 15 AND 42 THEN TRUE
        WHEN bb.launch_speed >= 108 AND bb.launch_angle BETWEEN 16 AND 41 THEN TRUE
        WHEN bb.launch_speed >= 107 AND bb.launch_angle BETWEEN 17 AND 40 THEN TRUE
        WHEN bb.launch_speed >= 106 AND bb.launch_angle BETWEEN 18 AND 39 THEN TRUE
        WHEN bb.launch_speed >= 105 AND bb.launch_angle BETWEEN 19 AND 38 THEN TRUE
        WHEN bb.launch_speed >= 104 AND bb.launch_angle BETWEEN 20 AND 37 THEN TRUE
        WHEN bb.launch_speed >= 103 AND bb.launch_angle BETWEEN 21 AND 36 THEN TRUE
        WHEN bb.launch_speed >= 102 AND bb.launch_angle BETWEEN 22 AND 35 THEN TRUE
        WHEN bb.launch_speed >= 101 AND bb.launch_angle BETWEEN 23 AND 34 THEN TRUE
        WHEN bb.launch_speed >= 100 AND bb.launch_angle BETWEEN 24 AND 33 THEN TRUE
        WHEN bb.launch_speed >= 99  AND bb.launch_angle BETWEEN 25 AND 31 THEN TRUE
        WHEN bb.launch_speed >= 98  AND bb.launch_angle BETWEEN 26 AND 30 THEN TRUE
        ELSE FALSE
    END AS is_barrel,

    CASE
        WHEN bb.launch_angle IS NULL THEN NULL
        WHEN bb.launch_angle < 10 THEN 'GB'
        WHEN bb.launch_angle < 25 THEN 'LD'
        WHEN bb.launch_angle < 50 THEN 'FB'
        ELSE 'PU'
    END AS la_band,

    CASE
        WHEN bb.launch_speed IS NULL THEN NULL
        WHEN bb.launch_speed < 95 THEN '<95'
        WHEN bb.launch_speed < 100 THEN '95-99'
        WHEN bb.launch_speed < 105 THEN '100-104'
        ELSE '105+'
    END AS ev_band,

    CASE WHEN bb.hc_x IS NULL THEN NULL ELSE (bb.hc_x - 125) END AS hc_x_centered,

    CASE
        WHEN bb.hc_x IS NULL OR bp.bat_side IS NULL THEN NULL
        WHEN bp.bat_side = 'R' AND (bb.hc_x-125) < -15 THEN 'pull'
        WHEN bp.bat_side = 'R' AND (bb.hc_x-125) > 15 THEN 'oppo'
        WHEN bp.bat_side = 'L' AND (bb.hc_x-125) > 15 THEN 'pull'
        WHEN bp.bat_side = 'L' AND (bb.hc_x-125) < -15 THEN 'oppo'
        ELSE 'middle'
    END AS spray_bucket

FROM staging.statcast_batted_balls bb
JOIN production.fact_pitch p
    ON bb.game_pk = p.game_pk
    AND bb.pitch_number = p.pitch_number
    AND bb.game_counter = p.game_counter
JOIN production.dim_player bp
    ON p.batter_id = bp.player_id
ON CONFLICT (pitch_id) DO UPDATE 
SET bb_type = EXCLUDED.bb_type,
    events = EXCLUDED.events,
    launch_speed = EXCLUDED.launch_speed,
    launch_angle =  EXCLUDED.launch_angle,
    hit_distance_sc =  EXCLUDED.hit_distance_sc,
    hc_x = EXCLUDED.hc_x,
    hc_y = EXCLUDED.hc_y,
    is_homerun = EXCLUDED.is_homerun,
    pa_id = EXCLUDED.pa_id,
    xba = EXCLUDED.xba,
    xslg = EXCLUDED.xslg,
    xwoba = EXCLUDED.xwoba,
    woba_value = EXCLUDED.woba_value,
    babip_value = EXCLUDED.babip_value,
    iso_value = EXCLUDED.iso_value,
    hit_location = EXCLUDED.hit_location,
    hard_hit = EXCLUDED.hard_hit,
    sweet_spot = EXCLUDED.sweet_spot,
    ideal_contact = EXCLUDED.ideal_contact,
    is_barrel = EXCLUDED.is_barrel,
    la_band = EXCLUDED.la_band,
    ev_band = EXCLUDED.ev_band,
    hc_x_centered = EXCLUDED.hc_x_centered,
    spray_bucket = EXCLUDED.spray_bucket;
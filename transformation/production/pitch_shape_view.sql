CREATE OR REPLACE VIEW production.v_pitch_shape_features AS
SELECT
    s.pitch_id,
    p.pa_id,
    p.game_pk,
    p.game_counter,
    p.pitcher_id,
    p.batter_id,
    p.pitch_number,
    p.pitch_type,
    p.pitch_name,
    p.description,
    CAST(pp.pitch_hand AS text) AS pitcher_throws,
    CAST(bp.bat_side AS text) AS batter_stand,
    p.balls,
    p.strikes,

    s.release_pos_x,
    s.release_pos_y,
    s.release_pos_z,
    s.release_extension,
    s.release_speed,
    s.release_spin_rate,
    s.spin_axis,
    s.pfx_x,
    s.pfx_z,
    s.vx0,
    s.vy0,
    s.vz0,
    s.ax,
    s.ay,
    s.az,
    s.plate_x,
    s.plate_z,
    s.sz_top,
    s.sz_bot,

    p.is_swing,
    p.is_whiff,
    p.is_called_strike,
    p.is_foul,
    p.is_bip,

    (p.is_called_strike OR p.is_whiff) AS is_csw,
    (p.is_swing AND NOT p.is_whiff) AS is_contact,
    (p.is_swing AND p.is_foul) AS is_foul_contact,
    (p.is_swing AND p.is_bip) AS is_inplay_contact,

    (12.0*s.pfx_x) AS horizontal_break_inches,
    (12.0*s.pfx_z) AS induced_vertical_break_inches,
    (12.0*sqrt( (s.pfx_x * s.pfx_x) + (s.pfx_z * s.pfx_z) )) AS movement_mag_inches,

    CASE
        WHEN s.release_spin_rate IS NULL OR s.release_spin_rate= 0 THEN NULL
        ELSE (12.0 * sqrt( (s.pfx_x*s.pfx_x) + (s.pfx_z*s.pfx_z) )) / (s.release_spin_rate / 1000.0)
    END AS movement_per_1000rpm,

    CASE
        WHEN pp.pitch_hand = bp.bat_side THEN 'same'
        WHEN pp.pitch_hand IS NULL OR bp.bat_side IS NULL THEN NULL
        ELSE 'opposite'
    END AS platoon,

    CASE
        WHEN pp.pitch_hand = 'R' THEN 12.0*s.pfx_x
        WHEN pp.pitch_hand = 'L' THEN -12.0*s.pfx_x
        ELSE 12.0*s.pfx_x
    END AS hb_in_arm_side_pos

FROM production.sat_pitch_shape s
JOIN production.fact_pitch p
    ON p.pitch_id = s.pitch_id
JOIN production.dim_player bp
    ON p.batter_id = bp.player_id
JOIN production.dim_player pp
    ON p.pitcher_id = pp.player_id;
INSERT INTO production.sat_pitch_shape (
    pitch_id, release_pos_x, release_pos_y, release_pos_z, release_spin_rate,
    release_extension, release_speed, spin_axis, pfx_x, pfx_z, vx0, vy0, vz0, 
    ax, ay, az, plate_x, plate_z, sz_top, sz_bot
)
SELECT
    p.pitch_id,
    sp.release_pos_x,
    sp.release_pos_y,
    sp.release_pos_z,
    sp.release_spin_rate,
    sp.release_extension,
    sp.release_speed,
    sp.spin_axis,
    sp.pfx_x,
    sp.pfx_z,
    sp.vx0,
    sp.vy0,
    sp.vz0,
    sp.ax,
    sp.ay,
    sp.az,
    sp.plate_x,
    sp.plate_z,
    sp.sz_top,
    sp.sz_bot
FROM production.fact_pitch p
JOIN staging.statcast_pitches sp
    ON p.game_pk = sp.game_pk
    AND p.game_counter = sp.game_counter
    AND p.pitch_number = sp.pitch_number
ON CONFLICT (pitch_id) DO UPDATE
SET
    release_pos_x = EXCLUDED.release_pos_x,
    release_pos_y = EXCLUDED.release_pos_y,
    release_pos_z = EXCLUDED.release_pos_z,
    release_spin_rate = EXCLUDED.release_spin_rate,
    release_extension = EXCLUDED.release_extension,
    release_speed = EXCLUDED.release_speed,
    spin_axis = EXCLUDED.spin_axis,
    pfx_x = EXCLUDED.pfx_x,
    pfx_z = EXCLUDED.pfx_z,
    vx0 = EXCLUDED.vx0,
    vy0 = EXCLUDED.vy0,
    vz0 = EXCLUDED.vz0,
    ax = EXCLUDED.ax,
    ay = EXCLUDED.ay,
    az = EXCLUDED.az,
    plate_x = EXCLUDED.plate_x,
    plate_z = EXCLUDED.plate_z,
    sz_top = EXCLUDED.sz_top,
    sz_bot = EXCLUDED.sz_bot;
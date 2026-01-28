INSERT INTO production.fact_pa(
    game_pk, pitcher_id, batter_id, game_counter,
    last_pitch_number, pitcher_pa_number, times_through_order,
    balls, strikes, outs_when_up, inning, inning_topbot, description,
    events, bat_score, fld_score, post_bat_score,
    bat_score_diff
)
SELECT 
    ab.game_pk,
    ab.pitcher AS pitcher_id,
    ab.batter AS batter_id,
    ab.game_counter,
    ab.last_pitch_number,
    ab.pitcher_pa_number,
    ab.times_through_order,
    ab.balls,
    ab.strikes,
    ab.outs_when_up,
    ab.inning,
    ab.inning_topbot,
    p.description,
    ab.events,
    ab.bat_score,
    ab.fld_score,
    ab.post_bat_score,
    ab.bat_score_diff
FROM staging.statcast_at_bats ab
JOIN production.dim_game g
    ON g.game_pk = ab.game_pk
JOIN staging.statcast_pitches p 
    ON p.game_pk = ab.game_pk 
    AND p.game_counter = ab.game_counter 
    AND p.pitch_number = ab.last_pitch_number
WHERE g.game_type NOT IN ('E', 'S')
ON CONFLICT (game_pk, game_counter) DO UPDATE
SET pitcher_id = EXCLUDED.pitcher_id,
    batter_id = EXCLUDED.batter_id,
    game_counter = EXCLUDED.game_counter,
    last_pitch_number = EXCLUDED.last_pitch_number,
    pitcher_pa_number = EXCLUDED.pitcher_pa_number,
    times_through_order = EXCLUDED.times_through_order,
    balls = EXCLUDED.balls,
    strikes = EXCLUDED.strikes,
    outs_when_up = EXCLUDED.outs_when_up,
    inning = EXCLUDED.inning,
    inning_topbot = EXCLUDED.inning_topbot,
    description = EXCLUDED.description,
    events = EXCLUDED.events,
    bat_score = EXCLUDED.bat_score,
    fld_score = EXCLUDED.fld_score,
    post_bat_score = EXCLUDED.fld_score,
    bat_score_diff = EXCLUDED.bat_score_diff;


INSERT INTO production.fact_pitch (
    pa_id, game_pk, pitcher_id, batter_id, game_counter, pitch_number,
    pitch_type, pitch_name, description, release_speed, effective_speed,
    release_spin_rate, release_extension, spin_axis, pfx_x, pfx_z,
    zone, plate_x, plate_z, balls, strikes, outs_when_up, bat_score_diff,
    is_whiff, is_called_strike, is_bip, is_swing, is_foul, batter_stand
)
SELECT
    pa.pa_id,
    p.game_pk,
    p.pitcher AS pitcher_id,
    p.batter AS batter_id,
    p.game_counter,
    p.pitch_number,
    p.pitch_type,
    p.pitch_name,
    p.description,
    p.release_speed,
    p.effective_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,
    p.pfx_x,
    p.pfx_z,
    p.zone,
    p.plate_x,
    p.plate_z,
    p.balls,
    p.strikes,
    p.outs_when_up,
    p.bat_score_diff,
    p.is_whiff,
    p.is_called_strike,
    p.is_bip,
    p.is_swing,
    p.is_foul,
    p.stand
FROM staging.statcast_pitches p
JOIN production.fact_pa pa
    ON pa.game_pk = p.game_pk
    AND pa.game_counter = p.game_counter
JOIN production.dim_game g
    ON pa.game_pk = g.game_pk
WHERE g.game_type NOT IN ('E', 'S')
ON CONFLICT (game_pk, game_counter, pitch_number) DO UPDATE
SET pa_id = EXCLUDED.pa_id,
    pitcher_id = EXCLUDED.pitcher_id,
    batter_id = EXCLUDED.batter_id,
    pitch_type = EXCLUDED.pitch_type,
    pitch_name = EXCLUDED.pitch_name,
    description = EXCLUDED.description,
    release_speed = EXCLUDED.release_speed,
    effective_speed = EXCLUDED.effective_speed,
    release_spin_rate = EXCLUDED.release_spin_rate,
    release_extension = EXCLUDED.release_extension,
    spin_axis = EXCLUDED.spin_axis,
    pfx_x = EXCLUDED.pfx_x,
    pfx_z = EXCLUDED.pfx_z,
    zone = EXCLUDED.zone,
    plate_x = EXCLUDED.plate_x,
    plate_z = EXCLUDED.plate_z,
    balls = EXCLUDED.balls,
    strikes = EXCLUDED.strikes,
    outs_when_up = EXCLUDED.outs_when_up,
    bat_score_diff = EXCLUDED.bat_score_diff,
    is_whiff = EXCLUDED.is_whiff,
    is_called_strike = EXCLUDED.is_called_strike,
    is_bip = EXCLUDED.is_bip,
    is_swing = EXCLUDED.is_swing,
    is_foul = EXCLUDED.is_foul,
    batter_stand = EXCLUDED.batter_stand
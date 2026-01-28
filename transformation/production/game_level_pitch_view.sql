CREATE OR REPLACE VIEW production.v_game_level_pitch_type_profile (
    game_pk,
    pitcher_id,
    pitch_type,
    batter_stand,
    pitch_name,
    pitches,
    avg_move_in,
    avg_ivb_in,
    bat_contact_rate,
    batted_balls,
    inplay_contact_rate,
    csw_pct,
    swing_rate,
    whiff_rate,
    avg_xwoba_on_bip,
    hard_hit_rate_on_bip,
    sweet_spot_rate_on_bip,
    usage_pct,
    total_pitches
) AS
WITH pitch_level AS (
    SELECT
        ps.pitch_id,
        ps.pa_id,
        ps.game_pk,
        ps.game_counter,
        ps.pitch_number,
        ps.pitcher_id,
        ps.batter_id,
        ps.pitch_type,
        ps.pitch_name,
        ps.pitcher_throws,
        ps.batter_stand,
        ps.movement_mag_inches,
        ps.platoon,
        ps.induced_vertical_break_inches AS ivb_in,
        ps.is_swing,
        ps.is_contact,
        ps.is_inplay_contact,
        ps.is_csw,
        ps.is_whiff,
        bb.xwoba,
        bb.hard_hit,
        bb.sweet_spot
    FROM production.v_pitch_shape_features ps
    LEFT JOIN production.sat_batted_balls bb
    ON ps.pitch_id = bb.pitch_id
    WHERE bb.xwoba IS NOT NULL OR ps.movement_mag_inches IS NOT NULL OR ps.induced_vertical_break_inches IS NOT NULL
),
game_level AS (
    SELECT 
        game_pk,
        pitcher_id,
        COUNT(*) AS total_pitches
    FROM production.fact_pitch
    GROUP BY 1,2
),
agg AS (
    SELECT
        p.game_pk,
        p.pitcher_id,
        p.pitch_type,
        p.batter_stand,
        p.pitch_name,

        COUNT(*) AS pitches,
        ROUND(AVG(p.movement_mag_inches)::numeric, 4) AS avg_move_in,
        ROUND(AVG(p.ivb_in)::numeric, 4) AS avg_ivb_in,
        ROUND(AVG(p.is_contact::int)::numeric, 4) AS bat_contact_rate,
        COUNT(p.xwoba) AS batted_balls,
        ROUND(AVG(p.is_inplay_contact::int)::numeric, 4) AS inplay_contact_rate,
        ROUND(AVG(p.is_csw::int)::numeric, 4) AS csw_pct,
        ROUND(AVG(p.is_swing::int)::numeric, 4) as swing_rate,
        ROUND(AVG(p.is_whiff::int)::numeric, 4) AS whiff_rate,
        ROUND(AVG(p.xwoba)::numeric, 4) AS avg_xwoba_on_bip,
        ROUND(AVG(p.hard_hit::int)::numeric, 4) AS hard_hit_rate_on_bip,
        ROUND(AVG(p.sweet_spot::int)::numeric, 4) AS sweet_spot_rate_on_bip
    FROM pitch_level p
    GROUP BY 1,2,3,4,5
), 
with_usage AS (
    SELECT
        a.*,
        ROUND((a.pitches::float / NULLIF(SUM(a.pitches) OVER (PARTITION BY a.game_pk, a.pitcher_id, a.batter_stand), 0))::numeric, 3) AS usage_pct,
        g.total_pitches
    FROM agg a
    LEFT JOIN game_level g
        ON g.game_pk = a.game_pk
        AND g.pitcher_id = a.pitcher_id
)
SELECT *
FROM with_usage;
CREATE OR REPLACE VIEW production.v_pitch_type_profile (
    pitcher_id,
    pitch_type,
    pitch_name,
    platoon,
    pitches,
    avg_velo,
    avg_spin,
    avg_extension,
    avg_hb_in,
    avg_ivb_in,
    avg_movement_in,
    avg_move_per_1000rpm,
    avg_release_side,
    avg_release_height,
    avg_ev_on_bip,
    avg_xwoba_on_bip,
    swing_rate,
    whiff_rate,
    csw_rate,
    contact_rate,
    foul_contact_rate,
    inplay_contact_rate,
    hard_hit_rate,
    ideal_contact_rate,
    zone_rate,
    chase_rate,
    zone_whiff_per_zone_swing,
    chase_whiff_per_chase_swing
) AS
WITH pitch_level AS (
    SELECT
        v.pitcher_id,
        v.pitch_id,
        v.pitch_name,
        v.pitch_type,
        v.platoon,

        v.release_speed,
        v.release_extension,
        v.release_spin_rate,
        v.horizontal_break_inches,
        v.induced_vertical_break_inches,
        v.movement_mag_inches,
        v.movement_per_1000rpm,
        v.release_pos_x,
        v.release_pos_z,
        bb.launch_speed,
        bb.xwoba,
        
        v.is_swing,
        v.is_whiff,
        v.is_csw,
        v.is_contact,
        v.is_foul_contact,
        v.is_inplay_contact,
        bb.hard_hit,
        bb.sweet_spot,
        bb.ideal_contact,

        v.plate_x,
        v.plate_z,
        v.sz_top,
        v.sz_bot,

        CASE
            WHEN v.plate_x IS NULL OR v.plate_z IS NULL OR v.sz_top IS NULL OR v.sz_bot IS NULL THEN NULL
            WHEN v.plate_x BETWEEN -0.83 AND 0.83
                AND v.plate_z BETWEEN v.sz_bot AND v.sz_top
            THEN TRUE
            ELSE FALSE
        END AS is_in_zone
    FROM production.v_pitch_shape_features v
    LEFT JOIN production.sat_batted_balls bb
        ON v.pitch_id = bb.pitch_id
    WHERE v.pitch_type IS NOT NULL
),
pitch_level2 AS (
    SELECT
        *,
        CASE 
            WHEN is_in_zone IS NULL OR is_swing IS NULL THEN NULL
            WHEN is_swing AND NOT is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_chase,

        CASE
            WHEN is_in_zone IS NULL OR is_swing IS NULL THEN NULL
            WHEN is_swing AND is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_zone_swing,

        CASE
            WHEN is_in_zone IS NULL OR is_whiff IS NULL THEN NULL
            WHEN is_whiff AND is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_zone_whiff,

        CASE
            WHEN is_in_zone IS NULL OR is_whiff IS NULL THEN NULL
            WHEN is_whiff AND NOT is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_chase_whiff,

        CASE
            WHEN is_in_zone IS NULL OR ideal_contact IS NULL THEN NULL
            WHEN ideal_contact AND NOT is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_ideal_contact_out_of_zone,

        CASE
            WHEN is_in_zone IS NULL OR ideal_contact IS NULL THEN NULL
            WHEN ideal_contact AND is_in_zone THEN TRUE
            ELSE FALSE
        END AS is_ideal_contact_in_zone
    FROM pitch_level
),
agg AS (
    SELECT
        pitcher_id,
        pitch_type,
        pitch_name,
        platoon,

        COUNT(*) AS pitches,

        AVG(release_speed) AS avg_velo,
        AVG(release_spin_rate) AS avg_spin,
        AVG(release_extension) AS avg_extension,
        AVG(horizontal_break_inches) AS avg_hb_in,
        AVG(induced_vertical_break_inches) AS avg_ivb_in,
        AVG(movement_mag_inches) AS avg_movement_in,
        AVG(movement_per_1000rpm) AS avg_move_per_1000rpm,
        AVG(release_pos_x) AS avg_release_side,
        AVG(release_pos_z) AS avg_release_height,
        AVG(launch_speed) FILTER (WHERE is_inplay_contact) AS avg_ev_on_bip,
        AVG(xwoba) FILTER (WHERE is_inplay_contact) AS avg_xwoba_on_bip,

        AVG((is_swing)::int) AS swing_rate,
        AVG((is_whiff)::int) AS whiff_rate,
        AVG((is_csw)::int) AS csw_rate,
        AVG((is_contact)::int) AS contact_rate,
        AVG((is_foul_contact)::int) AS foul_contact_rate,
        AVG((is_inplay_contact)::int) AS inplay_contact_rate,
        AVG((hard_hit)::int) AS hard_hit_rate,
        AVG((ideal_contact)::int) AS ideal_contact_rate,

        AVG((is_in_zone)::int) FILTER (WHERE is_in_zone IS NOT NULL) AS zone_rate,
        AVG((is_chase)::int) FILTER (WHERE is_chase IS NOT NULL) AS chase_rate,

        CASE
            WHEN SUM((is_zone_swing)::int) = 0 THEN NULL
            ELSE SUM((is_zone_whiff)::int)::float / SUM((is_zone_swing)::int)
        END AS zone_whiff_per_zone_swing,

        CASE
            WHEN SUM((is_chase)::int) = 0 THEN NULL
            ELSE SUM((is_chase_whiff)::int)::float / SUM((is_chase)::int)
        END AS chase_whiff_per_chase_swing
    FROM pitch_level2
    GROUP BY 1,2,3,4
), 
with_usage AS (
    SELECT 
        a.*,
        a.pitches::float / NULLIF(SUM(a.pitches) OVER (PARTITION BY a.pitcher_id, a.platoon), 0) AS usage_pct
    FROM agg a
)
SELECT * 
FROM with_usage;
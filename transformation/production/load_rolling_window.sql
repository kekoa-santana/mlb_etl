INSERT INTO production.fact_player_form_rolling (
    player_id, game_pk, player_role, game_date, season,
    bat_pa_15, bat_ab_15, bat_h_15, bat_2b_15, bat_3b_15, bat_hr_15,
    bat_bb_15, bat_k_15, bat_sb_15, bat_rbi_15, bat_tb_15, bat_hbp_15,
    bat_avg_15, bat_obp_15, bat_slg_15, bat_ops_15,
    bat_pa_30, bat_ab_30, bat_h_30, bat_2b_30, bat_3b_30, bat_hr_30,
    bat_bb_30, bat_k_30, bat_sb_30, bat_rbi_30, bat_tb_30, bat_hbp_30,
    bat_avg_30, bat_obp_30, bat_slg_30, bat_ops_30,
    pit_ip_15, pit_er_15, pit_h_15, pit_bb_15, pit_k_15, pit_hr_15, pit_bf_15,
    pit_era_15, pit_whip_15, pit_k9_15, pit_bb9_15,
    pit_ip_30, pit_er_30, pit_h_30, pit_bb_30, pit_k_30, pit_hr_30, pit_bf_30,
    pit_era_30, pit_whip_30, pit_k9_30, pit_bb9_30
)
WITH batter_games AS (
    SELECT
        b.batter_id AS player_id,
        b.game_pk,
        dg.game_date,
        dg.season,
        SUM(b.plate_appearances) AS pa,
        SUM(b.at_bats) AS ab,
        SUM(b.hits) AS h,
        SUM(b.doubles) AS "2b",
        SUM(b.triples) AS "3b",
        SUM(b.home_runs) AS hr,
        SUM(b.walks) AS bb,
        SUM(b.strikeouts) AS k,
        SUM(b.sb) AS sb,
        SUM(b.rbi) AS rbi,
        SUM(b.total_bases) AS tb,
        SUM(b.hit_by_pitch) AS hbp
    FROM staging.batting_boxscores b
    JOIN production.dim_game dg ON dg.game_pk = b.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY b.batter_id, b.game_pk, dg.game_date, dg.season
),
batter_rolling AS (
    SELECT
        player_id,
        game_pk,
        'batter' AS player_role,
        game_date,
        season,
        -- 15g
        SUM(pa)  OVER w15 AS bat_pa_15,
        SUM(ab)  OVER w15 AS bat_ab_15,
        SUM(h)   OVER w15 AS bat_h_15,
        SUM("2b") OVER w15 AS bat_2b_15,
        SUM("3b") OVER w15 AS bat_3b_15,
        SUM(hr)  OVER w15 AS bat_hr_15,
        SUM(bb)  OVER w15 AS bat_bb_15,
        SUM(k)   OVER w15 AS bat_k_15,
        SUM(sb)  OVER w15 AS bat_sb_15,
        SUM(rbi) OVER w15 AS bat_rbi_15,
        SUM(tb)  OVER w15 AS bat_tb_15,
        SUM(hbp) OVER w15 AS bat_hbp_15,
        -- 30g
        SUM(pa)  OVER w30 AS bat_pa_30,
        SUM(ab)  OVER w30 AS bat_ab_30,
        SUM(h)   OVER w30 AS bat_h_30,
        SUM("2b") OVER w30 AS bat_2b_30,
        SUM("3b") OVER w30 AS bat_3b_30,
        SUM(hr)  OVER w30 AS bat_hr_30,
        SUM(bb)  OVER w30 AS bat_bb_30,
        SUM(k)   OVER w30 AS bat_k_30,
        SUM(sb)  OVER w30 AS bat_sb_30,
        SUM(rbi) OVER w30 AS bat_rbi_30,
        SUM(tb)  OVER w30 AS bat_tb_30,
        SUM(hbp) OVER w30 AS bat_hbp_30
    FROM batter_games
    WINDOW
        w15 AS (PARTITION BY player_id ORDER BY game_date, game_pk ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
        w30 AS (PARTITION BY player_id ORDER BY game_date, game_pk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
),
pitcher_games AS (
    SELECT
        p.pitcher_id AS player_id,
        p.game_pk,
        dg.game_date,
        dg.season,
        SUM(p.innings_pitched) AS ip,
        SUM(p.earned_runs) AS er,
        SUM(p.hits) AS h,
        SUM(p.walks) AS bb,
        SUM(p.strike_outs) AS k,
        SUM(p.home_runs) AS hr,
        SUM(p.batters_faced) AS bf
    FROM staging.pitching_boxscores p
    JOIN production.dim_game dg ON dg.game_pk = p.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY p.pitcher_id, p.game_pk, dg.game_date, dg.season
),
pitcher_rolling AS (
    SELECT
        player_id,
        game_pk,
        'pitcher' AS player_role,
        game_date,
        season,
        -- 15g
        SUM(ip) OVER w15 AS pit_ip_15,
        SUM(er) OVER w15 AS pit_er_15,
        SUM(h)  OVER w15 AS pit_h_15,
        SUM(bb) OVER w15 AS pit_bb_15,
        SUM(k)  OVER w15 AS pit_k_15,
        SUM(hr) OVER w15 AS pit_hr_15,
        SUM(bf) OVER w15 AS pit_bf_15,
        -- 30g
        SUM(ip) OVER w30 AS pit_ip_30,
        SUM(er) OVER w30 AS pit_er_30,
        SUM(h)  OVER w30 AS pit_h_30,
        SUM(bb) OVER w30 AS pit_bb_30,
        SUM(k)  OVER w30 AS pit_k_30,
        SUM(hr) OVER w30 AS pit_hr_30,
        SUM(bf) OVER w30 AS pit_bf_30
    FROM pitcher_games
    WINDOW
        w15 AS (PARTITION BY player_id ORDER BY game_date, game_pk ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
        w30 AS (PARTITION BY player_id ORDER BY game_date, game_pk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
)
SELECT
    player_id, game_pk, player_role, game_date, season,
    bat_pa_15, bat_ab_15, bat_h_15, bat_2b_15, bat_3b_15, bat_hr_15,
    bat_bb_15, bat_k_15, bat_sb_15, bat_rbi_15, bat_tb_15, bat_hbp_15,
    bat_h_15::numeric / NULLIF(bat_ab_15, 0) AS bat_avg_15,
    (bat_h_15 + bat_bb_15 + bat_hbp_15)::numeric / NULLIF(bat_ab_15 + bat_bb_15 + bat_hbp_15, 0) AS bat_obp_15,
    bat_tb_15::numeric / NULLIF(bat_ab_15, 0) AS bat_slg_15,
    bat_h_15::numeric / NULLIF(bat_ab_15, 0)
        + (bat_h_15 + bat_bb_15 + bat_hbp_15)::numeric / NULLIF(bat_ab_15 + bat_bb_15 + bat_hbp_15, 0) AS bat_ops_15,
    bat_pa_30, bat_ab_30, bat_h_30, bat_2b_30, bat_3b_30, bat_hr_30,
    bat_bb_30, bat_k_30, bat_sb_30, bat_rbi_30, bat_tb_30, bat_hbp_30,
    bat_h_30::numeric / NULLIF(bat_ab_30, 0) AS bat_avg_30,
    (bat_h_30 + bat_bb_30 + bat_hbp_30)::numeric / NULLIF(bat_ab_30 + bat_bb_30 + bat_hbp_30, 0) AS bat_obp_30,
    bat_tb_30::numeric / NULLIF(bat_ab_30, 0) AS bat_slg_30,
    bat_h_30::numeric / NULLIF(bat_ab_30, 0)
        + (bat_h_30 + bat_bb_30 + bat_hbp_30)::numeric / NULLIF(bat_ab_30 + bat_bb_30 + bat_hbp_30, 0) AS bat_ops_30,
    NULL::real, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::real, NULL::real, NULL::real, NULL::real,
    NULL::real, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::real, NULL::real, NULL::real, NULL::real
FROM batter_rolling

UNION ALL

SELECT
    player_id, game_pk, player_role, game_date, season,
    NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::real, NULL::real, NULL::real, NULL::real,
    NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::int, NULL::int, NULL::int, NULL::int, NULL::int, NULL::int,
    NULL::real, NULL::real, NULL::real, NULL::real,
    pit_ip_15, pit_er_15, pit_h_15, pit_bb_15, pit_k_15, pit_hr_15, pit_bf_15,
    pit_er_15 * 9.0 / NULLIF(pit_ip_15, 0) AS pit_era_15,
    (pit_h_15 + pit_bb_15)::numeric / NULLIF(pit_ip_15, 0) AS pit_whip_15,
    pit_k_15 * 9.0 / NULLIF(pit_ip_15, 0) AS pit_k9_15,
    pit_bb_15 * 9.0 / NULLIF(pit_ip_15, 0) AS pit_bb9_15,
    pit_ip_30, pit_er_30, pit_h_30, pit_bb_30, pit_k_30, pit_hr_30, pit_bf_30,
    pit_er_30 * 9.0 / NULLIF(pit_ip_30, 0) AS pit_era_30,
    (pit_h_30 + pit_bb_30)::numeric / NULLIF(pit_ip_30, 0) AS pit_whip_30,
    pit_k_30 * 9.0 / NULLIF(pit_ip_30, 0) AS pit_k9_30,
    pit_bb_30 * 9.0 / NULLIF(pit_ip_30, 0) AS pit_bb9_30
FROM pitcher_rolling

ON CONFLICT (player_id, game_pk, player_role) DO UPDATE
SET game_date    = EXCLUDED.game_date,
    season       = EXCLUDED.season,
    bat_pa_15    = EXCLUDED.bat_pa_15,
    bat_ab_15    = EXCLUDED.bat_ab_15,
    bat_h_15     = EXCLUDED.bat_h_15,
    bat_2b_15    = EXCLUDED.bat_2b_15,
    bat_3b_15    = EXCLUDED.bat_3b_15,
    bat_hr_15    = EXCLUDED.bat_hr_15,
    bat_bb_15    = EXCLUDED.bat_bb_15,
    bat_k_15     = EXCLUDED.bat_k_15,
    bat_sb_15    = EXCLUDED.bat_sb_15,
    bat_rbi_15   = EXCLUDED.bat_rbi_15,
    bat_tb_15    = EXCLUDED.bat_tb_15,
    bat_hbp_15   = EXCLUDED.bat_hbp_15,
    bat_avg_15   = EXCLUDED.bat_avg_15,
    bat_obp_15   = EXCLUDED.bat_obp_15,
    bat_slg_15   = EXCLUDED.bat_slg_15,
    bat_ops_15   = EXCLUDED.bat_ops_15,
    bat_pa_30    = EXCLUDED.bat_pa_30,
    bat_ab_30    = EXCLUDED.bat_ab_30,
    bat_h_30     = EXCLUDED.bat_h_30,
    bat_2b_30    = EXCLUDED.bat_2b_30,
    bat_3b_30    = EXCLUDED.bat_3b_30,
    bat_hr_30    = EXCLUDED.bat_hr_30,
    bat_bb_30    = EXCLUDED.bat_bb_30,
    bat_k_30     = EXCLUDED.bat_k_30,
    bat_sb_30    = EXCLUDED.bat_sb_30,
    bat_rbi_30   = EXCLUDED.bat_rbi_30,
    bat_tb_30    = EXCLUDED.bat_tb_30,
    bat_hbp_30   = EXCLUDED.bat_hbp_30,
    bat_avg_30   = EXCLUDED.bat_avg_30,
    bat_obp_30   = EXCLUDED.bat_obp_30,
    bat_slg_30   = EXCLUDED.bat_slg_30,
    bat_ops_30   = EXCLUDED.bat_ops_30,
    pit_ip_15    = EXCLUDED.pit_ip_15,
    pit_er_15    = EXCLUDED.pit_er_15,
    pit_h_15     = EXCLUDED.pit_h_15,
    pit_bb_15    = EXCLUDED.pit_bb_15,
    pit_k_15     = EXCLUDED.pit_k_15,
    pit_hr_15    = EXCLUDED.pit_hr_15,
    pit_bf_15    = EXCLUDED.pit_bf_15,
    pit_era_15   = EXCLUDED.pit_era_15,
    pit_whip_15  = EXCLUDED.pit_whip_15,
    pit_k9_15    = EXCLUDED.pit_k9_15,
    pit_bb9_15   = EXCLUDED.pit_bb9_15,
    pit_ip_30    = EXCLUDED.pit_ip_30,
    pit_er_30    = EXCLUDED.pit_er_30,
    pit_h_30     = EXCLUDED.pit_h_30,
    pit_bb_30    = EXCLUDED.pit_bb_30,
    pit_k_30     = EXCLUDED.pit_k_30,
    pit_hr_30    = EXCLUDED.pit_hr_30,
    pit_bf_30    = EXCLUDED.pit_bf_30,
    pit_era_30   = EXCLUDED.pit_era_30,
    pit_whip_30  = EXCLUDED.pit_whip_30,
    pit_k9_30    = EXCLUDED.pit_k9_30,
    pit_bb9_30   = EXCLUDED.pit_bb9_30;

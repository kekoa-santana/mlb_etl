INSERT INTO production.fact_streak_indicator (
    player_id, game_pk, player_role, game_date, season, games_in_window,
    bat_avg_10g, bat_avg_season, bat_avg_zscore,
    bat_obp_10g, bat_obp_season, bat_obp_zscore,
    bat_slg_10g, bat_slg_season, bat_slg_zscore,
    pit_era_10g, pit_era_season, pit_era_zscore,
    pit_whip_10g, pit_whip_season, pit_whip_zscore,
    streak_flag
)
WITH batter_games AS (
    SELECT
        b.batter_id AS player_id,
        b.game_pk,
        dg.game_date,
        dg.season,
        SUM(b.at_bats) AS ab,
        SUM(b.hits) AS h,
        SUM(b.walks) AS bb,
        SUM(b.hit_by_pitch) AS hbp,
        SUM(b.plate_appearances - b.at_bats - b.walks - b.hit_by_pitch) AS sf,
        SUM(b.total_bases) AS tb
    FROM staging.batting_boxscores b
    JOIN production.dim_game dg ON dg.game_pk = b.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY b.batter_id, b.game_pk, dg.game_date, dg.season
),
batter_rolling AS (
    SELECT
        player_id, game_pk, game_date, season,
        -- per-game rates for stddev
        h::numeric / NULLIF(ab, 0) AS game_avg,
        (h + bb + hbp)::numeric / NULLIF(ab + bb + hbp + sf, 0) AS game_obp,
        tb::numeric / NULLIF(ab, 0) AS game_slg,
        -- 10g window
        SUM(h)  OVER w10 AS h_10,
        SUM(ab) OVER w10 AS ab_10,
        SUM(bb) OVER w10 AS bb_10,
        SUM(hbp) OVER w10 AS hbp_10,
        SUM(sf) OVER w10 AS sf_10,
        SUM(tb) OVER w10 AS tb_10,
        COUNT(*) OVER w10 AS games_in_window,
        -- season window
        SUM(h)  OVER wseason AS h_szn,
        SUM(ab) OVER wseason AS ab_szn,
        SUM(bb) OVER wseason AS bb_szn,
        SUM(hbp) OVER wseason AS hbp_szn,
        SUM(sf) OVER wseason AS sf_szn,
        SUM(tb) OVER wseason AS tb_szn,
        -- stddev of per-game rates over season
        STDDEV_SAMP(h::numeric / NULLIF(ab, 0)) OVER wseason AS sd_avg,
        STDDEV_SAMP((h + bb + hbp)::numeric / NULLIF(ab + bb + hbp + sf, 0)) OVER wseason AS sd_obp,
        STDDEV_SAMP(tb::numeric / NULLIF(ab, 0)) OVER wseason AS sd_slg
    FROM batter_games
    WINDOW
        w10     AS (PARTITION BY player_id, season ORDER BY game_date, game_pk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        wseason AS (PARTITION BY player_id, season ORDER BY game_date, game_pk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
batter_streak AS (
    SELECT
        player_id, game_pk, 'batter' AS player_role, game_date, season,
        games_in_window::smallint,
        h_10::numeric / NULLIF(ab_10, 0) AS bat_avg_10g,
        h_szn::numeric / NULLIF(ab_szn, 0) AS bat_avg_season,
        CASE WHEN games_in_window >= 5
            THEN (h_10::numeric / NULLIF(ab_10, 0) - h_szn::numeric / NULLIF(ab_szn, 0))
                 / NULLIF(sd_avg, 0)
            ELSE NULL END AS bat_avg_zscore,
        (h_10 + bb_10 + hbp_10)::numeric / NULLIF(ab_10 + bb_10 + hbp_10 + sf_10, 0) AS bat_obp_10g,
        (h_szn + bb_szn + hbp_szn)::numeric / NULLIF(ab_szn + bb_szn + hbp_szn + sf_szn, 0) AS bat_obp_season,
        CASE WHEN games_in_window >= 5
            THEN ((h_10 + bb_10 + hbp_10)::numeric / NULLIF(ab_10 + bb_10 + hbp_10 + sf_10, 0)
                  - (h_szn + bb_szn + hbp_szn)::numeric / NULLIF(ab_szn + bb_szn + hbp_szn + sf_szn, 0))
                 / NULLIF(sd_obp, 0)
            ELSE NULL END AS bat_obp_zscore,
        tb_10::numeric / NULLIF(ab_10, 0) AS bat_slg_10g,
        tb_szn::numeric / NULLIF(ab_szn, 0) AS bat_slg_season,
        CASE WHEN games_in_window >= 5
            THEN (tb_10::numeric / NULLIF(ab_10, 0) - tb_szn::numeric / NULLIF(ab_szn, 0))
                 / NULLIF(sd_slg, 0)
            ELSE NULL END AS bat_slg_zscore,
        NULL::real AS pit_era_10g,
        NULL::real AS pit_era_season,
        NULL::real AS pit_era_zscore,
        NULL::real AS pit_whip_10g,
        NULL::real AS pit_whip_season,
        NULL::real AS pit_whip_zscore,
        CASE
            WHEN games_in_window < 5 THEN NULL
            WHEN (h_10::numeric / NULLIF(ab_10, 0) - h_szn::numeric / NULLIF(ab_szn, 0)) / NULLIF(sd_avg, 0) > 1.0
              OR ((h_10 + bb_10 + hbp_10)::numeric / NULLIF(ab_10 + bb_10 + hbp_10 + sf_10, 0)
                  - (h_szn + bb_szn + hbp_szn)::numeric / NULLIF(ab_szn + bb_szn + hbp_szn + sf_szn, 0)) / NULLIF(sd_obp, 0) > 1.0
              OR (tb_10::numeric / NULLIF(ab_10, 0) - tb_szn::numeric / NULLIF(ab_szn, 0)) / NULLIF(sd_slg, 0) > 1.0
            THEN 'hot'
            WHEN (h_10::numeric / NULLIF(ab_10, 0) - h_szn::numeric / NULLIF(ab_szn, 0)) / NULLIF(sd_avg, 0) < -1.0
              OR ((h_10 + bb_10 + hbp_10)::numeric / NULLIF(ab_10 + bb_10 + hbp_10 + sf_10, 0)
                  - (h_szn + bb_szn + hbp_szn)::numeric / NULLIF(ab_szn + bb_szn + hbp_szn + sf_szn, 0)) / NULLIF(sd_obp, 0) < -1.0
              OR (tb_10::numeric / NULLIF(ab_10, 0) - tb_szn::numeric / NULLIF(ab_szn, 0)) / NULLIF(sd_slg, 0) < -1.0
            THEN 'cold'
            ELSE NULL
        END AS streak_flag
    FROM batter_rolling
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
        SUM(p.walks) AS bb
    FROM staging.pitching_boxscores p
    JOIN production.dim_game dg ON dg.game_pk = p.game_pk
    WHERE dg.game_type NOT IN ('E', 'S')
    GROUP BY p.pitcher_id, p.game_pk, dg.game_date, dg.season
),
pitcher_rolling AS (
    SELECT
        player_id, game_pk, game_date, season,
        -- per-game rates for stddev
        er * 9.0 / NULLIF(ip, 0) AS game_era,
        (h + bb)::numeric / NULLIF(ip, 0) AS game_whip,
        -- 10g window
        SUM(ip) OVER w10 AS ip_10,
        SUM(er) OVER w10 AS er_10,
        SUM(h)  OVER w10 AS h_10,
        SUM(bb) OVER w10 AS bb_10,
        COUNT(*) OVER w10 AS games_in_window,
        -- season window
        SUM(ip) OVER wseason AS ip_szn,
        SUM(er) OVER wseason AS er_szn,
        SUM(h)  OVER wseason AS h_szn,
        SUM(bb) OVER wseason AS bb_szn,
        -- stddev
        STDDEV_SAMP(er * 9.0 / NULLIF(ip, 0)) OVER wseason AS sd_era,
        STDDEV_SAMP((h + bb)::numeric / NULLIF(ip, 0)) OVER wseason AS sd_whip
    FROM pitcher_games
    WINDOW
        w10     AS (PARTITION BY player_id, season ORDER BY game_date, game_pk ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        wseason AS (PARTITION BY player_id, season ORDER BY game_date, game_pk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
pitcher_streak AS (
    SELECT
        player_id, game_pk, 'pitcher' AS player_role, game_date, season,
        games_in_window::smallint,
        NULL::real AS bat_avg_10g,
        NULL::real AS bat_avg_season,
        NULL::real AS bat_avg_zscore,
        NULL::real AS bat_obp_10g,
        NULL::real AS bat_obp_season,
        NULL::real AS bat_obp_zscore,
        NULL::real AS bat_slg_10g,
        NULL::real AS bat_slg_season,
        NULL::real AS bat_slg_zscore,
        er_10 * 9.0 / NULLIF(ip_10, 0) AS pit_era_10g,
        er_szn * 9.0 / NULLIF(ip_szn, 0) AS pit_era_season,
        CASE WHEN games_in_window >= 5
            THEN (er_10 * 9.0 / NULLIF(ip_10, 0) - er_szn * 9.0 / NULLIF(ip_szn, 0))
                 / NULLIF(sd_era, 0)
            ELSE NULL END AS pit_era_zscore,
        (h_10 + bb_10)::numeric / NULLIF(ip_10, 0) AS pit_whip_10g,
        (h_szn + bb_szn)::numeric / NULLIF(ip_szn, 0) AS pit_whip_season,
        CASE WHEN games_in_window >= 5
            THEN ((h_10 + bb_10)::numeric / NULLIF(ip_10, 0) - (h_szn + bb_szn)::numeric / NULLIF(ip_szn, 0))
                 / NULLIF(sd_whip, 0)
            ELSE NULL END AS pit_whip_zscore,
        -- for pitchers, lower ERA/WHIP is better, so invert: negative z = hot, positive z = cold
        CASE
            WHEN games_in_window < 5 THEN NULL
            WHEN (er_10 * 9.0 / NULLIF(ip_10, 0) - er_szn * 9.0 / NULLIF(ip_szn, 0)) / NULLIF(sd_era, 0) < -1.0
              OR ((h_10 + bb_10)::numeric / NULLIF(ip_10, 0) - (h_szn + bb_szn)::numeric / NULLIF(ip_szn, 0)) / NULLIF(sd_whip, 0) < -1.0
            THEN 'hot'
            WHEN (er_10 * 9.0 / NULLIF(ip_10, 0) - er_szn * 9.0 / NULLIF(ip_szn, 0)) / NULLIF(sd_era, 0) > 1.0
              OR ((h_10 + bb_10)::numeric / NULLIF(ip_10, 0) - (h_szn + bb_szn)::numeric / NULLIF(ip_szn, 0)) / NULLIF(sd_whip, 0) > 1.0
            THEN 'cold'
            ELSE NULL
        END AS streak_flag
    FROM pitcher_rolling
)
SELECT * FROM batter_streak
UNION ALL
SELECT * FROM pitcher_streak

ON CONFLICT (player_id, game_pk, player_role) DO UPDATE
SET game_date        = EXCLUDED.game_date,
    season           = EXCLUDED.season,
    games_in_window  = EXCLUDED.games_in_window,
    bat_avg_10g      = EXCLUDED.bat_avg_10g,
    bat_avg_season   = EXCLUDED.bat_avg_season,
    bat_avg_zscore   = EXCLUDED.bat_avg_zscore,
    bat_obp_10g      = EXCLUDED.bat_obp_10g,
    bat_obp_season   = EXCLUDED.bat_obp_season,
    bat_obp_zscore   = EXCLUDED.bat_obp_zscore,
    bat_slg_10g      = EXCLUDED.bat_slg_10g,
    bat_slg_season   = EXCLUDED.bat_slg_season,
    bat_slg_zscore   = EXCLUDED.bat_slg_zscore,
    pit_era_10g      = EXCLUDED.pit_era_10g,
    pit_era_season   = EXCLUDED.pit_era_season,
    pit_era_zscore   = EXCLUDED.pit_era_zscore,
    pit_whip_10g     = EXCLUDED.pit_whip_10g,
    pit_whip_season  = EXCLUDED.pit_whip_season,
    pit_whip_zscore  = EXCLUDED.pit_whip_zscore,
    streak_flag      = EXCLUDED.streak_flag;

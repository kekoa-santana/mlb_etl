INSERT INTO production.fact_player_game_mlb (
    player_id, game_pk, player_role,
    game_date, season, team_id,
    bat_pa, bat_ab, bat_h, bat_2b, bat_3b, bat_hr,
    bat_r, bat_rbi, bat_bb, bat_k, bat_hbp,
    bat_sb, bat_cs, bat_tb, bat_errors,
    bat_avg, bat_obp, bat_slg,
    pit_ip, pit_er, pit_r, pit_h, pit_bb, pit_k, pit_hr, pit_bf,
    pit_w, pit_l, pit_sv, pit_hld, pit_bs,
    pit_pitches, pit_strikes, pit_is_starter,
    pit_era, pit_whip
)

-- batter rows (GROUP BY to handle multi-team duplicates)
SELECT
    b.batter_id              AS player_id,
    b.game_pk,
    'batter'                 AS player_role,
    g.game_date,
    g.season,
    MAX(b.team_id)           AS team_id,
    SUM(b.plate_appearances) AS bat_pa,
    SUM(b.at_bats)           AS bat_ab,
    SUM(b.hits)              AS bat_h,
    SUM(b.doubles)           AS bat_2b,
    SUM(b.triples)           AS bat_3b,
    SUM(b.home_runs)         AS bat_hr,
    SUM(b.runs)              AS bat_r,
    SUM(b.rbi)               AS bat_rbi,
    SUM(b.walks)             AS bat_bb,
    SUM(b.strikeouts)        AS bat_k,
    SUM(b.hit_by_pitch)      AS bat_hbp,
    SUM(b.sb)                AS bat_sb,
    SUM(b.caught_stealing)   AS bat_cs,
    SUM(b.total_bases)       AS bat_tb,
    SUM(b.errors)            AS bat_errors,
    CASE WHEN SUM(b.at_bats) > 0
         THEN round(SUM(b.hits)::numeric / SUM(b.at_bats), 3)
         ELSE NULL END       AS bat_avg,
    CASE WHEN (SUM(b.at_bats) + SUM(b.walks) + SUM(b.hit_by_pitch)) > 0
         THEN round((SUM(b.hits) + SUM(b.walks) + SUM(b.hit_by_pitch))::numeric
                     / (SUM(b.at_bats) + SUM(b.walks) + SUM(b.hit_by_pitch)), 3)
         ELSE NULL END       AS bat_obp,
    CASE WHEN SUM(b.at_bats) > 0
         THEN round(SUM(b.total_bases)::numeric / SUM(b.at_bats), 3)
         ELSE NULL END       AS bat_slg,
    -- pitcher columns NULL for batters
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL
FROM staging.batting_boxscores b
JOIN production.dim_game g ON g.game_pk = b.game_pk
WHERE g.game_type NOT IN ('E', 'S')
GROUP BY b.batter_id, b.game_pk, g.game_date, g.season

UNION ALL

-- pitcher rows
SELECT
    p.pitcher_id             AS player_id,
    p.game_pk,
    'pitcher'                AS player_role,
    g.game_date,
    g.season,
    p.team_id,
    -- batter columns NULL for pitchers
    NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    -- pitcher stats
    p.innings_pitched        AS pit_ip,
    p.earned_runs            AS pit_er,
    p.runs                   AS pit_r,
    p.hits                   AS pit_h,
    p.walks                  AS pit_bb,
    p.strike_outs            AS pit_k,
    p.home_runs              AS pit_hr,
    p.batters_faced          AS pit_bf,
    p.wins                   AS pit_w,
    p.losses                 AS pit_l,
    p.saves                  AS pit_sv,
    p.holds                  AS pit_hld,
    p.blown_saves            AS pit_bs,
    p.number_of_pitches      AS pit_pitches,
    p.strikes                AS pit_strikes,
    p.is_starter             AS pit_is_starter,
    CASE WHEN p.innings_pitched > 0
         THEN round((p.earned_runs * 9.0 / p.innings_pitched)::numeric, 2)
         ELSE NULL END       AS pit_era,
    CASE WHEN p.innings_pitched > 0
         THEN round(((p.walks + p.hits)::numeric / p.innings_pitched)::numeric, 2)
         ELSE NULL END       AS pit_whip
FROM staging.pitching_boxscores p
JOIN production.dim_game g ON g.game_pk = p.game_pk
WHERE g.game_type NOT IN ('E', 'S')

ON CONFLICT (player_id, game_pk, player_role) DO UPDATE
SET game_date      = EXCLUDED.game_date,
    season         = EXCLUDED.season,
    team_id        = EXCLUDED.team_id,
    bat_pa         = EXCLUDED.bat_pa,
    bat_ab         = EXCLUDED.bat_ab,
    bat_h          = EXCLUDED.bat_h,
    bat_2b         = EXCLUDED.bat_2b,
    bat_3b         = EXCLUDED.bat_3b,
    bat_hr         = EXCLUDED.bat_hr,
    bat_r          = EXCLUDED.bat_r,
    bat_rbi        = EXCLUDED.bat_rbi,
    bat_bb         = EXCLUDED.bat_bb,
    bat_k          = EXCLUDED.bat_k,
    bat_hbp        = EXCLUDED.bat_hbp,
    bat_sb         = EXCLUDED.bat_sb,
    bat_cs         = EXCLUDED.bat_cs,
    bat_tb         = EXCLUDED.bat_tb,
    bat_errors     = EXCLUDED.bat_errors,
    bat_avg        = EXCLUDED.bat_avg,
    bat_obp        = EXCLUDED.bat_obp,
    bat_slg        = EXCLUDED.bat_slg,
    pit_ip         = EXCLUDED.pit_ip,
    pit_er         = EXCLUDED.pit_er,
    pit_r          = EXCLUDED.pit_r,
    pit_h          = EXCLUDED.pit_h,
    pit_bb         = EXCLUDED.pit_bb,
    pit_k          = EXCLUDED.pit_k,
    pit_hr         = EXCLUDED.pit_hr,
    pit_bf         = EXCLUDED.pit_bf,
    pit_w          = EXCLUDED.pit_w,
    pit_l          = EXCLUDED.pit_l,
    pit_sv         = EXCLUDED.pit_sv,
    pit_hld        = EXCLUDED.pit_hld,
    pit_bs         = EXCLUDED.pit_bs,
    pit_pitches    = EXCLUDED.pit_pitches,
    pit_strikes    = EXCLUDED.pit_strikes,
    pit_is_starter = EXCLUDED.pit_is_starter,
    pit_era        = EXCLUDED.pit_era,
    pit_whip       = EXCLUDED.pit_whip;

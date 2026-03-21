INSERT INTO fantasy.dk_pitcher_game_scores (
    pitcher_id, game_pk, team_id, pitcher_name, is_starter, game_date, season,
    "dk_pts_IP", "dk_pts_K", "dk_pts_W", "dk_pts_ER",
    "dk_pts_H", "dk_pts_BB", "dk_pts_HBP",
    "dk_pts_CG", "dk_pts_CGSO", "dk_pts_NH",
    dk_points
)
SELECT
    pb.pitcher_id,
    pb.game_pk,
    pb.team_id,
    pb.pitcher_name,
    pb.is_starter,
    g.game_date,
    g.season,
    pb.innings_pitched * 2.25,
    pb.strike_outs,
    pb.wins,
    pb.earned_runs,
    pb.hits,
    pb.walks,
    COALESCE(pb.hit_batsmen, 0),
    CASE WHEN pb.complete_game THEN 1 ELSE 0 END,
    CASE WHEN pb.complete_game AND pb.shutout THEN 1 ELSE 0 END,
    CASE WHEN pb.complete_game AND pb.hits = 0 THEN 1 ELSE 0 END,
    pb.innings_pitched * 2.25
    + pb.strike_outs * 2.0
    + pb.wins * 4.0
    + pb.earned_runs * -2.0
    + pb.hits * -0.6
    + pb.walks * -0.6
    + COALESCE(pb.hit_batsmen, 0) * -0.6
    + CASE WHEN pb.complete_game THEN 2.5 ELSE 0 END
    + CASE WHEN pb.complete_game AND pb.shutout THEN 2.5 ELSE 0 END
    + CASE WHEN pb.complete_game AND pb.hits = 0 THEN 5.0 ELSE 0 END
FROM staging.pitching_boxscores pb
JOIN production.dim_game g ON g.game_pk = pb.game_pk
ON CONFLICT (pitcher_id, game_pk) DO UPDATE
SET team_id      = EXCLUDED.team_id,
    pitcher_name = EXCLUDED.pitcher_name,
    is_starter   = EXCLUDED.is_starter,
    game_date    = EXCLUDED.game_date,
    season       = EXCLUDED.season,
    "dk_pts_IP"  = EXCLUDED."dk_pts_IP",
    "dk_pts_K"   = EXCLUDED."dk_pts_K",
    "dk_pts_W"   = EXCLUDED."dk_pts_W",
    "dk_pts_ER"  = EXCLUDED."dk_pts_ER",
    "dk_pts_H"   = EXCLUDED."dk_pts_H",
    "dk_pts_BB"  = EXCLUDED."dk_pts_BB",
    "dk_pts_HBP" = EXCLUDED."dk_pts_HBP",
    "dk_pts_CG"  = EXCLUDED."dk_pts_CG",
    "dk_pts_CGSO"= EXCLUDED."dk_pts_CGSO",
    "dk_pts_NH"  = EXCLUDED."dk_pts_NH",
    dk_points    = EXCLUDED.dk_points;

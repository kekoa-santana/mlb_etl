INSERT INTO fantasy.espn_pitcher_game_scores (
    pitcher_id, game_pk, team_id, pitcher_name, is_starter, game_date, season,
    "pts_H", "pts_RA", "pts_ER", "pts_BB", "pts_K", "pts_PKO",
    "pts_W", "pts_L", "pts_SV", "pts_BS",
    "pts_IP", "pts_CG", "pts_SO", "pts_NH", "pts_PG",
    fantasy_points
)
SELECT
    pb.pitcher_id,
    pb.game_pk,
    pb.team_id,
    pb.pitcher_name,
    pb.is_starter,
    g.game_date,
    g.season,
    pb.hits,
    pb.runs,
    pb.earned_runs,
    pb.walks,
    pb.strike_outs,
    COALESCE(pb.pickoffs, 0),
    pb.wins,
    pb.losses,
    pb.saves,
    pb.blown_saves,
    FLOOR(pb.innings_pitched)::bigint,
    CASE WHEN pb.complete_game THEN 1 ELSE 0 END,
    CASE WHEN pb.complete_game AND pb.shutout THEN 1 ELSE 0 END,
    CASE WHEN pb.complete_game AND pb.hits = 0 THEN 1 ELSE 0 END,
    CASE WHEN pb.complete_game AND pb.hits = 0 AND pb.walks = 0 AND COALESCE(pb.hit_batsmen, 0) = 0 THEN 1 ELSE 0 END,
    pb.hits * -1
    + pb.runs * -1
    + pb.earned_runs * -1
    + pb.walks * -1
    + pb.strike_outs
    + COALESCE(pb.pickoffs, 0)
    + pb.wins * 2
    + pb.losses * -2
    + pb.saves * 2
    + pb.blown_saves * -2
    + FLOOR(pb.innings_pitched)::bigint * 3
    + CASE WHEN pb.complete_game THEN 3 ELSE 0 END
    + CASE WHEN pb.complete_game AND pb.shutout THEN 3 ELSE 0 END
    + CASE WHEN pb.complete_game AND pb.hits = 0 THEN 5 ELSE 0 END
    + CASE WHEN pb.complete_game AND pb.hits = 0 AND pb.walks = 0 AND COALESCE(pb.hit_batsmen, 0) = 0 THEN 5 ELSE 0 END
FROM staging.pitching_boxscores pb
JOIN production.dim_game g ON g.game_pk = pb.game_pk
ON CONFLICT (pitcher_id, game_pk) DO UPDATE
SET team_id        = EXCLUDED.team_id,
    pitcher_name   = EXCLUDED.pitcher_name,
    is_starter     = EXCLUDED.is_starter,
    game_date      = EXCLUDED.game_date,
    season         = EXCLUDED.season,
    "pts_H"        = EXCLUDED."pts_H",
    "pts_RA"       = EXCLUDED."pts_RA",
    "pts_ER"       = EXCLUDED."pts_ER",
    "pts_BB"       = EXCLUDED."pts_BB",
    "pts_K"        = EXCLUDED."pts_K",
    "pts_PKO"      = EXCLUDED."pts_PKO",
    "pts_W"        = EXCLUDED."pts_W",
    "pts_L"        = EXCLUDED."pts_L",
    "pts_SV"       = EXCLUDED."pts_SV",
    "pts_BS"       = EXCLUDED."pts_BS",
    "pts_IP"       = EXCLUDED."pts_IP",
    "pts_CG"       = EXCLUDED."pts_CG",
    "pts_SO"       = EXCLUDED."pts_SO",
    "pts_NH"       = EXCLUDED."pts_NH",
    "pts_PG"       = EXCLUDED."pts_PG",
    fantasy_points = EXCLUDED.fantasy_points;

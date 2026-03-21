INSERT INTO fantasy.dk_batter_game_scores (
    batter_id, game_pk, team_id, batter_name, game_date, season,
    "dk_pts_1B", "dk_pts_2B", "dk_pts_3B", "dk_pts_HR",
    "dk_pts_RBI", "dk_pts_R", "dk_pts_BB", "dk_pts_HBP", "dk_pts_SB",
    dk_points
)
SELECT
    bb.batter_id,
    bb.game_pk,
    MAX(bb.team_id),
    MAX(bb.batter_name),
    g.game_date,
    g.season,
    SUM(bb.hits - bb.doubles - bb.triples - bb.home_runs),
    SUM(bb.doubles),
    SUM(bb.triples),
    SUM(bb.home_runs),
    SUM(bb.rbi),
    SUM(bb.runs),
    SUM(bb.walks),
    SUM(bb.hit_by_pitch),
    SUM(bb.sb),
    SUM(bb.hits - bb.doubles - bb.triples - bb.home_runs) * 3.0
    + SUM(bb.doubles) * 5.0
    + SUM(bb.triples) * 8.0
    + SUM(bb.home_runs) * 10.0
    + SUM(bb.rbi) * 2.0
    + SUM(bb.runs) * 2.0
    + SUM(bb.walks) * 2.0
    + SUM(bb.hit_by_pitch) * 2.0
    + SUM(bb.sb) * 5.0
FROM staging.batting_boxscores bb
JOIN production.dim_game g ON g.game_pk = bb.game_pk
GROUP BY bb.batter_id, bb.game_pk, g.game_date, g.season
ON CONFLICT (batter_id, game_pk) DO UPDATE
SET team_id      = EXCLUDED.team_id,
    batter_name  = EXCLUDED.batter_name,
    game_date    = EXCLUDED.game_date,
    season       = EXCLUDED.season,
    "dk_pts_1B"  = EXCLUDED."dk_pts_1B",
    "dk_pts_2B"  = EXCLUDED."dk_pts_2B",
    "dk_pts_3B"  = EXCLUDED."dk_pts_3B",
    "dk_pts_HR"  = EXCLUDED."dk_pts_HR",
    "dk_pts_RBI" = EXCLUDED."dk_pts_RBI",
    "dk_pts_R"   = EXCLUDED."dk_pts_R",
    "dk_pts_BB"  = EXCLUDED."dk_pts_BB",
    "dk_pts_HBP" = EXCLUDED."dk_pts_HBP",
    "dk_pts_SB"  = EXCLUDED."dk_pts_SB",
    dk_points    = EXCLUDED.dk_points;

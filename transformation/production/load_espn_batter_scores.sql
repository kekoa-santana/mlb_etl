INSERT INTO fantasy.espn_batter_game_scores (
    batter_id, game_pk, team_id, batter_name, game_date, season,
    "pts_H", "pts_R", "pts_TB", "pts_RBI", "pts_BB", "pts_K", "pts_SB", "pts_E",
    "pts_CYC", "pts_GWRBI", "pts_GSHR",
    fantasy_points
)
SELECT
    bb.batter_id,
    bb.game_pk,
    MAX(bb.team_id),
    MAX(bb.batter_name),
    g.game_date,
    g.season,
    SUM(bb.hits),
    SUM(bb.runs),
    SUM(bb.total_bases),
    SUM(bb.rbi),
    SUM(bb.walks),
    SUM(bb.strikeouts),
    SUM(bb.sb),
    SUM(bb.errors),
    CASE
        WHEN SUM(bb.hits - bb.doubles - bb.triples - bb.home_runs) >= 1
            AND SUM(bb.doubles) >= 1
            AND SUM(bb.triples) >= 1
            AND SUM(bb.home_runs) >= 1
        THEN 1 ELSE 0
    END,
    0,
    0,
    SUM(bb.hits)
    + SUM(bb.runs)
    + SUM(bb.total_bases)
    + SUM(bb.rbi)
    + SUM(bb.walks)
    - SUM(bb.strikeouts)
    + SUM(bb.sb)
    - SUM(bb.errors)
    + CASE
        WHEN SUM(bb.hits - bb.doubles - bb.triples - bb.home_runs) >= 1
            AND SUM(bb.doubles) >= 1
            AND SUM(bb.triples) >= 1
            AND SUM(bb.home_runs) >= 1
        THEN 10 ELSE 0
      END
FROM staging.batting_boxscores bb
JOIN production.dim_game g ON g.game_pk = bb.game_pk
GROUP BY bb.batter_id, bb.game_pk, g.game_date, g.season
ON CONFLICT (batter_id, game_pk) DO UPDATE
SET team_id        = EXCLUDED.team_id,
    batter_name    = EXCLUDED.batter_name,
    game_date      = EXCLUDED.game_date,
    season         = EXCLUDED.season,
    "pts_H"        = EXCLUDED."pts_H",
    "pts_R"        = EXCLUDED."pts_R",
    "pts_TB"       = EXCLUDED."pts_TB",
    "pts_RBI"      = EXCLUDED."pts_RBI",
    "pts_BB"       = EXCLUDED."pts_BB",
    "pts_K"        = EXCLUDED."pts_K",
    "pts_SB"       = EXCLUDED."pts_SB",
    "pts_E"        = EXCLUDED."pts_E",
    "pts_CYC"      = EXCLUDED."pts_CYC",
    "pts_GWRBI"    = EXCLUDED."pts_GWRBI",
    "pts_GSHR"     = EXCLUDED."pts_GSHR",
    fantasy_points = EXCLUDED.fantasy_points;

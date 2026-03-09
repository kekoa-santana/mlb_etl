INSERT INTO production.fact_game_totals (
    game_pk, team_id, season, home_away,
    runs, hits, doubles, triples, home_runs,
    walks, strikeouts, hit_by_pitch, sb, caught_stealing,
    at_bats, plate_appearances, total_bases, rbi, errors
)
SELECT
    bb.game_pk,
    bb.team_id,
    g.season,
    CASE WHEN bb.team_id = g.home_team_id THEN 'home' ELSE 'away' END AS home_away,
    SUM(bb.runs) AS runs,
    SUM(bb.hits) AS hits,
    SUM(bb.doubles) AS doubles,
    SUM(bb.triples) AS triples,
    SUM(bb.home_runs) AS home_runs,
    SUM(bb.walks) AS walks,
    SUM(bb.strikeouts) AS strikeouts,
    SUM(bb.hit_by_pitch) AS hit_by_pitch,
    SUM(bb.sb) AS sb,
    SUM(bb.caught_stealing) AS caught_stealing,
    SUM(bb.at_bats) AS at_bats,
    SUM(bb.plate_appearances) AS plate_appearances,
    SUM(bb.total_bases) AS total_bases,
    SUM(bb.rbi) AS rbi,
    SUM(bb.errors) AS errors
FROM staging.batting_boxscores bb
JOIN production.dim_game g ON g.game_pk = bb.game_pk
GROUP BY bb.game_pk, bb.team_id, g.season,
    CASE WHEN bb.team_id = g.home_team_id THEN 'home' ELSE 'away' END
ON CONFLICT (game_pk, team_id) DO UPDATE
SET runs = EXCLUDED.runs,
    hits = EXCLUDED.hits,
    doubles = EXCLUDED.doubles,
    triples = EXCLUDED.triples,
    home_runs = EXCLUDED.home_runs,
    walks = EXCLUDED.walks,
    strikeouts = EXCLUDED.strikeouts,
    hit_by_pitch = EXCLUDED.hit_by_pitch,
    sb = EXCLUDED.sb,
    caught_stealing = EXCLUDED.caught_stealing,
    at_bats = EXCLUDED.at_bats,
    plate_appearances = EXCLUDED.plate_appearances,
    total_bases = EXCLUDED.total_bases,
    rbi = EXCLUDED.rbi,
    errors = EXCLUDED.errors;

INSERT INTO staging.batting_boxscores(
    batter_id, game_pk, team_id, batter_name, team_name, position,
    ground_outs, air_outs, runs, doubles, triples, home_runs,
    strikeouts, walks, intentional_walks, hits, hit_by_pitch, at_bats,
    caught_stealing, sb, sb_pct, plate_appearances, total_bases, rbi,
    errors, source, load_id, ingested_at
)
SELECT
    CAST(r.batter_id AS BIGINT),
    CAST(r.game_pk AS BIGINT),
    CAST(r.team_id AS INT),
    LOWER(r.batter_name) AS batter_name,
    LOWER(r.team_name) AS team_name,
    LOWER(r.position) AS position,
    CAST(r.ground_outs_text AS SMALLINT),
    CAST(r.air_outs_text AS SMALLINT),
    CAST(r.runs_text AS SMALLINT),
    CAST(r.doubles_text AS SMALLINT),
    CAST(r.triples_text AS SMALLINT),
    CAST(r.home_runs_text AS SMALLINT),
    CAST(r.strikeouts_text AS SMALLINT),
    CAST(r.walks_text AS SMALLINT),
    CAST(r.intentional_walks_text AS SMALLINT),
    CAST(r.hits_text AS SMALLINT),
    CAST(r.hit_by_pitch_text AS SMALLINT),
    CAST(r.at_bats_text AS SMALLINT),
    CAST(r.caught_stealing_text AS SMALLINT),
    CAST(r.sb_text AS SMALLINT),
    CASE
        WHEN TRIM(r.sb_pct_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.sb_pct_text AS REAL)
    END AS sb_pct,
    CAST(r.plate_appearances_text AS SMALLINT),
    CAST(r.total_bases_text AS SMALLINT),
    CAST(r.rbi_text AS SMALLINT),
    CAST(r.errors_text AS SMALLINT),
    r.source,
    r.load_id,
    r.ingested_at
FROM (
    SELECT DISTINCT ON (batter_id, game_pk, team_id) *
    FROM raw.batting_boxscores
    ORDER BY batter_id, game_pk, ingested_at
) as r
ON CONFLICT ON CONSTRAINT batting_boxscores_pkey UPDATE staging.batting_boxscores (
    team_name= EXCLUDED.team_name,
    batter_name = EXCLUDED.batter_name,
    position = EXCLUDED.position,
    ground_outs = EXCLUDED.ground_outs,
    air_outs = EXCLUDED.air_outs,
    runs = EXCLUDED.runs,
    doubles = EXCLUDED.doubles,
    triples = EXCLUDED.triples,
    home_runs = EXCLUDED.home_runs,
    strike_outs = EXCLUDED.strike_outs,
    walks = EXCLUDED.walks,
    intentional_walks = EXCLUDED.intentional_walks,
    hits = EXCLUDED.hits,
    hit_by_pitch = EXCLUDED.hit_by_pitch,
    at_bats = EXCLUDED.at_bats,
    caught_stealing = EXCLUDED.caught_stealing,
    sb = EXCLUDED.sb,
    sb_pct = EXCLUDED.sb_pct,
    plate_appeances = EXCLUDED.plate_appearances,
    total_bases = EXCLUDED.total_bases,
    rbi = EXCLUDED.rbi,
    errors = EXCLUDED.errors,
    source = EXCLUDED.source,
    load_id = EXCLUDED.load_id,
    ingested_at = EXCLUDED.ingested_at
)
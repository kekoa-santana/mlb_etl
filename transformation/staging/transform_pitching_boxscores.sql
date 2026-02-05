INSERT INTO staging.pitching_boxscores (
    game_pk,
    pitcher_id,
    team_id,
    team_name,
    pitcher_name,
    is_starter,
    fly_outs,
    ground_outs,
    air_outs,
    runs,
    doubles,
    triples,
    home_runs,
    strike_outs,
    walks,
    intentional_walks,
    hits,
    hit_by_pitch,
    at_bats,
    caught_stealing,
    stolen_bases,
    stolen_base_pct,
    number_of_pitches,
    innings_pitched,
    wins,
    losses,
    saves,
    save_opportunities,
    holds,
    blown_saves,
    earned_runs,
    batters_faced,
    outs,
    complete_game,
    shutout,
    balls,
    strikes,
    strike_pct,
    hit_batsmen,
    balks,
    wild_pitches,
    pickoffs,
    rbi,
    games_finished,
    runs_scored_per_9,
    home_runs_per_9,
    inherited_runners,
    inherited_runners_scored,
    catchers_interference,
    sac_bunts,
    sac_flies,
    passed_ball,
    pop_outs,
    line_outs,
    source,
    load_id,
    ingested_at
) SELECT 
    CAST(r.game_pk AS BIGINT),
    CAST(r.pitcher_id AS BIGINT),
    CAST(r.team_id AS BIGINT),
    CAST(r.team_name AS VARCHAR(50)),
    CAST(r.pitcher_name AS VARCHAR(50)),
    CASE
        WHEN TRIM(r.is_starter_text) = '1' THEN TRUE
        WHEN TRIM(r.is_starter_text) = '0' THEN FALSE
        ELSE NULL
    END,
    CAST(r.fly_outs_text AS SMALLINT),
    CAST(r.ground_outs_text AS SMALLINT),
    CAST(r.air_outs_text AS SMALLINT),
    CAST(r.runs_text AS SMALLINT),
    CAST(r.doubles_text AS SMALLINT),
    CAST(r.triples_text AS SMALLINT),
    CAST(r.home_runs_text AS SMALLINT),
    CAST(r.strike_outs_text AS SMALLINT),
    CAST(r.walks_text AS SMALLINT),
    CAST(r.intentional_walks_text AS SMALLINT),
    CAST(r.hits_text AS SMALLINT),
    CAST(r.hit_by_pitch_text AS SMALLINT),
    CAST(r.at_bats_text AS SMALLINT),
    CAST(r.caught_stealing_text AS SMALLINT),
    CAST(r.stolen_bases_text AS SMALLINT),
    CASE
        WHEN TRIM(r.stolen_base_percentage_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.stolen_base_percentage_text AS NUMERIC(5, 3))
    END,
    CAST(r.number_of_pitches_text AS SMALLINT),
    CASE
        WHEN TRIM(r.innings_pitched_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.innings_pitched_text AS NUMERIC(4, 1))
    END,
    CAST(r.wins_text AS SMALLINT),
    CAST(r.losses_text AS SMALLINT),
    CAST(r.saves_text AS SMALLINT),
    CAST(r.save_opportunities_text AS SMALLINT),
    CAST(r.holds_text AS SMALLINT),
    CAST(r.blown_saves_text AS SMALLINT),
    CAST(r.earned_runs_text AS SMALLINT),
    CAST(r.batters_faced_text AS SMALLINT),
    CAST(r.outs_text AS SMALLINT),
    CASE
        WHEN TRIM(r.complete_game_text) = '1' THEN TRUE
        WHEN TRIM(r.complete_game_text) = '0' THEN FALSE
        ELSE NULL
    END,
    CASE
        WHEN TRIM(r.shutout_text) = '1' THEN TRUE
        WHEN TRIM(r.shutout_text) = '0' THEN FALSE
        ELSE NULL
    END,
    CAST(r.balls_text AS SMALLINT),
    CAST(r.strikes_text AS SMALLINT),
    CASE
        WHEN TRIM(r.strike_percentage_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.strike_percentage_text AS NUMERIC(5, 3))
    END,
    CAST(r.hit_batsmen_text AS SMALLINT),
    CAST(r.balks_text AS SMALLINT),
    CAST(r.wild_pitches_text AS SMALLINT),
    CAST(r.pickoffs_text AS SMALLINT),
    CAST(r.rbi_text AS SMALLINT),
    CASE
        WHEN TRIM(r.games_finished_text) = '1' THEN TRUE
        WHEN TRIM(r.games_finished_text) = '0' THEN FALSE
        ELSE NULL
    END,
    CASE
        WHEN TRIM(r.runs_scored_per_9_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.runs_scored_per_9_text AS NUMERIC(6,2))
    END,
    CASE
        WHEN TRIM(r.home_runs_per_9_text) IN ('.---', '-.--') THEN NULL
        ELSE CAST(r.home_runs_per_9_text AS NUMERIC(6,2))
    END,
    CAST(r.inherited_runners_text AS SMALLINT),
    CAST(r.inherited_runners_scored_text AS SMALLINT),
    CAST(r.catchers_interference_text AS SMALLINT),
    CAST(r.sac_bunts_text AS SMALLINT),
    CAST(r.sac_flies_text AS SMALLINT),
    CAST(r.passed_ball_text AS SMALLINT),
    CAST(r.pop_outs_text AS SMALLINT),
    CAST(r.line_outs_text AS SMALLINT),
    r.source,
    r.load_id,
    r.ingested_at
FROM (
    SELECT DISTINCT ON (game_pk, pitcher_id, team_id) *
    FROM raw.pitching_boxscores
    ORDER BY game_pk, pitcher_id, team_id, ingested_at DESC
) as r
ON CONFLICT ON CONSTRAINT pitching_boxscores_pkey DO UPDATE SET
    team_name = EXCLUDED.team_name,
    pitcher_name = EXCLUDED.pitcher_name,
    is_starter = EXCLUDED.is_starter,
    fly_outs = EXCLUDED.fly_outs,
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
    stolen_bases = EXCLUDED.stolen_bases,
    stolen_base_pct = EXCLUDED.stolen_base_pct,
    number_of_pitches = EXCLUDED.number_of_pitches,
    innings_pitched = EXCLUDED.innings_pitched,
    wins = EXCLUDED.wins,
    losses = EXCLUDED.losses,
    saves = EXCLUDED.saves,
    save_opportunities = EXCLUDED.save_opportunities,
    holds = EXCLUDED.holds,
    blown_saves = EXCLUDED.blown_saves,
    earned_runs = EXCLUDED.earned_runs,
    batters_faced = EXCLUDED.batters_faced,
    outs = EXCLUDED.outs,
    complete_game = EXCLUDED.complete_game,
    shutout = EXCLUDED.shutout,
    balls = EXCLUDED.balls,
    strikes = EXCLUDED.strikes,
    strike_pct = EXCLUDED.strike_pct,
    hit_batsmen = EXCLUDED.hit_batsmen,
    balks = EXCLUDED.balks,
    wild_pitches = EXCLUDED.wild_pitches,
    pickoffs = EXCLUDED.pickoffs,
    rbi = EXCLUDED.rbi,
    games_finished = EXCLUDED.games_finished,
    runs_scored_per_9 = EXCLUDED.runs_scored_per_9,
    home_runs_per_9 = EXCLUDED.home_runs_per_9,
    inherited_runners = EXCLUDED.inherited_runners,
    inherited_runners_scored = EXCLUDED.inherited_runners_scored,
    catchers_interference = EXCLUDED.catchers_interference,
    sac_bunts = EXCLUDED.sac_bunts,
    sac_flies = EXCLUDED.sac_flies,
    passed_ball = EXCLUDED.passed_ball,
    pop_outs = EXCLUDED.pop_outs,
    line_outs = EXCLUDED.line_outs,
    source = EXCLUDED.source,
    load_id = EXCLUDED.load_id,
    ingested_at = EXCLUDED.ingested_at;

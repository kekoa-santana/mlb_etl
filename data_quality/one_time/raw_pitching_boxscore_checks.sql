-- Row count check
SELECT COUNT(*) FROM raw.pitching_boxscores;

-- Duplicate PK rows
SELECT pitcher_id, team_id, game_pk, COUNT(*)
FROM raw.pitching_boxscores
GROUP BY pitcher_id, team_id, game_pk
HAVING COUNT(*) > 1;

-- Invalid IDs
SELECT *
FROM raw.pitching_boxscores
WHERE pitcher_id <= 0 OR team_id <= 0 OR game_pk <= 0;

-- BOOL unique values
SELECT *
FROM raw.pitching_boxscores
WHERE is_starter_text NOT IN ('0', '1') OR complete_game_text NOT IN ('0', '1') OR shutout_text NOT IN ('0', '1');

-- less than 2 pitchers per team (2 games for Yamamoto)
SELECT game_pk, COUNT(*) AS pitcher_rows
FROM raw.pitching_boxscores
GROUP BY game_pk
HAVING COUNT(*) < 2
ORDER BY pitcher_rows, game_pk;

-- Pitcher name present
SELECT COUNT(*)
FROM raw.pitching_boxscores
WHERE pitcher_name IS NULL OR TRIM(pitcher_name) = '';

-- Team name present
SELECT COUNT(*)
FROM raw.pitching_boxscores
WHERE team_name IS NULL OR TRIM(team_name) = '';

-- Runs is non-null and valid
SELECT *
FROM raw.pitching_boxscores
WHERE runs_text IS NOT NULL
    AND TRIM(runs_text) <> ''
    AND runs_text !~ '^[0-9]+$';

-- fly_outs valid
SELECT *
FROM raw.pitching_boxscores
WHERE fly_outs_text IS NOT NULL
    AND TRIM(fly_outs_text) <> ''
    AND fly_outs_text !~ '^[0-9]+$';
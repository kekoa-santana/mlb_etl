# Staging Mapping - pitching_boxscores

This document defines how fields in `raw.pitching_boxscores` map to `staging.pitching_boxscores`.

- **Grain**: 1 row per (game_pk, pitcher_id, team_id)
- **Source**: MLB StatsAPI boxscore endpoint
- **Load**: Insert-only, incremental (only new rows from raw)

## Field mapping

| Staging column        | Raw column                | Type              | Description                                   | Transform logic                                   |
|-----------------------|---------------------------|-------------------|-----------------------------------------------|---------------------------------------------------|
| game_pk               | game_pk_text              | INTEGER           | MLB game primary key                          | `CAST(game_pk_text AS INTEGER)`                   | 
| pitcher_id            | pitcher_id                | INTEGER           | MLBAM pitcher id                              | no transform                                      |
| team_id               | team_id                   | INTEGER           | MLBAM team id                                 | no transform                                      |
| team_name             | team_name                 | VARCHAR(50)       | MLB team name                                 | no transform                                      |
| pitcher_name          | pitcher_name              | VARCHAR(80)       | MLB player name                               | no transform                                      |
| is_starter            | is_starter_text           | BOOLEAN           | True if pitcher was starter of game           | `CASE WHEN is_starter_text IS '1' THEN TRUE ...`  |
| 
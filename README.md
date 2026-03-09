# MLB Analytics Data Warehouse

A production-grade data warehouse covering every MLB regular season and postseason game from 2018 through the present, with automated daily ingestion for the current season. The pipeline extracts data from the MLB Stats API and Baseball Savant (Statcast), transforms it through a three-layer architecture (raw, staging, production), and produces analytics-ready tables spanning pitch-level biomechanics, plate appearance outcomes, batted ball quality, game context, and environmental factors.

## What This Database Enables

This warehouse is purpose-built for the types of analyses that drive modern front office decision-making:

- **Pitch Design & Arsenal Evaluation** - Every pitch tracked with velocity, spin rate, spin axis, movement profiles, release position, and acceleration vectors. Identify how a pitcher's slider has evolved over a season, compare release tunneling between pitch types, or detect velocity trends across outings.
- **Batted Ball & Contact Quality Analysis** - Exit velocity, launch angle, expected batting average (xBA), expected weighted on-base average (xwOBA), expected slugging (xSLG), spray direction, and hard-hit/sweet-spot classifications on every ball in play. Separate luck from true offensive performance.
- **Pitcher Workload & Fatigue Modeling** - Times through the order, pitch counts per plate appearance, pitcher PA sequencing within a game, and complete boxscore stats (IP, pitches, balls/strikes) to build fatigue curves and optimize bullpen usage.
- **Platoon & Matchup Analysis** - Batter handedness tracked on every pitch, enabling granular platoon splits at the pitch-type level (e.g., a left-handed batter's whiff rate against right-handed sliders).
- **Park & Environmental Effects** - Home run park factors by venue, season, and batter handedness (single-season and 3-year rolling), plus per-game weather data (temperature, wind speed/direction/category, dome status) to contextualize offensive output.
- **Lineup & Roster Construction** - Batting order position, starter/bench status, and defensive position per game for every player, combined with player biographical data (age, draft year, debut date, primary position).
- **Game-Level Aggregation** - Team totals per game (R, H, 2B, 3B, HR, BB, K, SB, PA, TB, RBI, E) for quick team performance analysis without needing to re-aggregate from play-level data.
- **Umpire Tendencies** - Home plate umpire tracked per game, joinable to pitch-level strike zone data for studying called strike zones by umpire.

## Architecture

### Pipeline Flow

```
MLB Stats API ──┐                    ┌─── staging.statcast_pitches
                ├── raw layer ──────>├─── staging.statcast_at_bats
Baseball Savant ┘   (text/JSON)      ├─── staging.statcast_batted_balls
                                     ├─── staging.pitching_boxscores
                                     └─── staging.batting_boxscores
                                              │
                                              v
                                     production layer (star schema)
                                     ├── Dimensions: game, player, team, weather, park_factor, umpire
                                     ├── Facts: pitch, plate appearance, lineup, game_totals
                                     └── Satellites: pitch_shape, batted_balls
```

### Layer Design

| Layer | Purpose | Key Design Choice |
|-------|---------|-------------------|
| **Raw** | Immutable landing zone from source APIs | All numeric fields stored as text (`_text` suffix) to preserve original values exactly as received. Full boxscore JSON payloads retained. |
| **Staging** | Cleaned, typed, and enriched | Type casting, bounds checking, derived boolean flags (is_whiff, is_bip, is_swing), PA-level aggregation from pitches. Spec-driven schema validation via `TableSpec`. |
| **Production** | Analytics-ready star schema | Surrogate keys (BIGSERIAL), natural key uniqueness constraints, dimensional modeling. All loads idempotent via `ON CONFLICT DO UPDATE`. |

### Data Sources

| Source | Data | Method |
|--------|------|--------|
| **MLB Stats API** | Game schedule, boxscores (batting + pitching), team rosters, player attributes | REST API (`statsapi.mlb.com`) |
| **Baseball Savant** | Pitch-level Statcast (trajectory, spin, movement, batted ball outcomes, sprint speed) | `pybaseball` library |

## Database Schema

### Production Schema (Star Schema)

#### Dimension Tables

**`production.dim_game`** - One row per MLB game
| Column | Type | Description |
|--------|------|-------------|
| `game_pk` | bigint (PK) | MLB's unique game identifier |
| `game_date` | date | Date of game |
| `home_team_id` / `away_team_id` | integer | Team IDs (FK to dim_team) |
| `home_team_name` / `away_team_name` | text | Team names |
| `game_type` | text | R=Regular, P=Postseason, etc. (excludes Exhibition/Spring) |
| `season` | integer | Season year |
| `home_team_wins` / `home_team_losses` | smallint | Team record at time of game |
| `away_team_wins` / `away_team_losses` | smallint | Team record at time of game |
| `venue_id` | integer | Ballpark identifier |
| `doubleheader` | text | Doubleheader indicator |
| `day_night` | text | Day or night game |
| `games_in_series` / `series_in_game_number` | smallint | Series context |

**`production.dim_player`** - One row per player
| Column | Type | Description |
|--------|------|-------------|
| `player_id` | bigint (PK) | MLB player ID |
| `player_name` | text | Full name |
| `team_id` | bigint | Current team |
| `first_name` / `last_name` | text | Name components |
| `birth_date` | date | Date of birth |
| `age` | smallint | Current age |
| `height` / `weight` | text / smallint | Physical attributes |
| `primary_position_code` / `primary_position` | smallint / varchar | Fielding position |
| `draft_year` | smallint | Year drafted |
| `mlb_debut_date` | date | MLB debut |
| `bat_side` / `pitch_hand` | varchar | Handedness |
| `sz_top` / `sz_bot` | real | Strike zone boundaries (feet) |

**`production.dim_team`** - One row per franchise (30 teams)
| Column | Type | Description |
|--------|------|-------------|
| `team_id` | bigint | MLB team ID |
| `team_name` / `full_name` / `abbreviation` | text | Team identifiers |
| `venue` | text | Home ballpark name |
| `division` / `location` | text | Division and city |

**`production.dim_weather`** - Per-game weather conditions (PK: `game_pk`)
| Column | Type | Description |
|--------|------|-------------|
| `temperature` | smallint | Degrees Fahrenheit |
| `condition` | varchar | Sky condition (Clear, Cloudy, Dome, etc.) |
| `is_dome` | boolean | Whether game played in a dome |
| `wind_speed` | smallint | MPH |
| `wind_direction` / `wind_category` | varchar | Wind direction and categorical bucket |

**`production.dim_park_factor`** - HR park factors (PK: `venue_id`, `season`, `batter_stand`)
| Column | Type | Description |
|--------|------|-------------|
| `venue_name` | text | Ballpark name |
| `batter_stand` | varchar | L or R |
| `hr_pf_season` / `hr_pf_3yr` | real | Single-season and 3-year rolling HR park factor |
| `pa_season` / `hr_season` | integer | Supporting sample sizes |

**`production.dim_umpire`** - Home plate umpire per game (PK: `game_pk`)
| Column | Type | Description |
|--------|------|-------------|
| `hp_umpire_name` | text | Home plate umpire |

#### Fact Tables

**`production.fact_pitch`** - One row per pitch thrown (~5.4M rows per full load)
| Column | Type | Description |
|--------|------|-------------|
| `pitch_id` | bigint (PK) | Surrogate key (BIGSERIAL) |
| `pa_id` | bigint | FK to fact_pa |
| `game_pk` | bigint | FK to dim_game |
| `pitcher_id` / `batter_id` | bigint | FK to dim_player |
| `game_counter` / `pitch_number` | integer | Natural key (UK: game_pk + game_counter + pitch_number) |
| `pitch_type` / `pitch_name` | text | Pitch classification (FF, SL, CU, CH, etc.) |
| `release_speed` / `effective_speed` | real | Velocity (MPH) |
| `release_spin_rate` | real | Spin (RPM) |
| `release_extension` | real | Extension toward plate (feet) |
| `spin_axis` | real | Spin axis (degrees) |
| `pfx_x` / `pfx_z` | real | Horizontal and vertical movement (inches) |
| `zone` | smallint | Strike zone region (1-14) |
| `plate_x` / `plate_z` | real | Pitch location at plate (feet from center) |
| `balls` / `strikes` / `outs_when_up` | smallint | Count and game state |
| `bat_score_diff` | smallint | Run differential from batter's perspective |
| `batter_stand` | varchar | Batter handedness for this PA |
| `is_whiff` / `is_called_strike` / `is_bip` / `is_swing` / `is_foul` | boolean | Pitch outcome flags |

**`production.fact_pa`** - One row per plate appearance (~1.4M rows)
| Column | Type | Description |
|--------|------|-------------|
| `pa_id` | bigint (PK) | Surrogate key (BIGSERIAL) |
| `game_pk` / `game_counter` | bigint / integer | Natural key (UK: game_pk + game_counter) |
| `pitcher_id` / `batter_id` | bigint | FK to dim_player |
| `pitcher_pa_number` | integer | Nth PA faced by this pitcher in this game |
| `times_through_order` | smallint | Times through the order (1st, 2nd, 3rd+) |
| `events` | text | PA outcome (single, strikeout, home_run, walk, etc.) |
| `description` | text | Play description |
| `inning` / `inning_topbot` | integer / text | Inning and half |
| `balls` / `strikes` / `outs_when_up` | smallint | Final count and outs |
| `bat_score` / `fld_score` / `post_bat_score` / `bat_score_diff` | smallint | Score state |
| `last_pitch_number` | smallint | Number of pitches in the PA |

**`production.fact_lineup`** - One row per player per game (~458k rows)
| Column | Type | Description |
|--------|------|-------------|
| `game_pk` / `player_id` | bigint (PK) | Composite primary key |
| `team_id` | integer | Team |
| `batting_order` | smallint | Lineup position (1-9) |
| `is_starter` | boolean | In starting lineup |
| `position` | varchar | Defensive position |
| `home_away` | varchar | Home or away |
| `season` | integer | Season year |

**`production.fact_game_totals`** - Team-level aggregates per game (PK: `game_pk`, `team_id`)
| Column | Type | Description |
|--------|------|-------------|
| `runs` / `hits` / `doubles` / `triples` / `home_runs` | integer | Offensive counting stats |
| `walks` / `strikeouts` / `hit_by_pitch` | integer | Plate discipline |
| `sb` / `caught_stealing` | integer | Baserunning |
| `at_bats` / `plate_appearances` / `total_bases` / `rbi` / `errors` | integer | Additional stats |
| `home_away` | varchar | Home or away indicator |
| `season` | integer | Season year |

#### Satellite Tables

**`production.sat_pitch_shape`** - Pitch physics and trajectory (PK: `pitch_id`, 1:1 with fact_pitch)
| Column | Type | Description |
|--------|------|-------------|
| `release_pos_x` / `release_pos_y` / `release_pos_z` | real | 3D release point (feet) |
| `release_speed` / `release_spin_rate` / `release_extension` | real | Release characteristics |
| `spin_axis` | real | Spin axis (degrees) |
| `pfx_x` / `pfx_z` | real | Pitch movement (inches) |
| `vx0` / `vy0` / `vz0` | real | Initial velocity vector (ft/s) |
| `ax` / `ay` / `az` | real | Acceleration vector (ft/s^2) |
| `plate_x` / `plate_z` | real | Location at plate (feet) |
| `sz_top` / `sz_bot` | real | Batter's strike zone top/bottom (feet) |

**`production.sat_batted_balls`** - Batted ball quality (PK: `pitch_id`, one row per BIP)
| Column | Type | Description |
|--------|------|-------------|
| `pa_id` | bigint | FK to fact_pa |
| `bb_type` | text | ground_ball, line_drive, fly_ball, popup |
| `events` | text | Hit outcome |
| `launch_speed` | real | Exit velocity (MPH) |
| `launch_angle` | real | Launch angle (degrees) |
| `hit_distance_sc` | real | Projected distance (feet) |
| `hc_x` / `hc_y` / `hc_x_centered` | real | Hit coordinates and centered spray |
| `xba` / `xslg` / `xwoba` | real | Statcast expected stats |
| `woba_value` / `babip_value` / `iso_value` | real / smallint | Actual outcome values |
| `hard_hit` | boolean | Exit velo >= 95 MPH |
| `sweet_spot` | boolean | Launch angle 8-32 degrees |
| `ideal_contact` | boolean | Hard hit AND sweet spot |
| `la_band` / `ev_band` / `spray_bucket` | text | Categorical bins |

### Staging Schema

Intermediate cleaned and typed data before production modeling.

| Table | PK | Description |
|-------|-----|-------------|
| `statcast_pitches` | game_pk, game_counter, pitch_number | Every pitch with full Statcast measurements, derived flags (is_whiff, is_bip, is_swing, is_called_strike, is_foul), fielding alignment, arm angle |
| `statcast_at_bats` | game_pk, game_counter | PA-level aggregation: pitch counts, whiff/swing/foul totals, times through order, pitcher PA number, RBI, walk/strikeout/BIP flags |
| `statcast_batted_balls` | game_pk, game_counter, pitch_number | Batted ball events with exit velo, launch angle, expected stats, hit coordinates |
| `statcast_sprint_speed` | player_id, season | Player sprint speed, home-to-first times, bolts (30+ ft/s runs) |
| `pitching_boxscores` | game_pk, pitcher_id, team_id | Full pitching line: IP, K, BB, H, ER, pitch count, balls/strikes, saves, holds, inherited runners |
| `batting_boxscores` | batter_id, game_pk, team_id | Full batting line: AB, H, 2B, 3B, HR, RBI, BB, K, SB, PA, TB |

### Raw Schema

Immutable source data preserving original API responses.

| Table | PK | Description |
|-------|-----|-------------|
| `dim_game` | game_pk, home_team_id, away_team_id | Game schedule with all numeric fields as text |
| `pitching_boxscores` | pitcher_id, team_id, game_pk | Pitcher stats with `_text` suffix columns |
| `batting_boxscores` | batter_id, game_pk, team_id | Batter stats with `_text` suffix columns |
| `landing_boxscores` | load_id (UK: game_pk) | Full JSON boxscore payloads |
| `landing_statcast_files` | run_id | Parquet file registry with schema hashes for Statcast ingestion tracking |

### Fantasy Schema

Pre-computed fantasy point calculations for DraftKings and ESPN scoring systems.

| Table | Description |
|-------|-------------|
| `dk_pitcher_game_scores` | DraftKings pitcher points (IP, K, W, ER, H, BB, HBP, CG, CGSO, NH) |
| `dk_batter_game_scores` | DraftKings batter points (1B, 2B, 3B, HR, RBI, R, BB, HBP, SB) |
| `espn_pitcher_game_scores` | ESPN pitcher points (H, RA, ER, BB, K, PKO, W, L, SV, BS, IP, CG, SO, NH, PG) |
| `espn_batter_game_scores` | ESPN batter points (H, R, TB, RBI, BB, K, SB, E, CYC, GWRBI, GSHR) |

## Tech Stack

- **Python 3**, pandas, SQLAlchemy (psycopg driver), pybaseball
- **PostgreSQL** with schema-per-layer isolation
- **Alembic** for migration management
- **Spec-driven schema validation** via custom `TableSpec`/`ColumnSpec` system (type coercion, bounds checking, null enforcement, PK dedup)
- Idempotent loads (`ON CONFLICT DO UPDATE`) for safe re-runs and backfills

## Usage

```bash
# Full pipeline: ingest + staging + production for a date range
python full_pipeline.py --start-date 2025-03-18 --end-date 2025-11-01

# Skip ingestion, load from existing parquet
python full_pipeline.py --skip-ingestion --parquet data/file.parquet

# Ingestion only
python full_pipeline.py --skip-staging --skip-production
```

Daily automated runs pull the previous day's games:
```bash
python full_pipeline.py --start-date <yesterday> --end-date <yesterday>
```

## Data Volume (Full 2018-2025 Load)

| Layer | Table | Approximate Rows |
|-------|-------|-----------------|
| Production | fact_pitch + sat_pitch_shape | ~5.4M |
| Production | fact_pa | ~1.4M |
| Production | sat_batted_balls | ~483k |
| Production | fact_lineup | ~458k |
| Production | fact_game_totals | ~39k |
| Production | dim_game | ~19.5k |
| Staging | statcast_pitches | ~5.6M |
| Staging | statcast_at_bats | ~1.5M |
| Staging | statcast_batted_balls | ~985k |
| Staging | pitching_boxscores | ~173k |
| Staging | batting_boxscores | ~458k |

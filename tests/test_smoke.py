"""Smoke tests — read-only queries against the live database to verify data integrity.

Run with: pytest tests/test_smoke.py -v
These tests require a running PostgreSQL instance with the mlb_fantasy database.
"""
import pytest
from sqlalchemy import create_engine, text
from utils.utils import build_db_url

engine = create_engine(build_db_url(database="mlb_fantasy"), pool_pre_ping=True)

EXPECTED_SEASONS = {2018, 2019, 2021, 2022, 2023, 2024, 2025}


def _query_scalar(sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def _query_rows(sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


# ── Schema existence ──

class TestSchemasExist:
    @pytest.mark.parametrize("schema", ["raw", "staging", "production"])
    def test_schema_exists(self, schema):
        n = _query_scalar(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = :s",
            s=schema,
        )
        assert n == 1, f"Schema '{schema}' does not exist"


# ── Core table existence ──

EXPECTED_TABLES = {
    "raw": [
        "landing_boxscores", "dim_game", "pitching_boxscores",
        "batting_boxscores", "landing_statcast_files", "transactions",
    ],
    "staging": [
        "statcast_pitches", "statcast_at_bats", "statcast_batted_balls",
        "statcast_sprint_speed", "pitching_boxscores", "batting_boxscores",
        "milb_batting_game_logs", "milb_pitching_game_logs",
    ],
    "production": [
        "dim_game", "dim_player", "dim_team",
        "fact_pa", "fact_pitch", "sat_pitch_shape", "sat_batted_balls",
        "fact_lineup", "dim_weather", "dim_park_factor", "dim_umpire",
        "fact_game_totals", "dim_prospects", "dim_transaction",
    ],
}


class TestTablesExist:
    @pytest.mark.parametrize(
        "schema,table",
        [(s, t) for s, tables in EXPECTED_TABLES.items() for t in tables],
    )
    def test_table_exists(self, schema, table):
        n = _query_scalar(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = :s AND table_name = :t",
            s=schema, t=table,
        )
        assert n == 1, f"Table {schema}.{table} does not exist"


# ── Row count sanity ──

class TestRowCounts:
    def test_raw_dim_game_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM raw.dim_game")
        assert n > 10_000

    def test_staging_statcast_pitches_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM staging.statcast_pitches")
        assert n > 1_000_000

    def test_production_dim_player_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM production.dim_player")
        assert n > 1_000

    def test_production_dim_team_has_30(self):
        n = _query_scalar("SELECT count(*) FROM production.dim_team")
        assert n == 30

    def test_production_fact_pa_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM production.fact_pa")
        assert n > 500_000


# ── MiLB Game Logs ──

class TestMilbGameLogs:
    def test_batting_has_all_seasons(self):
        rows = _query_rows(
            "SELECT DISTINCT season FROM staging.milb_batting_game_logs ORDER BY season"
        )
        seasons = {r[0] for r in rows}
        assert EXPECTED_SEASONS.issubset(seasons)

    def test_pitching_has_all_seasons(self):
        rows = _query_rows(
            "SELECT DISTINCT season FROM staging.milb_pitching_game_logs ORDER BY season"
        )
        seasons = {r[0] for r in rows}
        assert EXPECTED_SEASONS.issubset(seasons)

    def test_batting_row_counts_by_season(self):
        rows = _query_rows(
            "SELECT season, count(*) as cnt "
            "FROM staging.milb_batting_game_logs GROUP BY season"
        )
        for season, cnt in rows:
            assert cnt > 100_000, f"Season {season} has only {cnt} batting rows"

    def test_pitching_row_counts_by_season(self):
        rows = _query_rows(
            "SELECT season, count(*) as cnt "
            "FROM staging.milb_pitching_game_logs GROUP BY season"
        )
        for season, cnt in rows:
            assert cnt > 50_000, f"Season {season} has only {cnt} pitching rows"

    def test_no_null_batter_ids(self):
        n = _query_scalar(
            "SELECT count(*) FROM staging.milb_batting_game_logs WHERE batter_id IS NULL"
        )
        assert n == 0

    def test_no_null_pitcher_ids(self):
        n = _query_scalar(
            "SELECT count(*) FROM staging.milb_pitching_game_logs WHERE pitcher_id IS NULL"
        )
        assert n == 0

    def test_valid_sport_ids(self):
        rows = _query_rows(
            "SELECT DISTINCT sport_id FROM staging.milb_batting_game_logs ORDER BY sport_id"
        )
        sport_ids = {r[0] for r in rows}
        assert sport_ids.issubset({11, 12, 13, 14, 16})

    def test_valid_levels(self):
        rows = _query_rows(
            "SELECT DISTINCT level FROM staging.milb_batting_game_logs ORDER BY level"
        )
        levels = {r[0] for r in rows}
        assert levels.issubset({"AAA", "AA", "A+", "A", "ROK"})

    def test_no_monthly_gaps(self):
        """Each season should have games in months 4-9 (April-September)."""
        rows = _query_rows("""
            SELECT season, EXTRACT(MONTH FROM game_date)::int as month
            FROM staging.milb_batting_game_logs
            GROUP BY season, EXTRACT(MONTH FROM game_date)
            ORDER BY season, month
        """)
        by_season = {}
        for season, month in rows:
            by_season.setdefault(season, set()).add(month)

        for season in EXPECTED_SEASONS:
            if season == 2021:
                # 2021 started in May
                expected_months = {5, 6, 7, 8, 9}
            else:
                expected_months = {4, 5, 6, 7, 8, 9}
            missing = expected_months - by_season.get(season, set())
            assert not missing, f"Season {season} missing months: {missing}"


# ── Dim Prospects ──

class TestDimProspects:
    def test_has_all_seasons(self):
        rows = _query_rows(
            "SELECT DISTINCT season FROM production.dim_prospects ORDER BY season"
        )
        seasons = {r[0] for r in rows}
        assert EXPECTED_SEASONS.issubset(seasons)

    def test_composite_pk_no_duplicates(self):
        n = _query_scalar("""
            SELECT count(*) FROM (
                SELECT player_id, season, count(*)
                FROM production.dim_prospects
                GROUP BY player_id, season
                HAVING count(*) > 1
            ) dupes
        """)
        assert n == 0, "Duplicate (player_id, season) found"

    def test_no_null_full_names(self):
        n = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects WHERE full_name IS NULL"
        )
        assert n == 0

    def test_no_null_pks(self):
        n = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects "
            "WHERE player_id IS NULL OR season IS NULL"
        )
        assert n == 0

    def test_bio_fields_populated(self):
        n_total = _query_scalar("SELECT count(*) FROM production.dim_prospects")
        n_birth = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects WHERE birth_date IS NOT NULL"
        )
        pct = n_birth / n_total
        assert pct > 0.95, f"Only {pct:.1%} have birth_date"

    def test_bat_side_populated(self):
        n_total = _query_scalar("SELECT count(*) FROM production.dim_prospects")
        n_bat = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects WHERE bat_side IS NOT NULL"
        )
        pct = n_bat / n_total
        assert pct > 0.95, f"Only {pct:.1%} have bat_side"

    def test_prospect_counts_by_season(self):
        rows = _query_rows(
            "SELECT season, count(*) FROM production.dim_prospects GROUP BY season"
        )
        for season, cnt in rows:
            assert cnt > 5_000, f"Season {season} has only {cnt} prospects"

    def test_valid_levels(self):
        rows = _query_rows(
            "SELECT DISTINCT level FROM production.dim_prospects"
        )
        levels = {r[0] for r in rows}
        assert levels.issubset({"AAA", "AA", "A+", "A", "ROK"})

    def test_parent_org_ids_mostly_valid(self):
        """Most parent org IDs should be MLB team IDs (108-158 range)."""
        n_total = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects WHERE parent_org_id IS NOT NULL"
        )
        n_valid = _query_scalar(
            "SELECT count(*) FROM production.dim_prospects "
            "WHERE parent_org_id BETWEEN 108 AND 158"
        )
        pct = n_valid / n_total if n_total > 0 else 0
        assert pct > 0.90, f"Only {pct:.1%} have standard MLB parent org IDs"


# ── Transactions ──

class TestTransactions:
    def test_raw_transactions_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM raw.transactions")
        assert n > 1_000

    def test_no_null_transaction_ids(self):
        n = _query_scalar(
            "SELECT count(*) FROM raw.transactions WHERE transaction_id IS NULL"
        )
        assert n == 0


# ── Production Facts ──

class TestProductionFacts:
    def test_fact_pa_pk_unique(self):
        n = _query_scalar("""
            SELECT count(*) FROM (
                SELECT game_pk, game_counter, count(*)
                FROM production.fact_pa
                GROUP BY game_pk, game_counter
                HAVING count(*) > 1
            ) dupes
        """)
        assert n == 0

    def test_fact_pitch_pk_unique(self):
        n = _query_scalar("""
            SELECT count(*) FROM (
                SELECT game_pk, game_counter, pitch_number, count(*)
                FROM production.fact_pitch
                GROUP BY game_pk, game_counter, pitch_number
                HAVING count(*) > 1
            ) dupes
        """)
        assert n == 0

    def test_dim_game_mostly_regular_season(self):
        """Production dim_game should be predominantly regular season games."""
        n_total = _query_scalar("SELECT count(*) FROM production.dim_game")
        n_regular = _query_scalar(
            "SELECT count(*) FROM production.dim_game WHERE game_type = 'R'"
        )
        pct = n_regular / n_total if n_total > 0 else 0
        assert pct > 0.80, f"Only {pct:.1%} are regular season games"


# ── Boxscores ──

class TestBoxscores:
    def test_staging_batting_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM staging.batting_boxscores")
        assert n > 100_000

    def test_staging_pitching_not_empty(self):
        n = _query_scalar("SELECT count(*) FROM staging.pitching_boxscores")
        assert n > 50_000


# ── Referential integrity spot checks ──

class TestReferentialIntegrity:
    def test_fact_pa_game_pks_in_dim_game(self):
        """Spot check: sample of fact_pa game_pks should exist in dim_game."""
        n = _query_scalar("""
            SELECT count(*) FROM (
                SELECT DISTINCT game_pk FROM production.fact_pa
                EXCEPT
                SELECT game_pk FROM production.dim_game
            ) orphans
        """)
        assert n == 0, f"{n} orphan game_pks in fact_pa"

    def test_dim_weather_game_pks_in_dim_game(self):
        n = _query_scalar("""
            SELECT count(*) FROM (
                SELECT DISTINCT game_pk FROM production.dim_weather
                EXCEPT
                SELECT game_pk FROM production.dim_game
            ) orphans
        """)
        assert n == 0

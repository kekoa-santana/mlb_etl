"""Unit tests for full_pipeline.py — CLI parsing and orchestration."""
import pytest
import sys
import argparse
from unittest.mock import patch, MagicMock


# Mock pybaseball before importing full_pipeline
sys.modules.setdefault("pybaseball", MagicMock())


class TestCLIParsing:
    def _parse(self, args):
        """Helper to parse CLI args matching full_pipeline.main()."""
        parser = argparse.ArgumentParser(description='MLB ETL Pipeline')
        parser.add_argument('--start-date', default="2025-03-18")
        parser.add_argument('--end-date', default="2025-11-01")
        parser.add_argument('--skip-ingestion', action='store_true')
        parser.add_argument('--skip-staging', action='store_true')
        parser.add_argument('--skip-production', action='store_true')
        parser.add_argument('--parquet', type=str)
        parser.add_argument('--data-dir', default='data')
        parser.add_argument('--transactions-only', action='store_true')
        parser.add_argument('--spring', action='store_true')
        parser.add_argument('--milb-only', action='store_true')
        parser.add_argument('-v', '--verbose', action='store_true')
        return parser.parse_args(args)

    def test_defaults(self):
        args = self._parse([])
        assert args.start_date == "2025-03-18"
        assert args.end_date == "2025-11-01"
        assert args.skip_ingestion is False
        assert args.skip_staging is False
        assert args.skip_production is False
        assert args.data_dir == "data"

    def test_custom_dates(self):
        args = self._parse(["--start-date", "2024-04-01", "--end-date", "2024-10-01"])
        assert args.start_date == "2024-04-01"
        assert args.end_date == "2024-10-01"

    def test_skip_flags(self):
        args = self._parse(["--skip-ingestion", "--skip-staging", "--parquet", "data/test.parquet"])
        assert args.skip_ingestion is True
        assert args.skip_staging is True
        assert args.parquet == "data/test.parquet"

    def test_transactions_only(self):
        args = self._parse(["--transactions-only"])
        assert args.transactions_only is True

    def test_spring_mode(self):
        args = self._parse(["--spring"])
        assert args.spring is True

    def test_milb_only(self):
        args = self._parse(["--milb-only"])
        assert args.milb_only is True


class TestTransactionsOnlyMode:
    @patch("utils.sql_runner.run_sql_file")
    @patch("full_pipeline.fetch_and_load_transactions")
    def test_runs_transactions_and_sql(self, mock_fetch, mock_sql):
        with patch.object(sys, 'argv', ['full_pipeline.py', '--transactions-only']):
            from full_pipeline import main
            main()

        mock_fetch.assert_called_once()


class TestSpringMode:
    @patch("full_pipeline.ingestion")
    def test_calls_ingestion_with_skip_statcast(self, mock_ingestion):
        mock_ingestion.return_value = (None, None)
        with patch.object(sys, 'argv', ['full_pipeline.py', '--spring']):
            from full_pipeline import main
            main()

        mock_ingestion.assert_called_once()
        call_args = mock_ingestion.call_args
        # 4th positional arg or skip_statcast kwarg should be True
        if len(call_args.args) >= 4:
            assert call_args.args[3] is True
        else:
            assert call_args.kwargs.get('skip_statcast') is True


class TestMilbOnlyMode:
    @patch("full_pipeline.fetch_and_load_prospects")
    @patch("full_pipeline.fetch_and_load_milb")
    def test_runs_milb_and_prospects(self, mock_milb, mock_prospects):
        with patch.object(sys, 'argv', [
            'full_pipeline.py', '--milb-only',
            '--start-date', '2025-06-01', '--end-date', '2025-06-30',
        ]):
            from full_pipeline import main
            main()

        mock_milb.assert_called_once_with('2025-06-01', '2025-06-30')
        mock_prospects.assert_called_once_with(2025)


# ── SQL Registry ──

class TestSqlRegistry:
    def test_registry_structure(self):
        from transformation.production.sql_registry import SQL_REGISTRY
        for entry in SQL_REGISTRY:
            assert "name" in entry
            assert "script" in entry
            assert "tables" in entry
            assert "depends_on" in entry
            assert isinstance(entry["tables"], list)
            assert isinstance(entry["depends_on"], list)

    def test_registry_order(self):
        from transformation.production.sql_registry import SQL_REGISTRY
        names = [e["name"] for e in SQL_REGISTRY]
        assert names.index("transform_dim_game") < names.index("load_facts")
        assert names.index("transform_pitching_boxscores") < names.index("load_facts")
        assert names.index("load_facts") < names.index("load_pitch_shape")
        assert names.index("load_facts") < names.index("load_batted_balls")

    def test_all_scripts_exist(self):
        import os
        from transformation.production.sql_registry import SQL_REGISTRY
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for entry in SQL_REGISTRY:
            path = os.path.join(base_dir, entry["script"])
            assert os.path.isfile(path), f"SQL script not found: {entry['script']}"

    def test_no_duplicate_names(self):
        from transformation.production.sql_registry import SQL_REGISTRY
        names = [e["name"] for e in SQL_REGISTRY]
        assert len(names) == len(set(names))

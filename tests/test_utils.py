"""Unit tests for utils/ modules — retry, sql_runner, utils, probable_pitchers."""
import pytest
import time
import os
from unittest.mock import patch, MagicMock, mock_open

from utils.retry import build_retry_session, retry_call, RETRYABLE_STATUS_CODES
from utils.sql_runner import run_sql_file, run_sql_registry


# ── build_retry_session ──

class TestBuildRetrySession:
    def test_returns_session_with_timeout(self):
        session = build_retry_session(timeout=30)
        assert session.timeout == 30

    def test_default_timeout(self):
        session = build_retry_session()
        assert session.timeout == 10

    def test_session_has_adapters(self):
        session = build_retry_session()
        assert "https://" in session.adapters
        assert "http://" in session.adapters

    def test_custom_max_retries(self):
        session = build_retry_session(max_retries=5)
        adapter = session.get_adapter("https://example.com")
        assert adapter.max_retries.total == 5

    def test_custom_status_forcelist(self):
        session = build_retry_session(status_forcelist=[500, 503])
        adapter = session.get_adapter("https://example.com")
        assert 500 in adapter.max_retries.status_forcelist
        assert 429 not in adapter.max_retries.status_forcelist

    def test_default_status_forcelist(self):
        session = build_retry_session()
        adapter = session.get_adapter("https://example.com")
        for code in RETRYABLE_STATUS_CODES:
            assert code in adapter.max_retries.status_forcelist


# ── retry_call ──

class TestRetryCall:
    def test_success_first_try(self):
        result = retry_call(lambda: 42)
        assert result == 42

    def test_retries_on_connection_error(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("fail")
            return "ok"

        result = retry_call(flaky, max_retries=3, backoff_factor=0.01)
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        def always_fail():
            raise ConnectionError("always fails")

        with pytest.raises(ConnectionError):
            retry_call(always_fail, max_retries=1, backoff_factor=0.01)

    def test_non_retryable_exception_raises_immediately(self):
        call_count = 0

        def bad_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            retry_call(bad_func, max_retries=3, backoff_factor=0.01)
        assert call_count == 1

    def test_with_args_and_kwargs(self):
        def add(a, b, c=0):
            return a + b + c

        result = retry_call(add, args=(1, 2), kwargs={"c": 3})
        assert result == 6

    def test_timeout_parameter(self):
        def slow_func():
            time.sleep(0.01)
            return "done"

        result = retry_call(slow_func, timeout=5)
        assert result == "done"


# ── run_sql_file ──

class TestRunSqlFile:
    @patch("utils.sql_runner.open", mock_open(read_data="SELECT 1"))
    def test_executes_sql(self, mock_engine):
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        rows = run_sql_file("some/script.sql", engine=mock_engine)
        assert rows == 5
        mock_conn.execute.assert_called_once()

    @patch("utils.sql_runner.open", mock_open(read_data="SELECT 1"))
    def test_returns_zero_for_negative_rowcount(self, mock_engine):
        mock_result = MagicMock()
        mock_result.rowcount = -1
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        rows = run_sql_file("some/script.sql", engine=mock_engine)
        assert rows == 0


# ── run_sql_registry ──

class TestRunSqlRegistry:
    @patch("utils.sql_runner.run_sql_file")
    def test_runs_all_scripts(self, mock_run):
        mock_run.return_value = 10
        registry = [
            {"name": "step1", "script": "a.sql"},
            {"name": "step2", "script": "b.sql"},
        ]
        engine = MagicMock()
        results = run_sql_registry(registry, engine=engine)
        assert len(results) == 2
        assert results["step1"]["status"] == "success"
        assert results["step1"]["rows"] == 10
        assert results["step2"]["status"] == "success"

    @patch("utils.sql_runner.run_sql_file")
    def test_stops_on_error(self, mock_run):
        mock_run.side_effect = [10, RuntimeError("SQL error")]
        registry = [
            {"name": "step1", "script": "a.sql"},
            {"name": "step2", "script": "b.sql"},
            {"name": "step3", "script": "c.sql"},
        ]
        engine = MagicMock()
        with pytest.raises(RuntimeError):
            run_sql_registry(registry, engine=engine)

    @patch("utils.sql_runner.run_sql_file")
    def test_empty_registry(self, mock_run):
        results = run_sql_registry([], engine=MagicMock())
        assert results == {}
        mock_run.assert_not_called()


# ── build_db_url ──

class TestBuildDbUrl:
    @patch.dict(os.environ, {}, clear=True)
    def test_defaults(self):
        from utils.utils import build_db_url
        url = build_db_url()
        assert "postgresql+psycopg" in str(url)
        assert "localhost" in str(url)
        assert "mlb_fantasy" in str(url)

    @patch.dict(os.environ, {
        "DB_USER": "testuser",
        "DB_PASSWORD": "testpass",
        "DB_HOST": "db.example.com",
        "DB_PORT": "5433",
        "DB_NAME": "testdb",
    })
    def test_env_overrides(self):
        from utils.utils import build_db_url
        url = build_db_url()
        url_str = str(url)
        assert "testuser" in url_str
        assert "db.example.com" in url_str
        assert "5433" in url_str
        assert "testdb" in url_str

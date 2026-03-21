"""Unit tests for ingestion modules — mocked API and DB calls."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime


# ── ingest_boxscores ──

class TestFetchGameTable:
    @patch("ingestion.ingest_boxscores.session")
    def test_parses_schedule(self, mock_session, sample_schedule_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_schedule_response
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_boxscores import _fetch_game_table
        game_pks, df = _fetch_game_table("2025-06-15", "2025-06-15")

        assert len(game_pks) == 2
        assert 745123 in game_pks
        assert 745124 in game_pks
        assert len(df) == 2
        assert "game_pk" in df.columns
        assert "home_team_id" in df.columns
        assert "away_team_id" in df.columns

    @patch("ingestion.ingest_boxscores.session")
    def test_empty_schedule(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dates": []}
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_boxscores import _fetch_game_table
        game_pks, df = _fetch_game_table("2025-12-25", "2025-12-25")
        assert game_pks == []
        assert df.empty


class TestFetchBoxscores:
    @patch("ingestion.ingest_boxscores.insert_raw_payload")
    @patch("ingestion.ingest_boxscores.session")
    def test_extracts_pitching_and_batting(self, mock_session, mock_insert,
                                           sample_boxscore_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_boxscore_response
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_boxscores import fetch_boxscores
        pitching, batting = fetch_boxscores([745123])

        assert len(pitching) >= 1
        assert len(batting) >= 1

        pitcher_row = pitching[0]
        assert pitcher_row["pitcher_id"] == 477132
        assert pitcher_row["pitcher_name"] == "Clayton Kershaw"
        assert pitcher_row["game_pk"] == 745123
        assert pitcher_row["source"] == "MLB_stats_api"

        batter_row = batting[0]
        assert batter_row["batter_id"] == 660271
        assert batter_row["batter_name"] == "Shohei Ohtani"

    @patch("ingestion.ingest_boxscores.insert_raw_payload")
    @patch("ingestion.ingest_boxscores.session")
    def test_skips_failed_game(self, mock_session, mock_insert):
        mock_session.get.side_effect = Exception("API down")
        from ingestion.ingest_boxscores import fetch_boxscores
        pitching, batting = fetch_boxscores([999999])
        assert pitching == []
        assert batting == []


# ── ingest_transactions ──

class TestFetchTransactions:
    @patch("ingestion.ingest_transactions.session")
    def test_single_chunk(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "transactions": [
                {
                    "id": 12345,
                    "person": {"id": 660271, "fullName": "Shohei Ohtani"},
                    "toTeam": {"id": 119, "name": "Los Angeles Dodgers"},
                    "fromTeam": {},
                    "date": "2025-06-15",
                    "effectiveDate": "2025-06-15",
                    "resolutionDate": None,
                    "typeCode": "ASG",
                    "typeDesc": "Assignment",
                    "description": "Test transaction",
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_transactions import _fetch_transactions
        rows = _fetch_transactions("2025-06-15", "2025-06-20")

        assert len(rows) == 1
        assert rows[0]["transaction_id"] == 12345
        assert rows[0]["player_name"] == "Shohei Ohtani"
        assert rows[0]["to_team_id"] == 119
        assert rows[0]["from_team_id"] is None

    @patch("ingestion.ingest_transactions.session")
    def test_date_chunking(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transactions": []}
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_transactions import _fetch_transactions
        # 60-day range: Jan1-Jan30 (30d), Jan31-Feb28 (29d), Mar1 (1d) = 3 chunks
        rows = _fetch_transactions("2025-01-01", "2025-03-01")
        assert mock_session.get.call_count >= 2  # at least 2 chunks for 60 days

    @patch("ingestion.ingest_transactions.session")
    def test_empty_response(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"transactions": []}
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_transactions import _fetch_transactions
        rows = _fetch_transactions("2025-12-25", "2025-12-25")
        assert rows == []


# ── ingest_milb ──

class TestMilbSchedule:
    @patch("ingestion.ingest_milb.session")
    def test_filters_final_games_only(self, mock_session, sample_milb_schedule_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_milb_schedule_response
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_milb import _fetch_milb_schedule, SPORT_IDS
        games = _fetch_milb_schedule("2025-06-15", "2025-06-15")

        # Only 1 Final game per sport_id * number of sport_ids
        assert all(g["game_pk"] == 900001 for g in games)
        assert len(games) == len(SPORT_IDS)  # one Final game per level

    @patch("ingestion.ingest_milb.session")
    def test_empty_schedule(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dates": []}
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_milb import _fetch_milb_schedule
        games = _fetch_milb_schedule("2020-06-15", "2020-06-15")
        assert games == []


class TestMilbCleanNumeric:
    def test_handles_dashes(self):
        from ingestion.ingest_milb import _clean_numeric_columns
        df = pd.DataFrame({
            "batter_id": [1, 2],
            "batter_name": ["A", "B"],
            "sb_pct": [".500", ".---"],
            "hits": [3, 5],
        })
        result = _clean_numeric_columns(df)
        assert pd.isna(result.loc[1, "sb_pct"])
        assert result.loc[0, "sb_pct"] == 0.5
        assert result.loc[1, "hits"] == 5

    def test_preserves_excluded_columns(self):
        from ingestion.ingest_milb import _clean_numeric_columns
        df = pd.DataFrame({
            "batter_id": [1],
            "batter_name": ["Test"],
            "game_pk": [12345],
            "level": ["AAA"],
        })
        result = _clean_numeric_columns(df)
        assert result.loc[0, "batter_name"] == "Test"
        assert result.loc[0, "level"] == "AAA"


class TestBuildTeamOrgMap:
    @patch("ingestion.ingest_milb.session")
    def test_maps_teams_to_parent_orgs(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "teams": [
                {
                    "id": 5001,
                    "parentOrgId": 119,
                    "parentOrgName": "Los Angeles Dodgers",
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_milb import _build_team_org_map
        team_map = _build_team_org_map(2025)

        assert 5001 in team_map
        assert team_map[5001]["parent_org_id"] == 119
        assert team_map[5001]["parent_org_name"] == "Los Angeles Dodgers"

    @patch("ingestion.ingest_milb.session")
    def test_fallback_parent_org_fields(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "teams": [
                {
                    "id": 5002,
                    "parentOrg": {"id": 137, "name": "San Francisco Giants"},
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_milb import _build_team_org_map
        team_map = _build_team_org_map(2025)

        assert team_map[5002]["parent_org_id"] == 137


# ── ingest_prospects ──

class TestFetchRoster:
    @patch("ingestion.ingest_prospects.session")
    def test_parses_roster(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "roster": [
                {
                    "person": {
                        "id": 700001,
                        "fullName": "Test Player",
                        "firstName": "Test",
                        "lastName": "Player",
                        "batSide": {"code": "R"},
                        "pitchHand": {"code": "R"},
                        "birthDate": "2000-01-01",
                        "currentAge": 25,
                        "height": "6' 0\"",
                        "weight": 200,
                        "mlbDebutDate": None,
                        "draftYear": 2021,
                    },
                    "position": {"abbreviation": "SS"},
                    "status": {"code": "A", "description": "Active"},
                    "jerseyNumber": "7",
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        from ingestion.ingest_prospects import _fetch_roster
        team = {
            "team_id": 5001,
            "team_name": "Test Team",
            "parent_org_id": 119,
            "parent_org_name": "Dodgers",
            "sport_id": 11,
            "level": "AAA",
        }
        rows = _fetch_roster(team, 2025)

        assert len(rows) == 1
        assert rows[0]["player_id"] == 700001
        assert rows[0]["full_name"] == "Test Player"
        assert rows[0]["bat_side"] == "R"
        assert rows[0]["level"] == "AAA"
        assert rows[0]["season"] == 2025

    @patch("ingestion.ingest_prospects.session")
    def test_handles_api_failure(self, mock_session):
        mock_session.get.side_effect = Exception("timeout")
        from ingestion.ingest_prospects import _fetch_roster
        team = {
            "team_id": 5001, "team_name": "Test",
            "parent_org_id": 119, "parent_org_name": "Dodgers",
            "sport_id": 11, "level": "AAA",
        }
        rows = _fetch_roster(team, 2025)
        assert rows == []


# ── hydrate_prospects ──

class TestFetchPeopleBulk:
    @patch("ingestion.hydrate_prospects.session")
    def test_parses_people_response(self, mock_session, sample_people_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = sample_people_response
        mock_session.get.return_value = mock_resp

        from ingestion.hydrate_prospects import _fetch_people_bulk
        result = _fetch_people_bulk([660271, 477132])

        assert len(result) == 2
        assert result[660271]["first_name"] == "Shohei"
        assert result[660271]["bat_side"] == "L"
        assert result[660271]["birth_date"] == "1994-07-05"
        assert result[477132]["draft_year"] == 2006

    @patch("ingestion.hydrate_prospects.session")
    def test_builds_correct_url(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"people": []}
        mock_session.get.return_value = mock_resp

        from ingestion.hydrate_prospects import _fetch_people_bulk
        _fetch_people_bulk([100, 200, 300])

        call_url = mock_session.get.call_args[0][0]
        assert "personIds=100,200,300" in call_url

    @patch("ingestion.hydrate_prospects.session")
    def test_handles_missing_fields(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "people": [
                {
                    "id": 999,
                    "firstName": "Unknown",
                    # Missing most fields
                }
            ]
        }
        mock_session.get.return_value = mock_resp

        from ingestion.hydrate_prospects import _fetch_people_bulk
        result = _fetch_people_bulk([999])
        assert result[999]["first_name"] == "Unknown"
        assert result[999]["bat_side"] is None
        assert result[999]["birth_date"] is None

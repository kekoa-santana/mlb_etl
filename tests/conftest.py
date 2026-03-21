import os
import sys
import pytest
from unittest.mock import MagicMock

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine for unit tests."""
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock()
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.fixture
def sample_schedule_response():
    """Sample MLB Stats API schedule response."""
    return {
        "dates": [
            {
                "date": "2025-06-15",
                "games": [
                    {
                        "gamePk": 745123,
                        "gameType": "R",
                        "season": "2025",
                        "status": {"abstractGameState": "Final"},
                        "teams": {
                            "away": {
                                "team": {"id": 119, "name": "Los Angeles Dodgers"},
                                "leagueRecord": {"wins": 45, "losses": 25},
                            },
                            "home": {
                                "team": {"id": 137, "name": "San Francisco Giants"},
                                "leagueRecord": {"wins": 35, "losses": 35},
                            },
                        },
                        "venue": {"id": 2395},
                        "doubleheader": "N",
                        "dayNight": "night",
                        "gamesInSeries": 3,
                        "seriesGameNumber": 1,
                    },
                    {
                        "gamePk": 745124,
                        "gameType": "R",
                        "season": "2025",
                        "status": {"abstractGameState": "Preview"},
                        "teams": {
                            "away": {
                                "team": {"id": 119, "name": "Los Angeles Dodgers"},
                                "leagueRecord": {"wins": 45, "losses": 25},
                            },
                            "home": {
                                "team": {"id": 137, "name": "San Francisco Giants"},
                                "leagueRecord": {"wins": 35, "losses": 35},
                            },
                        },
                        "venue": {"id": 2395},
                        "doubleheader": "N",
                        "dayNight": "night",
                        "gamesInSeries": 3,
                        "seriesGameNumber": 2,
                    },
                ],
            }
        ]
    }


@pytest.fixture
def sample_boxscore_response():
    """Sample MLB Stats API boxscore response."""
    return {
        "teams": {
            "away": {
                "team": {"id": 119, "name": "Los Angeles Dodgers"},
                "players": {
                    "ID660271": {
                        "person": {"id": 660271, "fullName": "Shohei Ohtani"},
                        "position": {"abbreviation": "DH"},
                        "stats": {
                            "batting": {
                                "runs": 1, "hits": 2, "homeRuns": 1,
                                "strikeOuts": 1, "baseOnBalls": 0,
                                "atBats": 4, "plateAppearances": 4,
                                "totalBases": 5, "rbi": 2,
                                "doubles": 0, "triples": 0,
                                "stolenBases": 0, "caughtStealing": 0,
                                "stolenBasePercentage": ".---",
                                "groundOuts": 1, "airOuts": 0,
                                "intentionalWalks": 0, "hitByPitch": 0,
                            },
                            "pitching": {},
                            "fielding": {"errors": 0},
                        },
                    },
                    "ID477132": {
                        "person": {"id": 477132, "fullName": "Clayton Kershaw"},
                        "position": {"abbreviation": "P"},
                        "stats": {
                            "batting": {},
                            "pitching": {
                                "gamesStarted": 1, "inningsPitched": "6.0",
                                "hits": 5, "runs": 2, "earnedRuns": 2,
                                "baseOnBalls": 1, "strikeOuts": 8,
                                "homeRuns": 1, "numberOfPitches": 95,
                                "wins": 1, "losses": 0,
                                "saves": 0, "saveOpportunities": 0,
                                "holds": 0, "blownSaves": 0,
                                "battersFaced": 24, "outs": 18,
                                "completeGames": 0, "shutouts": 0,
                                "balls": 35, "strikes": 60,
                                "strikePercentage": ".630",
                                "flyOuts": 5, "groundOuts": 5,
                                "airOuts": 7, "doubles": 1,
                                "triples": 0, "atBats": 23,
                                "caughtStealing": 0, "stolenBases": 0,
                                "stolenBasePercentage": ".---",
                                "hitByPitch": 0, "hitBatsmen": 0,
                                "balks": 0, "wildPitches": 0,
                                "pickoffs": 0, "rbi": 0,
                                "gamesFinished": 0,
                                "inheritedRunners": 0,
                                "inheritedRunnersScored": 0,
                                "catchersInterference": 0,
                                "sacBunts": 0, "sacFlies": 0,
                                "passedBall": 0, "intentionalWalks": 0,
                            },
                            "fielding": {},
                        },
                    },
                },
            },
            "home": {
                "team": {"id": 137, "name": "San Francisco Giants"},
                "players": {},
            },
        }
    }


@pytest.fixture
def sample_milb_schedule_response():
    """Sample MiLB schedule response for one sport level."""
    return {
        "dates": [
            {
                "date": "2025-06-15",
                "games": [
                    {
                        "gamePk": 900001,
                        "season": "2025",
                        "status": {"abstractGameState": "Final"},
                        "teams": {
                            "away": {"team": {"id": 5001}},
                            "home": {"team": {"id": 5002}},
                        },
                    },
                    {
                        "gamePk": 900002,
                        "season": "2025",
                        "status": {"abstractGameState": "Live"},
                        "teams": {
                            "away": {"team": {"id": 5003}},
                            "home": {"team": {"id": 5004}},
                        },
                    },
                ],
            }
        ]
    }


@pytest.fixture
def sample_people_response():
    """Sample MLB Stats API /people bulk response."""
    return {
        "people": [
            {
                "id": 660271,
                "firstName": "Shohei",
                "lastName": "Ohtani",
                "fullName": "Shohei Ohtani",
                "batSide": {"code": "L"},
                "pitchHand": {"code": "R"},
                "birthDate": "1994-07-05",
                "currentAge": 31,
                "height": "6' 4\"",
                "weight": 210,
                "mlbDebutDate": "2018-03-29",
                "draftYear": None,
            },
            {
                "id": 477132,
                "firstName": "Clayton",
                "lastName": "Kershaw",
                "fullName": "Clayton Kershaw",
                "batSide": {"code": "L"},
                "pitchHand": {"code": "L"},
                "birthDate": "1988-03-19",
                "currentAge": 38,
                "height": "6' 4\"",
                "weight": 225,
                "mlbDebutDate": "2008-05-25",
                "draftYear": 2006,
            },
        ]
    }

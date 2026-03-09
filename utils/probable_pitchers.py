"""
Fetch probable pitchers for a given date from the MLB Stats API.

Returns ephemeral data — not persisted to the database.

Usage:
    from utils.probable_pitchers import fetch_probable_pitchers
    matchups = fetch_probable_pitchers("2026-04-15")
    for m in matchups:
        print(f"{m['away_pitcher']} vs {m['home_pitcher']} at {m['home_team']}")
"""

from __future__ import annotations

from datetime import date

from utils.retry import build_retry_session

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

_session = None


def _get_session():
    global _session
    if _session is None:
        _session = build_retry_session(timeout=15)
    return _session


def fetch_probable_pitchers(game_date: str | None = None) -> list[dict]:
    """Fetch probable pitchers for a date.

    Parameters
    ----------
    game_date : str, optional
        Date in YYYY-MM-DD format. Defaults to today.

    Returns
    -------
    list[dict]
        One entry per game with keys:
            game_pk, game_date, game_time, status,
            away_team, away_pitcher, away_pitcher_id,
            home_team, home_pitcher, home_pitcher_id
        Pitcher fields are None when not yet announced.
    """
    if game_date is None:
        game_date = str(date.today())

    session = _get_session()
    resp = session.get(
        MLB_SCHEDULE_URL,
        params={"sportId": 1, "date": game_date, "hydrate": "probablePitcher(note)"},
        timeout=session.timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    dates = data.get("dates", [])
    if not dates:
        return []

    matchups = []
    for game in dates[0].get("games", []):
        teams = game.get("teams", {})
        away = teams.get("away", {})
        home = teams.get("home", {})
        away_pp = away.get("probablePitcher") or {}
        home_pp = home.get("probablePitcher") or {}

        matchups.append({
            "game_pk": game["gamePk"],
            "game_date": game_date,
            "game_time": game.get("gameDate", ""),
            "status": game.get("status", {}).get("abstractGameState", ""),
            "away_team": away.get("team", {}).get("name", ""),
            "away_pitcher": away_pp.get("fullName"),
            "away_pitcher_id": away_pp.get("id"),
            "home_team": home.get("team", {}).get("name", ""),
            "home_pitcher": home_pp.get("fullName"),
            "home_pitcher_id": home_pp.get("id"),
        })

    return matchups


def print_matchups(game_date: str | None = None) -> None:
    """Print today's probable pitcher matchups to stdout."""
    matchups = fetch_probable_pitchers(game_date)
    if not matchups:
        print(f"No games scheduled for {game_date or date.today()}")
        return

    dt = game_date or str(date.today())
    announced = sum(1 for m in matchups if m["away_pitcher"] and m["home_pitcher"])
    print(f"\nProbable Pitchers — {dt}  ({len(matchups)} games, {announced} fully announced)\n")

    for m in matchups:
        away_p = m["away_pitcher"] or "TBD"
        home_p = m["home_pitcher"] or "TBD"
        print(f"  {m['away_team']:24s}  {away_p:24s}  @  {home_p:24s}  {m['home_team']}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch MLB probable pitchers")
    parser.add_argument("--date", default=None, help="Date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    print_matchups(args.date)

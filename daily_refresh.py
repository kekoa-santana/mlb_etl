"""
Daily MLB data refresh orchestrator.

Runs the complete pipeline across three projects:
  1. mlb_fantasy_ETL  — Ingest, clean, and load data into PostgreSQL
  2. player_profiles  — Update Bayesian projection artifacts (selective groups)
  3. tdd-dashboard    — Conjugate-update projections, fetch schedule, run sims,
                        export roster, and generate dashboard artifacts

Transactions are covered: full_pipeline.py automatically backfills from
Oct 1 of the prior season through end_date, so all offseason and in-season
transactions, IL moves, trades, signings, etc. are captured every run.

Usage
-----
    # Standard daily refresh (yesterday's data)
    python daily_refresh.py

    # Specific date
    python daily_refresh.py --date 2026-04-15

    # Skip phases
    python daily_refresh.py --skip-etl              # DB already up to date
    python daily_refresh.py --skip-precompute        # skip artifact rebuild
    python daily_refresh.py --skip-dashboard         # skip conjugate update + sims

    # Full precompute refit (preseason or weekly)
    python daily_refresh.py --full-precompute
    python daily_refresh.py --full-precompute --quick

    # Custom precompute groups
    python daily_refresh.py --precompute-groups team,rankings,traditional
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("daily_refresh")

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

DATA_ANALYTICS = Path(__file__).resolve().parent.parent
ETL_DIR = DATA_ANALYTICS / "mlb_fantasy_ETL"
PROFILES_DIR = DATA_ANALYTICS / "player_profiles"
DASHBOARD_DIR = DATA_ANALYTICS / "tdd-dashboard"

PYTHON = {
    "etl": ETL_DIR / "myenv" / "Scripts" / "python.exe",
    "profiles": PROFILES_DIR / "myenv" / "Scripts" / "python.exe",
    "dashboard": DASHBOARD_DIR / "dash-env" / "Scripts" / "python.exe",
}

# Precompute groups that change daily and are fast to recompute.
# Excludes 'models' (heavy MCMC refit) and 'snapshots' (preseason only).
DAILY_PRECOMPUTE_GROUPS = "team,rankings,game_data,traditional,profiles,picks"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    """Format seconds as 'Xm Ys'."""
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s" if m else f"{s}s"


def _run(python: Path, script: Path, args: list[str], cwd: Path, label: str) -> bool:
    """Run a script in its project's virtualenv. Returns True on success."""
    if not python.exists():
        logger.error("%s: virtualenv not found at %s", label, python)
        return False
    if not script.exists():
        logger.error("%s: script not found at %s", label, script)
        return False

    cmd = [str(python), str(script)] + args
    logger.info("%s: %s", label, " ".join(cmd))

    t0 = time.time()
    result = subprocess.run(cmd, cwd=str(cwd))
    elapsed = time.time() - t0

    if result.returncode != 0:
        logger.error("%s: FAILED (exit %d) after %s", label, result.returncode, _fmt_duration(elapsed))
        return False

    logger.info("%s: completed in %s", label, _fmt_duration(elapsed))
    return True


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def run_etl(game_date: str) -> bool:
    """Step 1: Run full_pipeline.py for a single date.

    Ingests: boxscores, transactions (from Oct 1 prior year), rosters,
    MiLB, prospects, OAA, statcast.
    Transforms: raw -> staging -> production (all SQL + roster rebuild).
    """
    return _run(
        PYTHON["etl"],
        ETL_DIR / "full_pipeline.py",
        ["--start-date", game_date, "--end-date", game_date],
        ETL_DIR,
        "ETL",
    )


def run_precompute(groups: str, quick: bool = False) -> bool:
    """Step 2: Run precompute_dashboard_data.py for selected groups.

    Rebuilds projection artifacts that evolve with new game data:
    team ELO, player rankings, traditional stats, Glicko ratings, etc.

    The heavy MCMC model refit is NOT included by default — use
    --full-precompute for that (preseason or weekly).
    """
    args = ["--include", groups]
    if quick:
        args.append("--quick")
    return _run(
        PYTHON["profiles"],
        PROFILES_DIR / "scripts" / "precompute_dashboard_data.py",
        args,
        PROFILES_DIR,
        "Precompute",
    )


def run_full_precompute(quick: bool = False) -> bool:
    """Step 2 (full): Refit all Bayesian models + rebuild all artifacts.

    This is the heavy run (5 min --quick, 20+ min full).
    Use for preseason or weekly refresh.
    """
    args = []
    if quick:
        args.append("--quick")
    return _run(
        PYTHON["profiles"],
        PROFILES_DIR / "scripts" / "precompute_dashboard_data.py",
        args,
        PROFILES_DIR,
        "Precompute (full)",
    )


def run_schedule_lookahead(game_date: str, days: int = 7) -> bool:
    """Step 1b: Fetch and store upcoming schedule with probable pitchers.

    Stores the next N days of schedule in raw.schedule_lookahead so
    the dashboard has a fallback if the hourly API fetch fails.
    """
    return _run(
        PYTHON["etl"],
        ETL_DIR / "ingestion" / "ingest_schedule_lookahead.py",
        ["--start-date", game_date, "--days", str(days)],
        ETL_DIR,
        "Schedule lookahead",
    )


def run_collect_game_odds(game_date: str) -> bool:
    """Step 2b: Fetch DK + Bovada game-level odds into game_odds_daily.parquet.

    Powers vegas_line / model_edge / Kelly sizing downstream. Non-fatal
    on failure — the projection layer degrades to model-only picks.
    """
    return _run(
        PYTHON["dashboard"],
        DASHBOARD_DIR / "scripts" / "collect_game_odds.py",
        ["--date", game_date],
        DASHBOARD_DIR,
        "Collect game odds",
    )


def run_dashboard_update(game_date: str) -> bool:
    """Step 3: Run tdd-dashboard's update_in_season.py.

    Delegates to player_profiles for conjugate updating (Beta-Binomial
    on preseason projections with observed 2026 data), fetches today's
    schedule and lineups, runs game simulations, then handles dashboard
    bookkeeping: roster export, weekly snapshots, metadata, manifest.
    """
    return _run(
        PYTHON["dashboard"],
        DASHBOARD_DIR / "scripts" / "update_in_season.py",
        ["--date", game_date],
        DASHBOARD_DIR,
        "Dashboard",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Daily MLB data refresh across ETL, projections, and dashboard",
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Game date (YYYY-MM-DD). Default: yesterday.",
    )
    parser.add_argument("--skip-etl", action="store_true",
                        help="Skip the ETL pipeline (DB already fresh).")
    parser.add_argument("--skip-precompute", action="store_true",
                        help="Skip precompute artifact rebuild.")
    parser.add_argument("--skip-dashboard", action="store_true",
                        help="Skip dashboard update (conjugate + sims + bookkeeping).")
    parser.add_argument(
        "--precompute-groups", type=str, default=DAILY_PRECOMPUTE_GROUPS,
        help=f"Comma-separated precompute groups (default: {DAILY_PRECOMPUTE_GROUPS}). "
             "Run with --list-groups to see options.",
    )
    parser.add_argument("--full-precompute", action="store_true",
                        help="Run full precompute (all groups incl. MCMC refit).")
    parser.add_argument("--quick", action="store_true",
                        help="Use fewer MCMC draws (only relevant with --full-precompute).")
    parser.add_argument("--lookahead-days", type=int, default=7,
                        help="Days of schedule to fetch ahead (default: 7).")
    parser.add_argument("--list-groups", action="store_true",
                        help="Print available precompute groups and exit.")

    args = parser.parse_args()

    if args.list_groups:
        _run(
            PYTHON["profiles"],
            PROFILES_DIR / "scripts" / "precompute_dashboard_data.py",
            ["--list-groups"],
            PROFILES_DIR,
            "Precompute",
        )
        return

    game_date = args.date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("=" * 60)
    logger.info("Daily refresh for %s", game_date)
    logger.info("=" * 60)

    t_total = time.time()
    failed = []
    timings: list[tuple[str, float]] = []

    # --- Step 1: ETL ---
    if args.skip_etl:
        logger.info("Step 1: ETL — skipped")
    else:
        logger.info("Step 1: ETL pipeline")
        t0 = time.time()
        if not run_etl(game_date):
            logger.error("ETL failed — aborting (downstream depends on fresh DB data)")
            sys.exit(1)
        timings.append(("ETL", time.time() - t0))

    # --- Step 1b: Schedule lookahead ---
    logger.info("Step 1b: Schedule lookahead (next %d days)", args.lookahead_days)
    t0 = time.time()
    if not run_schedule_lookahead(game_date, args.lookahead_days):
        failed.append("Schedule lookahead")
        logger.warning("Schedule lookahead failed — non-fatal, continuing")
    timings.append(("Schedule lookahead", time.time() - t0))

    # --- Step 2: Precompute ---
    if args.skip_precompute:
        logger.info("Step 2: Precompute — skipped")
    elif args.full_precompute:
        logger.info("Step 2: Precompute (full refit)")
        t0 = time.time()
        if not run_full_precompute(quick=args.quick):
            failed.append("Precompute")
            logger.warning("Precompute failed — continuing with existing artifacts")
        timings.append(("Precompute (full)", time.time() - t0))
    else:
        logger.info("Step 2: Precompute (daily groups: %s)", args.precompute_groups)
        t0 = time.time()
        if not run_precompute(args.precompute_groups, quick=args.quick):
            failed.append("Precompute")
            logger.warning("Precompute failed — continuing with existing artifacts")
        timings.append(("Precompute", time.time() - t0))

    # --- Step 2b: Collect game-level odds (DK + Bovada) ---
    logger.info("Step 2b: Collecting game-level odds (DK + Bovada)")
    t0 = time.time()
    if not run_collect_game_odds(game_date):
        failed.append("Collect game odds")
        logger.warning("Odds collection failed - non-fatal, continuing")
    timings.append(("Collect game odds", time.time() - t0))

    # --- Step 3: Dashboard update ---
    if args.skip_dashboard:
        logger.info("Step 3: Dashboard — skipped")
    else:
        logger.info("Step 3: Dashboard update (conjugate + schedule + sims)")
        t0 = time.time()
        if not run_dashboard_update(game_date):
            failed.append("Dashboard")
        timings.append(("Dashboard", time.time() - t0))

    # --- Summary ---
    total_elapsed = time.time() - t_total
    logger.info("=" * 60)
    logger.info("TIMING SUMMARY")
    for label, elapsed in timings:
        logger.info("  %-25s %s", label, _fmt_duration(elapsed))
    logger.info("  %-25s %s", "TOTAL", _fmt_duration(total_elapsed))
    logger.info("=" * 60)

    if failed:
        logger.warning("Completed with failures: %s", ", ".join(failed))
        sys.exit(1)
    else:
        logger.info("All steps completed successfully")


if __name__ == "__main__":
    main()

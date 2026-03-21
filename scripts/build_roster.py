#!/usr/bin/env python
"""
One-time build of production.dim_roster from historical data.

Creates the table and populates it from the prior season's game data
plus post-season transactions. After initial build, the daily pipeline
(full_pipeline.py) handles ongoing rebuilds automatically.

Usage:
    python scripts/build_roster.py                  # build from 2025
    python scripts/build_roster.py --season 2024    # build from specific season
    python scripts/build_roster.py --rebuild        # drop & recreate first
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.utils import build_db_url  # noqa: E402
from transformation.production.load_roster import DDL, build_roster  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PRIOR_SEASON = 2025


def main():
    parser = argparse.ArgumentParser(description="Build dim_roster table")
    parser.add_argument(
        "--season", type=int, default=PRIOR_SEASON,
        help=f"Season to build from (default: {PRIOR_SEASON})",
    )
    parser.add_argument(
        "--rebuild", action="store_true",
        help="Drop and recreate the table before building",
    )
    args = parser.parse_args()

    engine = create_engine(build_db_url())

    if args.rebuild:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS production.dim_roster"))
            logger.info("Dropped existing dim_roster table")

    build_roster(season=args.season, engine=engine)


if __name__ == "__main__":
    main()

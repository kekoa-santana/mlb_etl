"""Ingest prospect rankings from FanGraphs "The Board" CSV exports.

Expects files in data/prospect_rankings/ named:
    <season>_report.csv   - preseason rankings
    <season>_updated.csv  - mid-season update

Uses a two-pass ID resolution strategy:
  1. Chadwick Bureau register (FanGraphs ID -> MLB AM ID)
  2. Name match against production.dim_prospects

Upserts into production.dim_prospect_ranking.
"""
import csv
import io
import logging
import urllib.request
from collections import defaultdict
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.utils import build_db_url

logger = logging.getLogger(__name__)
engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

CROSSWALK_PATH = Path('data/fg_mlbam_crosswalk.csv')
CHADWICK_BASE = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data'


def _build_crosswalk() -> dict[str, int]:
    """Load or download the FanGraphs -> MLB AM ID crosswalk."""
    if CROSSWALK_PATH.exists():
        crosswalk = {}
        with open(CROSSWALK_PATH) as f:
            for row in csv.DictReader(f):
                crosswalk[row['fg_id']] = int(row['mlbam_id'])
        logger.info(f"Loaded crosswalk from cache: {len(crosswalk)} entries")
        return crosswalk

    logger.info("Downloading Chadwick register for ID crosswalk...")
    crosswalk = {}
    suffixes = [str(i) for i in range(10)] + [chr(c) for c in range(ord('a'), ord('z') + 1)]
    for s in suffixes:
        url = f'{CHADWICK_BASE}/people-{s}.csv'
        try:
            resp = urllib.request.urlopen(url, timeout=30)
            content = resp.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                fg = row.get('key_fangraphs', '').strip()
                mlbam = row.get('key_mlbam', '').strip()
                if fg and mlbam:
                    crosswalk[fg] = int(mlbam)
        except Exception:
            pass

    # Cache for future runs
    CROSSWALK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CROSSWALK_PATH, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['fg_id', 'mlbam_id'])
        for fg, mlbam in crosswalk.items():
            w.writerow([fg, mlbam])

    logger.info(f"Built crosswalk: {len(crosswalk)} entries")
    return crosswalk


def _build_name_lookup() -> dict[str, list[tuple[int, str]]]:
    """Build name -> [(player_id, org_name)] lookup from dim_prospects."""
    with engine.connect() as conn:
        df = pd.read_sql(text(
            'SELECT DISTINCT player_id, full_name, parent_org_name '
            'FROM production.dim_prospects'
        ), conn)

    lookup = defaultdict(list)
    for _, row in df.iterrows():
        if row['full_name']:
            key = row['full_name'].strip().lower()
            lookup[key].append((
                int(row['player_id']),
                (row.get('parent_org_name') or '').strip(),
            ))
    logger.info(f"Name lookup built: {len(lookup)} unique names")
    return lookup


# MLB team abbreviation -> full name fragments for org matching
ORG_ALIASES = {
    'AZ': 'arizona', 'ARI': 'arizona', 'ATL': 'atlanta', 'BAL': 'baltimore',
    'BOS': 'boston', 'CHC': 'cubs', 'CHW': 'white sox', 'CWS': 'white sox',
    'CIN': 'cincinnati', 'CLE': 'cleveland', 'COL': 'colorado',
    'DET': 'detroit', 'HOU': 'houston', 'KC': 'kansas city', 'KCR': 'kansas city',
    'LAA': 'angels', 'LAD': 'dodgers', 'MIA': 'miami', 'MIL': 'milwaukee',
    'MIN': 'minnesota', 'NYM': 'mets', 'NYY': 'yankees',
    'OAK': 'oakland', 'ATH': 'athletics', 'PHI': 'philadelphia',
    'PIT': 'pittsburgh', 'SD': 'san diego', 'SDP': 'san diego',
    'SF': 'san francisco', 'SFG': 'san francisco',
    'SEA': 'seattle', 'STL': 'st. louis', 'TB': 'tampa bay', 'TBR': 'tampa bay',
    'TEX': 'texas', 'TOR': 'toronto', 'WSN': 'washington', 'WAS': 'washington',
}


def _resolve_player_id(
    fg_id: str,
    name: str,
    org: str,
    crosswalk: dict[str, int],
    name_lookup: dict[str, list[tuple[int, str]]],
) -> int | None:
    """Resolve a FanGraphs player to an MLB AM player_id."""
    # 1. Crosswalk (exact FG ID match)
    if fg_id in crosswalk:
        return crosswalk[fg_id]

    # 2. Name match
    name_key = name.strip().lower()
    candidates = name_lookup.get(name_key, [])
    if len(candidates) == 1:
        return candidates[0][0]
    if len(candidates) > 1:
        # Disambiguate by org
        org_hint = ORG_ALIASES.get(org.strip().upper(), org.strip().lower())
        for pid, org_name in candidates:
            if org_hint and org_hint in org_name.lower():
                return pid
        # Fall back to first match if no org match
        return candidates[0][0]

    return None


def _read_csv(csv_path: str, season: int, source: str,
              crosswalk: dict, name_lookup: dict) -> pd.DataFrame:
    """Read a FanGraphs CSV and resolve player IDs."""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')
    logger.info(f"Read {len(df)} rows from {csv_path}")

    rows = []
    unmatched = 0
    for _, row in df.iterrows():
        fg_id = str(row.get('PlayerId', '')).strip()
        name = str(row.get('Name', '')).strip().strip('"')
        org = str(row.get('Org', '')).strip().strip('"')

        player_id = _resolve_player_id(fg_id, name, org, crosswalk, name_lookup)
        if player_id is None:
            unmatched += 1
            continue

        # Parse rankings
        overall = pd.to_numeric(row.get('Top 100'), errors='coerce')
        org_rank = pd.to_numeric(row.get('Org Rk'), errors='coerce')
        fv = pd.to_numeric(row.get('FV'), errors='coerce')
        eta = pd.to_numeric(row.get('ETA'), errors='coerce')
        risk = str(row.get('Risk', '')).strip() if pd.notna(row.get('Risk')) else None
        pos = str(row.get('Pos', '')).strip() if pd.notna(row.get('Pos')) else None
        level = str(row.get('Current Level', '')).strip() if pd.notna(row.get('Current Level')) else None

        rec = {
            'player_id': player_id,
            'season': season,
            'source': source,
            'fg_id': fg_id,
            'player_name': name,
            'org': org if org else None,
            'position': pos,
            'current_level': level,
            'overall_rank': int(overall) if pd.notna(overall) else None,
            'org_rank': int(org_rank) if pd.notna(org_rank) else None,
            'future_value': int(fv) if pd.notna(fv) else None,
            'risk': risk if risk else None,
            'eta': int(eta) if pd.notna(eta) else None,
        }
        rows.append(rec)

    if unmatched:
        logger.warning(f"Could not resolve {unmatched} players in {csv_path}")

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.drop_duplicates(subset=['player_id', 'season', 'source'], keep='first')
    logger.info(f"Prepared {len(result)} rows for {season} {source} ({unmatched} unmatched)")
    return result


def _load_rankings(df: pd.DataFrame):
    """Upsert prospect rankings to production.dim_prospect_ranking."""
    if df.empty:
        return

    with engine.begin() as conn:
        table = Table(
            'dim_prospect_ranking', MetaData(),
            autoload_with=conn, schema='production'
        )
        pk_cols = {'player_id', 'season', 'source'}
        for chunk_start in range(0, len(df), 50):
            chunk = df.iloc[chunk_start:chunk_start + 50]
            records = chunk.to_dict(orient='records')
            for record in records:
                for k, v in record.items():
                    if pd.isna(v):
                        record[k] = None
            stmt = pg_insert(table).values(records)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.columns
                if c.name not in pk_cols
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=list(pk_cols),
                set_=update_cols,
            )
            conn.execute(stmt)

    logger.info(f"Loaded {len(df)} rows to production.dim_prospect_ranking")


def fetch_and_load_prospect_rankings(
    data_dir: str = 'data/prospect_rankings',
    seasons: list[int] | None = None,
):
    """Main entry point: load FanGraphs prospect ranking CSVs.

    Expects files named <season>_report.csv and <season>_updated.csv.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        logger.warning(f"Prospect rankings directory not found: {data_dir}")
        return

    csv_files = sorted(data_path.glob('*.csv'))
    if not csv_files:
        logger.warning(f"No CSV files found in {data_dir}")
        return

    # Build ID resolution lookups
    crosswalk = _build_crosswalk()
    name_lookup = _build_name_lookup()

    all_dfs = []
    for csv_file in csv_files:
        # Parse filename: "2024_report.csv" -> season=2024, source="fg_report"
        parts = csv_file.stem.split('_', 1)
        if len(parts) != 2:
            logger.warning(f"Skipping {csv_file.name} - expected <year>_<report|updated>.csv")
            continue

        try:
            season = int(parts[0])
        except ValueError:
            logger.warning(f"Skipping {csv_file.name} - could not parse year")
            continue

        if seasons and season not in seasons:
            continue

        source_type = parts[1].lower()
        source = f'fg_{source_type}'  # "fg_report" or "fg_updated"

        df = _read_csv(str(csv_file), season, source, crosswalk, name_lookup)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        _load_rankings(combined)
        logger.info(
            f"Finished: {len(combined)} total rows across {len(all_dfs)} files"
        )
    else:
        logger.warning("No valid prospect ranking data found")

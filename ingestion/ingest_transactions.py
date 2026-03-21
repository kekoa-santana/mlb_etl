import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils.utils import build_db_url
from utils.retry import build_retry_session

logger = logging.getLogger(__name__)
session = build_retry_session(timeout=15)

engine = create_engine(build_db_url(database='mlb_fantasy'), pool_pre_ping=True)

TRANSACTIONS_URL = "https://statsapi.mlb.com/api/v1/transactions"


def _fetch_transactions(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch transactions from MLB Stats API for a date range.
    The API works best with ranges of ~30 days, so we chunk larger ranges.
    """
    all_rows = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Chunk into 30-day windows
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=29), end)

        url = (
            f"{TRANSACTIONS_URL}"
            f"?startDate={chunk_start.strftime('%Y-%m-%d')}"
            f"&endDate={chunk_end.strftime('%Y-%m-%d')}"
        )

        response = session.get(url, timeout=session.timeout)
        response.raise_for_status()
        data = response.json()

        for txn in data.get("transactions", []):
            person = txn.get("person") or {}
            to_team = txn.get("toTeam") or {}
            from_team = txn.get("fromTeam") or {}

            all_rows.append({
                "transaction_id": txn.get("id"),
                "player_id": person.get("id"),
                "player_name": person.get("fullName"),
                "to_team_id": to_team.get("id"),
                "to_team_name": to_team.get("name"),
                "from_team_id": from_team.get("id"),
                "from_team_name": from_team.get("name"),
                "date": txn.get("date"),
                "effective_date": txn.get("effectiveDate"),
                "resolution_date": txn.get("resolutionDate"),
                "type_code": txn.get("typeCode"),
                "type_desc": txn.get("typeDesc"),
                "description": txn.get("description"),
                "source": "MLB_stats_api",
            })

        logger.info(
            f"Fetched {len(data.get('transactions', []))} transactions "
            f"for {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
        )

        chunk_start = chunk_end + timedelta(days=1)

    return all_rows


def _load_transactions_raw(df: pd.DataFrame):
    """Upsert transactions into raw.transactions."""
    # Convert ID columns to nullable integer to avoid float NaN issues
    for col in ['player_id', 'to_team_id', 'from_team_id', 'transaction_id']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Replace pandas NA with None for SQL compatibility
    df = df.where(df.notna(), None)

    with engine.begin() as conn:
        table = Table('transactions', MetaData(), autoload_with=conn, schema='raw')
        for chunk_start in range(0, len(df), 50):
            chunk = df.iloc[chunk_start:chunk_start + 50]
            records = chunk.to_dict(orient='records')
            # Convert pandas NA/NaT to None in each record
            for record in records:
                for k, v in record.items():
                    if pd.isna(v):
                        record[k] = None
            stmt = pg_insert(table).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=['transaction_id'])
            conn.execute(stmt)


def fetch_and_load_transactions(start_date: str, end_date: str):
    """Fetch transactions from MLB Stats API and load to raw.transactions."""
    rows = _fetch_transactions(start_date, end_date)

    if rows:
        df = pd.DataFrame(rows)
        _load_transactions_raw(df)
        logger.info(f"Loaded {len(df)} transactions to raw.transactions")
    else:
        logger.info("No transactions found for date range")

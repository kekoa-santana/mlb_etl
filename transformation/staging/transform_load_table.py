import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData, func
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, Tuple, Any

from schema.spec_engine import apply_table_spec, TableSpec, _coerce_series

def get_table_columns(engine, schema: str, table: str) -> list[str]:
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :table
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return [r[0] for r in rows]

def align_df_to_table(df: pd.DataFrame, table_cols: list[str]) -> pd.DataFrame:
    df = df.copy()

    # add missing cols as nulls
    missing = [c for c in table_cols if c not in df.columns]
    for c in missing:
        df[c] = np.nan
    
    # drop extras and reorder
    df = df[[c for c in table_cols if c in df.columns]].copy()
    return df

def prepare_for_postgres(df, spec: TableSpec):
    df = df.copy()

    df = df.replace("", None)

    for _, colspec in spec.columns.items():
        col = colspec.name

        if col not in df.columns:
            continue
        if colspec.dtype:
            df[col] = _coerce_series(df[col], colspec.dtype)

    df = df.where(df.notna(), None)

    return df

def insert_update_conflicts(
    engine,
    df,
    schema: str,
    table_name: str,
    spec: TableSpec,
    constraint: str,
    batch_size: int = 10_000,
):
    metadata = MetaData(schema=schema)
    table = Table(table_name, metadata, autoload_with=engine)

    pk = spec.pk
    null_pk = df[pk].isna().any(axis=1)
    if null_pk.any():
        bad = df.loc[null_pk, pk].head()
        raise ValueError(f"Nulls found in primary key columns:\n{bad}")

    rows = df.to_dict("records")
    inserted = 0

    with engine.begin() as conn:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            if not chunk:
                continue

            stmt = insert(table).values(chunk)
            
            excluded = stmt.excluded
            update_cols = [c.name for c in table.columns if c.name not in spec.pk]

            set_clause = {
                c: func.coalesce(getattr(table.c, c), getattr(excluded, c))
                for c in update_cols
            }

            stmt = stmt.on_conflict_do_update(
                constraint=constraint,
                set_=set_clause
            )

            try:
                result = conn.execute(stmt)
                inserted += result.rowcount or 0
            except sa.exc.DBAPIError as e:
                print("FAILED BATCH starting at row", i)
                print("ON CONFLICT target:", spec.pk)

                # --- Clean Postgres error (psycopg) ---
                orig = getattr(e, "orig", None)
                if orig is not None:
                    print("PG ERROR TYPE:", type(orig).__name__)
                    print("PG ERROR:", str(orig))

                    # psycopg3 often provides diagnostics
                    diag = getattr(orig, "diag", None)
                    if diag is not None:
                        primary = getattr(diag, "message_primary", None)
                        detail = getattr(diag, "message_detail", None)
                        hint = getattr(diag, "message_hint", None)
                        if primary:
                            print("PG PRIMARY:", primary)
                        if detail:
                            print("PG DETAIL:", detail)
                        if hint:
                            print("PG HINT:", hint)

                raise
            

    return inserted

def transform_and_load(
    engine,
    df_raw: pd.DataFrame,
    spec: TableSpec,
    schema: str,
    table: str,
    constraint: str,
    project: bool = False
) -> tuple[int, dict[str, Any]]:
    df_clean, report = apply_table_spec(df_raw, spec)

    table_cols = get_table_columns(engine, schema, table)
    df_load = align_df_to_table(df_clean, table_cols)
    df_prep = prepare_for_postgres(df_load, spec)
    n = insert_update_conflicts(
        engine=engine,
        df=df_prep,
        schema=schema,
        table_name=table,
        spec=spec,
        constraint=constraint,
        batch_size=1
    )

    report['rows_loaded'] = n
    report['db_columns'] = len(table_cols)

    return n, report
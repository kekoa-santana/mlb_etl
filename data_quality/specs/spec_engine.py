from __future__ import annotations
from typing import Callable, Optional, Any

import numpy as np
import pandas as pd
from dataclasses import dataclass, field

from utils.statcast_utils import assert_pk_unique

@dataclass
class ColumnSpec:
    name: str
    dtype: str | None
    nullable: bool = True
    bounds: Optional[tuple[float, float]] = None
    derive: Optional[Callable[[pd.DataFrame], pd.Series]] = None
    original_name: str | None = None

@dataclass
class TableSpec:
    name: str
    pk: list[str]
    columns: dict[str, ColumnSpec]
    table_rules: list[Callable[[pd.DataFrame], dict[str, int]]] | None = None
    row_filters: list[Callable[[pd.DataFrame], pd.DataFrame]] | None = None


def _coerce_series(s: pd.Series, dtype: str) -> pd.Series:
    if dtype in ("Int64", "int64", "int"):
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if dtype == "float64":
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if dtype == "string":
        return s.astype("string")
    if dtype == "boolean":
        return s.astype("boolean")
    if dtype == "datetime":
        return pd.to_datetime(s, errors="coerce")

    # fallback
    return s.astype(dtype)

def _apply_bounds_one(df: pd.DataFrame, col: str, bounds: tuple[float, float]) -> int:
    lo, hi = bounds
    s = pd.to_numeric(df[col], errors='coerce')
    mask = (s < lo) | (s > hi)
    n = int(mask.sum())
    if n:
        df.loc[mask, col] = np.nan
    return n

def apply_table_spec(df: pd.DataFrame, spec: TableSpec) -> tuple[pd.DataFrame, dict[str, Any]]:
    df = df.copy()

    report: dict[str, Any] = {
        'table': spec.name,
        'rows_in': int(len(df)),
        'missing_required_columns': [],
        'type_coercions': {},
        'invalid_bounds': {},
        'derived_columns': {},
        'rule_violations': {},
        'not_nullable_violations': {}
    }

    rename_map = {}
    for _, colspec in spec.columns.items():
        if colspec.original_name and colspec.original_name in df.columns:
            if colspec.name not in df.columns:
                rename_map[colspec.original_name] = colspec.name

    if rename_map:
        df = df.rename(columns=rename_map)
 
    # Coerce + Bound
    for _, colspec in spec.columns.items():
        col = colspec.name

        if colspec.derive is not None:
            try:
                df[col] = colspec.derive(df)
                report["derived_columns"][col] = True
            except KeyError as e:
                report['derived_columns'][col] = f"failed_missing_dep:{str(e)}"
                continue

            if colspec.dtype:
                df[col] = _coerce_series(df[col], colspec.dtype)
                report['type_coercions'][col] = colspec.dtype

            if colspec.bounds:
                n = _apply_bounds_one(df, col, colspec.bounds)
                report['invalid_bounds'][col] = n

            continue

        if col not in df.columns:
            if not colspec.nullable:
                report['missing_required_columns'].append(col)
            continue
        
        if colspec.dtype:
            df[col] = _coerce_series(df[col], colspec.dtype)
            report['type_coercions'][col] = colspec.dtype

        if colspec.bounds:
            n = _apply_bounds_one(df, col, colspec.bounds)
            report['invalid_bounds'][col] = n

    
    # Table rules
    if spec.table_rules:
        for rule_fn in spec.table_rules:
            violations = rule_fn(df)
            for k, v in violations.items():
                report['rule_violations'][k] = report['rule_violations'].get(k, 0) + int(v)

    if spec.row_filters:
        for fn in spec.row_filters:
            df = fn(df)

    # Not null checks
    for key, colspec in spec.columns.items():
        col = colspec.name
        if not colspec.nullable and col in df.columns:
            n_null = int(df[col].isna().sum())
            if n_null:
                report['not_nullable_violations'][col] = n_null


    # PK uniqueness enforcement (drops dupes)
    missing_pk = [k for k in spec.pk if k not in df.columns]
    if missing_pk:
        raise ValueError(f"Missing PK columns for {spec.name}: {missing_pk}. "
                         f"Available columns: {list(df.columns)}")


    df = assert_pk_unique(df, spec.pk)

    report['rows_out'] = int(len(df))
    return df, report
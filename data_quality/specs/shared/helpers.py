from data_quality.specs.spec_engine import ColumnSpec

def merge_columns(*maps: dict[str, ColumnSpec]) -> dict[str, ColumnSpec]:
    merged: dict[str, ColumnSpec] = {}
    for m in maps:
        merged |= m
    return merged
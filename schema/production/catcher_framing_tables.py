from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# production.fact_catcher_framing
# Grain: player_id + season
# Catcher framing metrics from Baseball Savant.
# ---------------------------------------------------------------------------
FACT_CATCHER_FRAMING_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'season': ColumnSpec(
        name='season',
        dtype='SmallInteger',
        nullable=False,
        primary_key=True
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text'
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    # -- volume --
    'pitches_called': ColumnSpec(
        name='pitches_called',
        dtype='Integer'
    ),
    # -- framing metrics --
    'runs_extra_strikes': ColumnSpec(
        name='runs_extra_strikes',
        dtype='REAL'
    ),
    'strike_rate': ColumnSpec(
        name='strike_rate',
        dtype='REAL'
    ),
    'expected_strike_rate': ColumnSpec(
        name='expected_strike_rate',
        dtype='REAL'
    ),
    'strike_rate_diff': ColumnSpec(
        name='strike_rate_diff',
        dtype='REAL'
    ),
    # -- shadow zone performance --
    'shadow_zone_pitches': ColumnSpec(
        name='shadow_zone_pitches',
        dtype='Integer'
    ),
    'shadow_zone_strike_rate': ColumnSpec(
        name='shadow_zone_strike_rate',
        dtype='REAL'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_CATCHER_FRAMING_SPEC = TableSpec(
    name='fact_catcher_framing',
    pk=['player_id', 'season'],
    columns=FACT_CATCHER_FRAMING_COLS
)

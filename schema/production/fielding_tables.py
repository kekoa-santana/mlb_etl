from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# production.fact_fielding_oaa
# Grain: player_id + season + position
# Outs Above Average from Baseball Savant, with directional and batter-hand
# splits plus fielding runs prevented.
# ---------------------------------------------------------------------------
FACT_FIELDING_OAA_COLS: dict[str, ColumnSpec] = {
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
    'position': ColumnSpec(
        name='position',
        dtype='String(5)',
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
    # -- core metrics --
    'outs_above_average': ColumnSpec(
        name='outs_above_average',
        dtype='SmallInteger'
    ),
    'fielding_runs_prevented': ColumnSpec(
        name='fielding_runs_prevented',
        dtype='SmallInteger'
    ),
    # -- directional splits --
    'oaa_infront': ColumnSpec(
        name='oaa_infront',
        dtype='SmallInteger'
    ),
    'oaa_lateral_3b': ColumnSpec(
        name='oaa_lateral_3b',
        dtype='SmallInteger'
    ),
    'oaa_lateral_1b': ColumnSpec(
        name='oaa_lateral_1b',
        dtype='SmallInteger'
    ),
    'oaa_behind': ColumnSpec(
        name='oaa_behind',
        dtype='SmallInteger'
    ),
    # -- batter hand splits --
    'oaa_vs_rhh': ColumnSpec(
        name='oaa_vs_rhh',
        dtype='SmallInteger'
    ),
    'oaa_vs_lhh': ColumnSpec(
        name='oaa_vs_lhh',
        dtype='SmallInteger'
    ),
    # -- success rates --
    'actual_success_rate': ColumnSpec(
        name='actual_success_rate',
        dtype='REAL'
    ),
    'expected_success_rate': ColumnSpec(
        name='expected_success_rate',
        dtype='REAL'
    ),
    'success_rate_diff': ColumnSpec(
        name='success_rate_diff',
        dtype='REAL'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_FIELDING_OAA_SPEC = TableSpec(
    name='fact_fielding_oaa',
    pk=['player_id', 'season', 'position'],
    columns=FACT_FIELDING_OAA_COLS
)

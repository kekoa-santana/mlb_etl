from schema.spec_engine import TableSpec, ColumnSpec


# ── dim_model_run ────────────────────────────────────────────────────

DIM_MODEL_RUN_COLS: dict[str, ColumnSpec] = {
    'run_id': ColumnSpec(
        name='run_id',
        dtype='Text',
        nullable=False,
        server_default='gen_random_uuid()',
    ),
    'model_name': ColumnSpec(name='model_name', dtype='Text', nullable=False),
    'model_version': ColumnSpec(name='model_version', dtype='Text'),
    'feature_cutoff_date': ColumnSpec(name='feature_cutoff_date', dtype='DATE'),
    'train_start_date': ColumnSpec(name='train_start_date', dtype='DATE'),
    'train_end_date': ColumnSpec(name='train_end_date', dtype='DATE'),
    'target_variable': ColumnSpec(name='target_variable', dtype='Text'),
    'hyperparameters': ColumnSpec(name='hyperparameters', dtype='Text'),
    'notes': ColumnSpec(name='notes', dtype='Text'),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

DIM_MODEL_RUN_SPEC = TableSpec(
    name='dim_model_run',
    pk=['run_id'],
    columns=DIM_MODEL_RUN_COLS,
)


# ── fact_player_projection ───────────────────────────────────────────

FACT_PLAYER_PROJECTION_COLS: dict[str, ColumnSpec] = {
    'run_id': ColumnSpec(name='run_id', dtype='Text', nullable=False),
    'player_id': ColumnSpec(name='player_id', dtype='BigInteger', nullable=False),
    'as_of_date': ColumnSpec(name='as_of_date', dtype='DATE', nullable=False),
    'horizon': ColumnSpec(name='horizon', dtype='String(10)', nullable=False),
    'scenario': ColumnSpec(name='scenario', dtype='String(10)', nullable=False),
    'player_role': ColumnSpec(name='player_role', dtype='String(7)'),

    # projected fantasy points — percentile bands
    'projected_pts_p10': ColumnSpec(name='projected_pts_p10', dtype='REAL'),
    'projected_pts_p50': ColumnSpec(name='projected_pts_p50', dtype='REAL'),
    'projected_pts_p90': ColumnSpec(name='projected_pts_p90', dtype='REAL'),

    # projected volume
    'projected_pa': ColumnSpec(name='projected_pa', dtype='REAL'),
    'projected_ip': ColumnSpec(name='projected_ip', dtype='REAL'),

    # projected batter rates
    'projected_avg': ColumnSpec(name='projected_avg', dtype='REAL'),
    'projected_obp': ColumnSpec(name='projected_obp', dtype='REAL'),
    'projected_slg': ColumnSpec(name='projected_slg', dtype='REAL'),

    # projected pitcher rates
    'projected_era': ColumnSpec(name='projected_era', dtype='REAL'),
    'projected_whip': ColumnSpec(name='projected_whip', dtype='REAL'),
    'projected_k9': ColumnSpec(name='projected_k9', dtype='REAL'),

    'confidence_score': ColumnSpec(
        name='confidence_score',
        dtype='REAL',
        bounds=(0.0, 1.0),
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PLAYER_PROJECTION_SPEC = TableSpec(
    name='fact_player_projection',
    pk=['run_id', 'player_id', 'as_of_date', 'horizon', 'scenario'],
    columns=FACT_PLAYER_PROJECTION_COLS,
)


# ── fact_projection_backtest ─────────────────────────────────────────

FACT_PROJECTION_BACKTEST_COLS: dict[str, ColumnSpec] = {
    'run_id': ColumnSpec(name='run_id', dtype='Text', nullable=False),
    'player_id': ColumnSpec(name='player_id', dtype='BigInteger', nullable=False),
    'target_date': ColumnSpec(name='target_date', dtype='DATE', nullable=False),
    'metric': ColumnSpec(name='metric', dtype='String(20)', nullable=False),

    'predicted_value': ColumnSpec(name='predicted_value', dtype='REAL'),
    'actual_value': ColumnSpec(name='actual_value', dtype='REAL'),
    'error': ColumnSpec(name='error', dtype='REAL'),
    'abs_error': ColumnSpec(name='abs_error', dtype='REAL'),
    'pct_error': ColumnSpec(name='pct_error', dtype='REAL'),
    'within_p10_p90': ColumnSpec(name='within_p10_p90', dtype='Boolean'),

    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PROJECTION_BACKTEST_SPEC = TableSpec(
    name='fact_projection_backtest',
    pk=['run_id', 'player_id', 'target_date', 'metric'],
    columns=FACT_PROJECTION_BACKTEST_COLS,
)

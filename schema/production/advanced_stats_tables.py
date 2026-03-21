from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# 1. production.fact_batting_advanced
#    Grain: batter_id + season
#    Player-season batting: wOBA, xwOBA, wRC+, Barrel%, HardHit%, K%, BB%
# ---------------------------------------------------------------------------
FACT_BATTING_ADVANCED_COLS: dict[str, ColumnSpec] = {
    'batter_id': ColumnSpec(
        name='batter_id',
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
    # -- counting stats --
    'pa': ColumnSpec(name='pa', dtype='Integer'),
    'ab': ColumnSpec(name='ab', dtype='Integer'),
    'hits': ColumnSpec(name='hits', dtype='Integer'),
    'singles': ColumnSpec(name='singles', dtype='Integer'),
    'doubles': ColumnSpec(name='doubles', dtype='Integer'),
    'triples': ColumnSpec(name='triples', dtype='Integer'),
    'hr': ColumnSpec(name='hr', dtype='Integer'),
    'bb': ColumnSpec(name='bb', dtype='Integer'),
    'ibb': ColumnSpec(name='ibb', dtype='Integer'),
    'hbp': ColumnSpec(name='hbp', dtype='Integer'),
    'k': ColumnSpec(name='k', dtype='Integer'),
    'sf': ColumnSpec(name='sf', dtype='Integer'),
    # -- rate stats --
    'avg': ColumnSpec(name='avg', dtype='REAL'),
    'obp': ColumnSpec(name='obp', dtype='REAL'),
    'slg': ColumnSpec(name='slg', dtype='REAL'),
    'ops': ColumnSpec(name='ops', dtype='REAL'),
    'k_pct': ColumnSpec(name='k_pct', dtype='REAL'),
    'bb_pct': ColumnSpec(name='bb_pct', dtype='REAL'),
    # -- weighted stats --
    'woba': ColumnSpec(name='woba', dtype='REAL'),
    'wrc_plus': ColumnSpec(name='wrc_plus', dtype='REAL'),
    # -- statcast quality of contact --
    'xba': ColumnSpec(name='xba', dtype='REAL'),
    'xslg': ColumnSpec(name='xslg', dtype='REAL'),
    'xwoba': ColumnSpec(name='xwoba', dtype='REAL'),
    'barrel_pct': ColumnSpec(name='barrel_pct', dtype='REAL'),
    'hard_hit_pct': ColumnSpec(name='hard_hit_pct', dtype='REAL'),
    'sweet_spot_pct': ColumnSpec(name='sweet_spot_pct', dtype='REAL'),
    'bip_count': ColumnSpec(name='bip_count', dtype='Integer'),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_BATTING_ADVANCED_SPEC = TableSpec(
    name='fact_batting_advanced',
    pk=['batter_id', 'season'],
    columns=FACT_BATTING_ADVANCED_COLS
)

# ---------------------------------------------------------------------------
# 2. production.fact_pitching_advanced
#    Grain: pitcher_id + season
#    Player-season pitching: K%, BB%, SwStr%, CSW%, Zone%, Chase%, xwOBA-against
# ---------------------------------------------------------------------------
FACT_PITCHING_ADVANCED_COLS: dict[str, ColumnSpec] = {
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
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
    # -- volume --
    'total_pitches': ColumnSpec(name='total_pitches', dtype='Integer'),
    'batters_faced': ColumnSpec(name='batters_faced', dtype='Integer'),
    'pa_against': ColumnSpec(name='pa_against', dtype='Integer'),
    # -- pitch outcome rates --
    'k_pct': ColumnSpec(name='k_pct', dtype='REAL'),
    'bb_pct': ColumnSpec(name='bb_pct', dtype='REAL'),
    'swstr_pct': ColumnSpec(name='swstr_pct', dtype='REAL'),
    'csw_pct': ColumnSpec(name='csw_pct', dtype='REAL'),
    'zone_pct': ColumnSpec(name='zone_pct', dtype='REAL'),
    'chase_pct': ColumnSpec(name='chase_pct', dtype='REAL'),
    'contact_pct': ColumnSpec(name='contact_pct', dtype='REAL'),
    # -- batted ball against --
    'xwoba_against': ColumnSpec(name='xwoba_against', dtype='REAL'),
    'barrel_pct_against': ColumnSpec(name='barrel_pct_against', dtype='REAL'),
    'hard_hit_pct_against': ColumnSpec(name='hard_hit_pct_against', dtype='REAL'),
    'bip_against': ColumnSpec(name='bip_against', dtype='Integer'),
    # -- woba against --
    'woba_against': ColumnSpec(name='woba_against', dtype='REAL'),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_PITCHING_ADVANCED_SPEC = TableSpec(
    name='fact_pitching_advanced',
    pk=['pitcher_id', 'season'],
    columns=FACT_PITCHING_ADVANCED_COLS
)

# ---------------------------------------------------------------------------
# 3. production.fact_pitch_type_run_value
#    Grain: pitcher_id + season + pitch_type
#    Per-pitch-type run values, usage, and outcome rates
# ---------------------------------------------------------------------------
FACT_PITCH_TYPE_RV_COLS: dict[str, ColumnSpec] = {
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
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
    'pitch_type': ColumnSpec(
        name='pitch_type',
        dtype='String(10)',
        nullable=False,
        primary_key=True
    ),
    'pitch_name': ColumnSpec(name='pitch_name', dtype='Text'),
    'pitches_thrown': ColumnSpec(name='pitches_thrown', dtype='Integer'),
    'usage_pct': ColumnSpec(name='usage_pct', dtype='REAL'),
    # -- run values --
    'run_value_total': ColumnSpec(name='run_value_total', dtype='REAL'),
    'run_value_per_100': ColumnSpec(name='run_value_per_100', dtype='REAL'),
    # -- outcome rates --
    'whiff_pct': ColumnSpec(name='whiff_pct', dtype='REAL'),
    'csw_pct': ColumnSpec(name='csw_pct', dtype='REAL'),
    'zone_pct': ColumnSpec(name='zone_pct', dtype='REAL'),
    'chase_pct': ColumnSpec(name='chase_pct', dtype='REAL'),
    'put_away_pct': ColumnSpec(name='put_away_pct', dtype='REAL'),
    # -- batted ball quality when hit --
    'xwoba_contact': ColumnSpec(name='xwoba_contact', dtype='REAL'),
    'avg_launch_speed': ColumnSpec(name='avg_launch_speed', dtype='REAL'),
    'avg_launch_angle': ColumnSpec(name='avg_launch_angle', dtype='REAL'),
    # -- pitch characteristics --
    'avg_velo': ColumnSpec(name='avg_velo', dtype='REAL'),
    'avg_spin': ColumnSpec(name='avg_spin', dtype='REAL'),
    'avg_pfx_x': ColumnSpec(name='avg_pfx_x', dtype='REAL'),
    'avg_pfx_z': ColumnSpec(name='avg_pfx_z', dtype='REAL'),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_PITCH_TYPE_RV_SPEC = TableSpec(
    name='fact_pitch_type_run_value',
    pk=['pitcher_id', 'season', 'pitch_type'],
    columns=FACT_PITCH_TYPE_RV_COLS
)

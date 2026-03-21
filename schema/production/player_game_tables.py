from schema.spec_engine import TableSpec, ColumnSpec

# ── fact_player_game_mlb ───────────────────────────────────────────
# Grain: one row per player_id + game_pk + player_role (batter/pitcher)

FACT_PLAYER_GAME_MLB_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
    ),
    'player_role': ColumnSpec(
        name='player_role',
        dtype='String(7)',
        nullable=False,
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE',
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer',
    ),
    # ── batter columns ─────────────────────────────────────────────
    'bat_pa': ColumnSpec(name='bat_pa', dtype='SmallInteger'),
    'bat_ab': ColumnSpec(name='bat_ab', dtype='SmallInteger'),
    'bat_h': ColumnSpec(name='bat_h', dtype='SmallInteger'),
    'bat_2b': ColumnSpec(name='bat_2b', dtype='SmallInteger'),
    'bat_3b': ColumnSpec(name='bat_3b', dtype='SmallInteger'),
    'bat_hr': ColumnSpec(name='bat_hr', dtype='SmallInteger'),
    'bat_r': ColumnSpec(name='bat_r', dtype='SmallInteger'),
    'bat_rbi': ColumnSpec(name='bat_rbi', dtype='SmallInteger'),
    'bat_bb': ColumnSpec(name='bat_bb', dtype='SmallInteger'),
    'bat_k': ColumnSpec(name='bat_k', dtype='SmallInteger'),
    'bat_hbp': ColumnSpec(name='bat_hbp', dtype='SmallInteger'),
    'bat_sb': ColumnSpec(name='bat_sb', dtype='SmallInteger'),
    'bat_cs': ColumnSpec(name='bat_cs', dtype='SmallInteger'),
    'bat_tb': ColumnSpec(name='bat_tb', dtype='SmallInteger'),
    'bat_errors': ColumnSpec(name='bat_errors', dtype='SmallInteger'),
    'bat_avg': ColumnSpec(name='bat_avg', dtype='REAL'),
    'bat_obp': ColumnSpec(name='bat_obp', dtype='REAL'),
    'bat_slg': ColumnSpec(name='bat_slg', dtype='REAL'),
    # ── pitcher columns ────────────────────────────────────────────
    'pit_ip': ColumnSpec(name='pit_ip', dtype='REAL'),
    'pit_er': ColumnSpec(name='pit_er', dtype='SmallInteger'),
    'pit_r': ColumnSpec(name='pit_r', dtype='SmallInteger'),
    'pit_h': ColumnSpec(name='pit_h', dtype='SmallInteger'),
    'pit_bb': ColumnSpec(name='pit_bb', dtype='SmallInteger'),
    'pit_k': ColumnSpec(name='pit_k', dtype='SmallInteger'),
    'pit_hr': ColumnSpec(name='pit_hr', dtype='SmallInteger'),
    'pit_bf': ColumnSpec(name='pit_bf', dtype='SmallInteger'),
    'pit_w': ColumnSpec(name='pit_w', dtype='SmallInteger'),
    'pit_l': ColumnSpec(name='pit_l', dtype='SmallInteger'),
    'pit_sv': ColumnSpec(name='pit_sv', dtype='SmallInteger'),
    'pit_hld': ColumnSpec(name='pit_hld', dtype='SmallInteger'),
    'pit_bs': ColumnSpec(name='pit_bs', dtype='SmallInteger'),
    'pit_pitches': ColumnSpec(name='pit_pitches', dtype='SmallInteger'),
    'pit_strikes': ColumnSpec(name='pit_strikes', dtype='SmallInteger'),
    'pit_is_starter': ColumnSpec(name='pit_is_starter', dtype='Boolean'),
    'pit_era': ColumnSpec(name='pit_era', dtype='REAL'),
    'pit_whip': ColumnSpec(name='pit_whip', dtype='REAL'),
    # ── metadata ───────────────────────────────────────────────────
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PLAYER_GAME_MLB_SPEC = TableSpec(
    name='fact_player_game_mlb',
    pk=['player_id', 'game_pk', 'player_role'],
    columns=FACT_PLAYER_GAME_MLB_COLS,
)


# ── fact_player_status_timeline ────────────────────────────────────
# Grain: one row per player_id + status_start_date + status_type

FACT_PLAYER_STATUS_TIMELINE_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
    ),
    'status_start_date': ColumnSpec(
        name='status_start_date',
        dtype='DATE',
        nullable=False,
    ),
    'status_type': ColumnSpec(
        name='status_type',
        dtype='String(20)',
        nullable=False,
    ),
    'status_end_date': ColumnSpec(
        name='status_end_date',
        dtype='DATE',
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer',
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text',
    ),
    'injury_description': ColumnSpec(
        name='injury_description',
        dtype='Text',
    ),
    'days_on_status': ColumnSpec(
        name='days_on_status',
        dtype='Integer',
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PLAYER_STATUS_TIMELINE_SPEC = TableSpec(
    name='fact_player_status_timeline',
    pk=['player_id', 'status_start_date', 'status_type'],
    columns=FACT_PLAYER_STATUS_TIMELINE_COLS,
)

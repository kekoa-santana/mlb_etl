from schema.spec_engine import TableSpec, ColumnSpec

FACT_PA_COLS: dict[str, ColumnSpec] = {
    'pa_id': ColumnSpec(
        name='pa_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True,
        identity=True  # BIGSERIAL
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False
    ),
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
        dtype='BigInteger',
        nullable=False
    ),
    'batter_id': ColumnSpec(
        name='batter_id',
        dtype='BigInteger',
        nullable=False
    ),
    'game_counter': ColumnSpec(
        name='game_counter',
        dtype='Integer',
        nullable=False
    ),
    'pitcher_pa_number': ColumnSpec(
        name='pitcher_pa_number',
        dtype='Integer',
        bounds=(1, 150)
    ),
    'times_through_order': ColumnSpec(
        name='times_through_order',
        dtype='SmallInteger',
        bounds=(1, 7)
    ),
    'balls': ColumnSpec(
        name='balls',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'strikes': ColumnSpec(
        name='strikes',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'outs_when_up': ColumnSpec(
        name='outs_when_up',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'inning': ColumnSpec(
        name='inning',
        dtype='Integer',
        bounds=(1, 50)
    ),
    'inning_topbot': ColumnSpec(
        name='inning_topbot',
        dtype='Text'
    ),
    'events': ColumnSpec(
        name='events',
        dtype='Text'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
    ),
    'bat_score': ColumnSpec(
        name='bat_score',
        dtype='SmallInteger'
    ),
    'fld_score': ColumnSpec(
        name='fld_score',
        dtype='SmallInteger'
    ),
    'post_bat_score': ColumnSpec(
        name='post_bat_score',
        dtype='SmallInteger'
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='SmallInteger'
    ),
    'last_pitch_number': ColumnSpec(
        name='last_pitch_number',
        dtype='SmallInteger'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

FACT_PA_SPEC = TableSpec(
    name='fact_pa',
    pk=['pa_id'],
    columns=FACT_PA_COLS,
    unique_constraints=[('uq_fact_pa_natural', ['game_pk', 'game_counter'])]
)

FACT_PITCH_COLS: dict[str, ColumnSpec] = {
    'pitch_id': ColumnSpec(
        name='pitch_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True,
        identity=True  # BIGSERIAL
    ),
    'pa_id': ColumnSpec(
        name='pa_id',
        dtype='BigInteger',
        nullable=False
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False
    ),
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
        dtype='BigInteger',
        nullable=False
    ),
    'batter_id': ColumnSpec(
        name='batter_id',
        dtype='BigInteger',
        nullable=False
    ),
    'game_counter': ColumnSpec(
        name='game_counter',
        dtype='Integer',
        nullable=False
    ),
    'pitch_number': ColumnSpec(
        name='pitch_number',
        dtype='Integer',
        nullable=False,
        bounds=(1, 130)
    ),
    'pitch_type': ColumnSpec(
        name='pitch_type',
        dtype='Text'
    ),
    'pitch_name': ColumnSpec(
        name='pitch_name',
        dtype='Text'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
    ),
    'release_speed': ColumnSpec(
        name='release_speed',
        dtype='REAL',
        bounds=(20, 108)
    ),
    'effective_speed': ColumnSpec(
        name='effective_speed',
        dtype='REAL',
        bounds=(60, 110)
    ),
    'release_spin_rate': ColumnSpec(
        name='release_spin_rate',
        dtype='REAL',
        bounds=(500, 3500)
    ),
    'release_extension': ColumnSpec(
        name='release_extension',
        dtype='REAL',
        bounds=(3.5, 9)
    ),
    'spin_axis': ColumnSpec(
        name='spin_axis',
        dtype='REAL',
        bounds=(0, 360)
    ),
    'pfx_x': ColumnSpec(
        name='pfx_x',
        dtype='REAL',
        bounds=(-5, 5)
    ),
    'pfx_z': ColumnSpec(
        name='pfx_z',
        dtype='REAL',
        bounds=(-4.5, 4.5)
    ),
    'zone': ColumnSpec(
        name='zone',
        dtype='SmallInteger',
        bounds=(1, 14)
    ),
    'plate_x': ColumnSpec(
        name='plate_x',
        dtype='REAL',
        bounds=(-3, 3)
    ),
    'plate_z': ColumnSpec(
        name='plate_z',
        dtype='REAL',
        bounds=(0, 7)
    ),
    'balls': ColumnSpec(
        name='balls',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'strikes': ColumnSpec(
        name='strikes',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'outs_when_up': ColumnSpec(
        name='outs_when_up',
        dtype='SmallInteger',
        bounds=(0, 2)
    ),
    'bat_score_diff': ColumnSpec(
        name='bat_score_diff',
        dtype='SmallInteger'
    ),
    'is_whiff': ColumnSpec(
        name='is_whiff',
        dtype='Boolean'
    ),
    'is_called_strike': ColumnSpec(
        name='is_called_strike',
        dtype='Boolean'
    ),
    'is_bip': ColumnSpec(
        name='is_bip',
        dtype='Boolean'
    ),
    'is_swing': ColumnSpec(
        name='is_swing',
        dtype='Boolean'
    ),
    'is_foul': ColumnSpec(
        name='is_foul',
        dtype='Boolean'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
    'batter_stand': ColumnSpec(
        name='batter_stand',
        dtype='String(1)'
    ),
}

FACT_PITCH_SPEC = TableSpec(
    name='fact_pitch',
    pk=['pitch_id'],
    columns=FACT_PITCH_COLS,
    unique_constraints=[('uq_fact_pitch_natural', ['game_pk', 'game_counter', 'pitch_number'])]
)


# ── fact_lineup ──────────────────────────────────────────────────────

FACT_LINEUP_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
    ),
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer',
        nullable=False,
    ),
    'batting_order': ColumnSpec(
        name='batting_order',
        dtype='SmallInteger',
        nullable=False,
        bounds=(1, 9),
    ),
    'is_starter': ColumnSpec(
        name='is_starter',
        dtype='Boolean',
        nullable=False,
    ),
    'position': ColumnSpec(
        name='position',
        dtype='String(4)',
    ),
    'home_away': ColumnSpec(
        name='home_away',
        dtype='String(4)',
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

FACT_LINEUP_SPEC = TableSpec(
    name='fact_lineup',
    pk=['game_pk', 'player_id'],
    columns=FACT_LINEUP_COLS,
)


# ── dim_weather ─────────────────────────────────────────────────────

DIM_WEATHER_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
    ),
    'temperature': ColumnSpec(
        name='temperature',
        dtype='SmallInteger',
        bounds=(0, 130),
    ),
    'condition': ColumnSpec(
        name='condition',
        dtype='String(20)',
    ),
    'is_dome': ColumnSpec(
        name='is_dome',
        dtype='Boolean',
    ),
    'wind_speed': ColumnSpec(
        name='wind_speed',
        dtype='SmallInteger',
        bounds=(0, 60),
    ),
    'wind_direction': ColumnSpec(
        name='wind_direction',
        dtype='String(20)',
    ),
    'wind_category': ColumnSpec(
        name='wind_category',
        dtype='String(5)',
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

DIM_WEATHER_SPEC = TableSpec(
    name='dim_weather',
    pk=['game_pk'],
    columns=DIM_WEATHER_COLS,
)


# ── dim_park_factor ─────────────────────────────────────────────────

DIM_PARK_FACTOR_COLS: dict[str, ColumnSpec] = {
    'venue_id': ColumnSpec(
        name='venue_id',
        dtype='Integer',
        nullable=False,
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
        nullable=False,
    ),
    'batter_stand': ColumnSpec(
        name='batter_stand',
        dtype='String(1)',
        nullable=False,
    ),
    'venue_name': ColumnSpec(
        name='venue_name',
        dtype='Text',
    ),
    'hr_pf_season': ColumnSpec(
        name='hr_pf_season',
        dtype='REAL',
    ),
    'pa_season': ColumnSpec(
        name='pa_season',
        dtype='Integer',
    ),
    'hr_season': ColumnSpec(
        name='hr_season',
        dtype='Integer',
    ),
    'hr_pf_3yr': ColumnSpec(
        name='hr_pf_3yr',
        dtype='REAL',
    ),
    'pa_3yr': ColumnSpec(
        name='pa_3yr',
        dtype='Integer',
    ),
    'hr_3yr': ColumnSpec(
        name='hr_3yr',
        dtype='Integer',
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

DIM_PARK_FACTOR_SPEC = TableSpec(
    name='dim_park_factor',
    pk=['venue_id', 'season', 'batter_stand'],
    columns=DIM_PARK_FACTOR_COLS,
)


# ── dim_umpire ──────────────────────────────────────────────────────

DIM_UMPIRE_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
    ),
    'hp_umpire_name': ColumnSpec(
        name='hp_umpire_name',
        dtype='Text',
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

DIM_UMPIRE_SPEC = TableSpec(
    name='dim_umpire',
    pk=['game_pk'],
    columns=DIM_UMPIRE_COLS,
)


# ── fact_game_totals ────────────────────────────────────────────────

FACT_GAME_TOTALS_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(name='game_pk', dtype='BigInteger', nullable=False),
    'team_id': ColumnSpec(name='team_id', dtype='Integer', nullable=False),
    'season': ColumnSpec(name='season', dtype='Integer'),
    'home_away': ColumnSpec(name='home_away', dtype='String(4)'),
    'runs': ColumnSpec(name='runs', dtype='Integer'),
    'hits': ColumnSpec(name='hits', dtype='Integer'),
    'doubles': ColumnSpec(name='doubles', dtype='Integer'),
    'triples': ColumnSpec(name='triples', dtype='Integer'),
    'home_runs': ColumnSpec(name='home_runs', dtype='Integer'),
    'walks': ColumnSpec(name='walks', dtype='Integer'),
    'strikeouts': ColumnSpec(name='strikeouts', dtype='Integer'),
    'hit_by_pitch': ColumnSpec(name='hit_by_pitch', dtype='Integer'),
    'sb': ColumnSpec(name='sb', dtype='Integer'),
    'caught_stealing': ColumnSpec(name='caught_stealing', dtype='Integer'),
    'at_bats': ColumnSpec(name='at_bats', dtype='Integer'),
    'plate_appearances': ColumnSpec(name='plate_appearances', dtype='Integer'),
    'total_bases': ColumnSpec(name='total_bases', dtype='Integer'),
    'rbi': ColumnSpec(name='rbi', dtype='Integer'),
    'errors': ColumnSpec(name='errors', dtype='Integer'),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_GAME_TOTALS_SPEC = TableSpec(
    name='fact_game_totals',
    pk=['game_pk', 'team_id'],
    columns=FACT_GAME_TOTALS_COLS,
)
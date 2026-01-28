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
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

FACT_PA_SPEC = TableSpec(
    name='fact_pa',
    pk=['pa_id'],
    columns=FACT_PA_COLS
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
    columns=FACT_PITCH_COLS
)
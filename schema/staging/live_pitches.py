from schema.spec_engine import ColumnSpec, TableSpec

LIVE_PITCHES_COLUMNS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk', dtype='BigInteger', nullable=False, primary_key=True
    ),
    'at_bat_index': ColumnSpec(
        name='at_bat_index', dtype='Integer', nullable=False, primary_key=True
    ),
    'pitch_number': ColumnSpec(
        name='pitch_number', dtype='SmallInteger', nullable=False, primary_key=True
    ),
    'game_date': ColumnSpec(name='game_date', dtype='DATE', nullable=False),
    'pitcher_id': ColumnSpec(name='pitcher_id', dtype='BigInteger', nullable=False),
    'batter_id': ColumnSpec(name='batter_id', dtype='BigInteger', nullable=False),
    'inning': ColumnSpec(name='inning', dtype='SmallInteger'),
    'half_inning': ColumnSpec(name='half_inning', dtype='Text'),
    'pitch_type': ColumnSpec(name='pitch_type', dtype='Text'),
    'pitch_name': ColumnSpec(name='pitch_name', dtype='Text'),
    'release_speed': ColumnSpec(name='release_speed', dtype='REAL', bounds=(40, 110)),
    'spin_rate': ColumnSpec(name='spin_rate', dtype='REAL', bounds=(0, 4000)),
    'pfx_x': ColumnSpec(name='pfx_x', dtype='REAL'),
    'pfx_z': ColumnSpec(name='pfx_z', dtype='REAL'),
    'plate_x': ColumnSpec(name='plate_x', dtype='REAL'),
    'plate_z': ColumnSpec(name='plate_z', dtype='REAL'),
    'zone': ColumnSpec(name='zone', dtype='SmallInteger', bounds=(1, 14)),
    'sz_top': ColumnSpec(name='sz_top', dtype='REAL'),
    'sz_bot': ColumnSpec(name='sz_bot', dtype='REAL'),
    'call_code': ColumnSpec(name='call_code', dtype='String(2)'),
    'call_description': ColumnSpec(name='call_description', dtype='Text'),
    'is_whiff': ColumnSpec(name='is_whiff', dtype='Boolean'),
    'is_called_strike': ColumnSpec(name='is_called_strike', dtype='Boolean'),
    'is_swing': ColumnSpec(name='is_swing', dtype='Boolean'),
    'is_bip': ColumnSpec(name='is_bip', dtype='Boolean'),
    'is_foul': ColumnSpec(name='is_foul', dtype='Boolean'),
    'balls': ColumnSpec(name='balls', dtype='SmallInteger', bounds=(0, 4)),
    'strikes': ColumnSpec(name='strikes', dtype='SmallInteger', bounds=(0, 3)),
    'outs_when_up': ColumnSpec(name='outs_when_up', dtype='SmallInteger', bounds=(0, 3)),
    'bat_side': ColumnSpec(name='bat_side', dtype='String(1)'),
    'pitch_hand': ColumnSpec(name='pitch_hand', dtype='String(1)'),
    'launch_speed': ColumnSpec(name='launch_speed', dtype='REAL', bounds=(0, 130)),
    'launch_angle': ColumnSpec(name='launch_angle', dtype='REAL', bounds=(-90, 90)),
    'hit_distance': ColumnSpec(name='hit_distance', dtype='REAL', bounds=(0, 550)),
    'event_type': ColumnSpec(name='event_type', dtype='Text'),
    'event': ColumnSpec(name='event', dtype='Text'),
    'created_at': ColumnSpec(
        name='created_at', dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

LIVE_PITCHES_SPEC = TableSpec(
    name='live_pitches',
    pk=['game_pk', 'at_bat_index', 'pitch_number'],
    columns=LIVE_PITCHES_COLUMNS,
)

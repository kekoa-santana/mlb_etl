from schema.spec_engine import ColumnSpec, TableSpec

LIVE_BATTED_BALLS_COLUMNS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk', dtype='BigInteger', nullable=False, primary_key=True
    ),
    'at_bat_index': ColumnSpec(
        name='at_bat_index', dtype='Integer', nullable=False, primary_key=True
    ),
    'game_date': ColumnSpec(name='game_date', dtype='DATE', nullable=False),
    'batter_id': ColumnSpec(name='batter_id', dtype='BigInteger', nullable=False),
    'pitcher_id': ColumnSpec(name='pitcher_id', dtype='BigInteger', nullable=False),
    'batter_name': ColumnSpec(name='batter_name', dtype='Text'),
    'pitcher_name': ColumnSpec(name='pitcher_name', dtype='Text'),
    'team_id': ColumnSpec(name='team_id', dtype='Integer'),
    'inning': ColumnSpec(name='inning', dtype='SmallInteger'),
    'half_inning': ColumnSpec(name='half_inning', dtype='Text'),
    'launch_speed': ColumnSpec(name='launch_speed', dtype='REAL', bounds=(0, 130)),
    'launch_angle': ColumnSpec(name='launch_angle', dtype='REAL', bounds=(-90, 90)),
    'hit_distance': ColumnSpec(name='hit_distance', dtype='REAL', bounds=(0, 550)),
    'event_type': ColumnSpec(name='event_type', dtype='Text'),
    'event': ColumnSpec(name='event', dtype='Text'),
    'pitch_speed': ColumnSpec(name='pitch_speed', dtype='REAL', bounds=(40, 110)),
    'pitch_type': ColumnSpec(name='pitch_type', dtype='Text'),
    'bat_side': ColumnSpec(name='bat_side', dtype='String(1)'),
    'pitch_hand': ColumnSpec(name='pitch_hand', dtype='String(1)'),
    'created_at': ColumnSpec(
        name='created_at', dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

LIVE_BATTED_BALLS_SPEC = TableSpec(
    name='live_batted_balls',
    pk=['game_pk', 'at_bat_index'],
    columns=LIVE_BATTED_BALLS_COLUMNS,
)

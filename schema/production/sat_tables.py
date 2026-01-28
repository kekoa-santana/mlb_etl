from schema.spec_engine import TableSpec, ColumnSpec

SAT_PITCH_SHAPE_COLS: dict[str, ColumnSpec] = {
    'pitch_id': ColumnSpec(
        name='pitch_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'release_pos_x': ColumnSpec(
        name='release_pos_x',
        dtype='REAL',
        bounds=(-6, 6)
    ),
    'release_pos_y': ColumnSpec(
        name='release_pos_y',
        dtype='REAL',
        bounds=(46, 60)
    ),
    'release_pos_z': ColumnSpec(
        name='release_pos_z',
        dtype='REAL',
        bounds=(2.5, 9)
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
    'release_speed': ColumnSpec(
        name='release_speed',
        dtype='REAL',
        bounds=(20, 108)
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
    'vx0': ColumnSpec(
        name='vx0',
        dtype='REAL',
        bounds=(-30, 30)
    ),
    'vy0': ColumnSpec(
        name='vy0',
        dtype='REAL',
        bounds=(-180, -50)
    ),
    'vz0': ColumnSpec(
        name='vz0',
        dtype='REAL',
        bounds=(-30, 30)
    ),
    'ax': ColumnSpec(
        name='ax',
        dtype='REAL',
        bounds=(-50, 50)
    ),
    'ay': ColumnSpec(
        name='ay',
        dtype='REAL',
        bounds=(0, 50)
    ),
    'az': ColumnSpec(
        name='az',
        dtype='REAL',
        bounds=(-60, 20)
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
    'sz_top': ColumnSpec(
        name='sz_top',
        dtype='REAL'
    ),
    'sz_bot': ColumnSpec(
        name='sz_bot',
        dtype='REAL'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        nullable=False,
        server_default='now()'
    ),
}

SAT_PITCH_SHAPE_SPEC = TableSpec(
    name='sat_pitch_shape',
    pk=['pitch_id'],
    columns=SAT_PITCH_SHAPE_COLS
)

SAT_BATTED_BALLS_COLS: dict[str, ColumnSpec] = {
    'pitch_id': ColumnSpec(
        name='pitch_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'pa_id': ColumnSpec(
        name='pa_id',
        dtype='BigInteger'
    ),
    'bb_type': ColumnSpec(
        name='bb_type',
        dtype='Text'
    ),
    'events': ColumnSpec(
        name='events',
        dtype='Text'
    ),
    'launch_speed': ColumnSpec(
        name='launch_speed',
        dtype='REAL',
        bounds=(30, 135)
    ),
    'launch_angle': ColumnSpec(
        name='launch_angle',
        dtype='REAL',
        bounds=(-90, 100)
    ),
    'hit_distance_sc': ColumnSpec(
        name='hit_distance_sc',
        dtype='REAL',
        bounds=(0, 550)
    ),
    'hc_x': ColumnSpec(
        name='hc_x',
        dtype='REAL',
        bounds=(0, 275)
    ),
    'hc_y': ColumnSpec(
        name='hc_y',
        dtype='REAL',
        bounds=(0, 275)
    ),
    'hc_x_centered': ColumnSpec(
        name='hc_x_centered',
        dtype='REAL',
        bounds=(-150, 150)
    ),
    'is_homerun': ColumnSpec(
        name='is_homerun',
        dtype='Boolean'
    ),
    'xba': ColumnSpec(
        name='xba',
        dtype='REAL',
        bounds=(0, 1)
    ),
    'xslg': ColumnSpec(
        name='xslg',
        dtype='REAL',
        bounds=(0, 4)
    ),
    'xwoba': ColumnSpec(
        name='xwoba',
        dtype='REAL',
        bounds=(0, 2.2)
    ),
    'woba_value': ColumnSpec(
        name='woba_value',
        dtype='REAL',
        bounds=(0, 2.5)
    ),
    'babip_value': ColumnSpec(
        name='babip_value',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'iso_value': ColumnSpec(
        name='iso_value',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'hit_location': ColumnSpec(
        name='hit_location',
        dtype='SmallInteger',
        bounds=(1, 9)
    ),
    'hard_hit': ColumnSpec(
        name='hard_hit',
        dtype='Boolean'
    ),
    'sweet_spot': ColumnSpec(
        name='sweet_spot',
        dtype='Boolean'
    ),
    'ideal_contact': ColumnSpec(
        name='ideal_contact',
        dtype='Boolean'
    ),
    'la_band': ColumnSpec(
        name='la_band',
        dtype='Text'
    ),
    'ev_band': ColumnSpec(
        name='ev_band',
        dtype='Text'
    ),
    'spray_bucket': ColumnSpec(
        name='spray_bucket',
        dtype='Text'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        nullable=False,
        server_default='now()'
    ),
}

SAT_BATTED_BALLS_SPEC = TableSpec(
    name='sat_batted_balls',
    pk=['pitch_id'],
    columns=SAT_BATTED_BALLS_COLS
)
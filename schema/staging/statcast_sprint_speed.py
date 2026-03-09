from schema.spec_engine import TableSpec, ColumnSpec

SPRINT_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
        nullable=False,
        primary_key=True
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text'
    ),
    'team': ColumnSpec(
        name='team',
        dtype='Text'
    ),
    'position': ColumnSpec(
        name='position',
        dtype='Text'
    ),
    'age': ColumnSpec(
        name='age',
        dtype='SmallInteger'
    ),
    'competitive_runs': ColumnSpec(
        name='competitive_runs',
        dtype='SmallInteger'
    ),
    'bolts': ColumnSpec(
        name='bolts',
        dtype='REAL'
    ),
    'hp_to_1b': ColumnSpec(
        name='hp_to_1b',
        dtype='REAL'
    ),
    'sprint_speed': ColumnSpec(
        name='sprint_speed',
        dtype='REAL'
    )
}

STATCAST_SPRINT_SPEC = TableSpec(
    name='statcast_sprint_speed',
    pk=['season', 'player_id'],
    columns=SPRINT_COLS
)
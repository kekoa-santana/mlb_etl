from schema.spec_engine import TableSpec, ColumnSpec

DIM_ROSTER_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text',
        nullable=False
    ),
    'org_id': ColumnSpec(
        name='org_id',
        dtype='Integer',
        nullable=False,
        bounds=(108, 158)
    ),
    'roster_status': ColumnSpec(
        name='roster_status',
        dtype='String(20)',
        nullable=False
    ),
    'level': ColumnSpec(
        name='level',
        dtype='String(5)'
    ),
    'primary_position': ColumnSpec(
        name='primary_position',
        dtype='String(4)',
        nullable=False
    ),
    'secondary_positions': ColumnSpec(
        name='secondary_positions',
        dtype='Text'
    ),
    'is_starter': ColumnSpec(
        name='is_starter',
        dtype='Boolean'
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer'
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'last_game_date': ColumnSpec(
        name='last_game_date',
        dtype='DATE'
    ),
    'status_date': ColumnSpec(
        name='status_date',
        dtype='DATE',
        nullable=False
    ),
    'updated_at': ColumnSpec(
        name='updated_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

DIM_ROSTER_SPEC = TableSpec(
    name='dim_roster',
    pk=['player_id'],
    columns=DIM_ROSTER_COLS
)

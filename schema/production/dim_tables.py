from schema.spec_engine import TableSpec, ColumnSpec

DIM_PLAYER_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key = True
    ),
    'full_name': ColumnSpec(
        name='full_name',
        dtype='Text',
        nullable=False
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='BigInteger',
        nullable=False
    ),
    'first_name': ColumnSpec(
        name='first_name',
        dtype='Text',
    ),
    'last_name': ColumnSpec(
        name='last_name',
        dtype='Text'
    ),
    'birth_date': ColumnSpec(
        name='birth_date',
        dtype='DATE'
    ),
    'age': ColumnSpec(
        name='age',
        dtype='SmallInteger'
    ),
    'height': ColumnSpec(
        name='height',
        dtype='Text'
    ),
    'weight': ColumnSpec(
        name='weight',
        dtype='SmallInteger'
    ),
    'active': ColumnSpec(
        name='active',
        dtype='Boolean'
    ),
    'primary_position_code': ColumnSpec(
        name='primary_position_code',
        dtype='SmallInteger'
    ),
    'primary_position': ColumnSpec(
        name='primary_position',
        dtype='String(4)'
    ),
    'draft_year': ColumnSpec(
        name='draft_year',
        dtype='SmallInteger'
    ),
    'mlb_debut_date': ColumnSpec(
        name='mlb_debut_date',
        dtype='DATE'
    ),
    'bat_side': ColumnSpec(
        name='bat_side',
        dtype='String(1)'
    ),
    'pitch_hand': ColumnSpec(
        name='pitch_hand',
        dtype='String(1)'
    ),
    'sz_top': ColumnSpec(
        name='sz_top',
        dtype='REAL'
    ),
    'sz_bot': ColumnSpec(
        name='sz_bot',
        dtype='REAL'
    )
}

DIM_PLAYER_SPEC = TableSpec(
    name='dim_player',
    pk=['player_id'],
    columns=DIM_PLAYER_COLS
)

DIM_TEAM_COLS: dict[str, ColumnSpec] = {
    'team_id': ColumnSpec(
        name='team_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text',
        nullable=False
    ),
    'full_name': ColumnSpec(
        name='full_name',
        dtype='Text'
    ),
    'abbreviation': ColumnSpec(
        name='abbreviation',
        dtype='String(4)'
    ),
    'venue': ColumnSpec(
        name='venue',
        dtype='Text'
    ),
    'division': ColumnSpec(
        name='division',
        dtype='Text'
    ),
    'division_id': ColumnSpec(
        name='division_id',
        dtype='BigInteger'
    ),
    'location': ColumnSpec(
        name='location',
        dtype='Text'
    )
}

DIM_TEAM_SPEC = TableSpec(
    name='dim_team',
    pk=['team_id'],
    columns=DIM_TEAM_COLS
)

DIM_GAME_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE',
        nullable=False
    ),
    'game_type': ColumnSpec(
        name='game_type',
        dtype='Text'
    ),
    'home_team': ColumnSpec(
        name='home_team',
        dtype='Text'
    ),
    'away_team': ColumnSpec(
        name='away_team',
        dtype='Text'
    )
}

DIM_GAME_SPEC = TableSpec(
    name='dim_game', 
    pk=['game_pk'],
    columns=DIM_GAME_COLS
)
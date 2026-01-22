from data_quality.specs.spec_engine import ColumnSpec, TableSpec

DIM_PLAYER_COLUMNS = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='Int64',
        nullable=False,
    ),
    'full_name': ColumnSpec(
        name='full_name',
        dtype='string',
        nullable=False
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Int64',
        nullable=False
    ),
    'first_name': ColumnSpec(
        name='first_name',
        dtype='string'
    ),
    'last_name': ColumnSpec(
        name='last_name',
        dtype='string'
    ),
    'birth_date': ColumnSpec(
        name='birth_date',
        dtype='datetime'
    ),
    'age': ColumnSpec(
        name='age',
        dtype='Int64'
    ),
    'height': ColumnSpec(
        name='height',
        dtype='string'
    ),
    'weight': ColumnSpec(
        name='weight',
        dtype='Int64'
    ),
    'active': ColumnSpec(
        name='active',
        dtype='boolean'
    ),
    'primary_position_code': ColumnSpec(
        name='primary_position_code',
        dtype='Int64'
    ),
    'primary_position': ColumnSpec(
        name='primary_position',
        dtype='string'
    ),
    'draft_year': ColumnSpec(
        name='draft_year',
        dtype='Int64'
    ),
    'mlb_debut_date': ColumnSpec(
        name='mlb_debut_date',
        dtype='datetime'
    ),
    'bat_side': ColumnSpec(
        name='bat_side',
        dtype='string'
    ),
    'pitch_hand': ColumnSpec(
        name='pitch_hand',
        dtype='string'
    ),
    'sz_top': ColumnSpec(
        name='sz_top',
        dtype='float',
        bounds=(2.5, 5.5)
    ),
    'sz_bot': ColumnSpec(
        name='sz_bot',
        dtype='float',
        bounds=(0.7, 2.5)
    )
}

DIM_PLAYER_SPEC = TableSpec(
    name='dim_player',
    pk=['player_id'],
    columns=DIM_PLAYER_COLUMNS
)
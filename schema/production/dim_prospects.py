from schema.spec_engine import TableSpec, ColumnSpec

DIM_PROSPECTS_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'full_name': ColumnSpec(
        name='full_name',
        dtype='Text',
        nullable=False
    ),
    'first_name': ColumnSpec(
        name='first_name',
        dtype='Text'
    ),
    'last_name': ColumnSpec(
        name='last_name',
        dtype='Text'
    ),
    'primary_position': ColumnSpec(
        name='primary_position',
        dtype='Text'
    ),
    'bat_side': ColumnSpec(
        name='bat_side',
        dtype='Text'
    ),
    'pitch_hand': ColumnSpec(
        name='pitch_hand',
        dtype='Text'
    ),
    'birth_date': ColumnSpec(
        name='birth_date',
        dtype='DATE'
    ),
    'current_age': ColumnSpec(
        name='current_age',
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
    'milb_team_id': ColumnSpec(
        name='milb_team_id',
        dtype='Integer'
    ),
    'milb_team_name': ColumnSpec(
        name='milb_team_name',
        dtype='Text'
    ),
    'parent_org_id': ColumnSpec(
        name='parent_org_id',
        dtype='Integer'
    ),
    'parent_org_name': ColumnSpec(
        name='parent_org_name',
        dtype='Text'
    ),
    'sport_id': ColumnSpec(
        name='sport_id',
        dtype='SmallInteger',
        nullable=False
    ),
    'level': ColumnSpec(
        name='level',
        dtype='Text',
        nullable=False
    ),
    'status_code': ColumnSpec(
        name='status_code',
        dtype='Text'
    ),
    'status_description': ColumnSpec(
        name='status_description',
        dtype='Text'
    ),
    'mlb_debut_date': ColumnSpec(
        name='mlb_debut_date',
        dtype='DATE'
    ),
    'draft_year': ColumnSpec(
        name='draft_year',
        dtype='SmallInteger'
    ),
    'season': ColumnSpec(
        name='season',
        dtype='SmallInteger',
        nullable=False,
        primary_key=True
    ),
    'jersey_number': ColumnSpec(
        name='jersey_number',
        dtype='Text'
    ),
    'updated_at': ColumnSpec(
        name='updated_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

DIM_PROSPECTS_SPEC = TableSpec(
    name='dim_prospects',
    pk=['player_id', 'season'],
    columns=DIM_PROSPECTS_COLS
)

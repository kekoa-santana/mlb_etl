from schema.spec_engine import TableSpec, ColumnSpec

DIM_TRANSACTION_COLS: dict[str, ColumnSpec] = {
    'transaction_id': ColumnSpec(
        name='transaction_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text'
    ),
    'to_team_id': ColumnSpec(
        name='to_team_id',
        dtype='Integer'
    ),
    'to_team_name': ColumnSpec(
        name='to_team_name',
        dtype='Text'
    ),
    'from_team_id': ColumnSpec(
        name='from_team_id',
        dtype='Integer'
    ),
    'from_team_name': ColumnSpec(
        name='from_team_name',
        dtype='Text'
    ),
    'transaction_date': ColumnSpec(
        name='transaction_date',
        dtype='DATE',
        nullable=False
    ),
    'effective_date': ColumnSpec(
        name='effective_date',
        dtype='DATE'
    ),
    'resolution_date': ColumnSpec(
        name='resolution_date',
        dtype='DATE'
    ),
    'type_code': ColumnSpec(
        name='type_code',
        dtype='Text',
        nullable=False
    ),
    'type_desc': ColumnSpec(
        name='type_desc',
        dtype='Text'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
    ),
    'is_il_placement': ColumnSpec(
        name='is_il_placement',
        dtype='Boolean'
    ),
    'is_il_activation': ColumnSpec(
        name='is_il_activation',
        dtype='Boolean'
    ),
    'is_il_transfer': ColumnSpec(
        name='is_il_transfer',
        dtype='Boolean'
    ),
    'il_type': ColumnSpec(
        name='il_type',
        dtype='Text'
    ),
    'injury_description': ColumnSpec(
        name='injury_description',
        dtype='Text'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
}

DIM_TRANSACTION_SPEC = TableSpec(
    name='dim_transaction',
    pk=['transaction_id'],
    columns=DIM_TRANSACTION_COLS
)
